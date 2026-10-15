"""
01_11_extract_respiratory_support.py

Trajectories project
Respiratory domain - canonical trajectory extraction

STATUS
------
Canonical and frozen respiratory-support trajectory extraction.

DECISIONS IMPLEMENTED
---------------------
Primary source:
    CHARTEVENTS only.

Itemids:
    226732 O2 Delivery Device(s)
    223849 Ventilator Mode
    229314 Ventilator Mode (Hamilton)

State priority:
    TRACHEOSTOMY
    INVASIVE_VENT
    NON_INVASIVE_VENT
    HFNC
    SUPPLEMENTAL_OXYGEN
    NONE
    UNCLASSIFIED

Temporal persistence:
    maximum 6 hours.

Left boundary:
    the first classified post-t0 state is assigned backward to t0 only when
    documented within 60 minutes after t0.

Right boundary:
    each interval ends at the earliest of:
        next classified respiratory timestamp;
        source timestamp + 6 hours;
        observation_end_time;
        t0 + 24 hours.

PROCEDUREEVENTS is not used to define the trajectory.

Dominant-state tie rule:
    when two or more states occupy exactly the same duration within a window,
    the more intensive respiratory-support state is selected according to the
    same clinical priority used for timestamp classification.

PRODUCTION OUTPUTS
------------------
The frozen release includes observability summaries and state-minute
conservation checks for both hourly and six-hour outputs.

OUTPUTS
-------
data/01_11_respiratory_support_hourly.parquet
data/01_11_respiratory_support_six_hour.parquet
data/01_11_respiratory_support_audit.csv

Inputs
------
data/00_04_final_analytic_cohort.csv
Raw MIMIC-IV 3.1 files relative to MIMIC_ROOT:
  icu/chartevents.csv.gz
"""

from pathlib import Path
import duckdb


# =============================================================================
# Paths and constants
# =============================================================================

# Project directory containing the Scripts and data directories.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
# Set this path to the local MIMIC-IV 3.1 directory.
MIMIC_ROOT = Path("path/to/mimic-iv-3.1")

COHORT_FILE = PROJECT_ROOT / "data" / "00_04_final_analytic_cohort.csv"
CHARTEVENTS_FILE = MIMIC_ROOT / "icu" / "chartevents.csv.gz"

OUT_HOURLY = (
    PROJECT_ROOT / "data" /
    "01_11_respiratory_support_hourly.parquet"
)
OUT_SIX_HOUR = (
    PROJECT_ROOT / "data" /
    "01_11_respiratory_support_six_hour.parquet"
)
OUT_AUDIT = (
    PROJECT_ROOT / "data" /
    "01_11_respiratory_support_audit.csv"
)

EXPECTED_COHORT_N = 9305

O2_DEVICE_ITEMID = 226732
VENT_MODE_ITEMID = 223849
HAMILTON_MODE_ITEMID = 229314

PERSISTENCE_HOURS = 6
BACKWARD_LIMIT_MINUTES = 60


# =============================================================================
# State mappings
# =============================================================================

TRACHEOSTOMY_DEVICES = ("Tracheostomy tube", "Trach mask ")
INVASIVE_DEVICES = ("Endotracheal tube",)
NIV_DEVICES = ("Bipap mask ", "CPAP mask ")
HFNC_DEVICES = ("High flow nasal cannula",)
SUPPLEMENTAL_DEVICES = (
    "Non-rebreather",
    "Face tent",
    "Aerosol-cool",
    "Venti mask ",
    "Medium conc mask ",
    "Ultrasonic neb",
    "Vapomist",
    "Oxymizer",
    "High flow neb",
    "Nasal cannula",
)
NONE_DEVICES = ("None",)

INVASIVE_STANDARD_MODES = (
    "(S) CMV", "APRV", "APRV/Biphasic+ApnPress", "APRV/Biphasic+ApnVol",
    "APV (cmv)", "Ambient", "Apnea Ventilation", "CMV", "CMV/ASSIST",
    "CMV/ASSIST/AutoFlow", "CMV/AutoFlow", "CPAP/PPS", "CPAP/PSV",
    "CPAP/PSV+Apn TCPL", "CPAP/PSV+ApnPres", "CPAP/PSV+ApnVol", "MMV",
    "MMV/AutoFlow", "MMV/PSV", "MMV/PSV/AutoFlow", "P-CMV", "PCV+",
    "PCV+/PSV", "PCV+Assist", "PRES/AC", "PRVC/AC", "PRVC/SIMV",
    "PSV/SBT", "SIMV", "SIMV/AutoFlow", "SIMV/PRES", "SIMV/PSV",
    "SIMV/PSV/AutoFlow", "SIMV/VOL", "SPONT", "SYNCHRON MASTER",
    "SYNCHRON SLAVE", "VOL/AC",
)

INVASIVE_HAMILTON_MODES = (
    "APRV", "APV (cmv)", "Ambient", "(S) CMV", "P-CMV", "SIMV",
    "APV (simv)", "P-SIMV", "VS", "ASV", "SPONT",
)

NIV_HAMILTON_MODES = ("DuoPaP", "NIV", "NIV-ST")


# =============================================================================
# Helpers
# =============================================================================

def qpath(path: Path) -> str:
    return str(path).replace("'", "''")


def sql_list(values: tuple[str, ...]) -> str:
    return ", ".join("'" + value.replace("'", "''") + "'" for value in values)


def require_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")


for required in (COHORT_FILE, CHARTEVENTS_FILE):
    require_file(required)

OUT_HOURLY.parent.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(database=":memory:")
con.execute("PRAGMA threads=4")
con.execute("PRAGMA preserve_insertion_order=false")


# =============================================================================
# Frozen cohort
# =============================================================================

con.execute(
    f"""
    CREATE TABLE cohort AS
    SELECT
        subject_id::BIGINT AS subject_id,
        hadm_id::BIGINT AS hadm_id,
        stay_id::BIGINT AS stay_id,
        surgery_group,
        sex,
        age_at_icu_admission,
        postoperative_time_zero::TIMESTAMP AS postoperative_time_zero,
        observation_end_time::TIMESTAMP AS observation_end_time,
        observation_end_type,
        least(
            postoperative_time_zero::TIMESTAMP + INTERVAL '24 hours',
            observation_end_time::TIMESTAMP
        ) AS analytical_end_time
    FROM read_csv_auto(
        '{qpath(COHORT_FILE)}',
        header=true,
        all_varchar=false
    )
    """
)

cohort_check = con.execute(
    """
    SELECT
        COUNT(*) AS rows_n,
        COUNT(DISTINCT stay_id) AS stays_n,
        COUNT(*) FILTER (
            WHERE postoperative_time_zero IS NULL
        ) AS missing_t0_n,
        COUNT(*) FILTER (
            WHERE observation_end_time IS NULL
        ) AS missing_end_n,
        COUNT(*) FILTER (
            WHERE observation_end_time < postoperative_time_zero
        ) AS invalid_time_n
    FROM cohort
    """
).fetchone()

if cohort_check[0] != EXPECTED_COHORT_N or cohort_check[1] != EXPECTED_COHORT_N:
    raise RuntimeError(
        f"Unexpected cohort dimensions: rows={cohort_check[0]}, "
        f"stays={cohort_check[1]}."
    )

if any(cohort_check[2:]):
    raise RuntimeError(f"Invalid cohort timing fields: {cohort_check[2:]}.")


# =============================================================================
# Focused CHARTEVENTS extraction
# =============================================================================

con.execute(
    f"""
    CREATE TABLE raw_chart AS
    SELECT
        ce.stay_id::BIGINT AS stay_id,
        ce.charttime::TIMESTAMP AS charttime,
        ce.itemid::BIGINT AS itemid,
        ce.value,
        c.postoperative_time_zero,
        c.observation_end_time,
        c.analytical_end_time,
        date_diff(
            'second',
            c.postoperative_time_zero,
            ce.charttime
        ) / 60.0 AS offset_minutes_from_t0
    FROM read_csv_auto(
        '{qpath(CHARTEVENTS_FILE)}',
        header=true,
        all_varchar=false
    ) ce
    INNER JOIN cohort c
        ON ce.stay_id = c.stay_id
       AND ce.charttime >= c.postoperative_time_zero
       AND ce.charttime < c.analytical_end_time
    WHERE ce.itemid IN (
        {O2_DEVICE_ITEMID},
        {VENT_MODE_ITEMID},
        {HAMILTON_MODE_ITEMID}
    )
    """
)


# =============================================================================
# Same-timestamp evidence and state classification
# =============================================================================

con.execute(
    f"""
    CREATE TABLE timestamp_evidence AS
    SELECT
        stay_id,
        charttime,
        any_value(postoperative_time_zero) AS postoperative_time_zero,
        any_value(observation_end_time) AS observation_end_time,
        any_value(analytical_end_time) AS analytical_end_time,
        any_value(offset_minutes_from_t0) AS offset_minutes_from_t0,

        string_agg(DISTINCT value, ' | ' ORDER BY value)
            FILTER (WHERE itemid = {O2_DEVICE_ITEMID})
            AS device_values,

        string_agg(DISTINCT value, ' | ' ORDER BY value)
            FILTER (WHERE itemid = {VENT_MODE_ITEMID})
            AS standard_mode_values,

        string_agg(DISTINCT value, ' | ' ORDER BY value)
            FILTER (WHERE itemid = {HAMILTON_MODE_ITEMID})
            AS hamilton_mode_values,

        bool_or(
            itemid = {O2_DEVICE_ITEMID}
            AND value IN ({sql_list(TRACHEOSTOMY_DEVICES)})
        ) AS tracheostomy_flag,

        bool_or(
            itemid = {O2_DEVICE_ITEMID}
            AND value IN ({sql_list(INVASIVE_DEVICES)})
        ) AS invasive_device_flag,

        bool_or(
            itemid = {O2_DEVICE_ITEMID}
            AND value IN ({sql_list(NIV_DEVICES)})
        ) AS niv_device_flag,

        bool_or(
            itemid = {O2_DEVICE_ITEMID}
            AND value IN ({sql_list(HFNC_DEVICES)})
        ) AS hfnc_flag,

        bool_or(
            itemid = {O2_DEVICE_ITEMID}
            AND value IN ({sql_list(SUPPLEMENTAL_DEVICES)})
        ) AS supplemental_flag,

        bool_or(
            itemid = {O2_DEVICE_ITEMID}
            AND value IN ({sql_list(NONE_DEVICES)})
        ) AS none_flag,

        bool_or(
            itemid = {VENT_MODE_ITEMID}
            AND value IN ({sql_list(INVASIVE_STANDARD_MODES)})
        ) AS invasive_standard_mode_flag,

        bool_or(
            itemid = {HAMILTON_MODE_ITEMID}
            AND value IN ({sql_list(INVASIVE_HAMILTON_MODES)})
        ) AS invasive_hamilton_mode_flag,

        bool_or(
            itemid = {HAMILTON_MODE_ITEMID}
            AND value IN ({sql_list(NIV_HAMILTON_MODES)})
        ) AS niv_hamilton_mode_flag,

        COUNT(*) AS raw_rows_n

    FROM raw_chart
    GROUP BY stay_id, charttime
    """
)

con.execute(
    """
    CREATE TABLE classified_timestamps AS
    SELECT
        *,
        CASE
            WHEN tracheostomy_flag
                THEN 'TRACHEOSTOMY'
            WHEN invasive_device_flag
              OR invasive_standard_mode_flag
              OR invasive_hamilton_mode_flag
                THEN 'INVASIVE_VENT'
            WHEN niv_device_flag
              OR niv_hamilton_mode_flag
                THEN 'NON_INVASIVE_VENT'
            WHEN hfnc_flag
                THEN 'HFNC'
            WHEN supplemental_flag
                THEN 'SUPPLEMENTAL_OXYGEN'
            WHEN none_flag
                THEN 'NONE'
            ELSE 'UNCLASSIFIED'
        END AS respiratory_state
    FROM timestamp_evidence
    """
)

con.execute(
    """
    CREATE TABLE ordered_states AS
    SELECT
        *,
        lead(charttime) OVER (
            PARTITION BY stay_id
            ORDER BY charttime
        ) AS next_charttime,
        lead(respiratory_state) OVER (
            PARTITION BY stay_id
            ORDER BY charttime
        ) AS next_state,
        lag(respiratory_state) OVER (
            PARTITION BY stay_id
            ORDER BY charttime
        ) AS previous_state,
        row_number() OVER (
            PARTITION BY stay_id
            ORDER BY charttime
        ) AS state_sequence_number
    FROM classified_timestamps
    WHERE respiratory_state <> 'UNCLASSIFIED'
    """
)


# =============================================================================
# Interval reconstruction
# =============================================================================

con.execute(
    f"""
    CREATE TABLE state_intervals AS
    SELECT
        stay_id,
        charttime AS source_charttime,
        respiratory_state,
        device_values,
        standard_mode_values,
        hamilton_mode_values,
        previous_state,
        next_state,
        state_sequence_number,

        CASE
            WHEN state_sequence_number = 1
             AND offset_minutes_from_t0 <= {BACKWARD_LIMIT_MINUTES}
                THEN postoperative_time_zero
            ELSE charttime
        END AS interval_start,

        least(
            coalesce(
                next_charttime,
                charttime + INTERVAL '{PERSISTENCE_HOURS} hours'
            ),
            charttime + INTERVAL '{PERSISTENCE_HOURS} hours',
            analytical_end_time
        ) AS interval_end,

        CASE
            WHEN next_charttime IS NOT NULL
             AND next_charttime
                 <= charttime + INTERVAL '{PERSISTENCE_HOURS} hours'
                THEN 'NEXT_CLASSIFIED_TIMESTAMP'
            WHEN analytical_end_time
                 <= charttime + INTERVAL '{PERSISTENCE_HOURS} hours'
                THEN 'ANALYTICAL_END'
            ELSE 'PERSISTENCE_CAP'
        END AS interval_end_reason,

        CASE
            WHEN state_sequence_number = 1
             AND offset_minutes_from_t0 <= {BACKWARD_LIMIT_MINUTES}
                THEN 1
            ELSE 0
        END AS backward_assigned_to_t0_flag

    FROM ordered_states
    """
)

con.execute(
    """
    CREATE TABLE valid_state_intervals AS
    SELECT
        *,
        date_diff(
            'second',
            interval_start,
            interval_end
        ) / 60.0 AS interval_minutes
    FROM state_intervals
    WHERE interval_end > interval_start
    """
)

overlap_n = con.execute(
    """
    SELECT COUNT(*)
    FROM (
        SELECT
            stay_id,
            interval_start,
            lag(interval_end) OVER (
                PARTITION BY stay_id
                ORDER BY interval_start, interval_end
            ) AS previous_interval_end
        FROM valid_state_intervals
    )
    WHERE previous_interval_end > interval_start
    """
).fetchone()[0]

if overlap_n:
    raise RuntimeError(f"Overlapping state intervals detected: {overlap_n}.")


# =============================================================================
# Window definitions
# =============================================================================

con.execute(
    """
    CREATE TABLE hourly_windows AS
    SELECT
        i::INTEGER AS window_index,
        i::INTEGER AS start_hour,
        (i + 1)::INTEGER AS end_hour,
        60::INTEGER AS nominal_minutes,
        'H' || lpad(i::VARCHAR, 2, '0')
            || '_H' || lpad((i + 1)::VARCHAR, 2, '0') AS window_label
    FROM range(0, 24) t(i)
    """
)

con.execute(
    """
    CREATE TABLE six_hour_windows AS
    SELECT * FROM (
        VALUES
            (0, 0, 6, 360, 'H00_H06'),
            (1, 6, 12, 360, 'H06_H12'),
            (2, 12, 18, 360, 'H12_H18'),
            (3, 18, 24, 360, 'H18_H24')
    ) AS t(
        window_index,
        start_hour,
        end_hour,
        nominal_minutes,
        window_label
    )
    """
)


def create_grid(table_name: str, windows_table: str) -> None:
    con.execute(
        f"""
        CREATE TABLE {table_name} AS
        SELECT
            c.subject_id,
            c.hadm_id,
            c.stay_id,
            c.surgery_group,
            c.sex,
            c.age_at_icu_admission,
            c.postoperative_time_zero,
            c.observation_end_time,
            c.observation_end_type,
            c.analytical_end_time,
            w.window_index,
            w.window_label,
            w.start_hour,
            w.end_hour,
            w.nominal_minutes,

            c.postoperative_time_zero
                + w.start_hour * INTERVAL '1 hour'
                AS window_start_time,

            c.postoperative_time_zero
                + w.end_hour * INTERVAL '1 hour'
                AS nominal_window_end_time,

            least(
                c.analytical_end_time,
                c.postoperative_time_zero
                    + w.end_hour * INTERVAL '1 hour'
            ) AS eligible_window_end_time,

            greatest(
                0.0,
                date_diff(
                    'second',
                    c.postoperative_time_zero
                        + w.start_hour * INTERVAL '1 hour',
                    least(
                        c.analytical_end_time,
                        c.postoperative_time_zero
                            + w.end_hour * INTERVAL '1 hour'
                    )
                ) / 60.0
            ) AS eligible_minutes,

            CASE
                WHEN c.analytical_end_time
                     <= c.postoperative_time_zero
                        + w.start_hour * INTERVAL '1 hour'
                    THEN 'NOT_OBSERVABLE'
                WHEN c.analytical_end_time
                     < c.postoperative_time_zero
                        + w.end_hour * INTERVAL '1 hour'
                    THEN 'PARTIALLY_OBSERVABLE'
                ELSE 'FULLY_OBSERVABLE'
            END AS observability_status

        FROM cohort c
        CROSS JOIN {windows_table} w
        """
    )


create_grid("hourly_grid", "hourly_windows")
create_grid("six_hour_grid", "six_hour_windows")


# =============================================================================
# Window allocation
# =============================================================================

def create_window_output(table_name: str, grid_table: str) -> None:
    con.execute(
        f"""
        CREATE TABLE {table_name} AS
        WITH overlap_rows AS (
            SELECT
                g.stay_id,
                g.window_index,
                i.respiratory_state,
                i.source_charttime,
                i.interval_start,
                i.interval_end,
                i.backward_assigned_to_t0_flag,
                greatest(
                    0.0,
                    date_diff(
                        'second',
                        greatest(i.interval_start, g.window_start_time),
                        least(i.interval_end, g.eligible_window_end_time)
                    ) / 60.0
                ) AS overlap_minutes
            FROM {grid_table} g
            INNER JOIN valid_state_intervals i
                ON g.stay_id = i.stay_id
               AND g.eligible_minutes > 0
               AND i.interval_end > g.window_start_time
               AND i.interval_start < g.eligible_window_end_time
        ),

        aggregate_overlap AS (
            SELECT
                stay_id,
                window_index,
                SUM(overlap_minutes) AS classified_minutes,

                SUM(overlap_minutes) FILTER (
                    WHERE respiratory_state = 'INVASIVE_VENT'
                ) AS invasive_vent_minutes,

                SUM(overlap_minutes) FILTER (
                    WHERE respiratory_state = 'TRACHEOSTOMY'
                ) AS tracheostomy_minutes,

                SUM(overlap_minutes) FILTER (
                    WHERE respiratory_state = 'NON_INVASIVE_VENT'
                ) AS noninvasive_vent_minutes,

                SUM(overlap_minutes) FILTER (
                    WHERE respiratory_state = 'HFNC'
                ) AS hfnc_minutes,

                SUM(overlap_minutes) FILTER (
                    WHERE respiratory_state = 'SUPPLEMENTAL_OXYGEN'
                ) AS supplemental_oxygen_minutes,

                SUM(overlap_minutes) FILTER (
                    WHERE respiratory_state = 'NONE'
                ) AS none_minutes,

                COUNT(*) FILTER (
                    WHERE overlap_minutes > 0
                ) AS contributing_intervals_n,

                MAX(backward_assigned_to_t0_flag)
                    AS backward_assignment_contributed_flag

            FROM overlap_rows
            WHERE overlap_minutes > 0
            GROUP BY stay_id, window_index
        ),

        transition_counts AS (
            SELECT
                g.stay_id,
                g.window_index,
                COUNT(*) AS transition_count
            FROM {grid_table} g
            INNER JOIN ordered_states s
                ON g.stay_id = s.stay_id
               AND s.previous_state IS NOT NULL
               AND s.previous_state <> s.respiratory_state
               AND s.charttime >= g.window_start_time
               AND s.charttime < g.eligible_window_end_time
            GROUP BY g.stay_id, g.window_index
        ),

        boundary_states AS (
            SELECT
                g.stay_id,
                g.window_index,

                arg_min(
                    i.respiratory_state,
                    greatest(i.interval_start, g.window_start_time)
                ) AS state_at_window_start,

                arg_max(
                    i.respiratory_state,
                    least(i.interval_end, g.eligible_window_end_time)
                ) AS state_at_window_end

            FROM {grid_table} g
            INNER JOIN valid_state_intervals i
                ON g.stay_id = i.stay_id
               AND g.eligible_minutes > 0
               AND i.interval_end > g.window_start_time
               AND i.interval_start < g.eligible_window_end_time
            GROUP BY g.stay_id, g.window_index
        )

        SELECT
            g.*,

            coalesce(a.classified_minutes, 0.0)
                AS respiratory_classified_minutes,

            greatest(
                0.0,
                g.eligible_minutes - coalesce(a.classified_minutes, 0.0)
            ) AS respiratory_uncovered_minutes,

            CASE
                WHEN g.eligible_minutes > 0 THEN
                    coalesce(a.classified_minutes, 0.0) / g.eligible_minutes
                ELSE NULL
            END AS respiratory_coverage_fraction,

            coalesce(a.invasive_vent_minutes, 0.0)
                AS invasive_vent_minutes,

            coalesce(a.tracheostomy_minutes, 0.0)
                AS tracheostomy_minutes,

            coalesce(a.noninvasive_vent_minutes, 0.0)
                AS noninvasive_vent_minutes,

            coalesce(a.hfnc_minutes, 0.0)
                AS hfnc_minutes,

            coalesce(a.supplemental_oxygen_minutes, 0.0)
                AS supplemental_oxygen_minutes,

            coalesce(a.none_minutes, 0.0)
                AS none_minutes,

            CASE
                WHEN g.eligible_minutes > 0 THEN
                    coalesce(a.invasive_vent_minutes, 0.0)
                    / g.eligible_minutes
                ELSE NULL
            END AS invasive_ventilation_fraction,

            CASE
                WHEN coalesce(a.classified_minutes, 0.0) > 0 THEN
                    coalesce(a.invasive_vent_minutes, 0.0)
                    / a.classified_minutes
                ELSE NULL
            END AS invasive_fraction_of_classified_time,

            coalesce(a.contributing_intervals_n, 0)
                AS respiratory_contributing_intervals_n,

            coalesce(t.transition_count, 0)
                AS respiratory_transition_count,

            coalesce(a.backward_assignment_contributed_flag, 0)
                AS backward_assignment_contributed_flag,

            b.state_at_window_start,
            b.state_at_window_end,

            CASE
                WHEN coalesce(a.classified_minutes, 0.0) = 0 THEN NULL

                WHEN greatest(
                    coalesce(a.tracheostomy_minutes, 0.0),
                    coalesce(a.invasive_vent_minutes, 0.0),
                    coalesce(a.noninvasive_vent_minutes, 0.0),
                    coalesce(a.hfnc_minutes, 0.0),
                    coalesce(a.supplemental_oxygen_minutes, 0.0),
                    coalesce(a.none_minutes, 0.0)
                ) = coalesce(a.tracheostomy_minutes, 0.0)
                    THEN 'TRACHEOSTOMY'

                WHEN greatest(
                    coalesce(a.invasive_vent_minutes, 0.0),
                    coalesce(a.noninvasive_vent_minutes, 0.0),
                    coalesce(a.hfnc_minutes, 0.0),
                    coalesce(a.supplemental_oxygen_minutes, 0.0),
                    coalesce(a.none_minutes, 0.0)
                ) = coalesce(a.invasive_vent_minutes, 0.0)
                    THEN 'INVASIVE_VENT'

                WHEN greatest(
                    coalesce(a.noninvasive_vent_minutes, 0.0),
                    coalesce(a.hfnc_minutes, 0.0),
                    coalesce(a.supplemental_oxygen_minutes, 0.0),
                    coalesce(a.none_minutes, 0.0)
                ) = coalesce(a.noninvasive_vent_minutes, 0.0)
                    THEN 'NON_INVASIVE_VENT'

                WHEN greatest(
                    coalesce(a.hfnc_minutes, 0.0),
                    coalesce(a.supplemental_oxygen_minutes, 0.0),
                    coalesce(a.none_minutes, 0.0)
                ) = coalesce(a.hfnc_minutes, 0.0)
                    THEN 'HFNC'

                WHEN greatest(
                    coalesce(a.supplemental_oxygen_minutes, 0.0),
                    coalesce(a.none_minutes, 0.0)
                ) = coalesce(a.supplemental_oxygen_minutes, 0.0)
                    THEN 'SUPPLEMENTAL_OXYGEN'

                ELSE 'NONE'
            END AS dominant_respiratory_state,

            CASE
                WHEN g.eligible_minutes = 0
                    THEN 'NOT_OBSERVABLE'
                WHEN coalesce(a.classified_minutes, 0.0) = 0
                    THEN 'OBSERVABLE_UNCLASSIFIED'
                WHEN coalesce(a.classified_minutes, 0.0)
                     < g.eligible_minutes - 0.000001
                    THEN 'PARTIALLY_CLASSIFIED'
                ELSE 'FULLY_CLASSIFIED'
            END AS respiratory_measurement_status

        FROM {grid_table} g

        LEFT JOIN aggregate_overlap a
            ON g.stay_id = a.stay_id
           AND g.window_index = a.window_index

        LEFT JOIN transition_counts t
            ON g.stay_id = t.stay_id
           AND g.window_index = t.window_index

        LEFT JOIN boundary_states b
            ON g.stay_id = b.stay_id
           AND g.window_index = b.window_index
        """
    )


create_window_output("hourly_output", "hourly_grid")
create_window_output("six_hour_output", "six_hour_grid")


# =============================================================================
# Blocking validation
# =============================================================================

expected_hourly_rows = EXPECTED_COHORT_N * 24
expected_six_hour_rows = EXPECTED_COHORT_N * 4

hourly_rows = con.execute(
    "SELECT COUNT(*) FROM hourly_output"
).fetchone()[0]

six_hour_rows = con.execute(
    "SELECT COUNT(*) FROM six_hour_output"
).fetchone()[0]

if hourly_rows != expected_hourly_rows:
    raise RuntimeError(
        f"Unexpected hourly dimensions: {hourly_rows}; "
        f"expected {expected_hourly_rows}."
    )

if six_hour_rows != expected_six_hour_rows:
    raise RuntimeError(
        f"Unexpected six-hour dimensions: {six_hour_rows}; "
        f"expected {expected_six_hour_rows}."
    )

for table_name in ("hourly_output", "six_hour_output"):
    duplicate_n = con.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT stay_id, window_index, COUNT(*) AS n
            FROM {table_name}
            GROUP BY stay_id, window_index
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]

    if duplicate_n:
        raise RuntimeError(
            f"Duplicate stay/window keys in {table_name}: {duplicate_n}."
        )

    excess_n = con.execute(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE respiratory_classified_minutes
              > eligible_minutes + 0.000001
        """
    ).fetchone()[0]

    if excess_n:
        raise RuntimeError(
            f"Classified minutes exceed eligible minutes in "
            f"{table_name}: {excess_n}."
        )

    negative_n = con.execute(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE respiratory_classified_minutes < -0.000001
           OR respiratory_uncovered_minutes < -0.000001
           OR invasive_vent_minutes < -0.000001
           OR tracheostomy_minutes < -0.000001
           OR noninvasive_vent_minutes < -0.000001
           OR hfnc_minutes < -0.000001
           OR supplemental_oxygen_minutes < -0.000001
           OR none_minutes < -0.000001
        """
    ).fetchone()[0]

    if negative_n:
        raise RuntimeError(
            f"Negative allocated minutes in {table_name}: {negative_n}."
        )

interval_total = con.execute(
    "SELECT SUM(interval_minutes) FROM valid_state_intervals"
).fetchone()[0]

hourly_total = con.execute(
    "SELECT SUM(respiratory_classified_minutes) FROM hourly_output"
).fetchone()[0]

six_hour_total = con.execute(
    "SELECT SUM(respiratory_classified_minutes) FROM six_hour_output"
).fetchone()[0]

if abs(hourly_total - interval_total) > 0.001:
    raise RuntimeError(
        f"Hourly conservation failed: intervals={interval_total}, "
        f"hourly={hourly_total}."
    )

if abs(six_hour_total - interval_total) > 0.001:
    raise RuntimeError(
        f"Six-hour conservation failed: intervals={interval_total}, "
        f"six_hour={six_hour_total}."
    )

for table_name in ("hourly_output", "six_hour_output"):
    state_total_check = con.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT
                respiratory_classified_minutes,
                invasive_vent_minutes
                    + tracheostomy_minutes
                    + noninvasive_vent_minutes
                    + hfnc_minutes
                    + supplemental_oxygen_minutes
                    + none_minutes AS state_minutes_sum
            FROM {table_name}
        )
        WHERE abs(
            respiratory_classified_minutes - state_minutes_sum
        ) > 0.001
        """
    ).fetchone()[0]

    if state_total_check:
        raise RuntimeError(
            f"State-minute conservation failed in {table_name}: "
            f"{state_total_check} rows."
        )


# =============================================================================
# Audit output
# =============================================================================

con.execute(
    """
    CREATE TABLE audit (
        audit_level VARCHAR,
        window_index INTEGER,
        window_label VARCHAR,
        metric VARCHAR,
        value_num DOUBLE,
        value_text VARCHAR
    )
    """
)

audit_rows = [
    ("OVERALL", None, None, "cohort_n", EXPECTED_COHORT_N, None),
    (
        "OVERALL",
        None,
        None,
        "raw_chart_rows_n",
        con.execute("SELECT COUNT(*) FROM raw_chart").fetchone()[0],
        None,
    ),
    (
        "OVERALL",
        None,
        None,
        "classified_timestamps_n",
        con.execute("SELECT COUNT(*) FROM ordered_states").fetchone()[0],
        None,
    ),
    (
        "OVERALL",
        None,
        None,
        "valid_intervals_n",
        con.execute(
            "SELECT COUNT(*) FROM valid_state_intervals"
        ).fetchone()[0],
        None,
    ),
    (
        "OVERALL",
        None,
        None,
        "classified_interval_minutes_total",
        interval_total,
        None,
    ),
    (
        "DESIGN",
        None,
        None,
        "persistence_hours",
        PERSISTENCE_HOURS,
        None,
    ),
    (
        "DESIGN",
        None,
        None,
        "backward_limit_minutes",
        BACKWARD_LIMIT_MINUTES,
        None,
    ),
    (
        "DESIGN",
        None,
        None,
        "procedureevents_used_for_definition_flag",
        0,
        "CHARTEVENTS only",
    ),
    (
        "DESIGN",
        None,
        None,
        "dominant_state_tie_rule",
        None,
        "More intensive state selected according to clinical priority",
    ),
    (
        "DESIGN",
        None,
        None,
        "canonical_frozen_flag",
        1,
        "Canonical respiratory-support trajectory",
    ),
]

con.executemany(
    "INSERT INTO audit VALUES (?, ?, ?, ?, ?, ?)",
    audit_rows,
)

for table_name, audit_level in (
    ("hourly_output", "HOURLY"),
    ("six_hour_output", "SIX_HOUR"),
):
    con.execute(
        f"""
        INSERT INTO audit
        SELECT
            '{audit_level}',
            window_index,
            window_label,
            metric,
            value_num,
            NULL
        FROM (
            SELECT
                window_index,
                window_label,
                'eligible_patients_n' AS metric,
                COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                )::DOUBLE AS value_num
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'fully_observable_patients_n',
                COUNT(*) FILTER (
                    WHERE observability_status = 'FULLY_OBSERVABLE'
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'partially_observable_patients_n',
                COUNT(*) FILTER (
                    WHERE observability_status = 'PARTIALLY_OBSERVABLE'
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'not_observable_patients_n',
                COUNT(*) FILTER (
                    WHERE observability_status = 'NOT_OBSERVABLE'
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'fully_classified_patients_n',
                COUNT(*) FILTER (
                    WHERE respiratory_measurement_status = 'FULLY_CLASSIFIED'
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'coverage_ge95_patients_n',
                COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND respiratory_coverage_fraction >= 0.95
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'patients_with_invasive_time_n',
                COUNT(*) FILTER (
                    WHERE invasive_vent_minutes > 0
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'median_invasive_fraction',
                median(invasive_ventilation_fraction)::DOUBLE
            FROM {table_name}
            WHERE eligible_minutes > 0
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'median_uncovered_minutes',
                median(respiratory_uncovered_minutes)::DOUBLE
            FROM {table_name}
            WHERE eligible_minutes > 0
            GROUP BY window_index, window_label
        )
        """
    )

con.execute(
    f"""
    COPY (
        SELECT *
        FROM audit
        ORDER BY
            CASE audit_level
                WHEN 'OVERALL' THEN 1
                WHEN 'DESIGN' THEN 2
                WHEN 'HOURLY' THEN 3
                WHEN 'SIX_HOUR' THEN 4
                ELSE 99
            END,
            window_index NULLS FIRST,
            metric
    )
    TO '{qpath(OUT_AUDIT)}'
    (HEADER, DELIMITER ',')
    """
)


# =============================================================================
# Export
# =============================================================================

con.execute(
    f"""
    COPY (
        SELECT *
        FROM hourly_output
        ORDER BY stay_id, window_index
    )
    TO '{qpath(OUT_HOURLY)}'
    (FORMAT PARQUET, COMPRESSION ZSTD)
    """
)

con.execute(
    f"""
    COPY (
        SELECT *
        FROM six_hour_output
        ORDER BY stay_id, window_index
    )
    TO '{qpath(OUT_SIX_HOUR)}'
    (FORMAT PARQUET, COMPRESSION ZSTD)
    """
)


# =============================================================================
# Console report
# =============================================================================

print("=" * 104)
print("TRAJECTORIES - RESPIRATORY SUPPORT CANONICAL TRAJECTORY V9")
print("=" * 104)
print(f"Cohort                              : {EXPECTED_COHORT_N}")
print(f"Raw focused CHARTEVENTS rows        : "
      f"{con.execute('SELECT COUNT(*) FROM raw_chart').fetchone()[0]}")
print(f"Classified timestamps               : "
      f"{con.execute('SELECT COUNT(*) FROM ordered_states').fetchone()[0]}")
print(f"Valid reconstructed intervals       : "
      f"{con.execute('SELECT COUNT(*) FROM valid_state_intervals').fetchone()[0]}")
print(f"Persistence cap                     : {PERSISTENCE_HOURS} h")
print(f"Backward assignment                 : <= {BACKWARD_LIMIT_MINUTES} min")
print(f"Conserved classified minutes        : {interval_total:.1f}")
print()

print("Six-hour candidate summary")
print("--------------------------")
print(
    con.execute(
        """
        SELECT
            window_label,
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
            ) AS eligible_n,
            COUNT(*) FILTER (
                WHERE observability_status = 'FULLY_OBSERVABLE'
            ) AS fully_observable_n,
            COUNT(*) FILTER (
                WHERE observability_status = 'PARTIALLY_OBSERVABLE'
            ) AS partially_observable_n,
            COUNT(*) FILTER (
                WHERE observability_status = 'NOT_OBSERVABLE'
            ) AS not_observable_n,
            COUNT(*) FILTER (
                WHERE respiratory_measurement_status = 'FULLY_CLASSIFIED'
            ) AS fully_classified_n,
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND respiratory_coverage_fraction >= 0.95
            ) AS coverage_ge95_n,
            COUNT(*) FILTER (
                WHERE invasive_vent_minutes > 0
            ) AS invasive_time_n,
            round(median(invasive_vent_minutes), 1)
                AS median_invasive_minutes,
            round(median(invasive_ventilation_fraction), 4)
                AS median_invasive_fraction,
            round(median(respiratory_uncovered_minutes), 1)
                AS median_uncovered_minutes
        FROM six_hour_output
        GROUP BY window_index, window_label
        ORDER BY window_index
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Dominant-state distribution by six-hour window")
print("----------------------------------------------")
print(
    con.execute(
        """
        SELECT
            window_label,
            coalesce(dominant_respiratory_state, 'UNCLASSIFIED')
                AS dominant_state,
            COUNT(*) AS stays_n
        FROM six_hour_output
        WHERE eligible_minutes > 0
        GROUP BY
            window_index,
            window_label,
            dominant_respiratory_state
        ORDER BY window_index, stays_n DESC
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Canonical outputs")
print("-------")
print(OUT_HOURLY)
print(OUT_SIX_HOUR)
print(OUT_AUDIT)
print()
print("Canonical respiratory-support trajectory v9 complete.")
print("The respiratory-support trajectory is canonical and frozen.")
