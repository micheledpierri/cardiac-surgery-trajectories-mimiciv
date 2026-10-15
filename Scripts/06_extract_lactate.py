"""
01_02_extract_lactate.py

Trajectories project
Postoperative lactate component - canonical production pipeline

Purpose
-------
Build the frozen postoperative lactate trajectory representation directly
from raw MIMIC-IV data and the definitive analytical cohort.

Frozen methodological decisions
-------------------------------------------------
Source:
    hosp/labevents

Lactate itemid:
    50813 - Lactate

Specimen-context itemid:
    52033 - Specimen Type

Unit:
    mmol/L

Numeric values:
    all observed positive numeric values are retained;
    no upper physiological truncation is applied.

Non-numeric lactate rows:
    excluded from numerical summaries.

Same stay + charttime:
    multiple numeric lactate samples are consolidated by arithmetic mean.
    The underlying specimen classes remain documented.

Temporal reconstruction:
    none.
    No LOCF, no interpolation, no extrapolation.

Primary observed window feature:
    lactate_median_observed

Secondary observed descriptors:
    lactate_last_observed
    lactate_max_observed
    lactate_min_observed
    lactate_first_observed
    lactate_mean_observed

Temporal grids:
    primary: 24 consecutive 1-hour windows;
    secondary: four consecutive 6-hour windows.

Important methodological point
------------------------------
Lactate measurement is potentially informative. During prior validation, almost all patients
without lactate still had postoperative blood-gas testing. Therefore absence
of a lactate measurement must remain explicit and must never be interpreted as
normal lactate.

Specimen strategy
-----------------
All blood specimen classes are retained in the primary series:
    ARTERIAL
    MIXED
    CENTRAL_VENOUS
    VENOUS
    UNKNOWN

An arterial-only sensitivity representation is calculated in parallel and
stored as columns within the same canonical outputs. Development testing
showed negligible differences in paired window summaries while retaining
additional observations and patients in the all-specimen primary series.

The primary observed lactate trajectory feature is lactate_median_observed.
Observed last and maximum lactate are retained as clinically relevant
secondary descriptors; no synthetic value is generated for unmeasured windows.

Inputs
------
data/00_04_final_analytic_cohort.csv
MIMIC-IV 3.1 raw:
  hosp/d_labitems.csv.gz
  hosp/labevents.csv.gz

Canonical outputs
-----------------
data/01_02_lactate_hourly_trajectory.parquet
data/01_02_lactate_six_hour_summary.parquet
data/01_02_lactate_extraction_audit.csv

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
D_LABITEMS_FILE = MIMIC_ROOT / "hosp" / "d_labitems.csv.gz"
LABEVENTS_FILE = MIMIC_ROOT / "hosp" / "labevents.csv.gz"

OUT_HOURLY = PROJECT_ROOT / "data" / "01_02_lactate_hourly_trajectory.parquet"
OUT_SIX_HOUR = PROJECT_ROOT / "data" / "01_02_lactate_six_hour_summary.parquet"
OUT_AUDIT = PROJECT_ROOT / "data" / "01_02_lactate_extraction_audit.csv"

EXPECTED_COHORT_N = 9305

LACTATE_ITEMID = 50813
SPECIMEN_ITEMID = 52033

# Fixed reproduction targets from the validated analysis.
EXPECTED_RAW_LACTATE_ROWS = 17678
EXPECTED_NUMERIC_LACTATE_ROWS = 17669
EXPECTED_NONNUMERIC_LACTATE_ROWS = 9
EXPECTED_PATIENTS_WITH_NUMERIC_LACTATE = 5988
EXPECTED_MULTIROW_TIMESTAMPS = 3
EXPECTED_ARTERIAL_LACTATE_PATIENTS = 5771
EXPECTED_NONARTERIAL_ONLY_PATIENTS = 217


# =============================================================================
# Helpers
# =============================================================================

def qpath(path: Path) -> str:
    """Return a DuckDB-safe path string."""
    return str(path).replace("'", "''")


def require_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")


for required in [COHORT_FILE, D_LABITEMS_FILE, LABEVENTS_FILE]:
    require_file(required)

OUT_HOURLY.parent.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(database=":memory:")
con.execute("PRAGMA threads=4")


# =============================================================================
# Frozen cohort and observation boundaries
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
        observation_end_type
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
        COUNT(DISTINCT stay_id) AS unique_stays_n,
        COUNT(*) FILTER (WHERE postoperative_time_zero IS NULL) AS missing_t0_n,
        COUNT(*) FILTER (WHERE observation_end_time IS NULL) AS missing_obs_end_n,
        COUNT(*) FILTER (
            WHERE observation_end_time < postoperative_time_zero
        ) AS invalid_time_order_n
    FROM cohort
    """
).fetchone()

(
    cohort_rows_n,
    cohort_unique_stays_n,
    missing_t0_n,
    missing_obs_end_n,
    invalid_time_order_n,
) = cohort_check

if cohort_rows_n != EXPECTED_COHORT_N or cohort_unique_stays_n != EXPECTED_COHORT_N:
    raise RuntimeError(
        f"Unexpected cohort size: rows={cohort_rows_n}, "
        f"unique stays={cohort_unique_stays_n}; expected {EXPECTED_COHORT_N}."
    )

if missing_t0_n or missing_obs_end_n or invalid_time_order_n:
    raise RuntimeError(
        "Invalid cohort timing fields: "
        f"missing_t0={missing_t0_n}, "
        f"missing_observation_end={missing_obs_end_n}, "
        f"observation_end_before_t0={invalid_time_order_n}."
    )

con.execute(
    """
    CREATE TEMP TABLE cohort_subjects AS
    SELECT DISTINCT subject_id
    FROM cohort
    """
)


# =============================================================================
# Dictionary verification
# =============================================================================

dictionary_rows = con.execute(
    f"""
    SELECT
        itemid,
        label,
        fluid,
        category
    FROM read_csv_auto(
        '{qpath(D_LABITEMS_FILE)}',
        header=true,
        all_varchar=false
    )
    WHERE itemid IN ({LACTATE_ITEMID}, {SPECIMEN_ITEMID})
    ORDER BY itemid
    """
).fetchall()

if len(dictionary_rows) != 2:
    raise RuntimeError(
        f"Expected exactly two dictionary rows for itemids "
        f"{LACTATE_ITEMID} and {SPECIMEN_ITEMID}; found {len(dictionary_rows)}."
    )

dictionary_by_itemid = {row[0]: row for row in dictionary_rows}

if LACTATE_ITEMID not in dictionary_by_itemid:
    raise RuntimeError(f"Lactate itemid {LACTATE_ITEMID} not found.")

if str(dictionary_by_itemid[LACTATE_ITEMID][1]).strip().lower() != "lactate":
    raise RuntimeError(
        f"Unexpected lactate label: {dictionary_by_itemid[LACTATE_ITEMID][1]}"
    )


# =============================================================================
# Single filtered LABEVENTS scan
# =============================================================================

# Lactate and specimen-context rows are read in a single pass through the large
# compressed file, restricted to subjects in the frozen cohort.
con.execute(
    f"""
    CREATE TABLE relevant_lab_rows AS
    SELECT
        le.labevent_id,
        le.subject_id,
        le.hadm_id,
        le.specimen_id,
        le.itemid,
        le.charttime,
        le.storetime,
        le.value,
        le.valuenum,
        le.valueuom,
        le.flag,
        le.comments
    FROM read_csv_auto(
        '{qpath(LABEVENTS_FILE)}',
        header=true,
        all_varchar=false
    ) le
    INNER JOIN cohort_subjects cs
        ON le.subject_id = cs.subject_id
    WHERE le.itemid IN ({LACTATE_ITEMID}, {SPECIMEN_ITEMID})
    """
)


# =============================================================================
# Assign postoperative lactate rows to the frozen patient-specific interval
# =============================================================================

# LABEVENTS has no stay_id. The cohort stay is assigned from subject_id plus
# the already frozen postoperative observation interval.
con.execute(
    f"""
    CREATE TABLE lactate_raw AS
    SELECT
        c.subject_id AS cohort_subject_id,
        c.hadm_id AS cohort_hadm_id,
        c.stay_id AS cohort_stay_id,
        c.postoperative_time_zero,
        c.observation_end_time,
        le.labevent_id,
        le.hadm_id AS event_hadm_id,
        le.specimen_id,
        le.charttime,
        le.storetime,
        le.value,
        le.valuenum,
        le.valueuom,
        le.flag,
        le.comments,
        CASE
            WHEN le.hadm_id = c.hadm_id THEN 'HADM_ID_MATCH'
            WHEN le.hadm_id IS NULL THEN 'EVENT_HADM_ID_NULL'
            ELSE 'EVENT_HADM_ID_OTHER'
        END AS hadm_linkage_status
    FROM relevant_lab_rows le
    INNER JOIN cohort c
        ON le.subject_id = c.subject_id
       AND le.itemid = {LACTATE_ITEMID}
       AND le.charttime >= c.postoperative_time_zero
       AND le.charttime < c.postoperative_time_zero + INTERVAL '24 hours'
       AND le.charttime <= c.observation_end_time
    """
)

# A source row must not map to multiple postoperative cohort windows.
join_duplicate_groups = con.execute(
    """
    SELECT COUNT(*)
    FROM (
        SELECT labevent_id, COUNT(*) AS n
        FROM lactate_raw
        GROUP BY labevent_id
        HAVING COUNT(*) > 1
    )
    """
).fetchone()[0]

if join_duplicate_groups:
    raise RuntimeError(
        "A LABEVENTS lactate row matched more than one cohort window. "
        f"Duplicate labevent_id groups: {join_duplicate_groups}."
    )


# =============================================================================
# Specimen context
# =============================================================================

con.execute(
    f"""
    CREATE TABLE specimen_context AS
    SELECT
        specimen_id,
        string_agg(DISTINCT value, ' | ' ORDER BY value) AS specimen_text,
        COUNT(*) AS specimen_context_rows_n,
        COUNT(DISTINCT value) AS specimen_context_values_n
    FROM relevant_lab_rows
    WHERE itemid = {SPECIMEN_ITEMID}
    GROUP BY specimen_id
    """
)

con.execute(
    """
    CREATE TABLE lactate_with_specimen AS
    SELECT
        l.*,
        s.specimen_text,
        coalesce(s.specimen_context_rows_n, 0) AS specimen_context_rows_n,
        coalesce(s.specimen_context_values_n, 0) AS specimen_context_values_n,
        CASE
            WHEN s.specimen_text IS NULL THEN 'UNKNOWN'
            WHEN upper(trim(s.specimen_text)) = 'ART.' THEN 'ARTERIAL'
            WHEN upper(trim(s.specimen_text)) = 'MIX.' THEN 'MIXED'
            WHEN upper(trim(s.specimen_text)) = 'CENTRAL VENOUS.'
                THEN 'CENTRAL_VENOUS'
            WHEN upper(trim(s.specimen_text)) = 'VEN.' THEN 'VENOUS'
            ELSE 'OTHER'
        END AS specimen_class
    FROM lactate_raw l
    LEFT JOIN specimen_context s
        USING (specimen_id)
    """
)

con.execute(
    """
    CREATE TABLE lactate_numeric AS
    SELECT *
    FROM lactate_with_specimen
    WHERE valuenum IS NOT NULL
      AND valuenum > 0
    """
)


# =============================================================================
# Fixed reproduction checks from the validated analysis
# =============================================================================

raw_n, numeric_n, nonnumeric_n, patients_n = con.execute(
    """
    SELECT
        COUNT(*) AS raw_n,
        COUNT(*) FILTER (
            WHERE valuenum IS NOT NULL AND valuenum > 0
        ) AS numeric_positive_n,
        COUNT(*) FILTER (
            WHERE valuenum IS NULL OR valuenum <= 0
        ) AS excluded_n,
        COUNT(DISTINCT cohort_stay_id) FILTER (
            WHERE valuenum IS NOT NULL AND valuenum > 0
        ) AS patients_n
    FROM lactate_with_specimen
    """
).fetchone()

if raw_n != EXPECTED_RAW_LACTATE_ROWS:
    raise RuntimeError(
        f"Raw lactate rows changed: observed={raw_n}, "
        f"expected={EXPECTED_RAW_LACTATE_ROWS}."
    )

if numeric_n != EXPECTED_NUMERIC_LACTATE_ROWS:
    raise RuntimeError(
        f"Positive numeric lactate rows changed: observed={numeric_n}, "
        f"expected={EXPECTED_NUMERIC_LACTATE_ROWS}."
    )

if nonnumeric_n != EXPECTED_NONNUMERIC_LACTATE_ROWS:
    raise RuntimeError(
        f"Excluded/non-numeric lactate rows changed: observed={nonnumeric_n}, "
        f"expected={EXPECTED_NONNUMERIC_LACTATE_ROWS}."
    )

if patients_n != EXPECTED_PATIENTS_WITH_NUMERIC_LACTATE:
    raise RuntimeError(
        f"Patients with lactate changed: observed={patients_n}, "
        f"expected={EXPECTED_PATIENTS_WITH_NUMERIC_LACTATE}."
    )

unit_rows = con.execute(
    """
    SELECT DISTINCT valueuom
    FROM lactate_with_specimen
    ORDER BY valueuom
    """
).fetchall()

if unit_rows != [("mmol/L",)]:
    raise RuntimeError(f"Unexpected lactate units: {unit_rows}")

bad_hadm_n = con.execute(
    """
    SELECT COUNT(*)
    FROM lactate_with_specimen
    WHERE hadm_linkage_status <> 'HADM_ID_MATCH'
    """
).fetchone()[0]

if bad_hadm_n:
    raise RuntimeError(f"Found {bad_hadm_n} lactate rows with non-matching HADM.")


# =============================================================================
# Same-timestamp consolidation
# =============================================================================

# Multiple numeric samples at the same cohort stay + charttime are averaged.
# The specimen composition remains explicitly represented in the point table.
con.execute(
    """
    CREATE TABLE lactate_points AS
    SELECT
        cohort_stay_id,
        charttime,

        COUNT(*) AS lactate_raw_rows_n,
        COUNT(DISTINCT specimen_id) AS specimen_ids_n,
        COUNT(DISTINCT specimen_class) AS specimen_classes_n,

        AVG(valuenum) AS lactate_value,

        AVG(valuenum) FILTER (
            WHERE specimen_class = 'ARTERIAL'
        ) AS lactate_arterial_value,

        COUNT(*) FILTER (
            WHERE specimen_class = 'ARTERIAL'
        ) AS arterial_rows_n,

        COUNT(*) FILTER (
            WHERE specimen_class = 'MIXED'
        ) AS mixed_rows_n,

        COUNT(*) FILTER (
            WHERE specimen_class = 'CENTRAL_VENOUS'
        ) AS central_venous_rows_n,

        COUNT(*) FILTER (
            WHERE specimen_class = 'VENOUS'
        ) AS venous_rows_n,

        COUNT(*) FILTER (
            WHERE specimen_class = 'UNKNOWN'
        ) AS unknown_rows_n,

        COUNT(*) FILTER (
            WHERE specimen_class = 'OTHER'
        ) AS other_rows_n,

        bool_or(specimen_class = 'ARTERIAL') AS has_arterial,
        bool_or(specimen_class <> 'ARTERIAL') AS has_nonarterial,

        string_agg(
            DISTINCT specimen_class,
            ' | '
            ORDER BY specimen_class
        ) AS specimen_classes,

        MIN(valuenum) AS min_raw_lactate,
        MAX(valuenum) AS max_raw_lactate,
        MAX(valuenum) - MIN(valuenum) AS same_time_raw_range

    FROM lactate_numeric
    GROUP BY cohort_stay_id, charttime
    """
)

multirow_timestamps_n = con.execute(
    """
    SELECT COUNT(*)
    FROM lactate_points
    WHERE lactate_raw_rows_n > 1
    """
).fetchone()[0]

if multirow_timestamps_n != EXPECTED_MULTIROW_TIMESTAMPS:
    raise RuntimeError(
        f"Multi-row lactate timestamps changed: observed={multirow_timestamps_n}, "
        f"expected={EXPECTED_MULTIROW_TIMESTAMPS}."
    )

point_rows_n = con.execute(
    "SELECT COUNT(*) FROM lactate_points"
).fetchone()[0]

expected_point_rows_n = EXPECTED_NUMERIC_LACTATE_ROWS - EXPECTED_MULTIROW_TIMESTAMPS
if point_rows_n != expected_point_rows_n:
    raise RuntimeError(
        f"Unexpected consolidated point count: observed={point_rows_n}, "
        f"expected={expected_point_rows_n}."
    )

arterial_patients_n = con.execute(
    """
    SELECT COUNT(DISTINCT cohort_stay_id)
    FROM lactate_points
    WHERE lactate_arterial_value IS NOT NULL
    """
).fetchone()[0]

if arterial_patients_n != EXPECTED_ARTERIAL_LACTATE_PATIENTS:
    raise RuntimeError(
        f"Arterial lactate patient count changed: observed={arterial_patients_n}, "
        f"expected={EXPECTED_ARTERIAL_LACTATE_PATIENTS}."
    )

nonarterial_only_patients_n = patients_n - arterial_patients_n

if nonarterial_only_patients_n != EXPECTED_NONARTERIAL_ONLY_PATIENTS:
    raise RuntimeError(
        f"Non-arterial-only patient count changed: "
        f"observed={nonarterial_only_patients_n}, "
        f"expected={EXPECTED_NONARTERIAL_ONLY_PATIENTS}."
    )


# =============================================================================
# Patient-level measurement availability
# =============================================================================

con.execute(
    """
    CREATE TABLE patient_lactate_availability AS
    WITH all_lactate AS (
        SELECT
            cohort_stay_id,
            COUNT(*) AS lactate_timestamps_24h_n,
            MIN(charttime) AS first_lactate_charttime_24h,
            MAX(charttime) AS last_lactate_charttime_24h,
            bool_or(lactate_arterial_value IS NOT NULL) AS has_arterial_lactate_24h,
            bool_or(lactate_arterial_value IS NULL) AS has_nonarterial_only_point_24h
        FROM lactate_points
        GROUP BY cohort_stay_id
    )
    SELECT
        c.stay_id,
        coalesce(a.lactate_timestamps_24h_n, 0) AS lactate_timestamps_24h_n,
        a.first_lactate_charttime_24h,
        a.last_lactate_charttime_24h,
        (coalesce(a.lactate_timestamps_24h_n, 0) > 0) AS lactate_measured_24h_flag,
        coalesce(a.has_arterial_lactate_24h, false) AS arterial_lactate_measured_24h_flag,
        coalesce(a.has_nonarterial_only_point_24h, false)
            AS nonarterial_only_point_present_24h_flag
    FROM cohort c
    LEFT JOIN all_lactate a
        ON c.stay_id = a.cohort_stay_id
    """
)


# =============================================================================
# Window definitions
# =============================================================================

con.execute(
    """
    CREATE TABLE hourly_windows AS
    SELECT
        i AS window_index,
        i AS start_hour,
        i + 1 AS end_hour,
        60 AS nominal_minutes,
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
    ) AS t(window_index, start_hour, end_hour, nominal_minutes, window_label)
    """
)


# =============================================================================
# Complete patient-by-window grids
# =============================================================================

def create_grid(grid_name: str, windows_table: str) -> None:
    con.execute(
        f"""
        CREATE TABLE {grid_name} AS
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
                c.observation_end_time,
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
                        c.observation_end_time,
                        c.postoperative_time_zero
                            + w.end_hour * INTERVAL '1 hour'
                    )
                ) / 60.0
            ) AS eligible_minutes,

            CASE
                WHEN c.observation_end_time
                     <= c.postoperative_time_zero
                        + w.start_hour * INTERVAL '1 hour'
                    THEN 'NOT_OBSERVABLE'
                WHEN c.observation_end_time
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
# Window-level observed summaries
# =============================================================================

def create_window_output(output_table: str, grid_table: str) -> None:
    con.execute(
        f"""
        CREATE TABLE {output_table} AS

        WITH all_obs AS (
            SELECT
                g.stay_id,
                g.window_index,

                COUNT(p.charttime) AS lactate_points_n,
                SUM(coalesce(p.lactate_raw_rows_n, 0)) AS lactate_raw_rows_n,

                COUNT(p.charttime) FILTER (
                    WHERE p.has_arterial
                ) AS lactate_points_with_arterial_n,

                COUNT(p.charttime) FILTER (
                    WHERE p.has_nonarterial
                ) AS lactate_points_with_nonarterial_n,

                COUNT(p.charttime) FILTER (
                    WHERE NOT p.has_arterial
                ) AS lactate_nonarterial_only_points_n,

                SUM(coalesce(p.arterial_rows_n, 0)) AS lactate_arterial_raw_rows_n,
                SUM(coalesce(p.mixed_rows_n, 0)) AS lactate_mixed_raw_rows_n,
                SUM(coalesce(p.central_venous_rows_n, 0))
                    AS lactate_central_venous_raw_rows_n,
                SUM(coalesce(p.venous_rows_n, 0)) AS lactate_venous_raw_rows_n,
                SUM(coalesce(p.unknown_rows_n, 0)) AS lactate_unknown_raw_rows_n,
                SUM(coalesce(p.other_rows_n, 0)) AS lactate_other_raw_rows_n,

                AVG(p.lactate_value) AS lactate_mean_observed,
                median(p.lactate_value) AS lactate_median_observed,
                stddev_samp(p.lactate_value) AS lactate_sd_observed,
                MIN(p.lactate_value) AS lactate_min_observed,
                MAX(p.lactate_value) AS lactate_max_observed,

                first(p.lactate_value ORDER BY p.charttime)
                    AS lactate_first_observed,
                last(p.lactate_value ORDER BY p.charttime)
                    AS lactate_last_observed,

                first(p.charttime ORDER BY p.charttime)
                    AS lactate_first_charttime,
                last(p.charttime ORDER BY p.charttime)
                    AS lactate_last_charttime

            FROM {grid_table} g
            LEFT JOIN lactate_points p
                ON p.cohort_stay_id = g.stay_id
               AND g.eligible_minutes > 0
               AND p.charttime >= g.window_start_time
               AND p.charttime < g.eligible_window_end_time

            GROUP BY g.stay_id, g.window_index
        ),

        arterial_obs AS (
            SELECT
                g.stay_id,
                g.window_index,

                COUNT(p.charttime) FILTER (
                    WHERE p.lactate_arterial_value IS NOT NULL
                ) AS lactate_arterial_points_n,

                AVG(p.lactate_arterial_value) FILTER (
                    WHERE p.lactate_arterial_value IS NOT NULL
                ) AS lactate_arterial_mean_observed,

                median(p.lactate_arterial_value) FILTER (
                    WHERE p.lactate_arterial_value IS NOT NULL
                ) AS lactate_arterial_median_observed,

                MIN(p.lactate_arterial_value) FILTER (
                    WHERE p.lactate_arterial_value IS NOT NULL
                ) AS lactate_arterial_min_observed,

                MAX(p.lactate_arterial_value) FILTER (
                    WHERE p.lactate_arterial_value IS NOT NULL
                ) AS lactate_arterial_max_observed,

                first(
                    p.lactate_arterial_value
                    ORDER BY p.charttime
                ) FILTER (
                    WHERE p.lactate_arterial_value IS NOT NULL
                ) AS lactate_arterial_first_observed,

                last(
                    p.lactate_arterial_value
                    ORDER BY p.charttime
                ) FILTER (
                    WHERE p.lactate_arterial_value IS NOT NULL
                ) AS lactate_arterial_last_observed

            FROM {grid_table} g
            LEFT JOIN lactate_points p
                ON p.cohort_stay_id = g.stay_id
               AND g.eligible_minutes > 0
               AND p.charttime >= g.window_start_time
               AND p.charttime < g.eligible_window_end_time

            GROUP BY g.stay_id, g.window_index
        ),

        gaps AS (
            SELECT
                stay_id,
                window_index,
                MAX(gap_minutes) AS lactate_max_internal_point_gap_minutes
            FROM (
                SELECT
                    g.stay_id,
                    g.window_index,
                    p.charttime,
                    date_diff(
                        'second',
                        lag(p.charttime) OVER (
                            PARTITION BY g.stay_id, g.window_index
                            ORDER BY p.charttime
                        ),
                        p.charttime
                    ) / 60.0 AS gap_minutes
                FROM {grid_table} g
                INNER JOIN lactate_points p
                    ON p.cohort_stay_id = g.stay_id
                   AND g.eligible_minutes > 0
                   AND p.charttime >= g.window_start_time
                   AND p.charttime < g.eligible_window_end_time
            )
            GROUP BY stay_id, window_index
        )

        SELECT
            g.subject_id,
            g.hadm_id,
            g.stay_id,
            g.surgery_group,
            g.sex,
            g.age_at_icu_admission,
            g.postoperative_time_zero,
            g.observation_end_time,
            g.observation_end_type,

            g.window_index,
            g.window_label,
            g.start_hour,
            g.end_hour,
            g.nominal_minutes,
            g.window_start_time,
            g.nominal_window_end_time,
            g.eligible_window_end_time,
            g.eligible_minutes,
            g.observability_status,

            pa.lactate_measured_24h_flag,
            pa.arterial_lactate_measured_24h_flag,
            pa.lactate_timestamps_24h_n,

            CASE
                WHEN g.eligible_minutes > 0 AND a.lactate_points_n > 0 THEN 1
                WHEN g.eligible_minutes > 0 THEN 0
                ELSE NULL
            END AS lactate_measured_in_window_flag,

            a.lactate_points_n,
            a.lactate_raw_rows_n,

            a.lactate_points_with_arterial_n,
            a.lactate_points_with_nonarterial_n,
            a.lactate_nonarterial_only_points_n,

            a.lactate_arterial_raw_rows_n,
            a.lactate_mixed_raw_rows_n,
            a.lactate_central_venous_raw_rows_n,
            a.lactate_venous_raw_rows_n,
            a.lactate_unknown_raw_rows_n,
            a.lactate_other_raw_rows_n,

            a.lactate_mean_observed,
            a.lactate_median_observed,
            a.lactate_sd_observed,
            a.lactate_min_observed,
            a.lactate_max_observed,
            a.lactate_first_observed,
            a.lactate_last_observed,

            CASE
                WHEN a.lactate_points_n >= 2
                THEN a.lactate_last_observed - a.lactate_first_observed
                ELSE NULL
            END AS lactate_delta_last_minus_first_observed,

            a.lactate_first_charttime,
            a.lactate_last_charttime,

            CASE
                WHEN a.lactate_first_charttime IS NOT NULL
                THEN date_diff(
                    'second',
                    g.window_start_time,
                    a.lactate_first_charttime
                ) / 60.0
                ELSE NULL
            END AS lactate_first_point_offset_minutes,

            CASE
                WHEN a.lactate_last_charttime IS NOT NULL
                THEN date_diff(
                    'second',
                    a.lactate_last_charttime,
                    g.eligible_window_end_time
                ) / 60.0
                ELSE NULL
            END AS lactate_last_point_to_eligible_end_minutes,

            gp.lactate_max_internal_point_gap_minutes,

            ar.lactate_arterial_points_n,
            ar.lactate_arterial_mean_observed,
            ar.lactate_arterial_median_observed,
            ar.lactate_arterial_min_observed,
            ar.lactate_arterial_max_observed,
            ar.lactate_arterial_first_observed,
            ar.lactate_arterial_last_observed,

            CASE
                WHEN a.lactate_median_observed IS NOT NULL
                 AND ar.lactate_arterial_median_observed IS NOT NULL
                THEN
                    a.lactate_median_observed
                    - ar.lactate_arterial_median_observed
                ELSE NULL
            END AS lactate_median_all_minus_arterial,

            CASE
                WHEN a.lactate_last_observed IS NOT NULL
                 AND ar.lactate_arterial_last_observed IS NOT NULL
                THEN
                    a.lactate_last_observed
                    - ar.lactate_arterial_last_observed
                ELSE NULL
            END AS lactate_last_all_minus_arterial,

            CASE
                WHEN a.lactate_max_observed IS NOT NULL
                 AND ar.lactate_arterial_max_observed IS NOT NULL
                THEN
                    a.lactate_max_observed
                    - ar.lactate_arterial_max_observed
                ELSE NULL
            END AS lactate_max_all_minus_arterial,

            CASE
                WHEN g.eligible_minutes = 0 THEN 'NOT_OBSERVABLE'
                WHEN a.lactate_points_n = 0 THEN 'OBSERVABLE_NOT_MEASURED'
                WHEN a.lactate_points_n = 1 THEN 'MEASURED_ONE_POINT'
                ELSE 'MEASURED_MULTIPLE_POINTS'
            END AS lactate_measurement_status

        FROM {grid_table} g
        INNER JOIN all_obs a
            ON a.stay_id = g.stay_id
           AND a.window_index = g.window_index
        INNER JOIN arterial_obs ar
            ON ar.stay_id = g.stay_id
           AND ar.window_index = g.window_index
        LEFT JOIN gaps gp
            ON gp.stay_id = g.stay_id
           AND gp.window_index = g.window_index
        INNER JOIN patient_lactate_availability pa
            ON pa.stay_id = g.stay_id
        """
    )


create_window_output("hourly_output", "hourly_grid")
create_window_output("six_hour_output", "six_hour_grid")


# =============================================================================
# Validation invariants
# =============================================================================

hourly_rows_n = con.execute(
    "SELECT COUNT(*) FROM hourly_output"
).fetchone()[0]

six_hour_rows_n = con.execute(
    "SELECT COUNT(*) FROM six_hour_output"
).fetchone()[0]

if hourly_rows_n != EXPECTED_COHORT_N * 24:
    raise RuntimeError(
        f"Unexpected hourly row count: {hourly_rows_n}; "
        f"expected {EXPECTED_COHORT_N * 24}."
    )

if six_hour_rows_n != EXPECTED_COHORT_N * 4:
    raise RuntimeError(
        f"Unexpected six-hour row count: {six_hour_rows_n}; "
        f"expected {EXPECTED_COHORT_N * 4}."
    )

# Patient-window keys must be unique.
for table_name in ["hourly_output", "six_hour_output"]:
    dup_n = con.execute(
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

    if dup_n:
        raise RuntimeError(
            f"Duplicate patient-window keys in {table_name}: {dup_n}."
        )

# Observed values can only exist in observable windows.
for table_name in ["hourly_output", "six_hour_output"]:
    bad_n = con.execute(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE eligible_minutes = 0
          AND lactate_points_n > 0
        """
    ).fetchone()[0]

    if bad_n:
        raise RuntimeError(
            f"Found observed lactate in non-observable windows of "
            f"{table_name}: {bad_n}."
        )

# Arterial availability cannot exceed all-specimen availability.
for table_name in ["hourly_output", "six_hour_output"]:
    bad_n = con.execute(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE lactate_arterial_points_n > lactate_points_n
        """
    ).fetchone()[0]

    if bad_n:
        raise RuntimeError(
            f"Arterial point count exceeds total in {table_name}: {bad_n}."
        )

# Reconstruct all consolidated points exactly once in each temporal resolution.
all_points_from_hourly = con.execute(
    "SELECT SUM(lactate_points_n) FROM hourly_output"
).fetchone()[0]

all_points_from_six = con.execute(
    "SELECT SUM(lactate_points_n) FROM six_hour_output"
).fetchone()[0]

if all_points_from_hourly != point_rows_n:
    raise RuntimeError(
        f"Hourly windows reconstruct {all_points_from_hourly} points; "
        f"expected {point_rows_n}."
    )

if all_points_from_six != point_rows_n:
    raise RuntimeError(
        f"Six-hour windows reconstruct {all_points_from_six} points; "
        f"expected {point_rows_n}."
    )

# All observed lactate summaries must stay within the observed numeric range.
observed_min, observed_max = con.execute(
    """
    SELECT MIN(lactate_value), MAX(lactate_value)
    FROM lactate_points
    """
).fetchone()

for table_name in ["hourly_output", "six_hour_output"]:
    bad_n = con.execute(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE
            (lactate_min_observed IS NOT NULL
             AND lactate_min_observed < {observed_min})
         OR (lactate_max_observed IS NOT NULL
             AND lactate_max_observed > {observed_max})
        """
    ).fetchone()[0]

    if bad_n:
        raise RuntimeError(
            f"Out-of-range window summary detected in {table_name}: {bad_n}."
        )


# =============================================================================
# Audit table
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

# Overall extraction / source / candidate decisions.
overall_rows = [
    ("OVERALL", None, None, "cohort_n", cohort_rows_n, None),
    ("OVERALL", None, None, "raw_lactate_rows_n", raw_n, None),
    ("OVERALL", None, None, "numeric_lactate_rows_n", numeric_n, None),
    ("OVERALL", None, None, "non_numeric_excluded_rows_n", nonnumeric_n, None),
    ("OVERALL", None, None, "patients_with_lactate_n", patients_n, None),
    (
        "OVERALL",
        None,
        None,
        "patients_without_lactate_n",
        EXPECTED_COHORT_N - patients_n,
        None,
    ),
    (
        "OVERALL",
        None,
        None,
        "consolidated_lactate_timestamps_n",
        point_rows_n,
        None,
    ),
    (
        "OVERALL",
        None,
        None,
        "multirow_timestamps_consolidated_n",
        multirow_timestamps_n,
        None,
    ),
    (
        "OVERALL",
        None,
        None,
        "arterial_lactate_patients_n",
        arterial_patients_n,
        None,
    ),
    (
        "OVERALL",
        None,
        None,
        "nonarterial_only_lactate_patients_n",
        nonarterial_only_patients_n,
        None,
    ),
    ("OVERALL", None, None, "observed_lactate_min", observed_min, "mmol/L"),
    ("OVERALL", None, None, "observed_lactate_max", observed_max, "mmol/L"),
]

con.executemany(
    """
    INSERT INTO audit
    VALUES (?, ?, ?, ?, ?, ?)
    """,
    overall_rows,
)

# Overall all-specimen vs arterial-only point availability.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'OVERALL',
        NULL,
        NULL,
        metric,
        value_num,
        NULL
    FROM (
        SELECT
            'all_specimen_points_n' AS metric,
            COUNT(*)::DOUBLE AS value_num
        FROM lactate_points

        UNION ALL

        SELECT
            'points_with_arterial_value_n',
            COUNT(*) FILTER (
                WHERE lactate_arterial_value IS NOT NULL
            )::DOUBLE
        FROM lactate_points

        UNION ALL

        SELECT
            'nonarterial_only_points_n',
            COUNT(*) FILTER (
                WHERE lactate_arterial_value IS NULL
            )::DOUBLE
        FROM lactate_points

        UNION ALL

        SELECT
            'same_time_all_vs_arterial_mean_abs_difference',
            AVG(
                abs(lactate_value - lactate_arterial_value)
            )::DOUBLE
        FROM lactate_points
        WHERE lactate_arterial_value IS NOT NULL

        UNION ALL

        SELECT
            'same_time_all_vs_arterial_p95_abs_difference',
            quantile_cont(
                abs(lactate_value - lactate_arterial_value),
                0.95
            )::DOUBLE
        FROM lactate_points
        WHERE lactate_arterial_value IS NOT NULL

        UNION ALL

        SELECT
            'same_time_all_vs_arterial_max_abs_difference',
            MAX(
                abs(lactate_value - lactate_arterial_value)
            )::DOUBLE
        FROM lactate_points
        WHERE lactate_arterial_value IS NOT NULL
    )
    """
)


def add_window_audit(table_name: str, level: str) -> None:
    # Coverage and point density by window.
    con.execute(
        f"""
        INSERT INTO audit
        SELECT
            '{level}' AS audit_level,
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
                'measured_all_specimen_patients_n',
                COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND lactate_points_n > 0
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'measured_all_specimen_pct',
                100.0
                * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND lactate_points_n > 0
                )
                / NULLIF(
                    COUNT(*) FILTER (
                        WHERE eligible_minutes > 0
                    ),
                    0
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'measured_arterial_patients_n',
                COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND lactate_arterial_points_n > 0
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'measured_arterial_pct',
                100.0
                * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND lactate_arterial_points_n > 0
                )
                / NULLIF(
                    COUNT(*) FILTER (
                        WHERE eligible_minutes > 0
                    ),
                    0
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'patients_gained_by_all_specimen_n',
                COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND lactate_points_n > 0
                      AND lactate_arterial_points_n = 0
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'windows_with_multiple_points_n',
                COUNT(*) FILTER (
                    WHERE lactate_points_n >= 2
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'median_points_among_measured',
                median(lactate_points_n)::DOUBLE
            FROM {table_name}
            WHERE lactate_points_n > 0
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'p95_points_among_measured',
                quantile_cont(lactate_points_n, 0.95)::DOUBLE
            FROM {table_name}
            WHERE lactate_points_n > 0
            GROUP BY window_index, window_label
        )
        """
    )

    # Candidate-summary comparisons in measured windows with >=2 points.
    con.execute(
        f"""
        INSERT INTO audit
        SELECT
            '{level}',
            window_index,
            window_label,
            metric,
            value_num,
            NULL
        FROM (
            SELECT
                window_index,
                window_label,
                'multi_point_median_abs_last_minus_median' AS metric,
                median(
                    abs(lactate_last_observed - lactate_median_observed)
                )::DOUBLE AS value_num
            FROM {table_name}
            WHERE lactate_points_n >= 2
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'multi_point_p95_abs_last_minus_median',
                quantile_cont(
                    abs(lactate_last_observed - lactate_median_observed),
                    0.95
                )::DOUBLE
            FROM {table_name}
            WHERE lactate_points_n >= 2
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'multi_point_median_abs_max_minus_median',
                median(
                    abs(lactate_max_observed - lactate_median_observed)
                )::DOUBLE
            FROM {table_name}
            WHERE lactate_points_n >= 2
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'multi_point_p95_abs_max_minus_median',
                quantile_cont(
                    abs(lactate_max_observed - lactate_median_observed),
                    0.95
                )::DOUBLE
            FROM {table_name}
            WHERE lactate_points_n >= 2
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'paired_all_vs_arterial_median_abs_difference',
                median(
                    abs(
                        lactate_median_observed
                        - lactate_arterial_median_observed
                    )
                )::DOUBLE
            FROM {table_name}
            WHERE lactate_median_observed IS NOT NULL
              AND lactate_arterial_median_observed IS NOT NULL
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'paired_all_vs_arterial_p95_median_abs_difference',
                quantile_cont(
                    abs(
                        lactate_median_observed
                        - lactate_arterial_median_observed
                    ),
                    0.95
                )::DOUBLE
            FROM {table_name}
            WHERE lactate_median_observed IS NOT NULL
              AND lactate_arterial_median_observed IS NOT NULL
            GROUP BY window_index, window_label
        )
        """
    )


add_window_audit("hourly_output", "HOURLY")
add_window_audit("six_hour_output", "SIX_HOUR")

# Compact global comparison of candidate summaries.
for table_name, level in [
    ("hourly_output", "HOURLY_GLOBAL"),
    ("six_hour_output", "SIX_HOUR_GLOBAL"),
]:
    con.execute(
        f"""
        INSERT INTO audit
        SELECT
            '{level}',
            NULL,
            NULL,
            metric,
            value_num,
            NULL
        FROM (
            SELECT
                'eligible_windows_n' AS metric,
                COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                )::DOUBLE AS value_num
            FROM {table_name}

            UNION ALL

            SELECT
                'measured_windows_n',
                COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND lactate_points_n > 0
                )::DOUBLE
            FROM {table_name}

            UNION ALL

            SELECT
                'multi_point_measured_windows_n',
                COUNT(*) FILTER (
                    WHERE lactate_points_n >= 2
                )::DOUBLE
            FROM {table_name}

            UNION ALL

            SELECT
                'multi_point_measured_windows_pct_of_measured',
                100.0
                * COUNT(*) FILTER (
                    WHERE lactate_points_n >= 2
                )
                / NULLIF(
                    COUNT(*) FILTER (
                        WHERE lactate_points_n > 0
                    ),
                    0
                )::DOUBLE
            FROM {table_name}

            UNION ALL

            SELECT
                'median_abs_last_minus_median_multi_point',
                median(
                    abs(lactate_last_observed - lactate_median_observed)
                )::DOUBLE
            FROM {table_name}
            WHERE lactate_points_n >= 2

            UNION ALL

            SELECT
                'p95_abs_last_minus_median_multi_point',
                quantile_cont(
                    abs(lactate_last_observed - lactate_median_observed),
                    0.95
                )::DOUBLE
            FROM {table_name}
            WHERE lactate_points_n >= 2

            UNION ALL

            SELECT
                'median_abs_max_minus_median_multi_point',
                median(
                    abs(lactate_max_observed - lactate_median_observed)
                )::DOUBLE
            FROM {table_name}
            WHERE lactate_points_n >= 2

            UNION ALL

            SELECT
                'p95_abs_max_minus_median_multi_point',
                quantile_cont(
                    abs(lactate_max_observed - lactate_median_observed),
                    0.95
                )::DOUBLE
            FROM {table_name}
            WHERE lactate_points_n >= 2

            UNION ALL

            SELECT
                'paired_all_vs_arterial_windows_n',
                COUNT(*) FILTER (
                    WHERE lactate_median_observed IS NOT NULL
                      AND lactate_arterial_median_observed IS NOT NULL
                )::DOUBLE
            FROM {table_name}

            UNION ALL

            SELECT
                'median_abs_all_vs_arterial_median_difference',
                median(
                    abs(
                        lactate_median_observed
                        - lactate_arterial_median_observed
                    )
                )::DOUBLE
            FROM {table_name}
            WHERE lactate_median_observed IS NOT NULL
              AND lactate_arterial_median_observed IS NOT NULL

            UNION ALL

            SELECT
                'p95_abs_all_vs_arterial_median_difference',
                quantile_cont(
                    abs(
                        lactate_median_observed
                        - lactate_arterial_median_observed
                    ),
                    0.95
                )::DOUBLE
            FROM {table_name}
            WHERE lactate_median_observed IS NOT NULL
              AND lactate_arterial_median_observed IS NOT NULL
        )
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

con.execute(
    f"""
    COPY (
        SELECT *
        FROM audit
        ORDER BY
            CASE audit_level
                WHEN 'OVERALL' THEN 1
                WHEN 'HOURLY_GLOBAL' THEN 2
                WHEN 'SIX_HOUR_GLOBAL' THEN 3
                WHEN 'HOURLY' THEN 4
                WHEN 'SIX_HOUR' THEN 5
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
# Console report
# =============================================================================

print("=" * 92)
print("TRAJECTORIES - LACTATE CANONICAL TRAJECTORY CONSTRUCTION")
print("=" * 92)
print(f"Cohort rows / unique stays      : {cohort_rows_n} / {cohort_unique_stays_n}")
print(f"Raw lactate rows                : {raw_n}")
print(f"Positive numeric lactate rows   : {numeric_n}")
print(f"Non-numeric/excluded rows       : {nonnumeric_n}")
print(f"Patients with lactate           : {patients_n}")
print(f"Patients without lactate        : {EXPECTED_COHORT_N - patients_n}")
print(f"Consolidated timestamps         : {point_rows_n}")
print(f"Multi-row timestamps averaged   : {multirow_timestamps_n}")
print(f"Arterial lactate patients       : {arterial_patients_n}")
print(f"Non-arterial-only patients      : {nonarterial_only_patients_n}")
print(f"Observed consolidated range     : {observed_min:.3f} - {observed_max:.3f} mmol/L")
print()

print("All-specimen vs arterial-only point comparison")
print("----------------------------------------------")
print(
    con.execute(
        """
        SELECT
            COUNT(*) AS all_points_n,
            COUNT(*) FILTER (
                WHERE lactate_arterial_value IS NOT NULL
            ) AS points_with_arterial_n,
            COUNT(*) FILTER (
                WHERE lactate_arterial_value IS NULL
            ) AS nonarterial_only_points_n,
            round(
                AVG(
                    abs(lactate_value - lactate_arterial_value)
                ) FILTER (
                    WHERE lactate_arterial_value IS NOT NULL
                ),
                4
            ) AS mean_abs_difference,
            round(
                quantile_cont(
                    abs(lactate_value - lactate_arterial_value),
                    0.95
                ) FILTER (
                    WHERE lactate_arterial_value IS NOT NULL
                ),
                4
            ) AS p95_abs_difference,
            round(
                MAX(
                    abs(lactate_value - lactate_arterial_value)
                ) FILTER (
                    WHERE lactate_arterial_value IS NOT NULL
                ),
                4
            ) AS max_abs_difference
        FROM lactate_points
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Hourly candidate summary")
print("------------------------")
print(
    con.execute(
        """
        SELECT
            window_label,
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
            ) AS eligible_n,
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND lactate_points_n > 0
            ) AS measured_all_n,
            round(
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND lactate_points_n > 0
                )
                / NULLIF(
                    COUNT(*) FILTER (
                        WHERE eligible_minutes > 0
                    ), 0
                ),
                2
            ) AS measured_all_pct,
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND lactate_arterial_points_n > 0
            ) AS measured_arterial_n,
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND lactate_points_n > 0
                  AND lactate_arterial_points_n = 0
            ) AS gained_by_all_specimen_n,
            COUNT(*) FILTER (
                WHERE lactate_points_n >= 2
            ) AS multi_point_windows_n,
            round(
                median(lactate_median_observed) FILTER (
                    WHERE lactate_points_n > 0
                ),
                2
            ) AS population_median_window_median
        FROM hourly_output
        GROUP BY window_index, window_label
        ORDER BY window_index
        """
    ).fetchdf().to_string(index=False)
)
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
                WHERE eligible_minutes > 0
                  AND lactate_points_n > 0
            ) AS measured_all_n,
            round(
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND lactate_points_n > 0
                )
                / NULLIF(
                    COUNT(*) FILTER (
                        WHERE eligible_minutes > 0
                    ), 0
                ),
                2
            ) AS measured_all_pct,
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND lactate_arterial_points_n > 0
            ) AS measured_arterial_n,
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND lactate_points_n > 0
                  AND lactate_arterial_points_n = 0
            ) AS gained_by_all_specimen_n,
            COUNT(*) FILTER (
                WHERE lactate_points_n >= 2
            ) AS multi_point_windows_n,
            round(
                median(lactate_median_observed) FILTER (
                    WHERE lactate_points_n > 0
                ),
                2
            ) AS population_median_window_median
        FROM six_hour_output
        GROUP BY window_index, window_label
        ORDER BY window_index
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Global candidate-feature comparison")
print("-----------------------------------")
print(
    con.execute(
        """
        SELECT
            audit_level,
            metric,
            round(value_num, 4) AS value_num
        FROM audit
        WHERE audit_level IN ('HOURLY_GLOBAL', 'SIX_HOUR_GLOBAL')
        ORDER BY audit_level, metric
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Measurement-status counts")
print("-------------------------")
print(
    con.execute(
        """
        SELECT
            'HOURLY' AS resolution,
            lactate_measurement_status,
            COUNT(*) AS windows_n
        FROM hourly_output
        GROUP BY lactate_measurement_status

        UNION ALL

        SELECT
            'SIX_HOUR',
            lactate_measurement_status,
            COUNT(*)
        FROM six_hour_output
        GROUP BY lactate_measurement_status

        ORDER BY resolution, lactate_measurement_status
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Canonical outputs")
print("-----------------")
print(OUT_HOURLY)
print(OUT_SIX_HOUR)
print(OUT_AUDIT)
print()
print("Canonical lactate extraction complete.")
print("No LOCF/interpolation was used.")
print("Measurement absence remains explicit.")
print("Final all-specimen vs arterial-only and primary feature selection remain")
print("to be frozen after review of this audit.")
