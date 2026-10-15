"""
01_14_extract_respiratory_outcomes.py

Trajectories project
Respiratory domain - canonical respiratory outcomes extraction

STATUS
------
Canonical and frozen respiratory-outcome extraction.

PURPOSE
-------
Derive postoperative respiratory outcomes while preserving the frozen
respiratory-state definition from component 01_11.

PRIMARY OUTCOMES
----------------
1. First extubation within 24 postoperative hours.
2. Early reintubation within 48 hours after first extubation.

SENSITIVITY WINDOWS
-------------------
Reintubation within 24 h and 72 h after first extubation.

RESPIRATORY DEFINITION
----------------------
The respiratory-state representation uses the SAME source itemids, exact value
mappings, same-timestamp priority, 6-hour persistence cap, and <=60-minute
initial backward assignment used by canonical component 01_11.

EXTUBATION DEFINITION
---------------------
First contiguous transition from INVASIVE_VENT to one of:
    NON_INVASIVE_VENT
    HFNC
    SUPPLEMENTAL_OXYGEN
    NONE

The following do NOT define extubation:
    TRACHEOSTOMY
    uncovered time
    expiration of invasive persistence without a new classified state

REINTUBATION DEFINITION
-----------------------
First subsequent state-derived INVASIVE_VENT episode after first extubation.

Ascertainment is hospital-aware and includes:
    index ICU stay
    subsequent ICU stays within the same hadm_id

Primary window:
    <=48 h after first extubation

Sensitivity:
    <=24 h
    <=72 h

PROCEDUREEVENTS
---------------
Validation only. Never used to define or modify the primary outcomes.

HOSPITAL FOLLOW-UP BOUNDARY
---------------------------
ADMISSIONS.dischtime can occasionally precede ICU data that are demonstrably
present. Therefore the effective hospital follow-up end is defined as the latest
of:
    ADMISSIONS.dischtime
    frozen index observation_end_time
    latest ICU outtime in the same hadm_id

and is capped at t0 + 96 h, which is sufficient to ascertain 72-h
reintubation after an extubation occurring within the first 24 h.

CANONICAL OUTPUTS
-----------------
data/01_14_respiratory_outcomes.parquet
data/01_14_respiratory_outcomes_audit.csv
data/01_14_reintubation_sensitivity.csv

Inputs
------
data/00_04_final_analytic_cohort.csv
Raw MIMIC-IV 3.1 files relative to MIMIC_ROOT:
  icu/chartevents.csv.gz
  icu/procedureevents.csv.gz
  icu/icustays.csv.gz
  hosp/admissions.csv.gz

Respiratory-state item IDs are 226732, 223849, and 229314. Supportive
procedure IDs are 227194, 225468, 225477 (extubation) and 224385 (intubation).
The 01_11 definition is implemented internally; no 01_11 output is read.
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
PROCEDUREEVENTS_FILE = MIMIC_ROOT / "icu" / "procedureevents.csv.gz"
ICUSTAYS_FILE = MIMIC_ROOT / "icu" / "icustays.csv.gz"
ADMISSIONS_FILE = MIMIC_ROOT / "hosp" / "admissions.csv.gz"

OUT_OUTCOMES = (
    PROJECT_ROOT / "data" /
    "01_14_respiratory_outcomes.parquet"
)
OUT_AUDIT = (
    PROJECT_ROOT / "data" /
    "01_14_respiratory_outcomes_audit.csv"
)
OUT_SENSITIVITY = (
    PROJECT_ROOT / "data" /
    "01_14_reintubation_sensitivity.csv"
)

EXPECTED_COHORT_N = 9305

O2_DEVICE_ITEMID = 226732
VENT_MODE_ITEMID = 223849
HAMILTON_MODE_ITEMID = 229314

EXTUBATION_PROCEDURE_ITEMIDS = (227194, 225468, 225477)
INTUBATION_PROCEDURE_ITEMID = 224385

PRIMARY_EXTUBATION_HORIZON_HOURS = 24
PRIMARY_REINTUBATION_HOURS = 48
REINTUBATION_SENSITIVITY_HOURS = (24, 72)
MAX_SOURCE_HORIZON_HOURS = 96

PERSISTENCE_HOURS = 6
BACKWARD_LIMIT_MINUTES = 60


# =============================================================================
# Frozen 01_11 state mappings
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

EXTUBATED_STATES = (
    "NON_INVASIVE_VENT",
    "HFNC",
    "SUPPLEMENTAL_OXYGEN",
    "NONE",
)


# =============================================================================
# Helpers
# =============================================================================

def qpath(path: Path) -> str:
    return str(path).replace("'", "''")


def sql_list(values: tuple[str, ...]) -> str:
    return ", ".join("'" + value.replace("'", "''") + "'" for value in values)


def sql_int_list(values: tuple[int, ...]) -> str:
    return ", ".join(str(value) for value in values)


def require_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Required file not found: {path}")


for required in (
    COHORT_FILE,
    CHARTEVENTS_FILE,
    PROCEDUREEVENTS_FILE,
    ICUSTAYS_FILE,
    ADMISSIONS_FILE,
):
    require_file(required)

OUT_OUTCOMES.parent.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(database=":memory:")
con.execute("PRAGMA threads=4")
con.execute("PRAGMA preserve_insertion_order=false")


# =============================================================================
# Admissions and frozen cohort
# =============================================================================

con.execute(
    f"""
    CREATE TABLE admissions AS
    SELECT
        subject_id::BIGINT AS subject_id,
        hadm_id::BIGINT AS hadm_id,
        admittime::TIMESTAMP AS admittime,
        dischtime::TIMESTAMP AS dischtime,
        deathtime::TIMESTAMP AS deathtime,
        hospital_expire_flag::INTEGER AS hospital_expire_flag
    FROM read_csv_auto(
        '{qpath(ADMISSIONS_FILE)}',
        header=true,
        all_varchar=false
    )
    """
)

con.execute(
    f"""
    CREATE TABLE cohort AS
    SELECT
        x.subject_id,
        x.hadm_id,
        x.stay_id,
        x.surgery_group,
        x.sex,
        x.age_at_icu_admission,
        x.t0,
        x.index_observation_end_time,
        x.observation_end_type,
        least(
            x.index_observation_end_time,
            x.t0 + INTERVAL '{PRIMARY_EXTUBATION_HORIZON_HOURS} hours'
        ) AS primary_extubation_end_time,

        a.admittime AS hospital_admittime,
        a.dischtime AS hospital_discharge_time,
        a.deathtime AS hospital_death_time,
        a.hospital_expire_flag

    FROM (
        SELECT
            subject_id::BIGINT AS subject_id,
            hadm_id::BIGINT AS hadm_id,
            stay_id::BIGINT AS stay_id,
            surgery_group,
            sex,
            age_at_icu_admission,
            postoperative_time_zero::TIMESTAMP AS t0,
            observation_end_time::TIMESTAMP AS index_observation_end_time,
            observation_end_type
        FROM read_csv_auto(
            '{qpath(COHORT_FILE)}',
            header=true,
            all_varchar=false
        )
    ) x

    LEFT JOIN admissions a
        ON x.subject_id = a.subject_id
       AND x.hadm_id = a.hadm_id
    """
)

cohort_check = con.execute(
    """
    SELECT
        COUNT(*) AS rows_n,
        COUNT(DISTINCT stay_id) AS stays_n,
        COUNT(*) FILTER (WHERE t0 IS NULL) AS missing_t0_n,
        COUNT(*) FILTER (
            WHERE index_observation_end_time IS NULL
        ) AS missing_index_end_n,
        COUNT(*) FILTER (
            WHERE index_observation_end_time < t0
        ) AS invalid_index_time_n,
        COUNT(*) FILTER (
            WHERE hospital_discharge_time IS NULL
        ) AS missing_discharge_n
    FROM cohort
    """
).fetchone()

if cohort_check[0] != EXPECTED_COHORT_N or cohort_check[1] != EXPECTED_COHORT_N:
    raise RuntimeError(
        f"Unexpected cohort dimensions: rows={cohort_check[0]}, "
        f"stays={cohort_check[1]}."
    )

if any(cohort_check[2:]):
    raise RuntimeError(f"Invalid cohort/admission fields: {cohort_check[2:]}.")


# =============================================================================
# ICU stays and effective hospital follow-up boundary
# =============================================================================

con.execute(
    f"""
    CREATE TABLE all_icu_stays AS
    SELECT
        subject_id::BIGINT AS subject_id,
        hadm_id::BIGINT AS hadm_id,
        stay_id::BIGINT AS source_stay_id,
        first_careunit,
        last_careunit,
        intime::TIMESTAMP AS icu_intime,
        outtime::TIMESTAMP AS icu_outtime
    FROM read_csv_auto(
        '{qpath(ICUSTAYS_FILE)}',
        header=true,
        all_varchar=false
    )
    """
)

con.execute(
    """
    CREATE TABLE hospital_icu_bounds AS
    SELECT
        c.stay_id AS index_stay_id,
        max(s.icu_outtime) AS latest_same_admission_icu_outtime
    FROM cohort c
    LEFT JOIN all_icu_stays s
        ON c.subject_id = s.subject_id
       AND c.hadm_id = s.hadm_id
    GROUP BY c.stay_id
    """
)

con.execute(
    """
    ALTER TABLE cohort
    ADD COLUMN hospital_followup_end_time TIMESTAMP
    """
)

con.execute(
    """
    ALTER TABLE cohort
    ADD COLUMN hospital_discharge_before_index_icu_end_flag INTEGER
    """
)

con.execute(
    """
    ALTER TABLE cohort
    ADD COLUMN hospital_discharge_before_any_icu_end_flag INTEGER
    """
)

con.execute(
    f"""
    UPDATE cohort
    SET
        hospital_followup_end_time = least(
            t0 + INTERVAL '{MAX_SOURCE_HORIZON_HOURS} hours',
            greatest(
                hospital_discharge_time,
                index_observation_end_time,
                b.latest_same_admission_icu_outtime
            )
        ),

        hospital_discharge_before_index_icu_end_flag =
            CASE
                WHEN hospital_discharge_time < index_observation_end_time
                    THEN 1 ELSE 0
            END,

        hospital_discharge_before_any_icu_end_flag =
            CASE
                WHEN hospital_discharge_time
                     < b.latest_same_admission_icu_outtime
                    THEN 1 ELSE 0
            END

    FROM hospital_icu_bounds b
    WHERE cohort.stay_id = b.index_stay_id
    """
)

con.execute(
    """
    CREATE TABLE followup_icu_stays AS
    SELECT
        c.stay_id AS index_stay_id,
        c.subject_id,
        c.hadm_id,
        c.t0,
        c.index_observation_end_time,
        c.hospital_followup_end_time,
        s.source_stay_id,
        s.icu_intime,
        s.icu_outtime,
        s.first_careunit,
        s.last_careunit,
        CASE
            WHEN s.source_stay_id = c.stay_id THEN 1
            ELSE 0
        END AS index_stay_flag
    FROM cohort c
    INNER JOIN all_icu_stays s
        ON c.subject_id = s.subject_id
       AND c.hadm_id = s.hadm_id
       AND s.icu_outtime > c.t0
       AND s.icu_intime < c.hospital_followup_end_time
    """
)


# =============================================================================
# Respiratory CHARTEVENTS across index and later ICU stays
# =============================================================================

con.execute(
    f"""
    CREATE TABLE raw_chart AS
    SELECT
        f.index_stay_id,
        f.hadm_id,
        f.source_stay_id,
        f.index_stay_flag,
        f.icu_intime,
        f.icu_outtime,
        ce.charttime::TIMESTAMP AS charttime,
        ce.itemid::BIGINT AS itemid,
        ce.value,
        f.t0,
        f.index_observation_end_time,
        f.hospital_followup_end_time,

        date_diff(
            'second',
            f.t0,
            ce.charttime::TIMESTAMP
        ) / 60.0 AS offset_minutes_from_t0,

        date_diff(
            'second',
            f.icu_intime,
            ce.charttime::TIMESTAMP
        ) / 60.0 AS offset_minutes_from_source_icu_intime

    FROM followup_icu_stays f

    INNER JOIN read_csv_auto(
        '{qpath(CHARTEVENTS_FILE)}',
        header=true,
        all_varchar=false
    ) ce
        ON ce.stay_id = f.source_stay_id
       AND ce.charttime >= f.t0
       AND ce.charttime < f.hospital_followup_end_time

    WHERE ce.itemid IN (
        {O2_DEVICE_ITEMID},
        {VENT_MODE_ITEMID},
        {HAMILTON_MODE_ITEMID}
    )
    """
)


# =============================================================================
# Exact frozen 01_11 same-timestamp classification
# =============================================================================

con.execute(
    f"""
    CREATE TABLE timestamp_evidence AS
    SELECT
        index_stay_id,
        hadm_id,
        source_stay_id,
        any_value(index_stay_flag) AS index_stay_flag,
        any_value(icu_intime) AS icu_intime,
        any_value(icu_outtime) AS icu_outtime,
        charttime,
        any_value(t0) AS t0,
        any_value(index_observation_end_time) AS index_observation_end_time,
        any_value(hospital_followup_end_time) AS hospital_followup_end_time,
        any_value(offset_minutes_from_t0) AS offset_minutes_from_t0,
        any_value(offset_minutes_from_source_icu_intime)
            AS offset_minutes_from_source_icu_intime,

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
        ) AS niv_hamilton_mode_flag

    FROM raw_chart
    GROUP BY
        index_stay_id,
        hadm_id,
        source_stay_id,
        charttime
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
        lag(respiratory_state) OVER (
            PARTITION BY index_stay_id, source_stay_id
            ORDER BY charttime
        ) AS previous_state,
        lead(charttime) OVER (
            PARTITION BY index_stay_id, source_stay_id
            ORDER BY charttime
        ) AS next_charttime,
        lead(respiratory_state) OVER (
            PARTITION BY index_stay_id, source_stay_id
            ORDER BY charttime
        ) AS next_state,
        row_number() OVER (
            PARTITION BY index_stay_id, source_stay_id
            ORDER BY charttime
        ) AS state_sequence_number
    FROM classified_timestamps
    WHERE respiratory_state <> 'UNCLASSIFIED'
    """
)


# =============================================================================
# Respiratory intervals
# =============================================================================

con.execute(
    f"""
    CREATE TABLE state_intervals AS
    SELECT
        index_stay_id,
        hadm_id,
        source_stay_id,
        index_stay_flag,
        icu_intime,
        icu_outtime,
        charttime AS source_charttime,
        respiratory_state,
        device_values,
        standard_mode_values,
        hamilton_mode_values,
        previous_state,
        next_state,
        state_sequence_number,

        CASE
            WHEN index_stay_flag = 1
             AND state_sequence_number = 1
             AND offset_minutes_from_t0 <= {BACKWARD_LIMIT_MINUTES}
                THEN t0

            WHEN index_stay_flag = 0
             AND state_sequence_number = 1
             AND offset_minutes_from_source_icu_intime
                 BETWEEN 0 AND {BACKWARD_LIMIT_MINUTES}
                THEN greatest(icu_intime, t0)

            ELSE charttime
        END AS interval_start,

        least(
            coalesce(
                next_charttime,
                charttime + INTERVAL '{PERSISTENCE_HOURS} hours'
            ),
            charttime + INTERVAL '{PERSISTENCE_HOURS} hours',
            icu_outtime,
            hospital_followup_end_time
        ) AS interval_end,

        CASE
            WHEN next_charttime IS NOT NULL
             AND next_charttime
                 <= charttime + INTERVAL '{PERSISTENCE_HOURS} hours'
                THEN 'NEXT_CLASSIFIED_TIMESTAMP'
            WHEN icu_outtime
                 <= charttime + INTERVAL '{PERSISTENCE_HOURS} hours'
                THEN 'ICU_OUTTIME'
            WHEN hospital_followup_end_time
                 <= charttime + INTERVAL '{PERSISTENCE_HOURS} hours'
                THEN 'HOSPITAL_FOLLOWUP_END'
            ELSE 'PERSISTENCE_CAP'
        END AS interval_end_reason

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
            index_stay_id,
            source_stay_id,
            interval_start,
            lag(interval_end) OVER (
                PARTITION BY index_stay_id, source_stay_id
                ORDER BY interval_start, interval_end
            ) AS previous_interval_end
        FROM valid_state_intervals
    )
    WHERE previous_interval_end > interval_start
    """
).fetchone()[0]

if overlap_n:
    raise RuntimeError(f"Overlapping respiratory intervals detected: {overlap_n}.")


# =============================================================================
# First extubation within 24 h
# =============================================================================

con.execute(
    f"""
    CREATE TABLE extubation_candidates AS
    WITH x AS (
        SELECT
            i.*,
            lag(respiratory_state) OVER (
                PARTITION BY index_stay_id, source_stay_id
                ORDER BY interval_start, source_charttime
            ) AS previous_interval_state,
            lag(interval_end) OVER (
                PARTITION BY index_stay_id, source_stay_id
                ORDER BY interval_start, source_charttime
            ) AS previous_interval_end
        FROM valid_state_intervals i
        WHERE index_stay_flag = 1
    )
    SELECT
        x.index_stay_id AS stay_id,
        x.interval_start AS extubation_time,
        date_diff(
            'second',
            c.t0,
            x.interval_start
        ) / 60.0 AS time_to_extubation_minutes,
        x.respiratory_state AS post_extubation_state
    FROM x
    INNER JOIN cohort c
        ON x.index_stay_id = c.stay_id
    WHERE x.previous_interval_state = 'INVASIVE_VENT'
      AND x.respiratory_state IN ({sql_list(EXTUBATED_STATES)})
      AND x.previous_interval_end = x.interval_start
      AND x.interval_start >= c.t0
      AND x.interval_start < c.primary_extubation_end_time
    """
)

con.execute(
    """
    CREATE TABLE first_extubation AS
    SELECT *
    FROM (
        SELECT
            e.*,
            row_number() OVER (
                PARTITION BY stay_id
                ORDER BY extubation_time
            ) AS rn
        FROM extubation_candidates e
    )
    WHERE rn = 1
    """
)


# =============================================================================
# Reintubation candidates
# =============================================================================

# Same index ICU stay: use reconstructed interval start.
con.execute(
    """
    CREATE TABLE same_stay_reintubation AS
    SELECT *
    FROM (
        SELECT
            f.stay_id,
            i.source_stay_id,
            i.interval_start AS candidate_time,
            'INDEX_STAY' AS candidate_source,
            row_number() OVER (
                PARTITION BY f.stay_id
                ORDER BY i.interval_start
            ) AS rn
        FROM first_extubation f
        INNER JOIN valid_state_intervals i
            ON i.index_stay_id = f.stay_id
           AND i.source_stay_id = f.stay_id
           AND i.respiratory_state = 'INVASIVE_VENT'
           AND i.interval_start > f.extubation_time
           AND i.interval_start
               <= f.extubation_time + INTERVAL '72 hours'
    )
    WHERE rn = 1
    """
)

# Later ICU stay: conservative timing at first observed invasive timestamp.
con.execute(
    """
    CREATE TABLE later_icu_reintubation AS
    SELECT *
    FROM (
        SELECT
            f.stay_id,
            i.source_stay_id,
            i.source_charttime AS candidate_time,
            'SUBSEQUENT_ICU_STAY' AS candidate_source,
            row_number() OVER (
                PARTITION BY f.stay_id
                ORDER BY i.source_charttime, i.source_stay_id
            ) AS rn
        FROM first_extubation f
        INNER JOIN valid_state_intervals i
            ON i.index_stay_id = f.stay_id
           AND i.source_stay_id <> f.stay_id
           AND i.respiratory_state = 'INVASIVE_VENT'
           AND i.source_charttime > f.extubation_time
           AND i.source_charttime
               <= f.extubation_time + INTERVAL '72 hours'
    )
    WHERE rn = 1
    """
)

con.execute(
    """
    CREATE TABLE first_reintubation AS
    WITH u AS (
        SELECT
            stay_id,
            source_stay_id,
            candidate_time,
            candidate_source
        FROM same_stay_reintubation

        UNION ALL

        SELECT
            stay_id,
            source_stay_id,
            candidate_time,
            candidate_source
        FROM later_icu_reintubation
    )
    SELECT *
    FROM (
        SELECT
            u.*,
            row_number() OVER (
                PARTITION BY stay_id
                ORDER BY candidate_time,
                         CASE
                             WHEN candidate_source='INDEX_STAY' THEN 0
                             ELSE 1
                         END
            ) AS rn
        FROM u
    )
    WHERE rn = 1
    """
)


# =============================================================================
# Contiguous invasive runs for sensitivity analysis only
# =============================================================================

con.execute(
    """
    CREATE TABLE invasive_interval_runs AS
    WITH x AS (
        SELECT
            i.*,
            lag(interval_end) OVER (
                PARTITION BY index_stay_id, source_stay_id
                ORDER BY interval_start, source_charttime
            ) AS previous_interval_end,
            lag(respiratory_state) OVER (
                PARTITION BY index_stay_id, source_stay_id
                ORDER BY interval_start, source_charttime
            ) AS previous_interval_state
        FROM valid_state_intervals i
    ),
    y AS (
        SELECT
            *,
            CASE
                WHEN respiratory_state <> 'INVASIVE_VENT' THEN NULL
                WHEN previous_interval_state = 'INVASIVE_VENT'
                 AND previous_interval_end = interval_start
                    THEN 0
                ELSE 1
            END AS invasive_run_start_flag
        FROM x
    ),
    z AS (
        SELECT
            *,
            sum(coalesce(invasive_run_start_flag, 0)) OVER (
                PARTITION BY index_stay_id, source_stay_id
                ORDER BY interval_start, source_charttime
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS invasive_run_id
        FROM y
    )
    SELECT
        index_stay_id,
        source_stay_id,
        invasive_run_id,
        min(interval_start) AS invasive_run_start,
        max(interval_end) AS invasive_run_end,
        date_diff(
            'second',
            min(interval_start),
            max(interval_end)
        ) / 60.0 AS invasive_run_minutes,

        string_agg(
            DISTINCT coalesce(device_values, ''),
            ' || '
            ORDER BY coalesce(device_values, '')
        ) FILTER (WHERE device_values IS NOT NULL)
            AS run_device_values,

        string_agg(
            DISTINCT coalesce(standard_mode_values, ''),
            ' || '
            ORDER BY coalesce(standard_mode_values, '')
        ) FILTER (WHERE standard_mode_values IS NOT NULL)
            AS run_standard_mode_values,

        string_agg(
            DISTINCT coalesce(hamilton_mode_values, ''),
            ' || '
            ORDER BY coalesce(hamilton_mode_values, '')
        ) FILTER (WHERE hamilton_mode_values IS NOT NULL)
            AS run_hamilton_mode_values

    FROM z
    WHERE respiratory_state = 'INVASIVE_VENT'
    GROUP BY
        index_stay_id,
        source_stay_id,
        invasive_run_id
    """
)


# =============================================================================
# PROCEDUREEVENTS validation
# =============================================================================

con.execute(
    f"""
    CREATE TABLE procedure_events AS
    SELECT
        c.stay_id AS index_stay_id,
        pe.stay_id::BIGINT AS procedure_stay_id,
        pe.starttime::TIMESTAMP AS event_time,
        pe.itemid::BIGINT AS itemid,

        CASE
            WHEN pe.itemid IN (
                {sql_int_list(EXTUBATION_PROCEDURE_ITEMIDS)}
            ) THEN 'EXTUBATION'
            WHEN pe.itemid = {INTUBATION_PROCEDURE_ITEMID}
                THEN 'INTUBATION'
            ELSE 'OTHER'
        END AS procedure_event_type

    FROM cohort c

    INNER JOIN read_csv_auto(
        '{qpath(PROCEDUREEVENTS_FILE)}',
        header=true,
        all_varchar=false
    ) pe
        ON c.subject_id = pe.subject_id
       AND c.hadm_id = pe.hadm_id

    WHERE pe.itemid IN (
        {sql_int_list(EXTUBATION_PROCEDURE_ITEMIDS)},
        {INTUBATION_PROCEDURE_ITEMID}
    )
      AND pe.starttime >= c.t0
      AND pe.starttime < c.hospital_followup_end_time
    """
)

con.execute(
    """
    CREATE TABLE first_procedure_extubation_24h AS
    SELECT
        p.index_stay_id AS stay_id,
        min(p.event_time) AS first_procedure_extubation_24h_time
    FROM procedure_events p
    INNER JOIN cohort c
        ON p.index_stay_id = c.stay_id
    WHERE p.procedure_event_type = 'EXTUBATION'
      AND p.event_time >= c.t0
      AND p.event_time < c.primary_extubation_end_time
    GROUP BY p.index_stay_id
    """
)

con.execute(
    """
    CREATE TABLE first_procedure_intubation_after_extubation AS
    SELECT
        f.stay_id,
        min(p.event_time)
            AS first_procedure_intubation_after_extubation_time
    FROM first_extubation f
    INNER JOIN procedure_events p
        ON p.index_stay_id = f.stay_id
       AND p.procedure_event_type = 'INTUBATION'
       AND p.event_time > f.extubation_time
       AND p.event_time
           <= f.extubation_time + INTERVAL '72 hours'
    GROUP BY f.stay_id
    """
)


# =============================================================================
# Canonical stay-level outcome table
# =============================================================================

con.execute(
    """
    CREATE TABLE outcomes AS
    SELECT
        c.subject_id,
        c.hadm_id,
        c.stay_id,
        c.surgery_group,
        c.sex,
        c.age_at_icu_admission,

        c.t0 AS postoperative_time_zero,
        c.index_observation_end_time,
        c.observation_end_type,
        c.hospital_discharge_time,
        c.hospital_death_time,
        c.hospital_expire_flag,
        c.hospital_followup_end_time,

        c.hospital_discharge_before_index_icu_end_flag,
        c.hospital_discharge_before_any_icu_end_flag,

        CASE
            WHEN f.extubation_time IS NOT NULL THEN 1
            ELSE 0
        END AS first_extubation_within_24h_flag,

        f.extubation_time AS first_extubation_time,
        f.time_to_extubation_minutes,
        f.time_to_extubation_minutes / 60.0
            AS time_to_extubation_hours,
        f.post_extubation_state,

        CASE
            WHEN f.extubation_time IS NOT NULL
             AND f.time_to_extubation_minutes <= 360
                THEN 1 ELSE 0
        END AS extubated_within_6h_flag,

        CASE
            WHEN f.extubation_time IS NOT NULL
             AND f.time_to_extubation_minutes <= 720
                THEN 1 ELSE 0
        END AS extubated_within_12h_flag,

        r.candidate_time AS first_reintubation_time,
        r.candidate_source AS first_reintubation_source,
        r.source_stay_id AS first_reintubation_stay_id,

        CASE
            WHEN r.candidate_time IS NULL THEN NULL
            ELSE date_diff(
                'second',
                f.extubation_time,
                r.candidate_time
            ) / 60.0
        END AS minutes_extubation_to_reintubation,

        CASE
            WHEN f.extubation_time IS NULL THEN 0
            WHEN r.candidate_time IS NOT NULL
             AND r.candidate_time
                 <= f.extubation_time + INTERVAL '24 hours'
                THEN 1 ELSE 0
        END AS reintubation_within_24h_flag,

        CASE
            WHEN f.extubation_time IS NULL THEN 0
            WHEN r.candidate_time IS NOT NULL
             AND r.candidate_time
                 <= f.extubation_time + INTERVAL '48 hours'
                THEN 1 ELSE 0
        END AS reintubation_within_48h_flag,

        CASE
            WHEN f.extubation_time IS NULL THEN 0
            WHEN r.candidate_time IS NOT NULL
             AND r.candidate_time
                 <= f.extubation_time + INTERVAL '72 hours'
                THEN 1 ELSE 0
        END AS reintubation_within_72h_flag,

        CASE
            WHEN f.extubation_time IS NULL
                THEN 'NOT_APPLICABLE_NO_EXTUBATION_WITHIN_24H'
            WHEN r.candidate_time IS NOT NULL
             AND r.candidate_time
                 <= f.extubation_time + INTERVAL '48 hours'
                THEN 'EVENT'
            WHEN c.hospital_death_time IS NOT NULL
             AND c.hospital_death_time
                 < f.extubation_time + INTERVAL '48 hours'
                THEN 'COMPETING_DEATH_BEFORE_48H'
            WHEN c.hospital_followup_end_time
                 >= f.extubation_time + INTERVAL '48 hours'
                THEN 'NO_EVENT_COMPLETE_HOSPITAL_FOLLOWUP'
            ELSE 'CENSORED_AT_HOSPITAL_DISCHARGE'
        END AS reintubation_48h_status,

        pe.first_procedure_extubation_24h_time,

        CASE
            WHEN f.extubation_time IS NULL
              OR pe.first_procedure_extubation_24h_time IS NULL
                THEN NULL
            ELSE date_diff(
                'second',
                f.extubation_time,
                pe.first_procedure_extubation_24h_time
            ) / 60.0
        END AS procedure_minus_state_extubation_minutes,

        pi.first_procedure_intubation_after_extubation_time,

        CASE
            WHEN r.candidate_time IS NULL
              OR pi.first_procedure_intubation_after_extubation_time IS NULL
                THEN NULL
            ELSE date_diff(
                'second',
                r.candidate_time,
                pi.first_procedure_intubation_after_extubation_time
            ) / 60.0
        END AS procedure_minus_state_reintubation_minutes

    FROM cohort c

    LEFT JOIN first_extubation f
        ON c.stay_id = f.stay_id

    LEFT JOIN first_reintubation r
        ON c.stay_id = r.stay_id

    LEFT JOIN first_procedure_extubation_24h pe
        ON c.stay_id = pe.stay_id

    LEFT JOIN first_procedure_intubation_after_extubation pi
        ON c.stay_id = pi.stay_id
    """
)


# =============================================================================
# Sensitivity analysis
# =============================================================================

con.execute(
    """
    CREATE TABLE reintubation_run_characterization AS
    SELECT
        o.stay_id,
        o.first_extubation_time,
        o.first_reintubation_time,
        o.first_reintubation_source,
        o.first_reintubation_stay_id,
        o.minutes_extubation_to_reintubation,

        r.invasive_run_minutes,

        CASE
            WHEN r.run_device_values IS NOT NULL
             AND r.run_device_values <> ''
                THEN 1 ELSE 0
        END AS run_device_evidence_flag,

        CASE
            WHEN (
                    r.run_device_values IS NULL
                    OR r.run_device_values = ''
                 )
             AND (
                    (
                        r.run_standard_mode_values IS NOT NULL
                        AND r.run_standard_mode_values <> ''
                    )
                    OR
                    (
                        r.run_hamilton_mode_values IS NOT NULL
                        AND r.run_hamilton_mode_values <> ''
                    )
                 )
                THEN 1 ELSE 0
        END AS mode_only_run_flag

    FROM outcomes o

    LEFT JOIN invasive_interval_runs r
        ON r.index_stay_id = o.stay_id
       AND r.source_stay_id = o.first_reintubation_stay_id
       AND o.first_reintubation_time
           >= r.invasive_run_start
       AND o.first_reintubation_time
           < r.invasive_run_end

    WHERE o.first_reintubation_time IS NOT NULL
    """
)

unmapped_run_n = con.execute(
    """
    SELECT COUNT(*)
    FROM reintubation_run_characterization
    WHERE invasive_run_minutes IS NULL
    """
).fetchone()[0]

if unmapped_run_n:
    raise RuntimeError(
        f"Unable to map {unmapped_run_n} reintubation events "
        "to contiguous invasive runs."
    )

con.execute(
    """
    CREATE TABLE reintubation_sensitivity AS
    WITH policies AS (
        SELECT * FROM (
            VALUES
                ('PRIMARY_ANY_STATE_DERIVED', 0),
                ('EXCLUDE_RUN_LT30_MIN', 1),
                ('EXCLUDE_RUN_LT60_MIN', 2),
                ('REQUIRE_DEVICE_EVIDENCE', 3),
                ('EXCLUDE_MODE_ONLY_LT30_MIN', 4),
                ('EXCLUDE_MODE_ONLY_LT60_MIN', 5)
        ) AS t(policy, policy_order)
    ),
    horizons AS (
        SELECT * FROM (
            VALUES (24), (48), (72)
        ) AS t(horizon_hours)
    ),
    e AS (
        SELECT
            p.policy,
            p.policy_order,
            h.horizon_hours,
            r.*,

            CASE
                WHEN r.minutes_extubation_to_reintubation
                     > h.horizon_hours * 60
                    THEN 0

                WHEN p.policy = 'PRIMARY_ANY_STATE_DERIVED'
                    THEN 1

                WHEN p.policy = 'EXCLUDE_RUN_LT30_MIN'
                 AND r.invasive_run_minutes >= 30
                    THEN 1

                WHEN p.policy = 'EXCLUDE_RUN_LT60_MIN'
                 AND r.invasive_run_minutes >= 60
                    THEN 1

                WHEN p.policy = 'REQUIRE_DEVICE_EVIDENCE'
                 AND r.run_device_evidence_flag = 1
                    THEN 1

                WHEN p.policy = 'EXCLUDE_MODE_ONLY_LT30_MIN'
                 AND NOT (
                        r.mode_only_run_flag = 1
                        AND r.invasive_run_minutes < 30
                 )
                    THEN 1

                WHEN p.policy = 'EXCLUDE_MODE_ONLY_LT60_MIN'
                 AND NOT (
                        r.mode_only_run_flag = 1
                        AND r.invasive_run_minutes < 60
                 )
                    THEN 1

                ELSE 0
            END AS retained_event_flag

        FROM policies p
        CROSS JOIN horizons h
        CROSS JOIN reintubation_run_characterization r
    )
    SELECT
        policy_order,
        policy,
        horizon_hours,

        COUNT(*) FILTER (
            WHERE retained_event_flag = 1
        ) AS retained_events_n,

        COUNT(*) FILTER (
            WHERE retained_event_flag = 0
              AND minutes_extubation_to_reintubation
                  <= horizon_hours * 60
        ) AS excluded_events_n,

        round(
            100.0 * COUNT(*) FILTER (
                WHERE retained_event_flag = 1
            ) / 8049.0,
            3
        ) AS retained_events_percent_of_extubated

    FROM e
    GROUP BY
        policy_order,
        policy,
        horizon_hours
    """
)


# =============================================================================
# Blocking validation
# =============================================================================

outcome_rows = con.execute(
    "SELECT COUNT(*) FROM outcomes"
).fetchone()[0]

outcome_stays = con.execute(
    "SELECT COUNT(DISTINCT stay_id) FROM outcomes"
).fetchone()[0]

if outcome_rows != EXPECTED_COHORT_N or outcome_stays != EXPECTED_COHORT_N:
    raise RuntimeError(
        f"Unexpected outcome dimensions: rows={outcome_rows}, "
        f"stays={outcome_stays}."
    )

extubated_n = con.execute(
    """
    SELECT SUM(first_extubation_within_24h_flag)
    FROM outcomes
    """
).fetchone()[0]

if extubated_n != 8049:
    raise RuntimeError(
        f"Unexpected extubation count: {extubated_n}; expected 8049."
    )

reint_48_n = con.execute(
    """
    SELECT SUM(reintubation_within_48h_flag)
    FROM outcomes
    """
).fetchone()[0]

if reint_48_n != 221:
    raise RuntimeError(
        f"Unexpected 48-h reintubation count: {reint_48_n}; expected 221."
    )

invalid_extubation_n = con.execute(
    """
    SELECT COUNT(*)
    FROM outcomes
    WHERE first_extubation_time IS NOT NULL
      AND (
            time_to_extubation_minutes < 0
         OR time_to_extubation_minutes >= 1440
      )
    """
).fetchone()[0]

if invalid_extubation_n:
    raise RuntimeError(
        f"Invalid extubation timing: {invalid_extubation_n}."
    )

invalid_reintubation_n = con.execute(
    """
    SELECT COUNT(*)
    FROM outcomes
    WHERE first_reintubation_time IS NOT NULL
      AND (
            first_extubation_time IS NULL
         OR first_reintubation_time <= first_extubation_time
         OR minutes_extubation_to_reintubation <= 0
         OR minutes_extubation_to_reintubation > 4320
      )
    """
).fetchone()[0]

if invalid_reintubation_n:
    raise RuntimeError(
        f"Invalid reintubation timing: {invalid_reintubation_n}."
    )

status_inconsistency_n = con.execute(
    """
    SELECT COUNT(*)
    FROM outcomes
    WHERE (
        reintubation_within_48h_flag = 1
        AND reintubation_48h_status <> 'EVENT'
    )
    OR (
        reintubation_48h_status = 'EVENT'
        AND reintubation_within_48h_flag <> 1
    )
    """
).fetchone()[0]

if status_inconsistency_n:
    raise RuntimeError(
        f"Inconsistent 48-h status: {status_inconsistency_n}."
    )

sensitivity_rows = con.execute(
    "SELECT COUNT(*) FROM reintubation_sensitivity"
).fetchone()[0]

if sensitivity_rows != 18:
    raise RuntimeError(
        f"Unexpected sensitivity rows: {sensitivity_rows}; expected 18."
    )


# =============================================================================
# Audit output
# =============================================================================

con.execute(
    """
    CREATE TABLE audit (
        audit_level VARCHAR,
        subgroup VARCHAR,
        metric VARCHAR,
        value_num DOUBLE,
        value_text VARCHAR
    )
    """
)

audit_rows = [
    ("DESIGN", None, "status", None, "CANONICAL_FROZEN"),
    (
        "DESIGN", None, "respiratory_definition", None,
        "Exact frozen 01_11 mappings and temporal semantics"
    ),
    (
        "DESIGN", None, "primary_extubation_horizon_hours",
        PRIMARY_EXTUBATION_HORIZON_HOURS, None
    ),
    (
        "DESIGN", None, "primary_reintubation_window_hours",
        PRIMARY_REINTUBATION_HOURS, None
    ),
    (
        "DESIGN", None, "reintubation_sensitivity_hours",
        None, "24,72"
    ),
    (
        "DESIGN", None, "procedureevents_used_for_definition_flag",
        0, "Validation only"
    ),
    (
        "DESIGN", None, "persistence_hours",
        PERSISTENCE_HOURS, None
    ),
    (
        "DESIGN", None, "backward_assignment_minutes",
        BACKWARD_LIMIT_MINUTES, None
    ),
    ("COHORT", None, "cohort_n", EXPECTED_COHORT_N, None),
]

con.executemany(
    "INSERT INTO audit VALUES (?, ?, ?, ?, ?)",
    audit_rows
)

metrics = {
    "extubated_within_24h_n":
        "SELECT SUM(first_extubation_within_24h_flag) FROM outcomes",
    "extubated_within_6h_n":
        "SELECT SUM(extubated_within_6h_flag) FROM outcomes",
    "extubated_within_12h_n":
        "SELECT SUM(extubated_within_12h_flag) FROM outcomes",
    "median_time_to_extubation_minutes":
        """
        SELECT median(time_to_extubation_minutes)
        FROM outcomes
        WHERE first_extubation_within_24h_flag=1
        """,
    "p25_time_to_extubation_minutes":
        """
        SELECT quantile_cont(time_to_extubation_minutes, 0.25)
        FROM outcomes
        WHERE first_extubation_within_24h_flag=1
        """,
    "p75_time_to_extubation_minutes":
        """
        SELECT quantile_cont(time_to_extubation_minutes, 0.75)
        FROM outcomes
        WHERE first_extubation_within_24h_flag=1
        """,
    "reintubation_within_24h_n":
        "SELECT SUM(reintubation_within_24h_flag) FROM outcomes",
    "reintubation_within_48h_n":
        "SELECT SUM(reintubation_within_48h_flag) FROM outcomes",
    "reintubation_within_72h_n":
        "SELECT SUM(reintubation_within_72h_flag) FROM outcomes",
    "hospital_discharge_before_index_icu_end_n":
        """
        SELECT SUM(hospital_discharge_before_index_icu_end_flag)
        FROM outcomes
        """,
    "hospital_discharge_before_any_icu_end_n":
        """
        SELECT SUM(hospital_discharge_before_any_icu_end_flag)
        FROM outcomes
        """,
}

for metric, sql in metrics.items():
    value = con.execute(sql).fetchone()[0]
    con.execute(
        """
        INSERT INTO audit
        VALUES ('OUTCOME', NULL, ?, ?, NULL)
        """,
        [metric, value],
    )

con.execute(
    """
    INSERT INTO audit
    SELECT
        'REINTUBATION_48H_STATUS',
        reintubation_48h_status,
        'stays_n',
        COUNT(*)::DOUBLE,
        NULL
    FROM outcomes
    GROUP BY reintubation_48h_status
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'REINTUBATION_SOURCE',
        coalesce(first_reintubation_source, 'NO_EVENT'),
        'stays_n',
        COUNT(*)::DOUBLE,
        NULL
    FROM outcomes
    GROUP BY first_reintubation_source
    """
)


# =============================================================================
# Export
# =============================================================================

con.execute(
    f"""
    COPY (
        SELECT *
        FROM outcomes
        ORDER BY stay_id
    )
    TO '{qpath(OUT_OUTCOMES)}'
    (FORMAT PARQUET, COMPRESSION ZSTD)
    """
)

con.execute(
    f"""
    COPY (
        SELECT *
        FROM audit
        ORDER BY audit_level, subgroup, metric
    )
    TO '{qpath(OUT_AUDIT)}'
    (HEADER, DELIMITER ',')
    """
)

con.execute(
    f"""
    COPY (
        SELECT *
        FROM reintubation_sensitivity
        ORDER BY policy_order, horizon_hours
    )
    TO '{qpath(OUT_SENSITIVITY)}'
    (HEADER, DELIMITER ',')
    """
)


# =============================================================================
# Console report
# =============================================================================

print("=" * 112)
print("TRAJECTORIES - RESPIRATORY OUTCOMES CANONICAL")
print("=" * 112)
print(f"Cohort                                      : {EXPECTED_COHORT_N}")
print(f"Primary extubation horizon                  : 0-{PRIMARY_EXTUBATION_HORIZON_HOURS} h")
print(f"Primary reintubation window                 : <= {PRIMARY_REINTUBATION_HOURS} h after extubation")
print(f"Maximum respiratory source horizon          : <= {MAX_SOURCE_HORIZON_HOURS} h from t0")
print(f"01_11 persistence cap                       : {PERSISTENCE_HOURS} h")
print(f"01_11 backward assignment                   : <= {BACKWARD_LIMIT_MINUTES} min")
print()

print("First extubation")
print("----------------")
print(
    con.execute(
        """
        SELECT
            SUM(first_extubation_within_24h_flag) AS extubated_24h_n,
            SUM(extubated_within_6h_flag) AS extubated_6h_n,
            SUM(extubated_within_12h_flag) AS extubated_12h_n,
            round(
                median(time_to_extubation_minutes)
                FILTER (WHERE first_extubation_within_24h_flag=1),
                1
            ) AS median_min,
            round(
                quantile_cont(time_to_extubation_minutes, 0.25)
                FILTER (WHERE first_extubation_within_24h_flag=1),
                1
            ) AS p25_min,
            round(
                quantile_cont(time_to_extubation_minutes, 0.75)
                FILTER (WHERE first_extubation_within_24h_flag=1),
                1
            ) AS p75_min
        FROM outcomes
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Primary 48-h reintubation status")
print("---------------------------------")
print(
    con.execute(
        """
        SELECT
            reintubation_48h_status,
            COUNT(*) AS stays_n
        FROM outcomes
        GROUP BY reintubation_48h_status
        ORDER BY stays_n DESC
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Reintubation sensitivity")
print("-------------------------")
print(
    con.execute(
        """
        SELECT
            policy,
            horizon_hours,
            retained_events_n,
            excluded_events_n,
            retained_events_percent_of_extubated
        FROM reintubation_sensitivity
        ORDER BY policy_order, horizon_hours
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Canonical outputs")
print("-----------------")
print(OUT_OUTCOMES)
print(OUT_AUDIT)
print(OUT_SENSITIVITY)
print()
print("Canonical respiratory outcomes extraction completed successfully.")
print("Component 01_14 is canonical and frozen.")
