"""
01_09_extract_creatinine_outcomes.py

Trajectories project
Creatinine-defined renal outcomes - canonical production pipeline

Purpose
-------
Creatinine is treated as an admission/ICU-stay biochemical renal outcome, not as an
hourly or six-hour trajectory. The canonical pipeline extracts the authoritative
serum chemistry source, defines an observed preoperative baseline, summarizes
early postoperative creatinine, and derives creatinine-defined AKI outcomes
during in-hospital follow-up through postoperative day 7.

Frozen methodological decisions
--------------------------------
Authoritative source:
    MIMIC-IV LABEVENTS itemid 50912 (Creatinine, blood chemistry)

Excluded auxiliary sources:
    LABEVENTS whole-blood creatinine items and ICU CHARTEVENTS creatinine
    displays are not merged into the canonical series. Source-closure audits
    demonstrated near-complete duplication for serum CHARTEVENTS and negligible
    incremental patient coverage from auxiliary whole-blood sources.

Technical validity:
    0 < creatinine <= 150 mg/dL

Primary observed baseline:
    Closest LABEVENTS 50912 value in:
        [postoperative_time_zero - 30 days,
         postoperative_time_zero - 6 hours)

    Baseline source categories:
        WITHIN_7D_EXCLUDING_FINAL_6H
        FALLBACK_8_TO_30D_EXCLUDING_FINAL_6H
        MISSING

Rationale:
    Values in the final six hours before postoperative time zero may represent
    perioperative or post-cardiopulmonary-bypass sampling. A 30-day observed
    lookback retains nearly complete empirical coverage without relying on
    estimated creatinine or very remote historical measurements.

Sensitivity baselines:
    - closest observed value within 7 days, including the final 6 hours;
    - closest observed value within 365 days, excluding the final 6 hours;
    - lowest observed value within 7 days.

Postoperative follow-up:
    From postoperative_time_zero to the earlier of hospital discharge and
    postoperative day 7.

Temporal semantics:
    Creatinine is a point laboratory measurement at charttime.
    No LOCF, interpolation, extrapolation, or right-boundary anchor is used.

Creatinine-defined AKI:
    Absolute criterion:
        increase >= 0.3 mg/dL from baseline within 48 hours
    Relative criterion:
        increase to >= 1.5 times baseline within 7 days

    The pipeline also derives the first qualifying measurement time and an
    incident-after-24-hours outcome for prognostic analyses based on predictors
    constructed during [t0, t0 + 24 h).

Creatinine-only stage:
    Stage 1:
        absolute criterion or ratio 1.5-1.9
    Stage 2:
        ratio 2.0-2.9
    Stage 3:
        ratio >=3.0 or creatinine >=4.0 mg/dL with an acute increase >=0.3

    Renal replacement therapy is not incorporated in this component. The stage
    variable is therefore explicitly named creatinine-only.

Canonical outputs
-----------------
data/01_09_creatinine_measurements.parquet
data/01_09_creatinine_renal_outcomes.parquet
data/01_09_creatinine_extraction_audit.csv

Inputs
------
data/00_04_final_analytic_cohort.csv
Raw MIMIC-IV 3.1 files relative to MIMIC_ROOT:
  hosp/admissions.csv.gz
  hosp/labevents.csv.gz
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
ADMISSIONS_FILE = MIMIC_ROOT / "hosp" / "admissions.csv.gz"
LABEVENTS_FILE = MIMIC_ROOT / "hosp" / "labevents.csv.gz"

OUT_MEASUREMENTS = (
    PROJECT_ROOT / "data" / "01_09_creatinine_measurements.parquet"
)
OUT_OUTCOMES = (
    PROJECT_ROOT / "data" / "01_09_creatinine_renal_outcomes.parquet"
)
OUT_AUDIT = (
    PROJECT_ROOT / "data" / "01_09_creatinine_extraction_audit.csv"
)

EXPECTED_COHORT_N = 9305
CREATININE_ITEMID = 50912

TECHNICAL_MIN_EXCLUSIVE = 0.0
TECHNICAL_MAX_MG_DL = 150.0

PRIMARY_BASELINE_LOOKBACK_DAYS = 30
SENSITIVITY_BASELINE_LOOKBACK_DAYS = 365
BASELINE_EXCLUSION_HOURS = 6
OUTCOME_FOLLOWUP_DAYS = 7


def qpath(path: Path) -> str:
    return str(path).replace("'", "''")


for required in [COHORT_FILE, ADMISSIONS_FILE, LABEVENTS_FILE]:
    if not required.exists():
        raise FileNotFoundError(f"Required file not found: {required}")

OUT_MEASUREMENTS.parent.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(database=":memory:")
con.execute("PRAGMA threads=4")


# =============================================================================
# Frozen cohort and hospital follow-up
# =============================================================================

con.execute(
    f"""
    CREATE TABLE cohort_raw AS
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

con.execute(
    f"""
    CREATE TABLE admissions_selected AS
    SELECT
        try_cast(hadm_id AS BIGINT) AS hadm_id,
        try_cast(admittime AS TIMESTAMP) AS admittime,
        try_cast(dischtime AS TIMESTAMP) AS dischtime
    FROM read_csv_auto(
        '{qpath(ADMISSIONS_FILE)}',
        header=true,
        all_varchar=true
    )
    WHERE try_cast(hadm_id AS BIGINT) IS NOT NULL
    """
)

con.execute(
    f"""
    CREATE TABLE cohort AS
    SELECT
        c.*,
        a.admittime,
        a.dischtime,
        c.postoperative_time_zero + INTERVAL '24 hours'
            AS predictor_end_24h,
        c.postoperative_time_zero + INTERVAL '48 hours'
            AS endpoint_48h,
        least(
            coalesce(
                a.dischtime,
                c.postoperative_time_zero
                    + INTERVAL '{OUTCOME_FOLLOWUP_DAYS} days'
            ),
            c.postoperative_time_zero
                + INTERVAL '{OUTCOME_FOLLOWUP_DAYS} days'
        ) AS endpoint_7d_observed
    FROM cohort_raw c
    LEFT JOIN admissions_selected a
        ON c.hadm_id = a.hadm_id
    """
)

(
    cohort_n,
    unique_stays_n,
    missing_t0_n,
    missing_dischtime_n,
    invalid_followup_n,
) = con.execute(
    """
    SELECT
        COUNT(*),
        COUNT(DISTINCT stay_id),
        COUNT(*) FILTER (WHERE postoperative_time_zero IS NULL),
        COUNT(*) FILTER (WHERE dischtime IS NULL),
        COUNT(*) FILTER (
            WHERE endpoint_7d_observed < postoperative_time_zero
        )
    FROM cohort
    """
).fetchone()

if cohort_n != EXPECTED_COHORT_N or unique_stays_n != EXPECTED_COHORT_N:
    raise RuntimeError(
        f"Unexpected cohort size: {cohort_n} rows / {unique_stays_n} stays; "
        f"expected {EXPECTED_COHORT_N}."
    )

if missing_t0_n or invalid_followup_n:
    raise RuntimeError(
        "Invalid cohort timing: "
        f"missing_t0={missing_t0_n}, invalid_followup={invalid_followup_n}."
    )

con.execute(
    """
    CREATE TEMP TABLE cohort_subjects AS
    SELECT DISTINCT subject_id
    FROM cohort
    """
)


# =============================================================================
# Canonical LABEVENTS source
# =============================================================================

con.execute(
    f"""
    CREATE TABLE creatinine_raw AS
    SELECT
        c.subject_id,
        c.hadm_id,
        c.stay_id,
        c.postoperative_time_zero,
        c.predictor_end_24h,
        c.endpoint_48h,
        c.endpoint_7d_observed,
        try_cast(le.labevent_id AS BIGINT) AS labevent_id,
        try_cast(le.charttime AS TIMESTAMP) AS charttime,
        try_cast(le.storetime AS TIMESTAMP) AS storetime,
        le.value,
        try_cast(le.valuenum AS DOUBLE) AS valuenum_raw,
        le.valueuom,
        le.flag,
        le.comments,
        CASE
            WHEN lower(trim(coalesce(le.valueuom, ''))) IN (
                'mg/dl', 'mg/dl.'
            )
                THEN try_cast(le.valuenum AS DOUBLE)
            WHEN lower(trim(coalesce(le.valueuom, ''))) IN (
                'umol/l', 'µmol/l', 'μmol/l', 'micromol/l'
            )
                THEN try_cast(le.valuenum AS DOUBLE) / 88.4
            ELSE NULL
        END AS creatinine_mg_dl,
        CASE
            WHEN lower(trim(coalesce(le.valueuom, ''))) IN (
                'mg/dl', 'mg/dl.'
            ) THEN 'MG_DL'
            WHEN lower(trim(coalesce(le.valueuom, ''))) IN (
                'umol/l', 'µmol/l', 'μmol/l', 'micromol/l'
            ) THEN 'UMOL_L_CONVERTED'
            ELSE 'UNRESOLVED_UNIT'
        END AS unit_resolution
    FROM read_csv_auto(
        '{qpath(LABEVENTS_FILE)}',
        header=true,
        all_varchar=true
    ) le
    INNER JOIN cohort_subjects cs
        ON try_cast(le.subject_id AS BIGINT) = cs.subject_id
    INNER JOIN cohort c
        ON try_cast(le.subject_id AS BIGINT) = c.subject_id
       AND try_cast(le.itemid AS BIGINT) = {CREATININE_ITEMID}
       AND try_cast(le.charttime AS TIMESTAMP)
            >= c.postoperative_time_zero
                - INTERVAL '{SENSITIVITY_BASELINE_LOOKBACK_DAYS} days'
       AND try_cast(le.charttime AS TIMESTAMP)
            < c.endpoint_7d_observed
    """
)

con.execute(
    """
    CREATE TABLE creatinine_numeric AS
    SELECT *
    FROM creatinine_raw
    WHERE valuenum_raw IS NOT NULL
    """
)

con.execute(
    f"""
    CREATE TABLE creatinine_valid AS
    SELECT *
    FROM creatinine_raw
    WHERE creatinine_mg_dl > {TECHNICAL_MIN_EXCLUSIVE}
      AND creatinine_mg_dl <= {TECHNICAL_MAX_MG_DL}
    """
)

con.execute(
    """
    CREATE TABLE creatinine_points AS
    SELECT
        stay_id,
        charttime,
        median(creatinine_mg_dl) AS creatinine_mg_dl,
        COUNT(*) AS source_rows_n,
        MIN(storetime) AS first_storetime,
        MAX(storetime) AS last_storetime,
        MIN(unit_resolution) AS unit_resolution
    FROM creatinine_valid
    GROUP BY stay_id, charttime
    """
)


# =============================================================================
# Observed baseline definitions
# =============================================================================

con.execute(
    f"""
    CREATE TABLE baseline_candidates AS
    SELECT
        c.stay_id,

        b7any.creatinine_mg_dl AS baseline_closest_7d_any,
        b7any.charttime AS baseline_closest_7d_any_time,

        b7e.creatinine_mg_dl AS baseline_closest_7d_excluding_6h,
        b7e.charttime AS baseline_closest_7d_excluding_6h_time,

        b30e.creatinine_mg_dl AS baseline_primary_30d_excluding_6h,
        b30e.charttime AS baseline_primary_30d_excluding_6h_time,

        b365e.creatinine_mg_dl AS baseline_sensitivity_365d_excluding_6h,
        b365e.charttime AS baseline_sensitivity_365d_excluding_6h_time,

        bmin7.creatinine_mg_dl AS baseline_lowest_7d,
        bmin7.charttime AS baseline_lowest_7d_time,

        x6.creatinine_mg_dl AS closest_within_final_6h,
        x6.charttime AS closest_within_final_6h_time,

        CASE
            WHEN b30e.creatinine_mg_dl IS NULL THEN 'MISSING'
            WHEN b30e.charttime
                 >= c.postoperative_time_zero - INTERVAL '7 days'
                THEN 'WITHIN_7D_EXCLUDING_FINAL_6H'
            ELSE 'FALLBACK_8_TO_30D_EXCLUDING_FINAL_6H'
        END AS baseline_primary_source_window

    FROM cohort c

    LEFT JOIN LATERAL (
        SELECT p.creatinine_mg_dl, p.charttime
        FROM creatinine_points p
        WHERE p.stay_id = c.stay_id
          AND p.charttime >= c.postoperative_time_zero - INTERVAL '7 days'
          AND p.charttime < c.postoperative_time_zero
        ORDER BY p.charttime DESC
        LIMIT 1
    ) b7any ON true

    LEFT JOIN LATERAL (
        SELECT p.creatinine_mg_dl, p.charttime
        FROM creatinine_points p
        WHERE p.stay_id = c.stay_id
          AND p.charttime >= c.postoperative_time_zero - INTERVAL '7 days'
          AND p.charttime
                < c.postoperative_time_zero
                    - INTERVAL '{BASELINE_EXCLUSION_HOURS} hours'
        ORDER BY p.charttime DESC
        LIMIT 1
    ) b7e ON true

    LEFT JOIN LATERAL (
        SELECT p.creatinine_mg_dl, p.charttime
        FROM creatinine_points p
        WHERE p.stay_id = c.stay_id
          AND p.charttime
                >= c.postoperative_time_zero
                    - INTERVAL '{PRIMARY_BASELINE_LOOKBACK_DAYS} days'
          AND p.charttime
                < c.postoperative_time_zero
                    - INTERVAL '{BASELINE_EXCLUSION_HOURS} hours'
        ORDER BY p.charttime DESC
        LIMIT 1
    ) b30e ON true

    LEFT JOIN LATERAL (
        SELECT p.creatinine_mg_dl, p.charttime
        FROM creatinine_points p
        WHERE p.stay_id = c.stay_id
          AND p.charttime
                >= c.postoperative_time_zero
                    - INTERVAL '{SENSITIVITY_BASELINE_LOOKBACK_DAYS} days'
          AND p.charttime
                < c.postoperative_time_zero
                    - INTERVAL '{BASELINE_EXCLUSION_HOURS} hours'
        ORDER BY p.charttime DESC
        LIMIT 1
    ) b365e ON true

    LEFT JOIN LATERAL (
        SELECT p.creatinine_mg_dl, p.charttime
        FROM creatinine_points p
        WHERE p.stay_id = c.stay_id
          AND p.charttime >= c.postoperative_time_zero - INTERVAL '7 days'
          AND p.charttime < c.postoperative_time_zero
        ORDER BY p.creatinine_mg_dl, p.charttime DESC
        LIMIT 1
    ) bmin7 ON true

    LEFT JOIN LATERAL (
        SELECT p.creatinine_mg_dl, p.charttime
        FROM creatinine_points p
        WHERE p.stay_id = c.stay_id
          AND p.charttime
                >= c.postoperative_time_zero
                    - INTERVAL '{BASELINE_EXCLUSION_HOURS} hours'
          AND p.charttime < c.postoperative_time_zero
        ORDER BY p.charttime DESC
        LIMIT 1
    ) x6 ON true
    """
)


# =============================================================================
# Postoperative measurement summaries
# =============================================================================

con.execute(
    """
    CREATE TABLE postoperative_summary AS
    SELECT
        c.stay_id,

        COUNT(p.charttime) FILTER (
            WHERE p.charttime >= c.postoperative_time_zero
              AND p.charttime < least(c.predictor_end_24h, c.endpoint_7d_observed)
        ) AS creatinine_points_0_24h_n,

        COUNT(p.charttime) FILTER (
            WHERE p.charttime >= c.predictor_end_24h
              AND p.charttime < least(c.endpoint_48h, c.endpoint_7d_observed)
        ) AS creatinine_points_24_48h_n,

        COUNT(p.charttime) FILTER (
            WHERE p.charttime >= c.postoperative_time_zero
              AND p.charttime < least(c.endpoint_48h, c.endpoint_7d_observed)
        ) AS creatinine_points_0_48h_n,

        COUNT(p.charttime) FILTER (
            WHERE p.charttime >= c.postoperative_time_zero
              AND p.charttime < c.endpoint_7d_observed
        ) AS creatinine_points_0_7d_n,

        COUNT(p.charttime) FILTER (
            WHERE p.charttime >= c.predictor_end_24h
              AND p.charttime < c.endpoint_7d_observed
        ) AS creatinine_points_after_24h_7d_n,

        first(p.creatinine_mg_dl ORDER BY p.charttime) FILTER (
            WHERE p.charttime >= c.postoperative_time_zero
              AND p.charttime < least(c.predictor_end_24h, c.endpoint_7d_observed)
        ) AS creatinine_first_0_24h,

        first(p.charttime ORDER BY p.charttime) FILTER (
            WHERE p.charttime >= c.postoperative_time_zero
              AND p.charttime < least(c.predictor_end_24h, c.endpoint_7d_observed)
        ) AS creatinine_first_0_24h_time,

        min(p.creatinine_mg_dl) FILTER (
            WHERE p.charttime >= c.postoperative_time_zero
              AND p.charttime < least(c.predictor_end_24h, c.endpoint_7d_observed)
        ) AS creatinine_min_0_24h,

        max(p.creatinine_mg_dl) FILTER (
            WHERE p.charttime >= c.postoperative_time_zero
              AND p.charttime < least(c.predictor_end_24h, c.endpoint_7d_observed)
        ) AS creatinine_max_0_24h,

        first(p.charttime ORDER BY p.creatinine_mg_dl DESC, p.charttime) FILTER (
            WHERE p.charttime >= c.postoperative_time_zero
              AND p.charttime < least(c.predictor_end_24h, c.endpoint_7d_observed)
        ) AS creatinine_max_0_24h_time,

        max(p.creatinine_mg_dl) FILTER (
            WHERE p.charttime >= c.postoperative_time_zero
              AND p.charttime < least(c.endpoint_48h, c.endpoint_7d_observed)
        ) AS creatinine_max_0_48h,

        first(p.charttime ORDER BY p.creatinine_mg_dl DESC, p.charttime) FILTER (
            WHERE p.charttime >= c.postoperative_time_zero
              AND p.charttime < least(c.endpoint_48h, c.endpoint_7d_observed)
        ) AS creatinine_max_0_48h_time,

        max(p.creatinine_mg_dl) FILTER (
            WHERE p.charttime >= c.postoperative_time_zero
              AND p.charttime < c.endpoint_7d_observed
        ) AS creatinine_max_0_7d,

        first(p.charttime ORDER BY p.creatinine_mg_dl DESC, p.charttime) FILTER (
            WHERE p.charttime >= c.postoperative_time_zero
              AND p.charttime < c.endpoint_7d_observed
        ) AS creatinine_max_0_7d_time,

        max(p.creatinine_mg_dl) FILTER (
            WHERE p.charttime >= c.predictor_end_24h
              AND p.charttime < c.endpoint_7d_observed
        ) AS creatinine_max_after_24h_7d,

        first(p.charttime ORDER BY p.creatinine_mg_dl DESC, p.charttime) FILTER (
            WHERE p.charttime >= c.predictor_end_24h
              AND p.charttime < c.endpoint_7d_observed
        ) AS creatinine_max_after_24h_7d_time

    FROM cohort c
    LEFT JOIN creatinine_points p
        ON p.stay_id = c.stay_id
       AND p.charttime >= c.postoperative_time_zero
       AND p.charttime < c.endpoint_7d_observed
    GROUP BY c.stay_id
    """
)


# =============================================================================
# First threshold-crossing times under the primary baseline
# =============================================================================

con.execute(
    """
    CREATE TABLE aki_threshold_times AS
    SELECT
        c.stay_id,
        x.charttime AS creatinine_aki_first_qualifying_time,
        x.creatinine_mg_dl AS creatinine_aki_first_qualifying_value,
        x.qualifying_rule AS creatinine_aki_first_qualifying_rule
    FROM cohort c
    INNER JOIN baseline_candidates b
        ON b.stay_id = c.stay_id
    LEFT JOIN LATERAL (
        SELECT
            p.charttime,
            p.creatinine_mg_dl,
            CASE
                WHEN p.charttime < least(c.endpoint_48h, c.endpoint_7d_observed)
                 AND p.creatinine_mg_dl
                        - b.baseline_primary_30d_excluding_6h >= 0.3
                 AND p.creatinine_mg_dl
                        >= 1.5 * b.baseline_primary_30d_excluding_6h
                    THEN 'ABSOLUTE_AND_RELATIVE'
                WHEN p.charttime < least(c.endpoint_48h, c.endpoint_7d_observed)
                 AND p.creatinine_mg_dl
                        - b.baseline_primary_30d_excluding_6h >= 0.3
                    THEN 'ABSOLUTE_0_3_WITHIN_48H'
                ELSE 'RELATIVE_1_5_WITHIN_7D'
            END AS qualifying_rule
        FROM creatinine_points p
        WHERE p.stay_id = c.stay_id
          AND b.baseline_primary_30d_excluding_6h IS NOT NULL
          AND p.charttime >= c.postoperative_time_zero
          AND p.charttime < c.endpoint_7d_observed
          AND (
                (
                    p.charttime
                        < least(c.endpoint_48h, c.endpoint_7d_observed)
                    AND p.creatinine_mg_dl
                        - b.baseline_primary_30d_excluding_6h >= 0.3
                )
                OR p.creatinine_mg_dl
                    >= 1.5 * b.baseline_primary_30d_excluding_6h
          )
        ORDER BY p.charttime
        LIMIT 1
    ) x ON true
    """
)


# =============================================================================
# Canonical patient-level output
# =============================================================================

con.execute(
    """
    CREATE TABLE patient_output AS
    SELECT
        c.subject_id,
        c.hadm_id,
        c.stay_id,
        c.surgery_group,
        c.sex,
        c.age_at_icu_admission,
        c.postoperative_time_zero,
        c.predictor_end_24h,
        c.endpoint_48h,
        c.endpoint_7d_observed,
        c.dischtime,

        b.baseline_primary_30d_excluding_6h
            AS creatinine_baseline_mg_dl,
        b.baseline_primary_30d_excluding_6h_time
            AS creatinine_baseline_charttime,
        b.baseline_primary_source_window
            AS creatinine_baseline_source_window,

        date_diff(
            'minute',
            b.baseline_primary_30d_excluding_6h_time,
            c.postoperative_time_zero
        ) / 60.0 AS creatinine_baseline_age_hours,

        CASE
            WHEN b.baseline_primary_30d_excluding_6h IS NOT NULL THEN 1
            ELSE 0
        END AS creatinine_baseline_available_flag,

        b.baseline_closest_7d_any
            AS creatinine_baseline_closest_7d_any_mg_dl,
        b.baseline_closest_7d_any_time
            AS creatinine_baseline_closest_7d_any_charttime,

        b.baseline_closest_7d_excluding_6h
            AS creatinine_baseline_closest_7d_excluding_6h_mg_dl,
        b.baseline_closest_7d_excluding_6h_time
            AS creatinine_baseline_closest_7d_excluding_6h_charttime,

        b.baseline_sensitivity_365d_excluding_6h
            AS creatinine_baseline_sensitivity_365d_mg_dl,
        b.baseline_sensitivity_365d_excluding_6h_time
            AS creatinine_baseline_sensitivity_365d_charttime,

        b.baseline_lowest_7d
            AS creatinine_baseline_lowest_7d_mg_dl,
        b.baseline_lowest_7d_time
            AS creatinine_baseline_lowest_7d_charttime,

        b.closest_within_final_6h
            AS creatinine_final_6h_pre_t0_mg_dl,
        b.closest_within_final_6h_time
            AS creatinine_final_6h_pre_t0_charttime,

        p.creatinine_points_0_24h_n,
        p.creatinine_points_24_48h_n,
        p.creatinine_points_0_48h_n,
        p.creatinine_points_0_7d_n,
        p.creatinine_points_after_24h_7d_n,

        p.creatinine_first_0_24h,
        p.creatinine_first_0_24h_time,
        p.creatinine_min_0_24h,
        p.creatinine_max_0_24h,
        p.creatinine_max_0_24h_time,
        p.creatinine_max_0_48h,
        p.creatinine_max_0_48h_time,
        p.creatinine_max_0_7d,
        p.creatinine_max_0_7d_time,
        p.creatinine_max_after_24h_7d,
        p.creatinine_max_after_24h_7d_time,

        CASE
            WHEN b.baseline_primary_30d_excluding_6h IS NULL
              OR p.creatinine_first_0_24h IS NULL THEN NULL
            ELSE p.creatinine_first_0_24h
                - b.baseline_primary_30d_excluding_6h
        END AS creatinine_delta_first_0_24h_from_baseline,

        CASE
            WHEN b.baseline_primary_30d_excluding_6h IS NULL
              OR p.creatinine_first_0_24h IS NULL THEN NULL
            ELSE p.creatinine_first_0_24h
                / b.baseline_primary_30d_excluding_6h
        END AS creatinine_ratio_first_0_24h_to_baseline,

        CASE
            WHEN b.baseline_primary_30d_excluding_6h IS NULL
              OR p.creatinine_max_0_24h IS NULL THEN NULL
            ELSE p.creatinine_max_0_24h
                - b.baseline_primary_30d_excluding_6h
        END AS creatinine_delta_max_0_24h_from_baseline,

        CASE
            WHEN b.baseline_primary_30d_excluding_6h IS NULL
              OR p.creatinine_max_0_48h IS NULL THEN NULL
            ELSE p.creatinine_max_0_48h
                - b.baseline_primary_30d_excluding_6h
        END AS creatinine_delta_max_0_48h_from_baseline,

        CASE
            WHEN b.baseline_primary_30d_excluding_6h IS NULL
              OR p.creatinine_max_0_7d IS NULL THEN NULL
            ELSE p.creatinine_max_0_7d
                - b.baseline_primary_30d_excluding_6h
        END AS creatinine_delta_max_0_7d_from_baseline,

        CASE
            WHEN b.baseline_primary_30d_excluding_6h IS NULL
              OR p.creatinine_max_0_7d IS NULL THEN NULL
            ELSE p.creatinine_max_0_7d
                / b.baseline_primary_30d_excluding_6h
        END AS creatinine_ratio_max_0_7d_to_baseline,

        CASE
            WHEN b.baseline_primary_30d_excluding_6h IS NULL
              OR p.creatinine_first_0_24h IS NULL THEN NULL
            WHEN p.creatinine_first_0_24h
                    <= 0.9 * b.baseline_primary_30d_excluding_6h THEN 1
            ELSE 0
        END AS creatinine_first_decrease_ge10pct_flag,

        CASE
            WHEN b.baseline_primary_30d_excluding_6h IS NULL
              OR p.creatinine_first_0_24h IS NULL THEN NULL
            WHEN p.creatinine_first_0_24h
                    >= 1.1 * b.baseline_primary_30d_excluding_6h THEN 1
            ELSE 0
        END AS creatinine_first_increase_ge10pct_flag,

        CASE
            WHEN b.baseline_primary_30d_excluding_6h IS NULL
              OR p.creatinine_max_0_48h IS NULL THEN NULL
            WHEN p.creatinine_max_0_48h
                    - b.baseline_primary_30d_excluding_6h >= 0.3 THEN 1
            ELSE 0
        END AS creatinine_aki_absolute_48h_flag,

        CASE
            WHEN b.baseline_primary_30d_excluding_6h IS NULL
              OR p.creatinine_max_0_7d IS NULL THEN NULL
            WHEN p.creatinine_max_0_7d
                    >= 1.5 * b.baseline_primary_30d_excluding_6h THEN 1
            ELSE 0
        END AS creatinine_aki_relative_7d_flag,

        CASE
            WHEN b.baseline_primary_30d_excluding_6h IS NULL
              OR p.creatinine_max_0_7d IS NULL THEN NULL
            WHEN (
                p.creatinine_max_0_48h
                    - b.baseline_primary_30d_excluding_6h >= 0.3
                OR p.creatinine_max_0_7d
                    >= 1.5 * b.baseline_primary_30d_excluding_6h
            ) THEN 1
            ELSE 0
        END AS creatinine_aki_any_7d_flag,

        CASE
            WHEN b.baseline_primary_30d_excluding_6h IS NULL
              OR p.creatinine_max_0_24h IS NULL THEN NULL
            WHEN (
                p.creatinine_max_0_24h
                    - b.baseline_primary_30d_excluding_6h >= 0.3
                OR p.creatinine_max_0_24h
                    >= 1.5 * b.baseline_primary_30d_excluding_6h
            ) THEN 1
            ELSE 0
        END AS creatinine_aki_by_24h_flag,

        t.creatinine_aki_first_qualifying_time,
        t.creatinine_aki_first_qualifying_value,
        t.creatinine_aki_first_qualifying_rule,

        CASE
            WHEN t.creatinine_aki_first_qualifying_time IS NULL THEN NULL
            ELSE date_diff(
                'minute',
                c.postoperative_time_zero,
                t.creatinine_aki_first_qualifying_time
            ) / 60.0
        END AS creatinine_aki_first_qualifying_hours_from_t0,

        CASE
            WHEN b.baseline_primary_30d_excluding_6h IS NULL
              OR p.creatinine_points_after_24h_7d_n = 0 THEN NULL
            WHEN t.creatinine_aki_first_qualifying_time
                    >= c.predictor_end_24h THEN 1
            ELSE 0
        END AS creatinine_aki_incident_after_24h_flag,

        CASE
            WHEN b.baseline_primary_30d_excluding_6h IS NULL
              OR p.creatinine_max_0_7d IS NULL THEN NULL

            -- Stage assignment is conditional on first satisfying the
            -- creatinine-defined AKI diagnosis. Reaching 4.0 mg/dL alone,
            -- without either an acute >=0.3 mg/dL increase within 48 hours
            -- or a >=1.5-fold increase within 7 days, is not sufficient to
            -- create an AKI diagnosis from a chronically high baseline.
            WHEN NOT (
                p.creatinine_max_0_48h
                    - b.baseline_primary_30d_excluding_6h >= 0.3
                OR p.creatinine_max_0_7d
                    >= 1.5 * b.baseline_primary_30d_excluding_6h
            ) THEN 0

            WHEN (
                p.creatinine_max_0_7d
                    >= 3.0 * b.baseline_primary_30d_excluding_6h
                OR p.creatinine_max_0_7d >= 4.0
            ) THEN 3

            WHEN p.creatinine_max_0_7d
                    >= 2.0 * b.baseline_primary_30d_excluding_6h THEN 2

            ELSE 1
        END AS creatinine_aki_stage_7d_creatinine_only,

        CASE
            WHEN b.baseline_primary_30d_excluding_6h IS NOT NULL
             AND p.creatinine_points_0_7d_n > 0 THEN 1
            ELSE 0
        END AS creatinine_outcome_evaluable_flag,

        date_diff(
            'minute',
            c.postoperative_time_zero,
            c.endpoint_7d_observed
        ) / 1440.0 AS creatinine_observed_followup_days,

        CASE
            WHEN c.dischtime >= c.endpoint_48h THEN 1 ELSE 0
        END AS creatinine_complete_48h_hospital_followup_flag,

        CASE
            WHEN c.dischtime
                    >= c.postoperative_time_zero + INTERVAL '7 days'
                THEN 1
            ELSE 0
        END AS creatinine_complete_7d_hospital_followup_flag,

        CASE
            WHEN b.baseline_sensitivity_365d_excluding_6h IS NULL
              OR p.creatinine_max_0_7d IS NULL THEN NULL
            WHEN (
                p.creatinine_max_0_48h
                    - b.baseline_sensitivity_365d_excluding_6h >= 0.3
                OR p.creatinine_max_0_7d
                    >= 1.5 * b.baseline_sensitivity_365d_excluding_6h
            ) THEN 1
            ELSE 0
        END AS creatinine_aki_any_7d_sensitivity_365d_flag,

        CASE
            WHEN b.baseline_closest_7d_any IS NULL
              OR p.creatinine_max_0_7d IS NULL THEN NULL
            WHEN (
                p.creatinine_max_0_48h
                    - b.baseline_closest_7d_any >= 0.3
                OR p.creatinine_max_0_7d
                    >= 1.5 * b.baseline_closest_7d_any
            ) THEN 1
            ELSE 0
        END AS creatinine_aki_any_7d_sensitivity_closest_7d_any_flag,

        CASE
            WHEN b.baseline_lowest_7d IS NULL
              OR p.creatinine_max_0_7d IS NULL THEN NULL
            WHEN (
                p.creatinine_max_0_48h - b.baseline_lowest_7d >= 0.3
                OR p.creatinine_max_0_7d >= 1.5 * b.baseline_lowest_7d
            ) THEN 1
            ELSE 0
        END AS creatinine_aki_any_7d_sensitivity_lowest_7d_flag

    FROM cohort c
    INNER JOIN baseline_candidates b
        ON b.stay_id = c.stay_id
    INNER JOIN postoperative_summary p
        ON p.stay_id = c.stay_id
    INNER JOIN aki_threshold_times t
        ON t.stay_id = c.stay_id
    """
)


# =============================================================================
# Canonical measurement-level output
# =============================================================================

con.execute(
    """
    CREATE TABLE measurement_output AS
    SELECT
        c.subject_id,
        c.hadm_id,
        c.stay_id,
        p.charttime,
        p.first_storetime,
        p.last_storetime,
        p.creatinine_mg_dl,
        p.source_rows_n,
        p.unit_resolution,
        date_diff(
            'minute',
            c.postoperative_time_zero,
            p.charttime
        ) / 60.0 AS hours_from_postoperative_time_zero,

        CASE
            WHEN p.charttime < c.postoperative_time_zero
                THEN 'PREOPERATIVE_LOOKBACK'
            WHEN p.charttime < least(c.predictor_end_24h, c.endpoint_7d_observed)
                THEN 'POSTOP_H00_H24'
            WHEN p.charttime < least(c.endpoint_48h, c.endpoint_7d_observed)
                THEN 'POSTOP_H24_H48'
            ELSE 'POSTOP_H48_DAY7'
        END AS measurement_phase,

        CASE
            WHEN p.charttime = b.baseline_primary_30d_excluding_6h_time THEN 1
            ELSE 0
        END AS primary_baseline_measurement_flag,

        CASE
            WHEN p.charttime
                    = b.baseline_sensitivity_365d_excluding_6h_time THEN 1
            ELSE 0
        END AS sensitivity_365d_baseline_measurement_flag,

        CASE
            WHEN p.charttime >= c.postoperative_time_zero THEN 1
            ELSE 0
        END AS postoperative_outcome_measurement_flag

    FROM creatinine_points p
    INNER JOIN cohort c
        ON c.stay_id = p.stay_id
    INNER JOIN baseline_candidates b
        ON b.stay_id = p.stay_id
    """
)


# =============================================================================
# Validation invariants
# =============================================================================

patient_rows_n, patient_unique_n = con.execute(
    """
    SELECT COUNT(*), COUNT(DISTINCT stay_id)
    FROM patient_output
    """
).fetchone()

if patient_rows_n != EXPECTED_COHORT_N or patient_unique_n != EXPECTED_COHORT_N:
    raise RuntimeError(
        f"Unexpected patient output: {patient_rows_n} rows / "
        f"{patient_unique_n} unique stays."
    )

invalid_primary_baseline_n = con.execute(
    f"""
    SELECT COUNT(*)
    FROM patient_output
    WHERE creatinine_baseline_charttime IS NOT NULL
      AND (
            creatinine_baseline_charttime
                >= postoperative_time_zero
                    - INTERVAL '{BASELINE_EXCLUSION_HOURS} hours'
            OR creatinine_baseline_charttime
                < postoperative_time_zero
                    - INTERVAL '{PRIMARY_BASELINE_LOOKBACK_DAYS} days'
      )
    """
).fetchone()[0]

if invalid_primary_baseline_n:
    raise RuntimeError(
        f"{invalid_primary_baseline_n} primary baseline values violate "
        "the frozen 30-day / final-6-hour rule."
    )

false_evaluable_n = con.execute(
    """
    SELECT COUNT(*)
    FROM patient_output
    WHERE creatinine_outcome_evaluable_flag = 1
      AND (
            creatinine_baseline_mg_dl IS NULL
            OR creatinine_points_0_7d_n = 0
      )
    """
).fetchone()[0]

if false_evaluable_n:
    raise RuntimeError(
        f"{false_evaluable_n} stays are falsely marked outcome-evaluable."
    )

stage_flag_mismatch_n = con.execute(
    """
    SELECT COUNT(*)
    FROM patient_output
    WHERE (
        creatinine_aki_stage_7d_creatinine_only > 0
        AND creatinine_aki_any_7d_flag <> 1
    )
    OR (
        creatinine_aki_stage_7d_creatinine_only = 0
        AND creatinine_aki_any_7d_flag <> 0
    )
    """
).fetchone()[0]

if stage_flag_mismatch_n:
    raise RuntimeError(
        f"{stage_flag_mismatch_n} stays have inconsistent AKI flag/stage."
    )

invalid_first_aki_time_n = con.execute(
    """
    SELECT COUNT(*)
    FROM patient_output
    WHERE creatinine_aki_first_qualifying_time IS NOT NULL
      AND (
            creatinine_aki_first_qualifying_time
                < postoperative_time_zero
            OR creatinine_aki_first_qualifying_time
                >= endpoint_7d_observed
      )
    """
).fetchone()[0]

if invalid_first_aki_time_n:
    raise RuntimeError(
        f"{invalid_first_aki_time_n} first AKI times are outside follow-up."
    )

measurement_points_n = con.execute(
    "SELECT COUNT(*) FROM creatinine_points"
).fetchone()[0]
measurement_output_n = con.execute(
    "SELECT COUNT(*) FROM measurement_output"
).fetchone()[0]

if measurement_points_n != measurement_output_n:
    raise RuntimeError(
        "Measurement output does not reconstruct canonical point count: "
        f"{measurement_output_n} vs {measurement_points_n}."
    )


# =============================================================================
# Audit
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

raw_rows_n = con.execute("SELECT COUNT(*) FROM creatinine_raw").fetchone()[0]
numeric_rows_n = con.execute(
    "SELECT COUNT(*) FROM creatinine_numeric"
).fetchone()[0]
valid_rows_n = con.execute("SELECT COUNT(*) FROM creatinine_valid").fetchone()[0]
invalid_rows_n = raw_rows_n - valid_rows_n
canonical_points_n = con.execute(
    "SELECT COUNT(*) FROM creatinine_points"
).fetchone()[0]
canonical_patients_n = con.execute(
    "SELECT COUNT(DISTINCT stay_id) FROM creatinine_points"
).fetchone()[0]

con.executemany(
    "INSERT INTO audit VALUES (?, ?, ?, ?, ?)",
    [
        ("OVERALL", None, "cohort_n", cohort_n, None),
        (
            "OVERALL", None, "missing_hospital_dischtime_n",
            missing_dischtime_n, None
        ),
        ("SOURCE", "LABEVENTS_50912", "raw_rows_n", raw_rows_n, None),
        ("SOURCE", "LABEVENTS_50912", "numeric_rows_n", numeric_rows_n, None),
        ("SOURCE", "LABEVENTS_50912", "valid_rows_n", valid_rows_n, None),
        (
            "SOURCE", "LABEVENTS_50912", "invalid_or_unresolved_rows_n",
            invalid_rows_n, None
        ),
        (
            "SOURCE", "LABEVENTS_50912", "consolidated_timestamps_n",
            canonical_points_n, None
        ),
        (
            "SOURCE", "LABEVENTS_50912", "patients_with_any_point_n",
            canonical_patients_n, None
        ),
        (
            "TECHNICAL_RULE", None, "creatinine_min_exclusive_mg_dl",
            TECHNICAL_MIN_EXCLUSIVE, "technical, not clinical"
        ),
        (
            "TECHNICAL_RULE", None, "creatinine_max_mg_dl",
            TECHNICAL_MAX_MG_DL, "technical, not clinical"
        ),
    ],
)

# Baseline source and age.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'BASELINE_PRIMARY',
        creatinine_baseline_source_window,
        'patients_n',
        COUNT(*)::DOUBLE,
        NULL
    FROM patient_output
    GROUP BY creatinine_baseline_source_window
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'BASELINE_PRIMARY',
        NULL,
        metric,
        value_num,
        NULL
    FROM (
        SELECT
            'available_n' AS metric,
            COUNT(*) FILTER (
                WHERE creatinine_baseline_available_flag = 1
            )::DOUBLE AS value_num
        FROM patient_output

        UNION ALL

        SELECT
            'missing_n',
            COUNT(*) FILTER (
                WHERE creatinine_baseline_available_flag = 0
            )::DOUBLE
        FROM patient_output

        UNION ALL

        SELECT
            'median_age_hours',
            median(creatinine_baseline_age_hours)::DOUBLE
        FROM patient_output
        WHERE creatinine_baseline_available_flag = 1

        UNION ALL

        SELECT
            'p90_age_hours',
            quantile_cont(creatinine_baseline_age_hours, 0.90)::DOUBLE
        FROM patient_output
        WHERE creatinine_baseline_available_flag = 1

        UNION ALL

        SELECT
            'median_baseline_mg_dl',
            median(creatinine_baseline_mg_dl)::DOUBLE
        FROM patient_output
        WHERE creatinine_baseline_available_flag = 1
    )
    """
)

# Baseline sensitivity coverage.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'BASELINE_SENSITIVITY',
        NULL,
        metric,
        value_num,
        NULL
    FROM (
        SELECT
            'closest_7d_any_available_n' AS metric,
            COUNT(*) FILTER (
                WHERE creatinine_baseline_closest_7d_any_mg_dl IS NOT NULL
            )::DOUBLE AS value_num
        FROM patient_output

        UNION ALL

        SELECT
            'closest_7d_excluding_6h_available_n',
            COUNT(*) FILTER (
                WHERE creatinine_baseline_closest_7d_excluding_6h_mg_dl
                    IS NOT NULL
            )::DOUBLE
        FROM patient_output

        UNION ALL

        SELECT
            'sensitivity_365d_available_n',
            COUNT(*) FILTER (
                WHERE creatinine_baseline_sensitivity_365d_mg_dl IS NOT NULL
            )::DOUBLE
        FROM patient_output

        UNION ALL

        SELECT
            'lowest_7d_available_n',
            COUNT(*) FILTER (
                WHERE creatinine_baseline_lowest_7d_mg_dl IS NOT NULL
            )::DOUBLE
        FROM patient_output
    )
    """
)

# Measurement density.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'POSTOPERATIVE_MEASUREMENT',
        NULL,
        metric,
        value_num,
        NULL
    FROM (
        SELECT
            'measured_0_24h_n' AS metric,
            COUNT(*) FILTER (
                WHERE creatinine_points_0_24h_n > 0
            )::DOUBLE AS value_num
        FROM patient_output

        UNION ALL

        SELECT
            'measured_24_48h_n',
            COUNT(*) FILTER (
                WHERE creatinine_points_24_48h_n > 0
            )::DOUBLE
        FROM patient_output

        UNION ALL

        SELECT
            'measured_0_7d_n',
            COUNT(*) FILTER (
                WHERE creatinine_points_0_7d_n > 0
            )::DOUBLE
        FROM patient_output

        UNION ALL

        SELECT
            'median_points_0_24h',
            median(creatinine_points_0_24h_n)::DOUBLE
        FROM patient_output

        UNION ALL

        SELECT
            'median_points_0_48h',
            median(creatinine_points_0_48h_n)::DOUBLE
        FROM patient_output

        UNION ALL

        SELECT
            'median_points_0_7d',
            median(creatinine_points_0_7d_n)::DOUBLE
        FROM patient_output
    )
    """
)

# Primary outcomes.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'CREATININE_OUTCOME',
        NULL,
        metric,
        value_num,
        NULL
    FROM (
        SELECT
            'evaluable_n' AS metric,
            COUNT(*) FILTER (
                WHERE creatinine_outcome_evaluable_flag = 1
            )::DOUBLE AS value_num
        FROM patient_output

        UNION ALL

        SELECT
            'aki_absolute_48h_n',
            COUNT(*) FILTER (
                WHERE creatinine_aki_absolute_48h_flag = 1
            )::DOUBLE
        FROM patient_output

        UNION ALL

        SELECT
            'aki_relative_7d_n',
            COUNT(*) FILTER (
                WHERE creatinine_aki_relative_7d_flag = 1
            )::DOUBLE
        FROM patient_output

        UNION ALL

        SELECT
            'aki_any_7d_n',
            COUNT(*) FILTER (
                WHERE creatinine_aki_any_7d_flag = 1
            )::DOUBLE
        FROM patient_output

        UNION ALL

        SELECT
            'aki_by_24h_n',
            COUNT(*) FILTER (
                WHERE creatinine_aki_by_24h_flag = 1
            )::DOUBLE
        FROM patient_output

        UNION ALL

        SELECT
            'aki_incident_after_24h_n',
            COUNT(*) FILTER (
                WHERE creatinine_aki_incident_after_24h_flag = 1
            )::DOUBLE
        FROM patient_output

        UNION ALL

        SELECT
            'first_creatinine_decrease_ge10pct_n',
            COUNT(*) FILTER (
                WHERE creatinine_first_decrease_ge10pct_flag = 1
            )::DOUBLE
        FROM patient_output

        UNION ALL

        SELECT
            'first_creatinine_increase_ge10pct_n',
            COUNT(*) FILTER (
                WHERE creatinine_first_increase_ge10pct_flag = 1
            )::DOUBLE
        FROM patient_output

        UNION ALL

        SELECT
            'median_max_0_7d_mg_dl',
            median(creatinine_max_0_7d)::DOUBLE
        FROM patient_output
        WHERE creatinine_max_0_7d IS NOT NULL
    )
    """
)

# Stage distribution.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'CREATININE_STAGE',
        cast(creatinine_aki_stage_7d_creatinine_only AS VARCHAR),
        'patients_n',
        COUNT(*)::DOUBLE,
        'creatinine-only; RRT not incorporated'
    FROM patient_output
    WHERE creatinine_aki_stage_7d_creatinine_only IS NOT NULL
    GROUP BY creatinine_aki_stage_7d_creatinine_only
    """
)

# Sensitivity outcome counts.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'OUTCOME_SENSITIVITY',
        NULL,
        metric,
        value_num,
        NULL
    FROM (
        SELECT
            'aki_any_7d_primary_30d_n' AS metric,
            COUNT(*) FILTER (
                WHERE creatinine_aki_any_7d_flag = 1
            )::DOUBLE AS value_num
        FROM patient_output

        UNION ALL

        SELECT
            'aki_any_7d_365d_n',
            COUNT(*) FILTER (
                WHERE creatinine_aki_any_7d_sensitivity_365d_flag = 1
            )::DOUBLE
        FROM patient_output

        UNION ALL

        SELECT
            'aki_any_7d_closest_7d_any_n',
            COUNT(*) FILTER (
                WHERE creatinine_aki_any_7d_sensitivity_closest_7d_any_flag = 1
            )::DOUBLE
        FROM patient_output

        UNION ALL

        SELECT
            'aki_any_7d_lowest_7d_n',
            COUNT(*) FILTER (
                WHERE creatinine_aki_any_7d_sensitivity_lowest_7d_flag = 1
            )::DOUBLE
        FROM patient_output
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
        FROM measurement_output
        ORDER BY stay_id, charttime
    )
    TO '{qpath(OUT_MEASUREMENTS)}'
    (FORMAT PARQUET, COMPRESSION ZSTD)
    """
)

con.execute(
    f"""
    COPY (
        SELECT *
        FROM patient_output
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
        ORDER BY
            CASE audit_level
                WHEN 'OVERALL' THEN 1
                WHEN 'SOURCE' THEN 2
                WHEN 'TECHNICAL_RULE' THEN 3
                WHEN 'BASELINE_PRIMARY' THEN 4
                WHEN 'BASELINE_SENSITIVITY' THEN 5
                WHEN 'POSTOPERATIVE_MEASUREMENT' THEN 6
                WHEN 'CREATININE_OUTCOME' THEN 7
                WHEN 'CREATININE_STAGE' THEN 8
                WHEN 'OUTCOME_SENSITIVITY' THEN 9
                ELSE 99
            END,
            subgroup NULLS FIRST,
            metric
    )
    TO '{qpath(OUT_AUDIT)}'
    (HEADER, DELIMITER ',')
    """
)


# =============================================================================
# Console report
# =============================================================================

print("=" * 104)
print("TRAJECTORIES - CREATININE-DEFINED RENAL OUTCOMES")
print("=" * 104)
print(f"Cohort rows / unique stays            : {cohort_n} / {unique_stays_n}")
print(f"Missing hospital discharge time       : {missing_dischtime_n}")
print(f"Raw LABEVENTS 50912 mapped rows        : {raw_rows_n}")
print(f"Numeric mapped rows                    : {numeric_rows_n}")
print(f"Valid mapped rows                      : {valid_rows_n}")
print(f"Invalid/unresolved mapped rows         : {invalid_rows_n}")
print(f"Consolidated creatinine timestamps     : {canonical_points_n}")
print(f"Patients with any creatinine point     : {canonical_patients_n}")
print()

print("Primary observed baseline")
print("-------------------------")
print(
    con.execute(
        """
        SELECT
            creatinine_baseline_source_window,
            COUNT(*) AS patients_n,
            round(
                median(creatinine_baseline_age_hours) / 24.0,
                2
            ) AS median_age_days,
            round(
                median(creatinine_baseline_mg_dl),
                2
            ) AS median_baseline_mg_dl
        FROM patient_output
        GROUP BY creatinine_baseline_source_window
        ORDER BY
            CASE creatinine_baseline_source_window
                WHEN 'WITHIN_7D_EXCLUDING_FINAL_6H' THEN 1
                WHEN 'FALLBACK_8_TO_30D_EXCLUDING_FINAL_6H' THEN 2
                ELSE 3
            END
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Postoperative measurement density")
print("---------------------------------")
print(
    con.execute(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE creatinine_points_0_24h_n > 0
            ) AS measured_0_24h_n,
            COUNT(*) FILTER (
                WHERE creatinine_points_24_48h_n > 0
            ) AS measured_24_48h_n,
            COUNT(*) FILTER (
                WHERE creatinine_points_0_7d_n > 0
            ) AS measured_0_7d_n,
            median(creatinine_points_0_24h_n) AS median_points_0_24h,
            median(creatinine_points_0_48h_n) AS median_points_0_48h,
            median(creatinine_points_0_7d_n) AS median_points_0_7d
        FROM patient_output
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Creatinine-defined AKI outcomes")
print("-------------------------------")
print(
    con.execute(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE creatinine_outcome_evaluable_flag = 1
            ) AS evaluable_n,
            COUNT(*) FILTER (
                WHERE creatinine_aki_absolute_48h_flag = 1
            ) AS abs_ge0_3_48h_n,
            COUNT(*) FILTER (
                WHERE creatinine_aki_relative_7d_flag = 1
            ) AS ratio_ge1_5_7d_n,
            COUNT(*) FILTER (
                WHERE creatinine_aki_any_7d_flag = 1
            ) AS any_aki_7d_n,
            round(
                100.0 * COUNT(*) FILTER (
                    WHERE creatinine_aki_any_7d_flag = 1
                ) / NULLIF(
                    COUNT(*) FILTER (
                        WHERE creatinine_aki_any_7d_flag IS NOT NULL
                    ),
                    0
                ),
                2
            ) AS any_aki_7d_pct,
            COUNT(*) FILTER (
                WHERE creatinine_aki_by_24h_flag = 1
            ) AS aki_by_24h_n,
            COUNT(*) FILTER (
                WHERE creatinine_aki_incident_after_24h_flag = 1
            ) AS incident_after_24h_n
        FROM patient_output
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Creatinine-only AKI stage")
print("-------------------------")
print(
    con.execute(
        """
        SELECT
            creatinine_aki_stage_7d_creatinine_only AS stage,
            COUNT(*) AS patients_n
        FROM patient_output
        WHERE creatinine_aki_stage_7d_creatinine_only IS NOT NULL
        GROUP BY creatinine_aki_stage_7d_creatinine_only
        ORDER BY stage
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Baseline sensitivity")
print("--------------------")
print(
    con.execute(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE creatinine_baseline_mg_dl IS NOT NULL
            ) AS primary_30d_evaluable_n,
            COUNT(*) FILTER (
                WHERE creatinine_aki_any_7d_flag = 1
            ) AS primary_30d_aki_n,
            COUNT(*) FILTER (
                WHERE creatinine_baseline_sensitivity_365d_mg_dl IS NOT NULL
            ) AS sensitivity_365d_evaluable_n,
            COUNT(*) FILTER (
                WHERE creatinine_aki_any_7d_sensitivity_365d_flag = 1
            ) AS sensitivity_365d_aki_n,
            COUNT(*) FILTER (
                WHERE creatinine_aki_any_7d_sensitivity_closest_7d_any_flag = 1
            ) AS closest_7d_any_aki_n,
            COUNT(*) FILTER (
                WHERE creatinine_aki_any_7d_sensitivity_lowest_7d_flag = 1
            ) AS lowest_7d_aki_n
        FROM patient_output
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Canonical outputs")
print("-----------------")
print(OUT_MEASUREMENTS)
print(OUT_OUTCOMES)
print(OUT_AUDIT)
print()
print("Canonical creatinine outcome extraction complete.")
print("Creatinine is represented as a patient-level renal outcome, not a trajectory.")
print("Primary baseline: closest observed value within 30 days, excluding final 6 h.")
print("No baseline imputation, LOCF, interpolation, or extrapolation was used.")
print("AKI stage is creatinine-only; renal replacement therapy is not incorporated.")
