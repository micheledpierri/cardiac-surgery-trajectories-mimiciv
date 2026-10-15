"""
01_04_extract_glucose.py

Trajectories project
Postoperative glucose component - canonical production pipeline

Purpose
-------
Construct the frozen method-aware postoperative glucose trajectory after
source and method-effect validation.

Frozen source architecture
-----------------------------
Accepted laboratory sources:
    LABEVENTS 50809 - Glucose, Blood / Blood Gas
    LABEVENTS 50931 - Glucose, Blood / Chemistry

Accepted point-of-care sources:
    CHARTEVENTS 225664 - Glucose finger stick
    CHARTEVENTS 226537 - Glucose (whole blood)

Excluded source:
    CHARTEVENTS 220621 - Glucose (serum)

Rationale:
- 220621 behaves overwhelmingly as a duplicate display of LABEVENTS 50931.
- 226537 duplicates LABEVENTS 50809 when recorded at the same timestamp, but
  also contributes thousands of independent POC timestamps; it is therefore
  retained as a POC source rather than treated as a pure display copy.

Exact-timestamp hierarchy
-------------------------
One candidate glucose point is created per stay_id + charttime.

1. If one or more LABEVENTS glucose values are present:
       use the median of LABEVENTS 50809/50931 values;
       ignore simultaneous CHARTEVENTS glucose values.

2. Otherwise, if CHARTEVENTS 225664 is present:
       use 225664.

3. Otherwise:
       use CHARTEVENTS 226537.

This prevents duplicate laboratory display values and prevents averaging
laboratory and POC glucose at the same timestamp.

Technical cleaning
------------------
- non-numeric values are excluded;
- glucose <= 0 mg/dL is excluded as technically invalid;
- glucose > 3000 mg/dL is excluded as grossly corrupted/sentinel-like.

No positive lower physiological cutoff is imposed. Very low positive glucose
values are retained because extreme hypoglycemia can be biologically real.

The upper bound is deliberately far above ordinary postoperative glucose
values and is a technical, not clinical, cutoff. In this cohort it removes
only gross chart artifacts such as 170130 and 999999 while retaining all
clinically plausible positive values observed in the canonical series.

Temporal reconstruction
-----------------------
None:
- no LOCF;
- no interpolation;
- no backward extrapolation;
- no forward extrapolation.

Canonical outputs
-----------------
data/01_04_glucose_hourly_trajectory.parquet
data/01_04_glucose_six_hour_summary.parquet
data/01_04_glucose_extraction_audit.csv

The primary trajectory feature is glucose_median_observed. Glycemic variability
is retained as a secondary feature using the integrated 24-hour coefficient of
variation when at least three valid timestamps are available.

Inputs
------
data/00_04_final_analytic_cohort.csv
Raw MIMIC-IV 3.1 files relative to MIMIC_ROOT:
  hosp/labevents.csv.gz
  icu/chartevents.csv.gz

The first 24 postoperative hours are represented by hourly and six-hour
windows, clipped at observation_end_time.
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
LABEVENTS_FILE = MIMIC_ROOT / "hosp" / "labevents.csv.gz"
CHARTEVENTS_FILE = MIMIC_ROOT / "icu" / "chartevents.csv.gz"

OUT_HOURLY = (
    PROJECT_ROOT / "data" / "01_04_glucose_hourly_trajectory.parquet"
)
OUT_SIX = (
    PROJECT_ROOT / "data" / "01_04_glucose_six_hour_summary.parquet"
)
OUT_AUDIT = (
    PROJECT_ROOT / "data" / "01_04_glucose_extraction_audit.csv"
)

EXPECTED_COHORT_N = 9305

LAB_BG = 50809
LAB_CHEM = 50931
POC_FS = 225664
POC_WB = 226537

TECHNICAL_MIN_EXCLUSIVE = 0.0
TECHNICAL_MAX = 3000.0


# =============================================================================
# Helpers
# =============================================================================

def qpath(path: Path) -> str:
    return str(path).replace("'", "''")


for required in [COHORT_FILE, LABEVENTS_FILE, CHARTEVENTS_FILE]:
    if not required.exists():
        raise FileNotFoundError(f"Required file not found: {required}")

OUT_HOURLY.parent.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(database=":memory:")
con.execute("PRAGMA threads=4")


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
        COUNT(*),
        COUNT(DISTINCT stay_id),
        COUNT(*) FILTER (WHERE postoperative_time_zero IS NULL),
        COUNT(*) FILTER (WHERE observation_end_time IS NULL),
        COUNT(*) FILTER (
            WHERE observation_end_time < postoperative_time_zero
        )
    FROM cohort
    """
).fetchone()

(
    cohort_n,
    unique_stays_n,
    missing_t0_n,
    missing_end_n,
    invalid_order_n,
) = cohort_check

if cohort_n != EXPECTED_COHORT_N or unique_stays_n != EXPECTED_COHORT_N:
    raise RuntimeError(
        f"Unexpected cohort size: {cohort_n} rows / {unique_stays_n} stays."
    )

if missing_t0_n or missing_end_n or invalid_order_n:
    raise RuntimeError(
        "Invalid cohort timing fields: "
        f"missing_t0={missing_t0_n}, missing_end={missing_end_n}, "
        f"invalid_order={invalid_order_n}."
    )


# =============================================================================
# Raw selected sources
# =============================================================================

con.execute(
    f"""
    CREATE TABLE lab_raw AS
    SELECT
        c.subject_id,
        c.hadm_id,
        c.stay_id,
        le.labevent_id,
        le.itemid,
        le.charttime,
        le.storetime,
        le.value,
        le.valuenum,
        le.valueuom,
        le.flag,
        le.comments,
        CASE
            WHEN le.itemid = {LAB_BG} THEN 'LAB_50809_BG'
            WHEN le.itemid = {LAB_CHEM} THEN 'LAB_50931_CHEM'
        END AS source_name
    FROM read_csv_auto(
        '{qpath(LABEVENTS_FILE)}',
        header=true,
        all_varchar=false
    ) le
    INNER JOIN cohort c
        ON le.subject_id = c.subject_id
       AND le.itemid IN ({LAB_BG}, {LAB_CHEM})
       AND le.charttime >= c.postoperative_time_zero
       AND le.charttime < c.postoperative_time_zero + INTERVAL '24 hours'
       AND le.charttime <= c.observation_end_time
    """
)

con.execute(
    f"""
    CREATE TABLE poc_raw AS
    SELECT
        c.subject_id,
        c.hadm_id,
        c.stay_id,
        ce.itemid,
        ce.charttime,
        ce.storetime,
        ce.value,
        ce.valuenum,
        ce.valueuom,
        ce.warning,
        CASE
            WHEN ce.itemid = {POC_FS}
                THEN 'POC_225664_FINGERSTICK'
            WHEN ce.itemid = {POC_WB}
                THEN 'POC_226537_WHOLE_BLOOD'
        END AS source_name
    FROM read_csv_auto(
        '{qpath(CHARTEVENTS_FILE)}',
        header=true,
        all_varchar=false
    ) ce
    INNER JOIN cohort c
        ON ce.stay_id = c.stay_id
       AND ce.itemid IN ({POC_FS}, {POC_WB})
       AND ce.charttime >= c.postoperative_time_zero
       AND ce.charttime < c.postoperative_time_zero + INTERVAL '24 hours'
       AND ce.charttime <= c.observation_end_time
    """
)


# =============================================================================
# Numeric and technically valid source rows
# =============================================================================

con.execute(
    """
    CREATE TABLE all_selected_numeric AS

    SELECT
        subject_id,
        hadm_id,
        stay_id,
        source_name,
        itemid,
        charttime,
        storetime,
        value,
        valuenum,
        valueuom,
        NULL::INTEGER AS warning,
        flag,
        comments,
        'LAB' AS method_family
    FROM lab_raw
    WHERE valuenum IS NOT NULL

    UNION ALL

    SELECT
        subject_id,
        hadm_id,
        stay_id,
        source_name,
        itemid,
        charttime,
        storetime,
        value,
        valuenum,
        valueuom,
        warning,
        NULL::VARCHAR AS flag,
        NULL::VARCHAR AS comments,
        'POC' AS method_family
    FROM poc_raw
    WHERE valuenum IS NOT NULL
    """
)

con.execute(
    f"""
    CREATE TABLE valid_rows AS
    SELECT *
    FROM all_selected_numeric
    WHERE valuenum > {TECHNICAL_MIN_EXCLUSIVE}
      AND valuenum <= {TECHNICAL_MAX}
    """
)


# =============================================================================
# Source-level consolidation
# =============================================================================

con.execute(
    """
    CREATE TABLE source_points AS
    SELECT
        stay_id,
        source_name,
        method_family,
        charttime,
        median(valuenum) AS glucose_value,
        COUNT(*) AS raw_rows_n,
        MIN(valuenum) AS min_raw_value,
        MAX(valuenum) AS max_raw_value
    FROM valid_rows
    GROUP BY stay_id, source_name, method_family, charttime
    """
)


# =============================================================================
# Exact-timestamp integrated point hierarchy
# =============================================================================

con.execute(
    """
    CREATE TABLE integrated_points AS
    WITH per_time AS (
        SELECT
            stay_id,
            charttime,

            COUNT(*) FILTER (
                WHERE method_family = 'LAB'
            ) AS lab_source_points_n,

            COUNT(*) FILTER (
                WHERE source_name = 'POC_225664_FINGERSTICK'
            ) AS fs_source_points_n,

            COUNT(*) FILTER (
                WHERE source_name = 'POC_226537_WHOLE_BLOOD'
            ) AS wb_source_points_n,

            median(glucose_value) FILTER (
                WHERE method_family = 'LAB'
            ) AS lab_value,

            median(glucose_value) FILTER (
                WHERE source_name = 'POC_225664_FINGERSTICK'
            ) AS fs_value,

            median(glucose_value) FILTER (
                WHERE source_name = 'POC_226537_WHOLE_BLOOD'
            ) AS wb_value,

            string_agg(
                source_name,
                ' | '
                ORDER BY source_name
            ) AS available_sources

        FROM source_points
        GROUP BY stay_id, charttime
    )
    SELECT
        stay_id,
        charttime,

        CASE
            WHEN lab_source_points_n > 0 THEN lab_value
            WHEN fs_source_points_n > 0 THEN fs_value
            ELSE wb_value
        END AS glucose_value,

        CASE
            WHEN lab_source_points_n > 0 THEN 'LAB'
            WHEN fs_source_points_n > 0 THEN 'POC_FINGERSTICK'
            ELSE 'POC_WHOLE_BLOOD'
        END AS selected_method,

        CASE
            WHEN lab_source_points_n > 0 THEN
                CASE
                    WHEN lab_source_points_n > 1
                        THEN 'LAB_50809_50931_COMBINED'
                    ELSE 'LAB_SINGLE_SOURCE'
                END
            WHEN fs_source_points_n > 0 THEN 'POC_225664_FINGERSTICK'
            ELSE 'POC_226537_WHOLE_BLOOD'
        END AS selected_source_class,

        lab_source_points_n,
        fs_source_points_n,
        wb_source_points_n,
        available_sources,

        CASE
            WHEN lab_source_points_n > 0
             AND (fs_source_points_n > 0 OR wb_source_points_n > 0)
                THEN 1
            ELSE 0
        END AS lab_poc_same_time_flag,

        CASE
            WHEN lab_source_points_n = 0
             AND fs_source_points_n > 0
             AND wb_source_points_n > 0
                THEN 1
            ELSE 0
        END AS two_poc_sources_same_time_flag

    FROM per_time
    """
)


# =============================================================================
# Patient-level 24-h variability
# =============================================================================

con.execute(
    """
    CREATE TABLE patient_variability AS
    WITH x AS (
        SELECT
            stay_id,
            COUNT(*) AS glucose_timestamps_24h_n,
            AVG(glucose_value) AS glucose_mean_24h,
            median(glucose_value) AS glucose_median_24h,
            stddev_samp(glucose_value) AS glucose_sd_24h,
            MIN(glucose_value) AS glucose_min_24h,
            MAX(glucose_value) AS glucose_max_24h,
            MAX(glucose_value) - MIN(glucose_value)
                AS glucose_range_24h,
            COUNT(*) FILTER (
                WHERE selected_method = 'LAB'
            ) AS glucose_lab_timestamps_24h_n,
            COUNT(*) FILTER (
                WHERE selected_method <> 'LAB'
            ) AS glucose_poc_timestamps_24h_n
        FROM integrated_points
        GROUP BY stay_id
    )
    SELECT
        c.stay_id,
        coalesce(x.glucose_timestamps_24h_n, 0)
            AS glucose_timestamps_24h_n,
        x.glucose_mean_24h,
        x.glucose_median_24h,
        x.glucose_sd_24h,
        CASE
            WHEN x.glucose_timestamps_24h_n >= 3
             AND x.glucose_mean_24h > 0
             AND x.glucose_sd_24h IS NOT NULL
            THEN 100.0 * x.glucose_sd_24h / x.glucose_mean_24h
            ELSE NULL
        END AS glucose_cv_24h_percent,
        x.glucose_min_24h,
        x.glucose_max_24h,
        x.glucose_range_24h,
        coalesce(x.glucose_lab_timestamps_24h_n, 0)
            AS glucose_lab_timestamps_24h_n,
        coalesce(x.glucose_poc_timestamps_24h_n, 0)
            AS glucose_poc_timestamps_24h_n,
        (coalesce(x.glucose_timestamps_24h_n, 0) > 0)
            AS glucose_measured_24h_flag
    FROM cohort c
    LEFT JOIN x
        ON c.stay_id = x.stay_id
    """
)


# =============================================================================
# Window definitions and complete grids
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
create_grid("six_grid", "six_hour_windows")


# =============================================================================
# Window summaries
# =============================================================================

def create_window_output(output_table: str, grid_table: str) -> None:
    con.execute(
        f"""
        CREATE TABLE {output_table} AS

        WITH obs AS (
            SELECT
                g.stay_id,
                g.window_index,

                COUNT(p.charttime) AS glucose_points_n,

                COUNT(p.charttime) FILTER (
                    WHERE p.selected_method = 'LAB'
                ) AS glucose_lab_points_n,

                COUNT(p.charttime) FILTER (
                    WHERE p.selected_method <> 'LAB'
                ) AS glucose_poc_points_n,

                COUNT(p.charttime) FILTER (
                    WHERE p.selected_method = 'POC_FINGERSTICK'
                ) AS glucose_fingerstick_points_n,

                COUNT(p.charttime) FILTER (
                    WHERE p.selected_method = 'POC_WHOLE_BLOOD'
                ) AS glucose_whole_blood_poc_points_n,

                AVG(p.glucose_value) AS glucose_mean_observed,
                median(p.glucose_value) AS glucose_median_observed,
                stddev_samp(p.glucose_value) AS glucose_sd_observed,
                MIN(p.glucose_value) AS glucose_min_observed,
                MAX(p.glucose_value) AS glucose_max_observed,

                first(p.glucose_value ORDER BY p.charttime)
                    AS glucose_first_observed,
                last(p.glucose_value ORDER BY p.charttime)
                    AS glucose_last_observed,

                first(p.charttime ORDER BY p.charttime)
                    AS glucose_first_charttime,
                last(p.charttime ORDER BY p.charttime)
                    AS glucose_last_charttime

            FROM {grid_table} g
            LEFT JOIN integrated_points p
                ON p.stay_id = g.stay_id
               AND g.eligible_minutes > 0
               AND p.charttime >= g.window_start_time
               AND p.charttime < g.eligible_window_end_time

            GROUP BY g.stay_id, g.window_index
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

            pv.glucose_measured_24h_flag,
            pv.glucose_timestamps_24h_n,
            pv.glucose_lab_timestamps_24h_n,
            pv.glucose_poc_timestamps_24h_n,
            pv.glucose_mean_24h,
            pv.glucose_median_24h,
            pv.glucose_sd_24h,
            pv.glucose_cv_24h_percent,
            pv.glucose_min_24h,
            pv.glucose_max_24h,
            pv.glucose_range_24h,

            CASE
                WHEN g.eligible_minutes > 0
                 AND o.glucose_points_n > 0 THEN 1
                WHEN g.eligible_minutes > 0 THEN 0
                ELSE NULL
            END AS glucose_measured_in_window_flag,

            o.glucose_points_n,
            o.glucose_lab_points_n,
            o.glucose_poc_points_n,
            o.glucose_fingerstick_points_n,
            o.glucose_whole_blood_poc_points_n,

            o.glucose_mean_observed,
            o.glucose_median_observed,
            o.glucose_sd_observed,

            CASE
                WHEN o.glucose_points_n >= 3
                 AND o.glucose_mean_observed > 0
                 AND o.glucose_sd_observed IS NOT NULL
                THEN 100.0 * o.glucose_sd_observed
                     / o.glucose_mean_observed
                ELSE NULL
            END AS glucose_cv_observed_percent,

            o.glucose_min_observed,
            o.glucose_max_observed,
            o.glucose_first_observed,
            o.glucose_last_observed,

            CASE
                WHEN o.glucose_points_n >= 2
                THEN o.glucose_last_observed
                     - o.glucose_first_observed
                ELSE NULL
            END AS glucose_delta_last_minus_first_observed,

            CASE
                WHEN o.glucose_points_n >= 2
                THEN o.glucose_max_observed
                     - o.glucose_min_observed
                ELSE NULL
            END AS glucose_range_observed,

            o.glucose_first_charttime,
            o.glucose_last_charttime,

            CASE
                WHEN g.eligible_minutes = 0 THEN 'NOT_OBSERVABLE'
                WHEN o.glucose_points_n = 0 THEN 'OBSERVABLE_NOT_MEASURED'
                WHEN o.glucose_points_n = 1 THEN 'MEASURED_ONE_POINT'
                ELSE 'MEASURED_MULTIPLE_POINTS'
            END AS glucose_measurement_status,

            CASE
                WHEN o.glucose_points_n = 0 THEN NULL
                WHEN o.glucose_lab_points_n > 0
                 AND o.glucose_poc_points_n > 0
                    THEN 'MIXED_LAB_POC'
                WHEN o.glucose_lab_points_n > 0
                    THEN 'LAB_ONLY'
                ELSE 'POC_ONLY'
            END AS glucose_window_method_mix

        FROM {grid_table} g
        INNER JOIN obs o
            ON o.stay_id = g.stay_id
           AND o.window_index = g.window_index
        INNER JOIN patient_variability pv
            ON pv.stay_id = g.stay_id
        """
    )


create_window_output("hourly_output", "hourly_grid")
create_window_output("six_output", "six_grid")


# =============================================================================
# Validation invariants
# =============================================================================

hourly_n = con.execute("SELECT COUNT(*) FROM hourly_output").fetchone()[0]
six_n = con.execute("SELECT COUNT(*) FROM six_output").fetchone()[0]

if hourly_n != EXPECTED_COHORT_N * 24:
    raise RuntimeError(
        f"Unexpected hourly rows: {hourly_n}; "
        f"expected {EXPECTED_COHORT_N * 24}."
    )

if six_n != EXPECTED_COHORT_N * 4:
    raise RuntimeError(
        f"Unexpected six-hour rows: {six_n}; "
        f"expected {EXPECTED_COHORT_N * 4}."
    )

integrated_points_n = con.execute(
    "SELECT COUNT(*) FROM integrated_points"
).fetchone()[0]

hourly_points_sum = con.execute(
    "SELECT SUM(glucose_points_n) FROM hourly_output"
).fetchone()[0]

six_points_sum = con.execute(
    "SELECT SUM(glucose_points_n) FROM six_output"
).fetchone()[0]

if hourly_points_sum != integrated_points_n:
    raise RuntimeError(
        f"Hourly windows reconstruct {hourly_points_sum} points; "
        f"expected {integrated_points_n}."
    )

if six_points_sum != integrated_points_n:
    raise RuntimeError(
        f"Six-hour windows reconstruct {six_points_sum} points; "
        f"expected {integrated_points_n}."
    )


# =============================================================================
# Audit
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

raw_selected_numeric_n = con.execute(
    "SELECT COUNT(*) FROM all_selected_numeric"
).fetchone()[0]

invalid_nonpositive_n = con.execute(
    "SELECT COUNT(*) FROM all_selected_numeric WHERE valuenum <= 0"
).fetchone()[0]

invalid_high_n = con.execute(
    f"""
    SELECT COUNT(*)
    FROM all_selected_numeric
    WHERE valuenum > {TECHNICAL_MAX}
    """
).fetchone()[0]

valid_rows_n = con.execute("SELECT COUNT(*) FROM valid_rows").fetchone()[0]

patients_with_integrated_n = con.execute(
    "SELECT COUNT(DISTINCT stay_id) FROM integrated_points"
).fetchone()[0]

selected_method_counts = con.execute(
    """
    SELECT selected_method, COUNT(*)
    FROM integrated_points
    GROUP BY selected_method
    ORDER BY selected_method
    """
).fetchall()

overall_rows = [
    ("OVERALL", None, None, "cohort_n", cohort_n, None),
    (
        "OVERALL",
        None,
        None,
        "selected_numeric_source_rows_n",
        raw_selected_numeric_n,
        None,
    ),
    (
        "OVERALL",
        None,
        None,
        "excluded_nonpositive_rows_n",
        invalid_nonpositive_n,
        "glucose <= 0 mg/dL",
    ),
    (
        "OVERALL",
        None,
        None,
        "excluded_gross_high_rows_n",
        invalid_high_n,
        f"glucose > {TECHNICAL_MAX} mg/dL",
    ),
    (
        "OVERALL",
        None,
        None,
        "valid_source_rows_n",
        valid_rows_n,
        None,
    ),
    (
        "OVERALL",
        None,
        None,
        "integrated_timestamps_n",
        integrated_points_n,
        None,
    ),
    (
        "OVERALL",
        None,
        None,
        "patients_with_integrated_glucose_n",
        patients_with_integrated_n,
        None,
    ),
    (
        "OVERALL",
        None,
        None,
        "patients_without_integrated_glucose_n",
        EXPECTED_COHORT_N - patients_with_integrated_n,
        None,
    ),
]

con.executemany(
    """
    INSERT INTO audit
    VALUES (?, ?, ?, ?, ?, ?)
    """,
    overall_rows,
)

for method, count_n in selected_method_counts:
    con.execute(
        """
        INSERT INTO audit
        VALUES ('OVERALL', NULL, NULL, ?, ?, NULL)
        """,
        [f"integrated_points_selected_{method.lower()}_n", float(count_n)],
    )

# Same-time source conflicts handled by hierarchy.
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
            'timestamps_with_lab_and_poc_same_time_n' AS metric,
            COUNT(*) FILTER (
                WHERE lab_poc_same_time_flag = 1
            )::DOUBLE AS value_num
        FROM integrated_points

        UNION ALL

        SELECT
            'timestamps_with_two_poc_sources_no_lab_n',
            COUNT(*) FILTER (
                WHERE two_poc_sources_same_time_flag = 1
            )::DOUBLE
        FROM integrated_points
    )
    """
)

# Extreme retained values.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'OVERALL',
        NULL,
        NULL,
        metric,
        value_num,
        'retained; descriptive, not a validity threshold'
    FROM (
        SELECT
            'retained_values_lt20_n' AS metric,
            COUNT(*) FILTER (
                WHERE glucose_value < 20
            )::DOUBLE AS value_num
        FROM integrated_points

        UNION ALL

        SELECT
            'patients_with_retained_value_lt20_n',
            COUNT(DISTINCT stay_id) FILTER (
                WHERE glucose_value < 20
            )::DOUBLE
        FROM integrated_points

        UNION ALL

        SELECT
            'retained_values_gt180_n',
            COUNT(*) FILTER (
                WHERE glucose_value > 180
            )::DOUBLE
        FROM integrated_points
    )
    """
)


def add_window_audit(table_name: str, audit_level: str) -> None:
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
                'measured_patients_n',
                COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND glucose_points_n > 0
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'measured_pct',
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND glucose_points_n > 0
                ) / NULLIF(
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
                'multi_point_windows_n',
                COUNT(*) FILTER (
                    WHERE glucose_points_n >= 2
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'mixed_lab_poc_measured_windows_n',
                COUNT(*) FILTER (
                    WHERE glucose_window_method_mix = 'MIXED_LAB_POC'
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'poc_only_measured_windows_n',
                COUNT(*) FILTER (
                    WHERE glucose_window_method_mix = 'POC_ONLY'
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'lab_only_measured_windows_n',
                COUNT(*) FILTER (
                    WHERE glucose_window_method_mix = 'LAB_ONLY'
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'population_median_window_median',
                median(glucose_median_observed)::DOUBLE
            FROM {table_name}
            WHERE glucose_points_n > 0
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'population_median_window_cv_percent',
                median(glucose_cv_observed_percent)::DOUBLE
            FROM {table_name}
            WHERE glucose_points_n >= 3
            GROUP BY window_index, window_label
        )
        """
    )


add_window_audit("hourly_output", "HOURLY")
add_window_audit("six_output", "SIX_HOUR")

# Patient-level 24-h variability.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'VARIABILITY_24H',
        NULL,
        NULL,
        metric,
        value_num,
        NULL
    FROM (
        SELECT
            'patients_with_at_least_3_points_n' AS metric,
            COUNT(*) FILTER (
                WHERE glucose_timestamps_24h_n >= 3
            )::DOUBLE AS value_num
        FROM patient_variability

        UNION ALL

        SELECT
            'median_cv_percent_ge3',
            median(glucose_cv_24h_percent)::DOUBLE
        FROM patient_variability
        WHERE glucose_timestamps_24h_n >= 3

        UNION ALL

        SELECT
            'p25_cv_percent_ge3',
            quantile_cont(glucose_cv_24h_percent, 0.25)::DOUBLE
        FROM patient_variability
        WHERE glucose_timestamps_24h_n >= 3

        UNION ALL

        SELECT
            'p75_cv_percent_ge3',
            quantile_cont(glucose_cv_24h_percent, 0.75)::DOUBLE
        FROM patient_variability
        WHERE glucose_timestamps_24h_n >= 3

        UNION ALL

        SELECT
            'median_range_mgdl_ge3',
            median(glucose_range_24h)::DOUBLE
        FROM patient_variability
        WHERE glucose_timestamps_24h_n >= 3

        UNION ALL

        SELECT
            'patients_cv_ge20pct_n',
            COUNT(*) FILTER (
                WHERE glucose_timestamps_24h_n >= 3
                  AND glucose_cv_24h_percent >= 20
            )::DOUBLE
        FROM patient_variability
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
        FROM six_output
        ORDER BY stay_id, window_index
    )
    TO '{qpath(OUT_SIX)}'
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
                WHEN 'VARIABILITY_24H' THEN 2
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
# Console report
# =============================================================================

print("=" * 96)
print("TRAJECTORIES - GLUCOSE CANONICAL INTEGRATED TRAJECTORY")
print("=" * 96)
print(f"Cohort rows / unique stays       : {cohort_n} / {unique_stays_n}")
print(f"Selected numeric source rows     : {raw_selected_numeric_n}")
print(f"Excluded glucose <= 0            : {invalid_nonpositive_n}")
print(f"Excluded glucose > {TECHNICAL_MAX:.0f}         : {invalid_high_n}")
print(f"Valid source rows                : {valid_rows_n}")
print(f"Integrated timestamps            : {integrated_points_n}")
print(f"Patients with integrated glucose : {patients_with_integrated_n}")
print(
    f"Patients without integrated      : "
    f"{EXPECTED_COHORT_N - patients_with_integrated_n}"
)
print()

print("Integrated point source selection")
print("---------------------------------")
print(
    con.execute(
        """
        SELECT
            selected_method,
            selected_source_class,
            COUNT(*) AS points_n
        FROM integrated_points
        GROUP BY selected_method, selected_source_class
        ORDER BY selected_method, selected_source_class
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Same-time hierarchy audit")
print("-------------------------")
print(
    con.execute(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE lab_poc_same_time_flag = 1
            ) AS lab_and_poc_same_time_n,
            COUNT(*) FILTER (
                WHERE two_poc_sources_same_time_flag = 1
            ) AS two_poc_sources_no_lab_n
        FROM integrated_points
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Retained positive extreme values")
print("--------------------------------")
print(
    con.execute(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE glucose_value < 20
            ) AS values_lt20_n,
            COUNT(DISTINCT stay_id) FILTER (
                WHERE glucose_value < 20
            ) AS patients_lt20_n,
            MIN(glucose_value) AS minimum_retained,
            MAX(glucose_value) AS maximum_retained
        FROM integrated_points
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Hourly coverage")
print("---------------")
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
                  AND glucose_points_n > 0
            ) AS measured_n,
            round(
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND glucose_points_n > 0
                ) / NULLIF(
                    COUNT(*) FILTER (
                        WHERE eligible_minutes > 0
                    ),
                    0
                ),
                2
            ) AS measured_pct,
            COUNT(*) FILTER (
                WHERE glucose_window_method_mix = 'LAB_ONLY'
            ) AS lab_only_windows_n,
            COUNT(*) FILTER (
                WHERE glucose_window_method_mix = 'POC_ONLY'
            ) AS poc_only_windows_n,
            COUNT(*) FILTER (
                WHERE glucose_window_method_mix = 'MIXED_LAB_POC'
            ) AS mixed_windows_n,
            round(
                median(glucose_median_observed) FILTER (
                    WHERE glucose_points_n > 0
                ),
                1
            ) AS population_median_window_median
        FROM hourly_output
        GROUP BY window_index, window_label
        ORDER BY window_index
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Six-hour coverage")
print("-----------------")
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
                  AND glucose_points_n > 0
            ) AS measured_n,
            round(
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND glucose_points_n > 0
                ) / NULLIF(
                    COUNT(*) FILTER (
                        WHERE eligible_minutes > 0
                    ),
                    0
                ),
                2
            ) AS measured_pct,
            COUNT(*) FILTER (
                WHERE glucose_window_method_mix = 'LAB_ONLY'
            ) AS lab_only_windows_n,
            COUNT(*) FILTER (
                WHERE glucose_window_method_mix = 'POC_ONLY'
            ) AS poc_only_windows_n,
            COUNT(*) FILTER (
                WHERE glucose_window_method_mix = 'MIXED_LAB_POC'
            ) AS mixed_windows_n,
            round(
                median(glucose_median_observed) FILTER (
                    WHERE glucose_points_n > 0
                ),
                1
            ) AS population_median_window_median,
            round(
                median(glucose_cv_observed_percent) FILTER (
                    WHERE glucose_points_n >= 3
                ),
                2
            ) AS median_window_cv_ge3
        FROM six_output
        GROUP BY window_index, window_label
        ORDER BY window_index
        """
    ).fetchdf().to_string(index=False)
)
print()

print("24-hour variability")
print("-------------------")
print(
    con.execute(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE glucose_timestamps_24h_n >= 3
            ) AS patients_ge3_n,
            round(
                median(glucose_cv_24h_percent) FILTER (
                    WHERE glucose_timestamps_24h_n >= 3
                ),
                2
            ) AS median_cv_percent,
            round(
                quantile_cont(
                    glucose_cv_24h_percent,
                    0.25
                ) FILTER (
                    WHERE glucose_timestamps_24h_n >= 3
                ),
                2
            ) AS p25_cv_percent,
            round(
                quantile_cont(
                    glucose_cv_24h_percent,
                    0.75
                ) FILTER (
                    WHERE glucose_timestamps_24h_n >= 3
                ),
                2
            ) AS p75_cv_percent,
            round(
                median(glucose_range_24h) FILTER (
                    WHERE glucose_timestamps_24h_n >= 3
                ),
                1
            ) AS median_range_mgdl,
            COUNT(*) FILTER (
                WHERE glucose_timestamps_24h_n >= 3
                  AND glucose_cv_24h_percent >= 20
            ) AS patients_cv_ge20_n
        FROM patient_variability
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Canonical outputs")
print("-----------------")
print(OUT_HOURLY)
print(OUT_SIX)
print(OUT_AUDIT)
print()
print("Canonical glucose extraction complete.")
print("No LOCF/interpolation was used.")
print("Primary trajectory feature: glucose_median_observed.")
print("Secondary variability feature: glucose_cv_24h_percent (>=3 timestamps).")
