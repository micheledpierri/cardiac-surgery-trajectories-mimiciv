"""
01_03_extract_ph.py

Trajectories project
Postoperative pH component - canonical production pipeline

Purpose
-------
Build the frozen postoperative pH trajectory representation directly from
raw MIMIC-IV LABEVENTS and the definitive analytical cohort.

Frozen methodological decisions
------------------------------------------------
Canonical source:
    hosp/labevents

Blood pH itemid:
    50820 - pH

Specimen context:
    52033 - Specimen Type

Unit:
    units (dimensionless pH scale as stored in MIMIC-IV)

Source policy:
    LABEVENTS only.
    CHARTEVENTS 223830/220274 is NOT used as fallback. Development audits
    showed near-complete duplication of LABEVENTS, only 3 additional patients,
    and technical sentinel values (999999) in CHARTEVENTS. These 3 patients are
    intentionally not recovered in the canonical series in order to preserve a
    single homogeneous laboratory source.

Primary specimen:
    ARTERIAL

Sensitivity specimen representation:
    ALL BLOOD-GAS SPECIMEN CLASSES
    (ARTERIAL, MIXED, CENTRAL_VENOUS, VENOUS, UNKNOWN)

Technical validity range:
    6.0 <= pH <= 8.0

This is deliberately much wider than the clinical reference range. It is used
only to remove technically impossible values. Development data contained one
LABEVENTS value of pH 0.94; all other numeric pH values were between 6.83 and
7.69. Published case reports document survival with arterial pH values below
6.8 and extreme alkalemia around 7.8, so severe clinical values must not be
truncated.

Same patient + same charttime:
    arithmetic mean within each representation.
    No same-specimen duplicate rows were observed in development.

Temporal reconstruction:
    none in the frozen trajectory.
    No LOCF, interpolation, backward extrapolation, or forward extrapolation.

Temporal grids:
    primary structural grid: 24 consecutive 1-hour windows
    secondary grid: four consecutive 6-hour windows

Primary window feature:
    median observed arterial pH

Secondary observed descriptors:
    mean, minimum, maximum, first, last, delta last-minus-first

The primary series is arterial pH. All-blood-specimen summaries are retained
as sensitivity columns in the same canonical outputs. No synthetic values are
generated for unmeasured windows.

Inputs
------
data/00_04_final_analytic_cohort.csv
MIMIC-IV 3.1 raw:
  hosp/d_labitems.csv.gz
  hosp/labevents.csv.gz

Canonical outputs
-----------------------------
data/01_03_ph_hourly_trajectory.parquet
data/01_03_ph_six_hour_summary.parquet
data/01_03_ph_extraction_audit.csv

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

OUT_HOURLY = PROJECT_ROOT / "data" / "01_03_ph_hourly_trajectory.parquet"
OUT_SIX_HOUR = PROJECT_ROOT / "data" / "01_03_ph_six_hour_summary.parquet"
OUT_AUDIT = PROJECT_ROOT / "data" / "01_03_ph_extraction_audit.csv"

EXPECTED_COHORT_N = 9305

PH_ITEMID = 50820
SPECIMEN_ITEMID = 52033

TECHNICAL_PH_MIN = 6.0
TECHNICAL_PH_MAX = 8.0

# Fixed reproduction targets from the validated analysis.
EXPECTED_RAW_PH_ROWS = 50678
EXPECTED_NUMERIC_PH_ROWS = 50660
EXPECTED_NONNUMERIC_PH_ROWS = 18
EXPECTED_PATIENTS_WITH_NUMERIC_PH = 9301
EXPECTED_ARTERIAL_NUMERIC_PATIENTS = 9301
EXPECTED_TECHNICALLY_INVALID_NUMERIC_ROWS = 1
EXPECTED_SAME_SPECIMEN_DUPLICATE_GROUPS = 0


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
    WHERE itemid IN ({PH_ITEMID}, {SPECIMEN_ITEMID})
    ORDER BY itemid
    """
).fetchall()

if len(dictionary_rows) != 2:
    raise RuntimeError(
        f"Expected exactly two dictionary rows for itemids "
        f"{PH_ITEMID} and {SPECIMEN_ITEMID}; found {len(dictionary_rows)}."
    )

dictionary_map = {row[0]: row for row in dictionary_rows}

ph_dict = dictionary_map[PH_ITEMID]

if str(ph_dict[1]).strip().lower() != "ph":
    raise RuntimeError(f"Unexpected pH label: {ph_dict[1]}")

if str(ph_dict[2]).strip().lower() != "blood":
    raise RuntimeError(f"Unexpected pH fluid: {ph_dict[2]}")

if str(ph_dict[3]).strip().lower() != "blood gas":
    raise RuntimeError(f"Unexpected pH category: {ph_dict[3]}")


# =============================================================================
# Single filtered LABEVENTS scan
# =============================================================================

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
        le.ref_range_lower,
        le.ref_range_upper,
        le.flag,
        le.priority,
        le.comments
    FROM read_csv_auto(
        '{qpath(LABEVENTS_FILE)}',
        header=true,
        all_varchar=false
    ) le
    INNER JOIN cohort_subjects cs
        ON le.subject_id = cs.subject_id
    WHERE le.itemid IN ({PH_ITEMID}, {SPECIMEN_ITEMID})
    """
)


# =============================================================================
# Assign postoperative pH rows to frozen observation intervals
# =============================================================================

con.execute(
    f"""
    CREATE TABLE ph_raw AS
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
        le.ref_range_lower,
        le.ref_range_upper,
        le.flag,
        le.priority,
        le.comments,
        CASE
            WHEN le.hadm_id = c.hadm_id THEN 'HADM_ID_MATCH'
            WHEN le.hadm_id IS NULL THEN 'EVENT_HADM_ID_NULL'
            ELSE 'EVENT_HADM_ID_OTHER'
        END AS hadm_linkage_status
    FROM relevant_lab_rows le
    INNER JOIN cohort c
        ON le.subject_id = c.subject_id
       AND le.itemid = {PH_ITEMID}
       AND le.charttime >= c.postoperative_time_zero
       AND le.charttime < c.postoperative_time_zero + INTERVAL '24 hours'
       AND le.charttime <= c.observation_end_time
    """
)

# One LABEVENTS source row must map to at most one cohort window.
join_duplicate_groups = con.execute(
    """
    SELECT COUNT(*)
    FROM (
        SELECT labevent_id, COUNT(*) AS n
        FROM ph_raw
        GROUP BY labevent_id
        HAVING COUNT(*) > 1
    )
    """
).fetchone()[0]

if join_duplicate_groups:
    raise RuntimeError(
        "A LABEVENTS pH row matched more than one cohort postoperative interval. "
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
    CREATE TABLE ph_with_specimen AS
    SELECT
        p.*,
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
    FROM ph_raw p
    LEFT JOIN specimen_context s
        USING (specimen_id)
    """
)


# =============================================================================
# Quality subsets
# =============================================================================

con.execute(
    """
    CREATE TABLE ph_numeric_all AS
    SELECT *
    FROM ph_with_specimen
    WHERE valuenum IS NOT NULL
    """
)

con.execute(
    f"""
    CREATE TABLE ph_valid_all AS
    SELECT *
    FROM ph_numeric_all
    WHERE valuenum >= {TECHNICAL_PH_MIN}
      AND valuenum <= {TECHNICAL_PH_MAX}
      AND specimen_class IN (
          'ARTERIAL',
          'MIXED',
          'CENTRAL_VENOUS',
          'VENOUS',
          'UNKNOWN'
      )
    """
)

con.execute(
    """
    CREATE TABLE ph_valid_arterial AS
    SELECT *
    FROM ph_valid_all
    WHERE specimen_class = 'ARTERIAL'
    """
)


# =============================================================================
# Reproduction and quality checks
# =============================================================================

raw_n, numeric_n, nonnumeric_n = con.execute(
    """
    SELECT
        COUNT(*),
        COUNT(*) FILTER (WHERE valuenum IS NOT NULL),
        COUNT(*) FILTER (WHERE valuenum IS NULL)
    FROM ph_with_specimen
    """
).fetchone()

numeric_patients_n = con.execute(
    """
    SELECT COUNT(DISTINCT cohort_stay_id)
    FROM ph_numeric_all
    """
).fetchone()[0]

arterial_numeric_patients_n = con.execute(
    """
    SELECT COUNT(DISTINCT cohort_stay_id)
    FROM ph_numeric_all
    WHERE specimen_class = 'ARTERIAL'
    """
).fetchone()[0]

technical_invalid_n = con.execute(
    f"""
    SELECT COUNT(*)
    FROM ph_numeric_all
    WHERE valuenum < {TECHNICAL_PH_MIN}
       OR valuenum > {TECHNICAL_PH_MAX}
    """
).fetchone()[0]

bad_hadm_n = con.execute(
    """
    SELECT COUNT(*)
    FROM ph_with_specimen
    WHERE hadm_linkage_status <> 'HADM_ID_MATCH'
    """
).fetchone()[0]

if raw_n != EXPECTED_RAW_PH_ROWS:
    raise RuntimeError(
        f"Raw pH rows changed: observed={raw_n}, expected={EXPECTED_RAW_PH_ROWS}."
    )

if numeric_n != EXPECTED_NUMERIC_PH_ROWS:
    raise RuntimeError(
        f"Numeric pH rows changed: observed={numeric_n}, "
        f"expected={EXPECTED_NUMERIC_PH_ROWS}."
    )

if nonnumeric_n != EXPECTED_NONNUMERIC_PH_ROWS:
    raise RuntimeError(
        f"Non-numeric pH rows changed: observed={nonnumeric_n}, "
        f"expected={EXPECTED_NONNUMERIC_PH_ROWS}."
    )

if numeric_patients_n != EXPECTED_PATIENTS_WITH_NUMERIC_PH:
    raise RuntimeError(
        f"Patients with numeric pH changed: observed={numeric_patients_n}, "
        f"expected={EXPECTED_PATIENTS_WITH_NUMERIC_PH}."
    )

if arterial_numeric_patients_n != EXPECTED_ARTERIAL_NUMERIC_PATIENTS:
    raise RuntimeError(
        f"Patients with arterial numeric pH changed: "
        f"observed={arterial_numeric_patients_n}, "
        f"expected={EXPECTED_ARTERIAL_NUMERIC_PATIENTS}."
    )

if technical_invalid_n != EXPECTED_TECHNICALLY_INVALID_NUMERIC_ROWS:
    raise RuntimeError(
        f"Technically invalid pH rows changed: observed={technical_invalid_n}, "
        f"expected={EXPECTED_TECHNICALLY_INVALID_NUMERIC_ROWS}."
    )

if bad_hadm_n:
    raise RuntimeError(f"Found {bad_hadm_n} pH rows with non-matching HADM.")

unit_rows = con.execute(
    """
    SELECT DISTINCT valueuom
    FROM ph_with_specimen
    ORDER BY valueuom
    """
).fetchall()

if unit_rows != [("units",)]:
    raise RuntimeError(f"Unexpected pH units: {unit_rows}")


# =============================================================================
# Same-timestamp consolidation
# =============================================================================

# All-blood representation.
con.execute(
    """
    CREATE TABLE ph_points_all AS
    SELECT
        cohort_stay_id,
        charttime,
        COUNT(*) AS raw_rows_n,
        COUNT(DISTINCT specimen_id) AS specimen_ids_n,
        COUNT(DISTINCT specimen_class) AS specimen_classes_n,
        AVG(valuenum) AS ph_value,
        MIN(valuenum) AS min_raw_ph,
        MAX(valuenum) AS max_raw_ph,
        MAX(valuenum) - MIN(valuenum) AS same_time_raw_range,
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
        bool_or(specimen_class = 'ARTERIAL') AS has_arterial,
        bool_or(specimen_class <> 'ARTERIAL') AS has_nonarterial,
        string_agg(
            DISTINCT specimen_class,
            ' | '
            ORDER BY specimen_class
        ) AS specimen_classes
    FROM ph_valid_all
    GROUP BY cohort_stay_id, charttime
    """
)

# Primary arterial representation.
con.execute(
    """
    CREATE TABLE ph_points_arterial AS
    SELECT
        cohort_stay_id,
        charttime,
        COUNT(*) AS raw_rows_n,
        COUNT(DISTINCT specimen_id) AS specimen_ids_n,
        AVG(valuenum) AS ph_value,
        MIN(valuenum) AS min_raw_ph,
        MAX(valuenum) AS max_raw_ph,
        MAX(valuenum) - MIN(valuenum) AS same_time_raw_range
    FROM ph_valid_arterial
    GROUP BY cohort_stay_id, charttime
    """
)

# Same-specimen duplicates should remain absent.
same_specimen_duplicate_groups_n = con.execute(
    """
    SELECT COUNT(*)
    FROM (
        SELECT
            cohort_stay_id,
            charttime,
            specimen_id,
            COUNT(*) AS n
        FROM ph_valid_all
        GROUP BY cohort_stay_id, charttime, specimen_id
        HAVING COUNT(*) > 1
    )
    """
).fetchone()[0]

if same_specimen_duplicate_groups_n != EXPECTED_SAME_SPECIMEN_DUPLICATE_GROUPS:
    raise RuntimeError(
        f"Same-specimen duplicate groups changed: "
        f"observed={same_specimen_duplicate_groups_n}, "
        f"expected={EXPECTED_SAME_SPECIMEN_DUPLICATE_GROUPS}."
    )

all_points_n = con.execute(
    "SELECT COUNT(*) FROM ph_points_all"
).fetchone()[0]

arterial_points_n = con.execute(
    "SELECT COUNT(*) FROM ph_points_arterial"
).fetchone()[0]

valid_all_patients_n = con.execute(
    """
    SELECT COUNT(DISTINCT cohort_stay_id)
    FROM ph_points_all
    """
).fetchone()[0]

valid_arterial_patients_n = con.execute(
    """
    SELECT COUNT(DISTINCT cohort_stay_id)
    FROM ph_points_arterial
    """
).fetchone()[0]

if valid_all_patients_n != 9301 or valid_arterial_patients_n != 9301:
    raise RuntimeError(
        "Unexpected patient coverage after technical validity filtering: "
        f"all={valid_all_patients_n}, arterial={valid_arterial_patients_n}."
    )


# =============================================================================
# Patient-level availability
# =============================================================================

con.execute(
    """
    CREATE TABLE patient_ph_availability AS
    WITH a AS (
        SELECT
            cohort_stay_id,
            COUNT(*) AS arterial_timestamps_24h_n,
            MIN(charttime) AS first_arterial_ph_charttime_24h,
            MAX(charttime) AS last_arterial_ph_charttime_24h
        FROM ph_points_arterial
        GROUP BY cohort_stay_id
    ),
    x AS (
        SELECT
            cohort_stay_id,
            COUNT(*) AS allblood_timestamps_24h_n,
            MIN(charttime) AS first_allblood_ph_charttime_24h,
            MAX(charttime) AS last_allblood_ph_charttime_24h
        FROM ph_points_all
        GROUP BY cohort_stay_id
    )
    SELECT
        c.stay_id,
        coalesce(a.arterial_timestamps_24h_n, 0)
            AS ph_arterial_timestamps_24h_n,
        coalesce(x.allblood_timestamps_24h_n, 0)
            AS ph_allblood_timestamps_24h_n,
        a.first_arterial_ph_charttime_24h,
        a.last_arterial_ph_charttime_24h,
        x.first_allblood_ph_charttime_24h,
        x.last_allblood_ph_charttime_24h,
        (coalesce(a.arterial_timestamps_24h_n, 0) > 0)
            AS ph_measured_24h_flag,
        (coalesce(x.allblood_timestamps_24h_n, 0) > 0)
            AS ph_allblood_measured_24h_flag
    FROM cohort c
    LEFT JOIN a
        ON c.stay_id = a.cohort_stay_id
    LEFT JOIN x
        ON c.stay_id = x.cohort_stay_id
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
# Window-level summaries
# =============================================================================

def create_window_output(output_table: str, grid_table: str) -> None:
    con.execute(
        f"""
        CREATE TABLE {output_table} AS

        WITH arterial_obs AS (
            SELECT
                g.stay_id,
                g.window_index,

                COUNT(p.charttime) AS ph_points_n,
                SUM(coalesce(p.raw_rows_n, 0)) AS ph_raw_rows_n,

                AVG(p.ph_value) AS ph_mean_observed,
                median(p.ph_value) AS ph_median_observed,
                stddev_samp(p.ph_value) AS ph_sd_observed,
                MIN(p.ph_value) AS ph_min_observed,
                MAX(p.ph_value) AS ph_max_observed,

                first(p.ph_value ORDER BY p.charttime)
                    AS ph_first_observed,
                last(p.ph_value ORDER BY p.charttime)
                    AS ph_last_observed,

                first(p.charttime ORDER BY p.charttime)
                    AS ph_first_charttime,
                last(p.charttime ORDER BY p.charttime)
                    AS ph_last_charttime

            FROM {grid_table} g
            LEFT JOIN ph_points_arterial p
                ON p.cohort_stay_id = g.stay_id
               AND g.eligible_minutes > 0
               AND p.charttime >= g.window_start_time
               AND p.charttime < g.eligible_window_end_time

            GROUP BY g.stay_id, g.window_index
        ),

        allblood_obs AS (
            SELECT
                g.stay_id,
                g.window_index,

                COUNT(p.charttime) AS ph_allblood_points_n,
                SUM(coalesce(p.raw_rows_n, 0)) AS ph_allblood_raw_rows_n,

                AVG(p.ph_value) AS ph_allblood_mean_observed,
                median(p.ph_value) AS ph_allblood_median_observed,
                MIN(p.ph_value) AS ph_allblood_min_observed,
                MAX(p.ph_value) AS ph_allblood_max_observed,

                first(p.ph_value ORDER BY p.charttime)
                    AS ph_allblood_first_observed,
                last(p.ph_value ORDER BY p.charttime)
                    AS ph_allblood_last_observed

            FROM {grid_table} g
            LEFT JOIN ph_points_all p
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
                MAX(gap_minutes) AS ph_max_internal_point_gap_minutes
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
                INNER JOIN ph_points_arterial p
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

            pa.ph_measured_24h_flag,
            pa.ph_allblood_measured_24h_flag,
            pa.ph_arterial_timestamps_24h_n,
            pa.ph_allblood_timestamps_24h_n,

            CASE
                WHEN g.eligible_minutes > 0 AND a.ph_points_n > 0 THEN 1
                WHEN g.eligible_minutes > 0 THEN 0
                ELSE NULL
            END AS ph_measured_in_window_flag,

            CASE
                WHEN g.eligible_minutes > 0
                 AND x.ph_allblood_points_n > 0 THEN 1
                WHEN g.eligible_minutes > 0 THEN 0
                ELSE NULL
            END AS ph_allblood_measured_in_window_flag,

            a.ph_points_n,
            a.ph_raw_rows_n,
            a.ph_mean_observed,
            a.ph_median_observed,
            a.ph_sd_observed,
            a.ph_min_observed,
            a.ph_max_observed,
            a.ph_first_observed,
            a.ph_last_observed,

            CASE
                WHEN a.ph_points_n >= 2
                THEN a.ph_last_observed - a.ph_first_observed
                ELSE NULL
            END AS ph_delta_last_minus_first_observed,

            a.ph_first_charttime,
            a.ph_last_charttime,

            CASE
                WHEN a.ph_first_charttime IS NOT NULL
                THEN date_diff(
                    'second',
                    g.window_start_time,
                    a.ph_first_charttime
                ) / 60.0
                ELSE NULL
            END AS ph_first_point_offset_minutes,

            CASE
                WHEN a.ph_last_charttime IS NOT NULL
                THEN date_diff(
                    'second',
                    a.ph_last_charttime,
                    g.eligible_window_end_time
                ) / 60.0
                ELSE NULL
            END AS ph_last_point_to_eligible_end_minutes,

            gp.ph_max_internal_point_gap_minutes,

            x.ph_allblood_points_n,
            x.ph_allblood_raw_rows_n,
            x.ph_allblood_mean_observed,
            x.ph_allblood_median_observed,
            x.ph_allblood_min_observed,
            x.ph_allblood_max_observed,
            x.ph_allblood_first_observed,
            x.ph_allblood_last_observed,

            CASE
                WHEN a.ph_median_observed IS NOT NULL
                 AND x.ph_allblood_median_observed IS NOT NULL
                THEN
                    x.ph_allblood_median_observed
                    - a.ph_median_observed
                ELSE NULL
            END AS ph_allblood_minus_arterial_median,

            CASE
                WHEN a.ph_last_observed IS NOT NULL
                 AND x.ph_allblood_last_observed IS NOT NULL
                THEN
                    x.ph_allblood_last_observed
                    - a.ph_last_observed
                ELSE NULL
            END AS ph_allblood_minus_arterial_last,

            CASE
                WHEN a.ph_max_observed IS NOT NULL
                 AND x.ph_allblood_max_observed IS NOT NULL
                THEN
                    x.ph_allblood_max_observed
                    - a.ph_max_observed
                ELSE NULL
            END AS ph_allblood_minus_arterial_max,

            CASE
                WHEN g.eligible_minutes = 0 THEN 'NOT_OBSERVABLE'
                WHEN a.ph_points_n = 0 THEN 'OBSERVABLE_NOT_MEASURED'
                WHEN a.ph_points_n = 1 THEN 'MEASURED_ONE_POINT'
                ELSE 'MEASURED_MULTIPLE_POINTS'
            END AS ph_measurement_status

        FROM {grid_table} g
        INNER JOIN arterial_obs a
            ON a.stay_id = g.stay_id
           AND a.window_index = g.window_index
        INNER JOIN allblood_obs x
            ON x.stay_id = g.stay_id
           AND x.window_index = g.window_index
        LEFT JOIN gaps gp
            ON gp.stay_id = g.stay_id
           AND gp.window_index = g.window_index
        INNER JOIN patient_ph_availability pa
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

    bad_observation_n = con.execute(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE eligible_minutes = 0
          AND ph_points_n > 0
        """
    ).fetchone()[0]

    if bad_observation_n:
        raise RuntimeError(
            f"Observed arterial pH in non-observable {table_name} windows: "
            f"{bad_observation_n}."
        )

# Both resolutions must reconstruct all consolidated arterial points exactly.
arterial_points_hourly = con.execute(
    "SELECT SUM(ph_points_n) FROM hourly_output"
).fetchone()[0]

arterial_points_six = con.execute(
    "SELECT SUM(ph_points_n) FROM six_hour_output"
).fetchone()[0]

if arterial_points_hourly != arterial_points_n:
    raise RuntimeError(
        f"Hourly windows reconstruct {arterial_points_hourly} arterial points; "
        f"expected {arterial_points_n}."
    )

if arterial_points_six != arterial_points_n:
    raise RuntimeError(
        f"Six-hour windows reconstruct {arterial_points_six} arterial points; "
        f"expected {arterial_points_n}."
    )

all_points_hourly = con.execute(
    "SELECT SUM(ph_allblood_points_n) FROM hourly_output"
).fetchone()[0]

all_points_six = con.execute(
    "SELECT SUM(ph_allblood_points_n) FROM six_hour_output"
).fetchone()[0]

if all_points_hourly != all_points_n:
    raise RuntimeError(
        f"Hourly windows reconstruct {all_points_hourly} all-blood points; "
        f"expected {all_points_n}."
    )

if all_points_six != all_points_n:
    raise RuntimeError(
        f"Six-hour windows reconstruct {all_points_six} all-blood points; "
        f"expected {all_points_n}."
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

valid_numeric_n = con.execute(
    "SELECT COUNT(*) FROM ph_valid_all"
).fetchone()[0]

valid_arterial_raw_n = con.execute(
    "SELECT COUNT(*) FROM ph_valid_arterial"
).fetchone()[0]

observed_valid_min, observed_valid_max = con.execute(
    """
    SELECT MIN(valuenum), MAX(valuenum)
    FROM ph_valid_all
    """
).fetchone()

overall_rows = [
    ("OVERALL", None, None, "cohort_n", cohort_rows_n, None),
    ("OVERALL", None, None, "raw_ph_rows_n", raw_n, None),
    ("OVERALL", None, None, "numeric_ph_rows_n", numeric_n, None),
    ("OVERALL", None, None, "non_numeric_excluded_rows_n", nonnumeric_n, None),
    (
        "OVERALL",
        None,
        None,
        "technical_invalid_numeric_rows_n",
        technical_invalid_n,
        f"outside [{TECHNICAL_PH_MIN}, {TECHNICAL_PH_MAX}]",
    ),
    ("OVERALL", None, None, "valid_allblood_numeric_rows_n", valid_numeric_n, None),
    (
        "OVERALL",
        None,
        None,
        "valid_arterial_numeric_rows_n",
        valid_arterial_raw_n,
        None,
    ),
    (
        "OVERALL",
        None,
        None,
        "patients_with_valid_arterial_ph_n",
        valid_arterial_patients_n,
        None,
    ),
    (
        "OVERALL",
        None,
        None,
        "patients_without_valid_arterial_ph_n",
        EXPECTED_COHORT_N - valid_arterial_patients_n,
        None,
    ),
    ("OVERALL", None, None, "arterial_consolidated_points_n", arterial_points_n, None),
    ("OVERALL", None, None, "allblood_consolidated_points_n", all_points_n, None),
    (
        "OVERALL",
        None,
        None,
        "observed_valid_ph_min",
        observed_valid_min,
        "pH units",
    ),
    (
        "OVERALL",
        None,
        None,
        "observed_valid_ph_max",
        observed_valid_max,
        "pH units",
    ),
]

con.executemany(
    """
    INSERT INTO audit
    VALUES (?, ?, ?, ?, ?, ?)
    """,
    overall_rows,
)


def add_window_audit(table_name: str, level: str) -> None:
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
                'measured_arterial_patients_n',
                COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND ph_points_n > 0
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'measured_arterial_pct',
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND ph_points_n > 0
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
                'measured_allblood_patients_n',
                COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND ph_allblood_points_n > 0
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'patients_gained_by_allblood_n',
                COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND ph_allblood_points_n > 0
                      AND ph_points_n = 0
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'arterial_multi_point_windows_n',
                COUNT(*) FILTER (
                    WHERE ph_points_n >= 2
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'median_points_among_measured_arterial',
                median(ph_points_n)::DOUBLE
            FROM {table_name}
            WHERE ph_points_n > 0
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'paired_allblood_vs_arterial_median_abs_difference',
                median(
                    abs(
                        ph_allblood_median_observed
                        - ph_median_observed
                    )
                )::DOUBLE
            FROM {table_name}
            WHERE ph_allblood_median_observed IS NOT NULL
              AND ph_median_observed IS NOT NULL
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'paired_allblood_vs_arterial_p95_median_abs_difference',
                quantile_cont(
                    abs(
                        ph_allblood_median_observed
                        - ph_median_observed
                    ),
                    0.95
                )::DOUBLE
            FROM {table_name}
            WHERE ph_allblood_median_observed IS NOT NULL
              AND ph_median_observed IS NOT NULL
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'multi_point_median_abs_last_minus_median',
                median(
                    abs(ph_last_observed - ph_median_observed)
                )::DOUBLE
            FROM {table_name}
            WHERE ph_points_n >= 2
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'multi_point_p95_abs_last_minus_median',
                quantile_cont(
                    abs(ph_last_observed - ph_median_observed),
                    0.95
                )::DOUBLE
            FROM {table_name}
            WHERE ph_points_n >= 2
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'multi_point_median_abs_max_minus_median',
                median(
                    abs(ph_max_observed - ph_median_observed)
                )::DOUBLE
            FROM {table_name}
            WHERE ph_points_n >= 2
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'multi_point_p95_abs_max_minus_median',
                quantile_cont(
                    abs(ph_max_observed - ph_median_observed),
                    0.95
                )::DOUBLE
            FROM {table_name}
            WHERE ph_points_n >= 2
            GROUP BY window_index, window_label
        )
        """
    )


add_window_audit("hourly_output", "HOURLY")
add_window_audit("six_hour_output", "SIX_HOUR")


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
                'measured_arterial_windows_n',
                COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND ph_points_n > 0
                )::DOUBLE
            FROM {table_name}

            UNION ALL

            SELECT
                'measured_allblood_windows_n',
                COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND ph_allblood_points_n > 0
                )::DOUBLE
            FROM {table_name}

            UNION ALL

            SELECT
                'windows_gained_by_allblood_n',
                COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND ph_allblood_points_n > 0
                      AND ph_points_n = 0
                )::DOUBLE
            FROM {table_name}

            UNION ALL

            SELECT
                'arterial_multi_point_measured_windows_n',
                COUNT(*) FILTER (
                    WHERE ph_points_n >= 2
                )::DOUBLE
            FROM {table_name}

            UNION ALL

            SELECT
                'arterial_multi_point_pct_of_measured',
                100.0 * COUNT(*) FILTER (
                    WHERE ph_points_n >= 2
                ) / NULLIF(
                    COUNT(*) FILTER (
                        WHERE ph_points_n > 0
                    ),
                    0
                )::DOUBLE
            FROM {table_name}

            UNION ALL

            SELECT
                'median_abs_last_minus_median_multi_point',
                median(
                    abs(ph_last_observed - ph_median_observed)
                )::DOUBLE
            FROM {table_name}
            WHERE ph_points_n >= 2

            UNION ALL

            SELECT
                'p95_abs_last_minus_median_multi_point',
                quantile_cont(
                    abs(ph_last_observed - ph_median_observed),
                    0.95
                )::DOUBLE
            FROM {table_name}
            WHERE ph_points_n >= 2

            UNION ALL

            SELECT
                'median_abs_max_minus_median_multi_point',
                median(
                    abs(ph_max_observed - ph_median_observed)
                )::DOUBLE
            FROM {table_name}
            WHERE ph_points_n >= 2

            UNION ALL

            SELECT
                'p95_abs_max_minus_median_multi_point',
                quantile_cont(
                    abs(ph_max_observed - ph_median_observed),
                    0.95
                )::DOUBLE
            FROM {table_name}
            WHERE ph_points_n >= 2

            UNION ALL

            SELECT
                'paired_allblood_vs_arterial_windows_n',
                COUNT(*) FILTER (
                    WHERE ph_allblood_median_observed IS NOT NULL
                      AND ph_median_observed IS NOT NULL
                )::DOUBLE
            FROM {table_name}

            UNION ALL

            SELECT
                'median_abs_allblood_vs_arterial_median_difference',
                median(
                    abs(
                        ph_allblood_median_observed
                        - ph_median_observed
                    )
                )::DOUBLE
            FROM {table_name}
            WHERE ph_allblood_median_observed IS NOT NULL
              AND ph_median_observed IS NOT NULL

            UNION ALL

            SELECT
                'p95_abs_allblood_vs_arterial_median_difference',
                quantile_cont(
                    abs(
                        ph_allblood_median_observed
                        - ph_median_observed
                    ),
                    0.95
                )::DOUBLE
            FROM {table_name}
            WHERE ph_allblood_median_observed IS NOT NULL
              AND ph_median_observed IS NOT NULL
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
print("TRAJECTORIES - pH CANONICAL TRAJECTORY CONSTRUCTION")
print("=" * 92)
print(f"Cohort rows / unique stays       : {cohort_rows_n} / {cohort_unique_stays_n}")
print(f"Raw pH rows                      : {raw_n}")
print(f"Numeric pH rows                  : {numeric_n}")
print(f"Non-numeric excluded rows        : {nonnumeric_n}")
print(f"Technical-invalid numeric rows   : {technical_invalid_n}")
print(
    f"Technical validity range         : "
    f"{TECHNICAL_PH_MIN:.1f} - {TECHNICAL_PH_MAX:.1f}"
)
print(f"Valid all-blood numeric rows     : {valid_numeric_n}")
print(f"Valid arterial numeric rows      : {valid_arterial_raw_n}")
print(f"Patients with valid arterial pH  : {valid_arterial_patients_n}")
print(
    f"Patients without valid arterial pH: "
    f"{EXPECTED_COHORT_N - valid_arterial_patients_n}"
)
print(f"Arterial consolidated timestamps : {arterial_points_n}")
print(f"All-blood consolidated timestamps: {all_points_n}")
print(
    f"Observed valid range             : "
    f"{observed_valid_min:.3f} - {observed_valid_max:.3f}"
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
                  AND ph_points_n > 0
            ) AS measured_arterial_n,
            round(
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND ph_points_n > 0
                ) / NULLIF(
                    COUNT(*) FILTER (
                        WHERE eligible_minutes > 0
                    ), 0
                ),
                2
            ) AS measured_arterial_pct,
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND ph_allblood_points_n > 0
            ) AS measured_allblood_n,
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND ph_allblood_points_n > 0
                  AND ph_points_n = 0
            ) AS gained_by_allblood_n,
            COUNT(*) FILTER (
                WHERE ph_points_n >= 2
            ) AS multi_point_windows_n,
            round(
                median(ph_median_observed) FILTER (
                    WHERE ph_points_n > 0
                ),
                3
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
                  AND ph_points_n > 0
            ) AS measured_arterial_n,
            round(
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND ph_points_n > 0
                ) / NULLIF(
                    COUNT(*) FILTER (
                        WHERE eligible_minutes > 0
                    ), 0
                ),
                2
            ) AS measured_arterial_pct,
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND ph_allblood_points_n > 0
            ) AS measured_allblood_n,
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND ph_allblood_points_n > 0
                  AND ph_points_n = 0
            ) AS gained_by_allblood_n,
            COUNT(*) FILTER (
                WHERE ph_points_n >= 2
            ) AS multi_point_windows_n,
            round(
                median(ph_median_observed) FILTER (
                    WHERE ph_points_n > 0
                ),
                3
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
            round(value_num, 5) AS value_num
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
            ph_measurement_status,
            COUNT(*) AS windows_n
        FROM hourly_output
        GROUP BY ph_measurement_status

        UNION ALL

        SELECT
            'SIX_HOUR',
            ph_measurement_status,
            COUNT(*)
        FROM six_hour_output
        GROUP BY ph_measurement_status

        ORDER BY resolution, ph_measurement_status
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
print("Canonical pH extraction complete.")
print("Primary series: LABEVENTS 50820, arterial only.")
print("Technical range: 6.0-8.0.")
print("No LOCF/interpolation was used.")
print("Primary trajectory feature: ph_median_observed.")
print("All-blood specimen summaries are retained as sensitivity features.")
