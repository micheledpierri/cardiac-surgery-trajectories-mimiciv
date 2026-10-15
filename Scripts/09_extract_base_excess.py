"""
01_06_extract_base_excess.py

Trajectories project
Postoperative Base Excess component - canonical production pipeline

Frozen methodological decisions
-------------------------------
Primary source:
    MIMIC-IV LABEVENTS itemid 50802 (Base Excess)

Specimen:
    Primary representation: ARTERIAL
    Sensitivity representation: all blood specimens

Specimen type source:
    LABEVENTS itemid 52033 (Specimen Type), linked by specimen_id

Excluded ICU display source:
    CHARTEVENTS itemid 224828 (Arterial Base Excess)

Rationale:
    Source-closure audits demonstrated near-complete exact-time duplication
    between CHARTEVENTS 224828 and LABEVENTS 50802. LABEVENTS is therefore
    retained as the authoritative laboratory source.

Technical validity:
    -50 <= Base Excess <= +50 mEq/L

This deliberately broad interval is a technical data-quality rule, not a
clinical reference interval. In the development cohort it excludes one isolated
value of -413 mEq/L, which was internally inconsistent with its surrounding
measurements and lacked corroborating same-specimen blood-gas data.

Temporal representation:
    First 24 postoperative hours
    24 consecutive 1-hour windows (primary grid)
    Four consecutive 6-hour windows (secondary grid)

Temporal reconstruction:
    None
    No LOCF
    No interpolation
    No backward extrapolation
    No forward extrapolation

Primary trajectory feature:
    be_median_observed

Base Excess is bidirectional. Negative and positive extremes represent
different acid-base states, so minimum and maximum values are retained
separately and no generic "worst BE" feature is created.

Canonical outputs
-----------------
data/01_06_base_excess_hourly_trajectory.parquet
data/01_06_base_excess_six_hour_summary.parquet
data/01_06_base_excess_extraction_audit.csv

Inputs
------
data/00_04_final_analytic_cohort.csv
Raw MIMIC-IV 3.1 files relative to MIMIC_ROOT:
  hosp/labevents.csv.gz

Values sharing stay_id and charttime are consolidated by median separately
for the arterial and all-blood representations.
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

OUT_HOURLY = (
    PROJECT_ROOT / "data" / "01_06_base_excess_hourly_trajectory.parquet"
)
OUT_SIX = (
    PROJECT_ROOT / "data" / "01_06_base_excess_six_hour_summary.parquet"
)
OUT_AUDIT = (
    PROJECT_ROOT / "data" / "01_06_base_excess_extraction_audit.csv"
)

EXPECTED_COHORT_N = 9305

BE_ITEMID = 50802
SPECIMEN_ITEMID = 52033

TECHNICAL_MIN = -50.0
TECHNICAL_MAX = 50.0


# =============================================================================
# Helpers
# =============================================================================

def qpath(path: Path) -> str:
    return str(path).replace("'", "''")


for required in [COHORT_FILE, LABEVENTS_FILE]:
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

cohort_n, unique_stays_n, missing_t0_n, missing_end_n, invalid_order_n = (
    con.execute(
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
)

if cohort_n != EXPECTED_COHORT_N or unique_stays_n != EXPECTED_COHORT_N:
    raise RuntimeError(
        f"Unexpected cohort size: rows={cohort_n}, stays={unique_stays_n}; "
        f"expected {EXPECTED_COHORT_N}."
    )

if missing_t0_n or missing_end_n or invalid_order_n:
    raise RuntimeError(
        "Invalid cohort timing fields: "
        f"missing_t0={missing_t0_n}, missing_end={missing_end_n}, "
        f"invalid_order={invalid_order_n}."
    )

con.execute(
    """
    CREATE TEMP TABLE cohort_subjects AS
    SELECT DISTINCT subject_id
    FROM cohort
    """
)


# =============================================================================
# Extract Base Excess and specimen type
# =============================================================================

con.execute(
    f"""
    CREATE TABLE selected_lab AS
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
    WHERE le.itemid IN ({BE_ITEMID}, {SPECIMEN_ITEMID})
    """
)

# Normalize abbreviated MIMIC specimen labels such as ART., VEN., and MIX.
con.execute(
    f"""
    CREATE TABLE specimen_map AS
    WITH raw AS (
        SELECT
            specimen_id,
            upper(trim(max(value))) AS raw_specimen
        FROM selected_lab
        WHERE itemid = {SPECIMEN_ITEMID}
          AND specimen_id IS NOT NULL
          AND value IS NOT NULL
        GROUP BY specimen_id
    )
    SELECT
        specimen_id,
        raw_specimen,
        CASE
            WHEN raw_specimen LIKE 'ART%' THEN 'ARTERIAL'
            WHEN raw_specimen LIKE 'CENTRAL%VEN%' THEN 'CENTRAL_VENOUS'
            WHEN raw_specimen LIKE 'VEN%' THEN 'VENOUS'
            WHEN raw_specimen LIKE 'MIX%' THEN 'MIXED'
            ELSE 'UNKNOWN'
        END AS specimen_type
    FROM raw
    """
)

con.execute(
    f"""
    CREATE TABLE be_raw_postop AS
    SELECT
        c.subject_id,
        c.hadm_id,
        c.stay_id,
        le.labevent_id,
        le.specimen_id,
        le.charttime,
        le.storetime,
        le.value,
        le.valuenum,
        le.valueuom,
        le.flag,
        le.comments,
        coalesce(sm.specimen_type, 'UNKNOWN') AS specimen_type,
        sm.raw_specimen
    FROM selected_lab le
    INNER JOIN cohort c
        ON le.subject_id = c.subject_id
       AND le.itemid = {BE_ITEMID}
       AND le.charttime >= c.postoperative_time_zero
       AND le.charttime < c.postoperative_time_zero + INTERVAL '24 hours'
       AND le.charttime <= c.observation_end_time
    LEFT JOIN specimen_map sm
        ON le.specimen_id = sm.specimen_id
    """
)

assignment_dups_n = con.execute(
    """
    SELECT COUNT(*)
    FROM (
        SELECT labevent_id
        FROM be_raw_postop
        GROUP BY labevent_id
        HAVING COUNT(*) > 1
    )
    """
).fetchone()[0]

if assignment_dups_n:
    raise RuntimeError(
        f"{assignment_dups_n} Base Excess rows mapped to multiple cohort stays."
    )


# =============================================================================
# Numeric and technically valid rows
# =============================================================================

con.execute(
    """
    CREATE TABLE be_numeric AS
    SELECT *
    FROM be_raw_postop
    WHERE valuenum IS NOT NULL
    """
)

con.execute(
    f"""
    CREATE TABLE be_valid AS
    SELECT *
    FROM be_numeric
    WHERE valuenum >= {TECHNICAL_MIN}
      AND valuenum <= {TECHNICAL_MAX}
    """
)


# =============================================================================
# Exact-timestamp consolidation
# =============================================================================

# Primary arterial representation.
con.execute(
    """
    CREATE TABLE be_arterial_points AS
    SELECT
        stay_id,
        charttime,
        median(valuenum) AS be_value,
        COUNT(*) AS source_rows_n,
        MIN(valuenum) AS source_min,
        MAX(valuenum) AS source_max
    FROM be_valid
    WHERE specimen_type = 'ARTERIAL'
    GROUP BY stay_id, charttime
    """
)

# All-blood sensitivity representation.
con.execute(
    """
    CREATE TABLE be_all_blood_points AS
    SELECT
        stay_id,
        charttime,
        median(valuenum) AS be_value,
        COUNT(*) AS source_rows_n,
        MIN(valuenum) AS source_min,
        MAX(valuenum) AS source_max
    FROM be_valid
    GROUP BY stay_id, charttime
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

        WITH arterial AS (
            SELECT
                g.stay_id,
                g.window_index,
                COUNT(p.charttime) AS be_points_n,
                AVG(p.be_value) AS be_mean_observed,
                median(p.be_value) AS be_median_observed,
                MIN(p.be_value) AS be_min_observed,
                MAX(p.be_value) AS be_max_observed,
                first(p.be_value ORDER BY p.charttime)
                    AS be_first_observed,
                last(p.be_value ORDER BY p.charttime)
                    AS be_last_observed,
                first(p.charttime ORDER BY p.charttime)
                    AS be_first_charttime,
                last(p.charttime ORDER BY p.charttime)
                    AS be_last_charttime
            FROM {grid_table} g
            LEFT JOIN be_arterial_points p
                ON p.stay_id = g.stay_id
               AND g.eligible_minutes > 0
               AND p.charttime >= g.window_start_time
               AND p.charttime < g.eligible_window_end_time
            GROUP BY g.stay_id, g.window_index
        ),

        all_blood AS (
            SELECT
                g.stay_id,
                g.window_index,
                COUNT(p.charttime) AS be_all_blood_points_n,
                AVG(p.be_value) AS be_all_blood_mean_observed,
                median(p.be_value) AS be_all_blood_median_observed,
                MIN(p.be_value) AS be_all_blood_min_observed,
                MAX(p.be_value) AS be_all_blood_max_observed,
                first(p.be_value ORDER BY p.charttime)
                    AS be_all_blood_first_observed,
                last(p.be_value ORDER BY p.charttime)
                    AS be_all_blood_last_observed
            FROM {grid_table} g
            LEFT JOIN be_all_blood_points p
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

            CASE
                WHEN g.eligible_minutes > 0 AND a.be_points_n > 0 THEN 1
                WHEN g.eligible_minutes > 0 THEN 0
                ELSE NULL
            END AS be_measured_in_window_flag,

            a.be_points_n,
            a.be_mean_observed,
            a.be_median_observed,
            a.be_min_observed,
            a.be_max_observed,
            a.be_first_observed,
            a.be_last_observed,

            CASE
                WHEN a.be_points_n >= 2
                THEN a.be_last_observed - a.be_first_observed
                ELSE NULL
            END AS be_delta_last_minus_first_observed,

            a.be_first_charttime,
            a.be_last_charttime,

            CASE
                WHEN g.eligible_minutes = 0 THEN 'NOT_OBSERVABLE'
                WHEN a.be_points_n = 0 THEN 'OBSERVABLE_NOT_MEASURED'
                WHEN a.be_points_n = 1 THEN 'MEASURED_ONE_POINT'
                ELSE 'MEASURED_MULTIPLE_POINTS'
            END AS be_measurement_status,

            CASE
                WHEN g.eligible_minutes > 0
                 AND ab.be_all_blood_points_n > 0 THEN 1
                WHEN g.eligible_minutes > 0 THEN 0
                ELSE NULL
            END AS be_all_blood_measured_in_window_flag,

            ab.be_all_blood_points_n,
            ab.be_all_blood_mean_observed,
            ab.be_all_blood_median_observed,
            ab.be_all_blood_min_observed,
            ab.be_all_blood_max_observed,
            ab.be_all_blood_first_observed,
            ab.be_all_blood_last_observed,

            CASE
                WHEN ab.be_all_blood_points_n >= 2
                THEN ab.be_all_blood_last_observed
                     - ab.be_all_blood_first_observed
                ELSE NULL
            END AS be_all_blood_delta_last_minus_first_observed,

            CASE
                WHEN g.eligible_minutes = 0 THEN 'NOT_OBSERVABLE'
                WHEN ab.be_all_blood_points_n = 0
                    THEN 'OBSERVABLE_NOT_MEASURED'
                WHEN ab.be_all_blood_points_n = 1
                    THEN 'MEASURED_ONE_POINT'
                ELSE 'MEASURED_MULTIPLE_POINTS'
            END AS be_all_blood_measurement_status

        FROM {grid_table} g
        INNER JOIN arterial a
            ON a.stay_id = g.stay_id
           AND a.window_index = g.window_index
        INNER JOIN all_blood ab
            ON ab.stay_id = g.stay_id
           AND ab.window_index = g.window_index
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

arterial_points_n = con.execute(
    "SELECT COUNT(*) FROM be_arterial_points"
).fetchone()[0]

all_blood_points_n = con.execute(
    "SELECT COUNT(*) FROM be_all_blood_points"
).fetchone()[0]

hourly_arterial_sum = con.execute(
    "SELECT SUM(be_points_n) FROM hourly_output"
).fetchone()[0]

six_arterial_sum = con.execute(
    "SELECT SUM(be_points_n) FROM six_output"
).fetchone()[0]

hourly_all_sum = con.execute(
    "SELECT SUM(be_all_blood_points_n) FROM hourly_output"
).fetchone()[0]

six_all_sum = con.execute(
    "SELECT SUM(be_all_blood_points_n) FROM six_output"
).fetchone()[0]

if hourly_arterial_sum != arterial_points_n or six_arterial_sum != arterial_points_n:
    raise RuntimeError(
        "Arterial point reconstruction mismatch: "
        f"raw={arterial_points_n}, hourly={hourly_arterial_sum}, "
        f"six={six_arterial_sum}."
    )

if hourly_all_sum != all_blood_points_n or six_all_sum != all_blood_points_n:
    raise RuntimeError(
        "All-blood point reconstruction mismatch: "
        f"raw={all_blood_points_n}, hourly={hourly_all_sum}, "
        f"six={six_all_sum}."
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

raw_rows_n = con.execute("SELECT COUNT(*) FROM be_raw_postop").fetchone()[0]
numeric_rows_n = con.execute("SELECT COUNT(*) FROM be_numeric").fetchone()[0]
nonnumeric_rows_n = raw_rows_n - numeric_rows_n
invalid_rows_n = con.execute(
    f"""
    SELECT COUNT(*)
    FROM be_numeric
    WHERE valuenum < {TECHNICAL_MIN}
       OR valuenum > {TECHNICAL_MAX}
    """
).fetchone()[0]
valid_rows_n = con.execute("SELECT COUNT(*) FROM be_valid").fetchone()[0]

arterial_patients_n = con.execute(
    "SELECT COUNT(DISTINCT stay_id) FROM be_arterial_points"
).fetchone()[0]
all_blood_patients_n = con.execute(
    "SELECT COUNT(DISTINCT stay_id) FROM be_all_blood_points"
).fetchone()[0]

overall_rows = [
    ("OVERALL", None, None, "cohort_n", cohort_n, None),
    ("OVERALL", None, None, "raw_postoperative_rows_n", raw_rows_n, None),
    ("OVERALL", None, None, "numeric_rows_n", numeric_rows_n, None),
    ("OVERALL", None, None, "nonnumeric_rows_n", nonnumeric_rows_n, None),
    (
        "OVERALL", None, None, "technical_invalid_rows_n", invalid_rows_n,
        f"excluded if BE < {TECHNICAL_MIN} or BE > {TECHNICAL_MAX} mEq/L"
    ),
    ("OVERALL", None, None, "valid_rows_n", valid_rows_n, None),
    ("OVERALL", None, None, "arterial_timestamps_n", arterial_points_n, None),
    ("OVERALL", None, None, "all_blood_timestamps_n", all_blood_points_n, None),
    ("OVERALL", None, None, "arterial_patients_n", arterial_patients_n, None),
    ("OVERALL", None, None, "all_blood_patients_n", all_blood_patients_n, None),
    (
        "OVERALL", None, None, "patients_without_arterial_be_n",
        EXPECTED_COHORT_N - arterial_patients_n, None
    ),
    (
        "OVERALL", None, None, "patients_without_all_blood_be_n",
        EXPECTED_COHORT_N - all_blood_patients_n, None
    ),
    (
        "OVERALL", None, None, "technical_validity_min_mEq_L",
        TECHNICAL_MIN, "technical, not clinical"
    ),
    (
        "OVERALL", None, None, "technical_validity_max_mEq_L",
        TECHNICAL_MAX, "technical, not clinical"
    ),
]

con.executemany(
    """
    INSERT INTO audit
    VALUES (?, ?, ?, ?, ?, ?)
    """,
    overall_rows,
)

# Specimen distribution after technical cleaning.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'SPECIMEN',
        NULL,
        NULL,
        specimen_type,
        COUNT(*)::DOUBLE,
        'valid numeric rows'
    FROM be_valid
    GROUP BY specimen_type
    """
)

# Invalid rows, including the isolated -413 value.
con.execute(
    f"""
    INSERT INTO audit
    SELECT
        'TECHNICAL_INVALID',
        NULL,
        NULL,
        'invalid_value_' || cast(valuenum AS VARCHAR),
        COUNT(*)::DOUBLE,
        string_agg(
            DISTINCT coalesce(specimen_type, 'UNKNOWN'),
            ' | '
            ORDER BY coalesce(specimen_type, 'UNKNOWN')
        )
    FROM be_numeric
    WHERE valuenum < {TECHNICAL_MIN}
       OR valuenum > {TECHNICAL_MAX}
    GROUP BY valuenum
    """
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
                'arterial_measured_patients_n',
                COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND be_points_n > 0
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'arterial_measured_pct',
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND be_points_n > 0
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
                'all_blood_measured_patients_n',
                COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND be_all_blood_points_n > 0
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'all_blood_measured_pct',
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND be_all_blood_points_n > 0
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
                'population_median_arterial_window_median',
                median(be_median_observed)::DOUBLE
            FROM {table_name}
            WHERE be_points_n > 0
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'population_median_all_blood_window_median',
                median(be_all_blood_median_observed)::DOUBLE
            FROM {table_name}
            WHERE be_all_blood_points_n > 0
            GROUP BY window_index, window_label
        )
        """
    )


add_window_audit("hourly_output", "HOURLY")
add_window_audit("six_output", "SIX_HOUR")


# =============================================================================
# Export canonical outputs
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
                WHEN 'SPECIMEN' THEN 2
                WHEN 'TECHNICAL_INVALID' THEN 3
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

print("=" * 96)
print("TRAJECTORIES - BASE EXCESS CANONICAL TRAJECTORY")
print("=" * 96)
print(f"Cohort rows / unique stays       : {cohort_n} / {unique_stays_n}")
print(f"Raw postoperative rows           : {raw_rows_n}")
print(f"Numeric rows                     : {numeric_rows_n}")
print(f"Nonnumeric rows                  : {nonnumeric_rows_n}")
print(f"Technical-invalid rows           : {invalid_rows_n}")
print(f"Valid rows                       : {valid_rows_n}")
print(f"Arterial consolidated timestamps : {arterial_points_n}")
print(f"All-blood consolidated timestamps: {all_blood_points_n}")
print(f"Patients with arterial BE        : {arterial_patients_n}")
print(f"Patients with all-blood BE       : {all_blood_patients_n}")
print()

print("Valid specimen distribution")
print("---------------------------")
print(
    con.execute(
        """
        SELECT specimen_type, COUNT(*) AS rows_n
        FROM be_valid
        GROUP BY specimen_type
        ORDER BY rows_n DESC
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Technical-invalid values")
print("------------------------")
invalid_df = con.execute(
    f"""
    SELECT
        stay_id,
        charttime,
        specimen_type,
        value,
        valuenum,
        valueuom,
        flag,
        comments
    FROM be_numeric
    WHERE valuenum < {TECHNICAL_MIN}
       OR valuenum > {TECHNICAL_MAX}
    ORDER BY valuenum
    """
).fetchdf()
print(invalid_df.to_string(index=False) if len(invalid_df) else "<none>")
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
                  AND be_points_n > 0
            ) AS arterial_measured_n,
            round(
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND be_points_n > 0
                ) / NULLIF(
                    COUNT(*) FILTER (
                        WHERE eligible_minutes > 0
                    ),
                    0
                ),
                2
            ) AS arterial_pct,
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND be_all_blood_points_n > 0
            ) AS all_blood_measured_n,
            round(
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND be_all_blood_points_n > 0
                ) / NULLIF(
                    COUNT(*) FILTER (
                        WHERE eligible_minutes > 0
                    ),
                    0
                ),
                2
            ) AS all_blood_pct,
            round(
                median(be_median_observed) FILTER (
                    WHERE be_points_n > 0
                ),
                2
            ) AS population_median_arterial_be
        FROM six_output
        GROUP BY window_index, window_label
        ORDER BY window_index
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
print("Canonical Base Excess extraction complete.")
print("Primary trajectory feature: be_median_observed.")
print("All-blood representation is retained for sensitivity analysis.")
print("No LOCF, interpolation, or extrapolation was used.")
