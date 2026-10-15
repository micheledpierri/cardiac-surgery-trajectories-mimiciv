#!/usr/bin/env python3
"""
Canonical postoperative heart-rate extractor and trajectory builder.

PURPOSE
-------
This autonomous script reconstructs postoperative heart rate (HR) directly
from raw MIMIC-IV data for the frozen Trajectories cohort. It applies the
validated HR cleaning and temporal-support rules, creates 24 hourly trajectory
windows and four six-hour descriptive windows, retains a 60-minute LOCF
sensitivity analysis, validates the complete pipeline, and exports only three
canonical-style outputs.

The script does not read, modify, or depend on any MAP output.

VALIDATED HEART-RATE RULES
--------------------------
1. Source table: MIMIC-IV icu/chartevents.
2. Itemid: 220045 (Heart Rate) only.
3. Expected unit: bpm; no unit conversion is performed.
4. Numerical HR values are valid when 0 < HR < 300 bpm.
5. Positive extreme values inside this interval are retained.
6. Invalid timestamps, including HR = 0, are excluded from the numerical
   trajectory but retained internally as temporal blockers.
7. LOCF support cannot cross an invalid HR timestamp.
8. MIMIC warning values are retained as quality descriptors and do not trigger
   exclusion.
9. No duplicate consolidation is expected. The script stops if duplicate
   stay_id + charttime groups are found.
10. The primary representation contains 24 consecutive one-hour windows.
11. Four consecutive six-hour windows are retained as secondary summaries.
12. Primary temporal reconstruction uses right-continuous LOCF for at most
    90 minutes.
13. Sensitivity reconstruction uses right-continuous LOCF for at most
    60 minutes.
14. LOCF stops at the next valid HR, the next invalid HR timestamp,
    observation_end_time, 24 postoperative hours, or the carry limit,
    whichever occurs first.
15. No backward extrapolation and no linear interpolation are used.

INPUTS
------
data/00_04_final_analytic_cohort.csv
MIMIC_ROOT/icu/chartevents.csv.gz
MIMIC_ROOT/icu/d_items.csv.gz

CANONICAL OUTPUTS
---------------------------
data/01_01_heart_rate_hourly_trajectory.parquet
data/01_01_heart_rate_six_hour_summary.parquet
data/01_01_heart_rate_extraction_audit.csv

All raw, valid, invalid, LOCF-segment, fragment, and quality objects remain
in-memory DuckDB tables and are not exported.

This is the frozen production script for the postoperative heart-rate
component. It must remain the single canonical HR extraction pipeline.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb


# =============================================================================
# CONFIGURATION
# =============================================================================

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = (
    SCRIPT_PATH.parent.parent
    if SCRIPT_PATH.parent.name.lower() == "scripts"
    else SCRIPT_PATH.parent
)
DATA_DIR = PROJECT_ROOT / "data"

COHORT_PATH = DATA_DIR / "00_04_final_analytic_cohort.csv"

# Set this path to the local MIMIC-IV 3.1 directory.
MIMIC_ROOT = Path("path/to/mimic-iv-3.1")
CHARTEVENTS_PATH = MIMIC_ROOT / "icu" / "chartevents.csv.gz"
D_ITEMS_PATH = MIMIC_ROOT / "icu" / "d_items.csv.gz"

HOURLY_OUTPUT_PATH = DATA_DIR / "01_01_heart_rate_hourly_trajectory.parquet"
SIX_HOUR_OUTPUT_PATH = DATA_DIR / "01_01_heart_rate_six_hour_summary.parquet"
AUDIT_OUTPUT_PATH = DATA_DIR / "01_01_heart_rate_extraction_audit.csv"

EXPECTED_COHORT_N = 9305
HEART_RATE_ITEMID = 220045
EXPECTED_ITEM_LABEL = "Heart Rate"
EXPECTED_UNIT = "bpm"

VALID_HR_LOWER_EXCLUSIVE = 0.0
VALID_HR_UPPER_EXCLUSIVE = 300.0

LOCF_PRIMARY_MINUTES = 90.0
LOCF_SENSITIVITY_MINUTES = 60.0

HOURLY_WINDOWS_N = 24
SIX_HOUR_WINDOWS_N = 4


class HeartRateExtractionError(RuntimeError):
    """Raised when a mandatory HR extraction invariant fails."""


def sql_path(path: Path) -> str:
    """Return an absolute DuckDB-safe path."""
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def validate_inputs() -> None:
    """Check the frozen cohort and raw MIMIC-IV inputs."""
    required = {
        "definitive analytical cohort": COHORT_PATH,
        "chartevents": CHARTEVENTS_PATH,
        "d_items": D_ITEMS_PATH,
    }

    missing = [
        f"{name}: {path}"
        for name, path in required.items()
        if not path.is_file()
    ]

    if missing:
        raise HeartRateExtractionError(
            "Required file(s) not found:\n- " + "\n- ".join(missing)
        )


def create_source_views(con: duckdb.DuckDBPyConnection) -> None:
    """Create typed source views."""
    cohort = sql_path(COHORT_PATH)
    chartevents = sql_path(CHARTEVENTS_PATH)
    d_items = sql_path(D_ITEMS_PATH)

    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW cohort AS
        SELECT
            CAST(subject_id AS BIGINT) AS subject_id,
            CAST(hadm_id AS BIGINT) AS hadm_id,
            CAST(stay_id AS BIGINT) AS stay_id,
            CAST(surgery_group AS VARCHAR) AS surgery_group,
            CAST(sex AS VARCHAR) AS sex,
            CAST(age_at_icu_admission AS DOUBLE) AS age_at_icu_admission,
            CAST(postoperative_time_zero AS TIMESTAMP)
                AS postoperative_time_zero,
            CAST(observation_end_time AS TIMESTAMP)
                AS observation_end_time,
            CAST(observation_end_type AS VARCHAR)
                AS observation_end_type
        FROM read_csv_auto(
            '{cohort}',
            header = true,
            sample_size = -1
        )
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW d_items AS
        SELECT
            CAST(itemid AS BIGINT) AS itemid,
            CAST(label AS VARCHAR) AS label,
            CAST(abbreviation AS VARCHAR) AS abbreviation,
            CAST(category AS VARCHAR) AS category,
            CAST(unitname AS VARCHAR) AS unitname
        FROM read_csv_auto(
            '{d_items}',
            header = true,
            all_varchar = true,
            sample_size = -1,
            strict_mode = false,
            null_padding = true,
            escape = '"'
        )
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW chartevents AS
        SELECT
            subject_id,
            hadm_id,
            stay_id,
            charttime,
            storetime,
            itemid,
            value,
            valuenum,
            valueuom,
            warning
        FROM read_csv(
            '{chartevents}',
            header = true,
            compression = 'gzip',
            delim = ',',
            quote = '"',
            escape = '"',
            strict_mode = false,
            null_padding = true,
            columns = {{
                'subject_id': 'BIGINT',
                'hadm_id': 'BIGINT',
                'stay_id': 'BIGINT',
                'caregiver_id': 'BIGINT',
                'charttime': 'TIMESTAMP',
                'storetime': 'TIMESTAMP',
                'itemid': 'BIGINT',
                'value': 'VARCHAR',
                'valuenum': 'DOUBLE',
                'valueuom': 'VARCHAR',
                'warning': 'BIGINT'
            }}
        )
        """
    )


def validate_cohort_and_dictionary(con: duckdb.DuckDBPyConnection) -> None:
    """Validate the frozen cohort and the selected HR itemid."""
    cohort_rows, unique_stays = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM cohort"
    ).fetchone()

    if cohort_rows != EXPECTED_COHORT_N or unique_stays != EXPECTED_COHORT_N:
        raise HeartRateExtractionError(
            f"Cohort has {cohort_rows:,} rows and {unique_stays:,} unique "
            f"stays; expected {EXPECTED_COHORT_N:,}."
        )

    invalid_times = con.execute(
        """
        SELECT COUNT(*)
        FROM cohort
        WHERE postoperative_time_zero IS NULL
           OR observation_end_time IS NULL
           OR postoperative_time_zero > observation_end_time
        """
    ).fetchone()[0]

    if invalid_times:
        raise HeartRateExtractionError(
            f"{invalid_times:,} cohort rows have invalid observation times."
        )

    item_rows = con.execute(
        f"""
        SELECT label, abbreviation, category, unitname
        FROM d_items
        WHERE itemid = {HEART_RATE_ITEMID}
        """
    ).fetchall()

    if len(item_rows) != 1:
        raise HeartRateExtractionError(
            f"Expected exactly one d_items row for itemid {HEART_RATE_ITEMID}; "
            f"found {len(item_rows)}."
        )

    label, _abbreviation, _category, unitname = item_rows[0]

    if label != EXPECTED_ITEM_LABEL:
        raise HeartRateExtractionError(
            f"Unexpected label for itemid {HEART_RATE_ITEMID}: {label!r}."
        )

    if (unitname or "").lower() != EXPECTED_UNIT:
        raise HeartRateExtractionError(
            f"Unexpected dictionary unit for itemid {HEART_RATE_ITEMID}: "
            f"{unitname!r}."
        )


def extract_and_classify_hr(con: duckdb.DuckDBPyConnection) -> None:
    """Extract all numeric HR rows and split valid values from blockers."""
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE hr_raw AS
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
            ce.charttime,
            ce.storetime,
            ce.itemid,
            ce.value AS original_value,
            ce.valuenum AS hr_value,
            ce.valueuom,
            ce.warning,
            DATE_DIFF(
                'second',
                c.postoperative_time_zero,
                ce.charttime
            ) / 60.0 AS minutes_from_time_zero,
            CASE
                WHEN ce.valuenum > {VALID_HR_LOWER_EXCLUSIVE}
                 AND ce.valuenum < {VALID_HR_UPPER_EXCLUSIVE}
                    THEN 1
                ELSE 0
            END AS valid_hr_flag,
            CASE
                WHEN ce.valuenum <= {VALID_HR_LOWER_EXCLUSIVE}
                    THEN 'INVALID_NON_POSITIVE'
                WHEN ce.valuenum >= {VALID_HR_UPPER_EXCLUSIVE}
                    THEN 'INVALID_GE300'
                ELSE 'VALID_0_TO_LT300'
            END AS validity_class
        FROM cohort AS c
        INNER JOIN chartevents AS ce
            ON c.stay_id = ce.stay_id
        WHERE ce.itemid = {HEART_RATE_ITEMID}
          AND ce.valuenum IS NOT NULL
          AND ce.charttime >= c.postoperative_time_zero
          AND ce.charttime < c.postoperative_time_zero + INTERVAL '24 hours'
          AND ce.charttime <= c.observation_end_time
        ORDER BY c.stay_id, ce.charttime
        """
    )

    raw_n = con.execute("SELECT COUNT(*) FROM hr_raw").fetchone()[0]
    if raw_n == 0:
        raise HeartRateExtractionError("No numeric heart-rate rows extracted.")

    invalid_window_rows = con.execute(
        """
        SELECT COUNT(*)
        FROM hr_raw
        WHERE minutes_from_time_zero < 0
           OR minutes_from_time_zero >= 1440
           OR charttime > observation_end_time
        """
    ).fetchone()[0]

    if invalid_window_rows:
        raise HeartRateExtractionError(
            f"{invalid_window_rows:,} HR rows violate extraction-window rules."
        )

    unexpected_units = con.execute(
        f"""
        SELECT COUNT(*)
        FROM hr_raw
        WHERE valueuom IS NULL
           OR LOWER(valueuom) <> '{EXPECTED_UNIT}'
        """
    ).fetchone()[0]

    if unexpected_units:
        raise HeartRateExtractionError(
            f"{unexpected_units:,} extracted rows have unexpected or missing "
            "units."
        )

    duplicate_timestamp_groups = con.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT stay_id, charttime
            FROM hr_raw
            GROUP BY stay_id, charttime
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]

    if duplicate_timestamp_groups:
        raise HeartRateExtractionError(
            f"{duplicate_timestamp_groups:,} duplicate stay_id + charttime "
            "groups were found. The frozen no-consolidation rule must be "
            "reviewed before proceeding."
        )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE hr_valid AS
        SELECT *
        FROM hr_raw
        WHERE valid_hr_flag = 1
        ORDER BY stay_id, charttime
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE hr_invalid_times AS
        SELECT *
        FROM hr_raw
        WHERE valid_hr_flag = 0
        ORDER BY stay_id, charttime
        """
    )


def create_temporal_windows(con: duckdb.DuckDBPyConnection) -> None:
    """Create 24 hourly and four six-hour windows."""
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE temporal_windows AS
        SELECT
            'HOURLY' AS resolution,
            hour_index AS window_index,
            'H'
            || LPAD(CAST(hour_index AS VARCHAR), 2, '0')
            || '_H'
            || LPAD(CAST(hour_index + 1 AS VARCHAR), 2, '0')
                AS window_label,
            CAST(hour_index AS DOUBLE) AS start_hour,
            CAST(hour_index + 1 AS DOUBLE) AS end_hour,
            60.0 AS nominal_minutes
        FROM range(0, 24) AS r(hour_index)

        UNION ALL

        SELECT
            'SIX_HOUR' AS resolution,
            block_index AS window_index,
            'H'
            || LPAD(CAST(block_index * 6 AS VARCHAR), 2, '0')
            || '_H'
            || LPAD(CAST((block_index + 1) * 6 AS VARCHAR), 2, '0')
                AS window_label,
            CAST(block_index * 6 AS DOUBLE) AS start_hour,
            CAST((block_index + 1) * 6 AS DOUBLE) AS end_hour,
            360.0 AS nominal_minutes
        FROM range(0, 4) AS r(block_index)
        """
    )

    windows_n = con.execute(
        "SELECT COUNT(*) FROM temporal_windows"
    ).fetchone()[0]

    expected_n = HOURLY_WINDOWS_N + SIX_HOUR_WINDOWS_N
    if windows_n != expected_n:
        raise HeartRateExtractionError(
            f"Temporal windows table has {windows_n:,} rows; expected "
            f"{expected_n:,}."
        )


def create_patient_windows(con: duckdb.DuckDBPyConnection) -> None:
    """Create the complete patient-by-window observability grid."""
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE patient_windows AS
        WITH base AS (
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
                w.resolution,
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
                    AS nominal_window_end_time
            FROM cohort AS c
            CROSS JOIN temporal_windows AS w
        )
        SELECT
            *,
            LEAST(observation_end_time, nominal_window_end_time)
                AS eligible_window_end_time,
            GREATEST(
                0.0,
                DATE_DIFF(
                    'second',
                    window_start_time,
                    LEAST(observation_end_time, nominal_window_end_time)
                ) / 60.0
            ) AS eligible_minutes,
            CASE
                WHEN observation_end_time <= window_start_time
                    THEN 'NOT_OBSERVABLE'
                WHEN observation_end_time < nominal_window_end_time
                    THEN 'PARTIALLY_OBSERVABLE'
                ELSE 'FULLY_OBSERVABLE'
            END AS observability_status
        FROM base
        ORDER BY stay_id, resolution, window_index
        """
    )


def create_point_and_blocker_summaries(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Summarize observed valid points and invalid timestamps by window."""
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE hr_points_by_window AS
        SELECT
            w.resolution,
            w.window_index,
            w.window_label,
            v.subject_id,
            v.hadm_id,
            v.stay_id,
            v.charttime,
            v.hr_value,
            v.warning
        FROM temporal_windows AS w
        INNER JOIN hr_valid AS v
            ON v.minutes_from_time_zero >= w.start_hour * 60.0
           AND v.minutes_from_time_zero < w.end_hour * 60.0
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE hr_point_gaps AS
        WITH ordered AS (
            SELECT
                *,
                LAG(charttime) OVER (
                    PARTITION BY resolution, window_index, stay_id
                    ORDER BY charttime
                ) AS previous_charttime
            FROM hr_points_by_window
        )
        SELECT
            resolution,
            window_index,
            stay_id,
            MAX(
                DATE_DIFF('second', previous_charttime, charttime) / 60.0
            ) FILTER (
                WHERE previous_charttime IS NOT NULL
                  AND charttime > previous_charttime
            ) AS hr_max_internal_point_gap_minutes
        FROM ordered
        GROUP BY resolution, window_index, stay_id
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE hr_point_summary AS
        SELECT
            resolution,
            window_index,
            window_label,
            subject_id,
            hadm_id,
            stay_id,
            COUNT(*) AS hr_points_n,
            COUNT(*) FILTER (WHERE COALESCE(warning, 0) <> 0)
                AS hr_warning_points_n,
            AVG(hr_value) AS hr_arithmetic_mean_observed,
            MEDIAN(hr_value) AS hr_median_observed,
            STDDEV_SAMP(hr_value) AS hr_sd_observed,
            MIN(hr_value) AS hr_min_observed,
            MAX(hr_value) AS hr_max_observed,
            ARG_MIN(hr_value, charttime) AS hr_first_observed,
            ARG_MAX(hr_value, charttime) AS hr_last_observed,
            ARG_MAX(hr_value, charttime) - ARG_MIN(hr_value, charttime)
                AS hr_delta_last_minus_first_observed,
            MIN(charttime) AS hr_first_charttime,
            MAX(charttime) AS hr_last_charttime
        FROM hr_points_by_window
        GROUP BY
            resolution,
            window_index,
            window_label,
            subject_id,
            hadm_id,
            stay_id
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE invalid_times_by_window AS
        SELECT
            w.resolution,
            w.window_index,
            i.stay_id,
            COUNT(*) AS invalid_hr_timestamps_n,
            COUNT(*) FILTER (WHERE i.hr_value <= 0)
                AS non_positive_hr_timestamps_n,
            COUNT(*) FILTER (WHERE i.hr_value >= 300)
                AS hr_ge300_timestamps_n
        FROM temporal_windows AS w
        INNER JOIN hr_invalid_times AS i
            ON i.minutes_from_time_zero >= w.start_hour * 60.0
           AND i.minutes_from_time_zero < w.end_hour * 60.0
        GROUP BY w.resolution, w.window_index, i.stay_id
        """
    )


def create_locf_segments(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    carry_minutes: float,
    carry_end_reason: str,
) -> None:
    """Create right-continuous LOCF segments stopped by invalid timestamps."""
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {table_name} AS
        WITH ordered AS (
            SELECT
                v.*,
                LEAD(v.charttime) OVER (
                    PARTITION BY v.stay_id
                    ORDER BY v.charttime
                ) AS next_valid_charttime,
                (
                    SELECT MIN(i.charttime)
                    FROM hr_invalid_times AS i
                    WHERE i.stay_id = v.stay_id
                      AND i.charttime > v.charttime
                ) AS next_invalid_charttime
            FROM hr_valid AS v
        ),
        bounded AS (
            SELECT
                *,
                charttime AS segment_start,
                charttime + {carry_minutes} * INTERVAL '1 minute'
                    AS carry_limit_time,
                postoperative_time_zero + INTERVAL '24 hours'
                    AS postoperative_24h_time,
                LEAST(
                    COALESCE(
                        next_valid_charttime,
                        TIMESTAMP '9999-12-31 23:59:59'
                    ),
                    COALESCE(
                        next_invalid_charttime,
                        TIMESTAMP '9999-12-31 23:59:59'
                    ),
                    observation_end_time,
                    postoperative_time_zero + INTERVAL '24 hours',
                    charttime + {carry_minutes} * INTERVAL '1 minute'
                ) AS segment_end
            FROM ordered
        )
        SELECT
            subject_id,
            hadm_id,
            stay_id,
            surgery_group,
            sex,
            age_at_icu_admission,
            postoperative_time_zero,
            observation_end_time,
            observation_end_type,
            charttime AS source_charttime,
            hr_value,
            warning,
            segment_start,
            segment_end,
            DATE_DIFF('second', segment_start, segment_end) / 60.0
                AS segment_minutes,
            next_valid_charttime,
            next_invalid_charttime,
            carry_limit_time,
            postoperative_24h_time,
            CASE
                WHEN next_valid_charttime IS NOT NULL
                 AND segment_end = next_valid_charttime
                    THEN 'NEXT_VALID_HR'
                WHEN next_invalid_charttime IS NOT NULL
                 AND segment_end = next_invalid_charttime
                    THEN 'INVALID_HR_BLOCKER'
                WHEN segment_end = observation_end_time
                    THEN 'OBSERVATION_END'
                WHEN segment_end = postoperative_24h_time
                    THEN 'POSTOPERATIVE_24H_END'
                WHEN segment_end = carry_limit_time
                    THEN '{carry_end_reason}'
                ELSE 'OTHER_BOUNDARY'
            END AS segment_end_reason
        FROM bounded
        WHERE segment_end > segment_start
        ORDER BY stay_id, segment_start
        """
    )


def create_interval_locf_summary(
    con: duckdb.DuckDBPyConnection,
    segment_table: str,
    output_table: str,
    suffix: str,
    carry_end_reason: str,
) -> None:
    """Clip LOCF segments to windows and calculate TWA and support."""
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {output_table}_fragments AS
        SELECT
            pw.resolution,
            pw.window_index,
            pw.window_label,
            pw.stay_id,
            s.hr_value,
            s.segment_end_reason,
            s.segment_end AS original_segment_end,
            GREATEST(s.segment_start, pw.window_start_time)
                AS fragment_start,
            LEAST(s.segment_end, pw.eligible_window_end_time)
                AS fragment_end
        FROM patient_windows AS pw
        INNER JOIN {segment_table} AS s
            ON pw.stay_id = s.stay_id
           AND pw.eligible_minutes > 0
           AND s.segment_end > pw.window_start_time
           AND s.segment_start < pw.eligible_window_end_time
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {output_table} AS
        WITH valid_fragments AS (
            SELECT
                *,
                DATE_DIFF('second', fragment_start, fragment_end) / 60.0
                    AS fragment_minutes
            FROM {output_table}_fragments
            WHERE fragment_end > fragment_start
        )
        SELECT
            resolution,
            window_index,
            window_label,
            stay_id,
            SUM(fragment_minutes) AS hr_locf_supported_minutes_{suffix},
            SUM(hr_value * fragment_minutes)
                AS hr_locf_weighted_sum_bpm_minutes_{suffix},
            SUM(hr_value * fragment_minutes)
                / NULLIF(SUM(fragment_minutes), 0.0)
                AS hr_time_weighted_mean_{suffix},
            COUNT(*) AS hr_locf_fragments_n_{suffix},
            COUNT(*) FILTER (
                WHERE segment_end_reason = 'INVALID_HR_BLOCKER'
                  AND fragment_end = original_segment_end
            ) AS hr_fragments_ending_invalid_blocker_n_{suffix},
            COUNT(*) FILTER (
                WHERE segment_end_reason = '{carry_end_reason}'
                  AND fragment_end = original_segment_end
            ) AS hr_fragments_ending_carry_limit_n_{suffix}
        FROM valid_fragments
        GROUP BY resolution, window_index, window_label, stay_id
        """
    )


def support_class_sql(minutes_column: str, pct_column: str) -> str:
    """Return SQL defining the common support-quality categories."""
    return f"""
        CASE
            WHEN eligible_minutes <= 0 THEN 'NOT_OBSERVABLE'
            WHEN {minutes_column} <= 0 THEN 'NO_LOCF_SUPPORT'
            WHEN {pct_column} >= 90 THEN 'LOCF_SUPPORT_GE90_PCT'
            WHEN {pct_column} >= 75
                THEN 'LOCF_SUPPORT_75_TO_LT90_PCT'
            WHEN {pct_column} >= 50
                THEN 'LOCF_SUPPORT_50_TO_LT75_PCT'
            ELSE 'LOCF_SUPPORT_GT0_TO_LT50_PCT'
        END
    """


def create_final_temporal_summary(con: duckdb.DuckDBPyConnection) -> None:
    """Combine observability, points, blockers, primary LOCF, and sensitivity."""
    support_class_60 = support_class_sql(
        "hr_locf_supported_minutes_60", "hr_locf_supported_pct_60"
    )
    support_class_90 = support_class_sql(
        "hr_locf_supported_minutes_90", "hr_locf_supported_pct_90"
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE hr_temporal_summary AS
        WITH joined AS (
            SELECT
                pw.*,
                COALESCE(p.hr_points_n, 0) AS hr_points_n,
                COALESCE(p.hr_warning_points_n, 0)
                    AS hr_warning_points_n,
                COALESCE(i.invalid_hr_timestamps_n, 0)
                    AS invalid_hr_timestamps_n,
                COALESCE(i.non_positive_hr_timestamps_n, 0)
                    AS non_positive_hr_timestamps_n,
                COALESCE(i.hr_ge300_timestamps_n, 0)
                    AS hr_ge300_timestamps_n,
                p.hr_arithmetic_mean_observed,
                p.hr_median_observed,
                p.hr_sd_observed,
                p.hr_min_observed,
                p.hr_max_observed,
                p.hr_first_observed,
                p.hr_last_observed,
                p.hr_delta_last_minus_first_observed,
                p.hr_first_charttime,
                p.hr_last_charttime,
                g.hr_max_internal_point_gap_minutes,
                COALESCE(l60.hr_locf_supported_minutes_60, 0.0)
                    AS hr_locf_supported_minutes_60,
                l60.hr_time_weighted_mean_60,
                COALESCE(l60.hr_locf_fragments_n_60, 0)
                    AS hr_locf_fragments_n_60,
                COALESCE(
                    l60.hr_fragments_ending_invalid_blocker_n_60,
                    0
                ) AS hr_fragments_ending_invalid_blocker_n_60,
                COALESCE(
                    l60.hr_fragments_ending_carry_limit_n_60,
                    0
                ) AS hr_fragments_ending_carry_limit_n_60,
                COALESCE(l90.hr_locf_supported_minutes_90, 0.0)
                    AS hr_locf_supported_minutes_90,
                l90.hr_time_weighted_mean_90,
                COALESCE(l90.hr_locf_fragments_n_90, 0)
                    AS hr_locf_fragments_n_90,
                COALESCE(
                    l90.hr_fragments_ending_invalid_blocker_n_90,
                    0
                ) AS hr_fragments_ending_invalid_blocker_n_90,
                COALESCE(
                    l90.hr_fragments_ending_carry_limit_n_90,
                    0
                ) AS hr_fragments_ending_carry_limit_n_90
            FROM patient_windows AS pw
            LEFT JOIN hr_point_summary AS p
                ON pw.resolution = p.resolution
               AND pw.window_index = p.window_index
               AND pw.stay_id = p.stay_id
            LEFT JOIN hr_point_gaps AS g
                ON pw.resolution = g.resolution
               AND pw.window_index = g.window_index
               AND pw.stay_id = g.stay_id
            LEFT JOIN invalid_times_by_window AS i
                ON pw.resolution = i.resolution
               AND pw.window_index = i.window_index
               AND pw.stay_id = i.stay_id
            LEFT JOIN hr_locf_summary_60 AS l60
                ON pw.resolution = l60.resolution
               AND pw.window_index = l60.window_index
               AND pw.stay_id = l60.stay_id
            LEFT JOIN hr_locf_summary_90 AS l90
                ON pw.resolution = l90.resolution
               AND pw.window_index = l90.window_index
               AND pw.stay_id = l90.stay_id
        ),
        calculated AS (
            SELECT
                *,
                CASE
                    WHEN hr_first_charttime IS NULL THEN NULL
                    ELSE DATE_DIFF(
                        'second',
                        window_start_time,
                        hr_first_charttime
                    ) / 60.0
                END AS hr_first_point_offset_minutes,
                CASE
                    WHEN hr_last_charttime IS NULL THEN NULL
                    ELSE DATE_DIFF(
                        'second',
                        hr_last_charttime,
                        eligible_window_end_time
                    ) / 60.0
                END AS hr_last_point_to_eligible_end_minutes,
                CASE
                    WHEN eligible_minutes <= 0 THEN NULL
                    ELSE LEAST(
                        100.0,
                        100.0 * hr_locf_supported_minutes_60
                            / eligible_minutes
                    )
                END AS hr_locf_supported_pct_60,
                CASE
                    WHEN eligible_minutes <= 0 THEN NULL
                    ELSE LEAST(
                        100.0,
                        100.0 * hr_locf_supported_minutes_90
                            / eligible_minutes
                    )
                END AS hr_locf_supported_pct_90,
                hr_locf_supported_minutes_90
                    - hr_locf_supported_minutes_60
                    AS hr_supported_minutes_gain_90_vs_60,
                hr_time_weighted_mean_90
                    - hr_time_weighted_mean_60
                    AS hr_twa_difference_90_minus_60,
                ABS(
                    hr_time_weighted_mean_90
                    - hr_time_weighted_mean_60
                ) AS hr_twa_absolute_difference_60_vs_90
            FROM joined
        ),
        classified AS (
            SELECT
                *,
                {support_class_60} AS hr_locf_support_class_60,
                {support_class_90} AS hr_locf_support_class_90
            FROM calculated
        )
        SELECT
            *,
            CASE
                WHEN eligible_minutes <= 0 THEN 0
                WHEN hr_locf_support_class_60
                     <> hr_locf_support_class_90 THEN 1
                ELSE 0
            END AS hr_locf_support_class_changed_60_vs_90_flag,
            CASE
                WHEN eligible_minutes <= 0 THEN 0
                WHEN hr_locf_supported_minutes_60 <= 0
                 AND hr_locf_supported_minutes_90 > 0 THEN 1
                ELSE 0
            END AS hr_available_only_with_90_flag,
            CASE
                WHEN hr_twa_absolute_difference_60_vs_90 IS NULL THEN NULL
                WHEN hr_twa_absolute_difference_60_vs_90 > 5
                    THEN 'GT5_BPM'
                WHEN hr_twa_absolute_difference_60_vs_90 > 2
                    THEN 'GT2_TO_5_BPM'
                WHEN hr_twa_absolute_difference_60_vs_90 > 1
                    THEN 'GT1_TO_2_BPM'
                WHEN hr_twa_absolute_difference_60_vs_90 > 0.5
                    THEN 'GT0_5_TO_1_BPM'
                ELSE 'LE0_5_BPM'
            END AS hr_twa_difference_band_60_vs_90,
            CASE
                WHEN hr_time_weighted_mean_90 IS NULL
                  OR hr_arithmetic_mean_observed IS NULL THEN NULL
                ELSE hr_time_weighted_mean_90
                    - hr_arithmetic_mean_observed
            END AS hr_twa_minus_arithmetic_mean,
            CASE
                WHEN hr_time_weighted_mean_90 IS NULL
                  OR hr_median_observed IS NULL THEN NULL
                ELSE hr_time_weighted_mean_90
                    - hr_median_observed
            END AS hr_twa_minus_median_observed
        FROM classified
        ORDER BY stay_id, resolution, window_index
        """
    )


def create_extraction_audit(con: duckdb.DuckDBPyConnection) -> None:
    """Create one overall row and 28 temporal-window audit rows."""
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE hr_extraction_audit (
            audit_order INTEGER,
            audit_scope VARCHAR,
            resolution VARCHAR,
            window_index INTEGER,
            window_label VARCHAR,
            start_hour DOUBLE,
            end_hour DOUBLE,
            nominal_minutes DOUBLE,
            hr_itemid BIGINT,
            valid_lower_exclusive DOUBLE,
            valid_upper_exclusive DOUBLE,
            locf_primary_minutes DOUBLE,
            locf_sensitivity_minutes DOUBLE,
            cohort_n BIGINT,
            raw_hr_rows_n BIGINT,
            valid_hr_rows_n BIGINT,
            invalid_hr_rows_n BIGINT,
            non_positive_hr_rows_n BIGINT,
            hr_ge300_rows_n BIGINT,
            warning_rows_n BIGINT,
            duplicate_timestamp_groups_n BIGINT,
            stays_with_valid_hr_n BIGINT,
            stays_without_valid_hr_n BIGINT,
            eligible_patients_n BIGINT,
            fully_observable_patients_n BIGINT,
            partially_observable_patients_n BIGINT,
            patients_with_any_valid_hr_point_n BIGINT,
            patients_with_any_valid_hr_point_pct DOUBLE,
            primary_twa_available_n BIGINT,
            primary_twa_available_pct DOUBLE,
            primary_twa_median DOUBLE,
            primary_locf_supported_pct_median DOUBLE,
            primary_support_ge90_n BIGINT,
            sensitivity_twa_available_n BIGINT,
            sensitivity_twa_available_pct DOUBLE,
            sensitivity_locf_supported_pct_median DOUBLE,
            windows_available_only_with_primary_n BIGINT,
            paired_twa_windows_n BIGINT,
            twa_abs_difference_median DOUBLE,
            twa_abs_difference_p95 DOUBLE,
            twa_abs_difference_p99 DOUBLE,
            twa_abs_difference_maximum DOUBLE,
            twa_abs_difference_gt1_n BIGINT,
            twa_abs_difference_gt2_n BIGINT,
            twa_abs_difference_gt5_n BIGINT,
            support_class_changed_n BIGINT,
            windows_with_invalid_hr_blocker_n BIGINT,
            note VARCHAR
        )
        """
    )

    con.execute(
        f"""
        INSERT INTO hr_extraction_audit
        SELECT
            0 AS audit_order,
            'OVERALL_EXTRACTION' AS audit_scope,
            'OVERALL' AS resolution,
            NULL::INTEGER AS window_index,
            'H00_H24' AS window_label,
            0.0 AS start_hour,
            24.0 AS end_hour,
            1440.0 AS nominal_minutes,
            {HEART_RATE_ITEMID}::BIGINT AS hr_itemid,
            {VALID_HR_LOWER_EXCLUSIVE} AS valid_lower_exclusive,
            {VALID_HR_UPPER_EXCLUSIVE} AS valid_upper_exclusive,
            {LOCF_PRIMARY_MINUTES} AS locf_primary_minutes,
            {LOCF_SENSITIVITY_MINUTES} AS locf_sensitivity_minutes,
            (SELECT COUNT(*) FROM cohort)::BIGINT AS cohort_n,
            (SELECT COUNT(*) FROM hr_raw)::BIGINT AS raw_hr_rows_n,
            (SELECT COUNT(*) FROM hr_valid)::BIGINT AS valid_hr_rows_n,
            (SELECT COUNT(*) FROM hr_invalid_times)::BIGINT
                AS invalid_hr_rows_n,
            (
                SELECT COUNT(*) FROM hr_invalid_times
                WHERE hr_value <= 0
            )::BIGINT AS non_positive_hr_rows_n,
            (
                SELECT COUNT(*) FROM hr_invalid_times
                WHERE hr_value >= 300
            )::BIGINT AS hr_ge300_rows_n,
            (
                SELECT COUNT(*) FROM hr_raw
                WHERE COALESCE(warning, 0) <> 0
            )::BIGINT AS warning_rows_n,
            0::BIGINT AS duplicate_timestamp_groups_n,
            (SELECT COUNT(DISTINCT stay_id) FROM hr_valid)::BIGINT
                AS stays_with_valid_hr_n,
            (
                SELECT COUNT(*)
                FROM cohort AS c
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM hr_valid AS v
                    WHERE v.stay_id = c.stay_id
                )
            )::BIGINT AS stays_without_valid_hr_n,
            (
                SELECT COUNT(*)
                FROM hr_temporal_summary
                WHERE resolution = 'HOURLY'
                  AND eligible_minutes > 0
            )::BIGINT AS eligible_patients_n,
            NULL::BIGINT AS fully_observable_patients_n,
            NULL::BIGINT AS partially_observable_patients_n,
            NULL::BIGINT AS patients_with_any_valid_hr_point_n,
            NULL::DOUBLE AS patients_with_any_valid_hr_point_pct,
            (
                SELECT COUNT(*)
                FROM hr_temporal_summary
                WHERE resolution = 'HOURLY'
                  AND eligible_minutes > 0
                  AND hr_time_weighted_mean_90 IS NOT NULL
            )::BIGINT AS primary_twa_available_n,
            (
                SELECT 100.0 * COUNT(*) FILTER (
                    WHERE hr_time_weighted_mean_90 IS NOT NULL
                ) / NULLIF(COUNT(*), 0)
                FROM hr_temporal_summary
                WHERE resolution = 'HOURLY'
                  AND eligible_minutes > 0
            ) AS primary_twa_available_pct,
            NULL::DOUBLE AS primary_twa_median,
            NULL::DOUBLE AS primary_locf_supported_pct_median,
            NULL::BIGINT AS primary_support_ge90_n,
            (
                SELECT COUNT(*)
                FROM hr_temporal_summary
                WHERE resolution = 'HOURLY'
                  AND eligible_minutes > 0
                  AND hr_time_weighted_mean_60 IS NOT NULL
            )::BIGINT AS sensitivity_twa_available_n,
            (
                SELECT 100.0 * COUNT(*) FILTER (
                    WHERE hr_time_weighted_mean_60 IS NOT NULL
                ) / NULLIF(COUNT(*), 0)
                FROM hr_temporal_summary
                WHERE resolution = 'HOURLY'
                  AND eligible_minutes > 0
            ) AS sensitivity_twa_available_pct,
            NULL::DOUBLE AS sensitivity_locf_supported_pct_median,
            (
                SELECT COUNT(*)
                FROM hr_temporal_summary
                WHERE resolution = 'HOURLY'
                  AND hr_available_only_with_90_flag = 1
            )::BIGINT AS windows_available_only_with_primary_n,
            (
                SELECT COUNT(*)
                FROM hr_temporal_summary
                WHERE resolution = 'HOURLY'
                  AND hr_twa_absolute_difference_60_vs_90 IS NOT NULL
            )::BIGINT AS paired_twa_windows_n,
            (
                SELECT MEDIAN(hr_twa_absolute_difference_60_vs_90)
                FROM hr_temporal_summary
                WHERE resolution = 'HOURLY'
                  AND hr_twa_absolute_difference_60_vs_90 IS NOT NULL
            ) AS twa_abs_difference_median,
            (
                SELECT QUANTILE_CONT(
                    hr_twa_absolute_difference_60_vs_90, 0.95
                )
                FROM hr_temporal_summary
                WHERE resolution = 'HOURLY'
                  AND hr_twa_absolute_difference_60_vs_90 IS NOT NULL
            ) AS twa_abs_difference_p95,
            (
                SELECT QUANTILE_CONT(
                    hr_twa_absolute_difference_60_vs_90, 0.99
                )
                FROM hr_temporal_summary
                WHERE resolution = 'HOURLY'
                  AND hr_twa_absolute_difference_60_vs_90 IS NOT NULL
            ) AS twa_abs_difference_p99,
            (
                SELECT MAX(hr_twa_absolute_difference_60_vs_90)
                FROM hr_temporal_summary
                WHERE resolution = 'HOURLY'
                  AND hr_twa_absolute_difference_60_vs_90 IS NOT NULL
            ) AS twa_abs_difference_maximum,
            (
                SELECT COUNT(*)
                FROM hr_temporal_summary
                WHERE resolution = 'HOURLY'
                  AND hr_twa_absolute_difference_60_vs_90 > 1
            )::BIGINT AS twa_abs_difference_gt1_n,
            (
                SELECT COUNT(*)
                FROM hr_temporal_summary
                WHERE resolution = 'HOURLY'
                  AND hr_twa_absolute_difference_60_vs_90 > 2
            )::BIGINT AS twa_abs_difference_gt2_n,
            (
                SELECT COUNT(*)
                FROM hr_temporal_summary
                WHERE resolution = 'HOURLY'
                  AND hr_twa_absolute_difference_60_vs_90 > 5
            )::BIGINT AS twa_abs_difference_gt5_n,
            (
                SELECT COUNT(*)
                FROM hr_temporal_summary
                WHERE resolution = 'HOURLY'
                  AND hr_locf_support_class_changed_60_vs_90_flag = 1
            )::BIGINT AS support_class_changed_n,
            (
                SELECT COUNT(*)
                FROM hr_temporal_summary
                WHERE resolution = 'HOURLY'
                  AND invalid_hr_timestamps_n > 0
            )::BIGINT AS windows_with_invalid_hr_blocker_n,
            'Primary trajectory uses 90-minute LOCF; 60-minute LOCF is retained as sensitivity. Invalid HR timestamps block carry-forward.'
                AS note
        """
    )

    con.execute(
        f"""
        INSERT INTO hr_extraction_audit
        SELECT
            1 AS audit_order,
            'TEMPORAL_WINDOW' AS audit_scope,
            resolution,
            CAST(window_index AS INTEGER),
            window_label,
            MIN(start_hour) AS start_hour,
            MIN(end_hour) AS end_hour,
            MIN(nominal_minutes) AS nominal_minutes,
            {HEART_RATE_ITEMID}::BIGINT AS hr_itemid,
            {VALID_HR_LOWER_EXCLUSIVE} AS valid_lower_exclusive,
            {VALID_HR_UPPER_EXCLUSIVE} AS valid_upper_exclusive,
            {LOCF_PRIMARY_MINUTES} AS locf_primary_minutes,
            {LOCF_SENSITIVITY_MINUTES} AS locf_sensitivity_minutes,
            NULL::BIGINT AS cohort_n,
            NULL::BIGINT AS raw_hr_rows_n,
            NULL::BIGINT AS valid_hr_rows_n,
            NULL::BIGINT AS invalid_hr_rows_n,
            NULL::BIGINT AS non_positive_hr_rows_n,
            NULL::BIGINT AS hr_ge300_rows_n,
            NULL::BIGINT AS warning_rows_n,
            NULL::BIGINT AS duplicate_timestamp_groups_n,
            NULL::BIGINT AS stays_with_valid_hr_n,
            NULL::BIGINT AS stays_without_valid_hr_n,
            COUNT(*) FILTER (WHERE eligible_minutes > 0)
                AS eligible_patients_n,
            COUNT(*) FILTER (
                WHERE observability_status = 'FULLY_OBSERVABLE'
            ) AS fully_observable_patients_n,
            COUNT(*) FILTER (
                WHERE observability_status = 'PARTIALLY_OBSERVABLE'
            ) AS partially_observable_patients_n,
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0 AND hr_points_n > 0
            ) AS patients_with_any_valid_hr_point_n,
            ROUND(
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0 AND hr_points_n > 0
                ) / NULLIF(COUNT(*) FILTER (WHERE eligible_minutes > 0), 0),
                2
            ) AS patients_with_any_valid_hr_point_pct,
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND hr_time_weighted_mean_90 IS NOT NULL
            ) AS primary_twa_available_n,
            ROUND(
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND hr_time_weighted_mean_90 IS NOT NULL
                ) / NULLIF(COUNT(*) FILTER (WHERE eligible_minutes > 0), 0),
                2
            ) AS primary_twa_available_pct,
            MEDIAN(hr_time_weighted_mean_90) FILTER (
                WHERE hr_time_weighted_mean_90 IS NOT NULL
            ) AS primary_twa_median,
            MEDIAN(hr_locf_supported_pct_90) FILTER (
                WHERE eligible_minutes > 0
            ) AS primary_locf_supported_pct_median,
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND hr_locf_supported_pct_90 >= 90
            ) AS primary_support_ge90_n,
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND hr_time_weighted_mean_60 IS NOT NULL
            ) AS sensitivity_twa_available_n,
            ROUND(
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND hr_time_weighted_mean_60 IS NOT NULL
                ) / NULLIF(COUNT(*) FILTER (WHERE eligible_minutes > 0), 0),
                2
            ) AS sensitivity_twa_available_pct,
            MEDIAN(hr_locf_supported_pct_60) FILTER (
                WHERE eligible_minutes > 0
            ) AS sensitivity_locf_supported_pct_median,
            COUNT(*) FILTER (
                WHERE hr_available_only_with_90_flag = 1
            ) AS windows_available_only_with_primary_n,
            COUNT(*) FILTER (
                WHERE hr_twa_absolute_difference_60_vs_90 IS NOT NULL
            ) AS paired_twa_windows_n,
            MEDIAN(hr_twa_absolute_difference_60_vs_90) FILTER (
                WHERE hr_twa_absolute_difference_60_vs_90 IS NOT NULL
            ) AS twa_abs_difference_median,
            QUANTILE_CONT(
                hr_twa_absolute_difference_60_vs_90, 0.95
            ) FILTER (
                WHERE hr_twa_absolute_difference_60_vs_90 IS NOT NULL
            ) AS twa_abs_difference_p95,
            QUANTILE_CONT(
                hr_twa_absolute_difference_60_vs_90, 0.99
            ) FILTER (
                WHERE hr_twa_absolute_difference_60_vs_90 IS NOT NULL
            ) AS twa_abs_difference_p99,
            MAX(hr_twa_absolute_difference_60_vs_90) FILTER (
                WHERE hr_twa_absolute_difference_60_vs_90 IS NOT NULL
            ) AS twa_abs_difference_maximum,
            COUNT(*) FILTER (
                WHERE hr_twa_absolute_difference_60_vs_90 > 1
            ) AS twa_abs_difference_gt1_n,
            COUNT(*) FILTER (
                WHERE hr_twa_absolute_difference_60_vs_90 > 2
            ) AS twa_abs_difference_gt2_n,
            COUNT(*) FILTER (
                WHERE hr_twa_absolute_difference_60_vs_90 > 5
            ) AS twa_abs_difference_gt5_n,
            COUNT(*) FILTER (
                WHERE hr_locf_support_class_changed_60_vs_90_flag = 1
            ) AS support_class_changed_n,
            COUNT(*) FILTER (
                WHERE invalid_hr_timestamps_n > 0
            ) AS windows_with_invalid_hr_blocker_n,
            'Window-level primary trajectory and 60-minute sensitivity audit.'
                AS note
        FROM hr_temporal_summary
        GROUP BY resolution, window_index, window_label
        """
    )


def validate_outputs(con: duckdb.DuckDBPyConnection) -> None:
    """Validate extraction, blockers, LOCF, grids, and audit reconstruction."""
    raw_n = con.execute("SELECT COUNT(*) FROM hr_raw").fetchone()[0]
    valid_n = con.execute("SELECT COUNT(*) FROM hr_valid").fetchone()[0]
    invalid_n = con.execute("SELECT COUNT(*) FROM hr_invalid_times").fetchone()[0]

    if raw_n != valid_n + invalid_n:
        raise HeartRateExtractionError(
            "Raw rows do not equal valid plus invalid rows."
        )

    invalid_valid_values = con.execute(
        f"""
        SELECT COUNT(*)
        FROM hr_valid
        WHERE hr_value <= {VALID_HR_LOWER_EXCLUSIVE}
           OR hr_value >= {VALID_HR_UPPER_EXCLUSIVE}
        """
    ).fetchone()[0]

    if invalid_valid_values:
        raise HeartRateExtractionError(
            f"{invalid_valid_values:,} invalid values remain in hr_valid."
        )

    duplicate_keys = con.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT stay_id, resolution, window_index
            FROM hr_temporal_summary
            GROUP BY stay_id, resolution, window_index
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]

    if duplicate_keys:
        raise HeartRateExtractionError(
            f"{duplicate_keys:,} duplicate patient-window keys remain."
        )

    hourly_rows = con.execute(
        """
        SELECT COUNT(*) FROM hr_temporal_summary
        WHERE resolution = 'HOURLY'
        """
    ).fetchone()[0]
    six_hour_rows = con.execute(
        """
        SELECT COUNT(*) FROM hr_temporal_summary
        WHERE resolution = 'SIX_HOUR'
        """
    ).fetchone()[0]

    expected_hourly = EXPECTED_COHORT_N * HOURLY_WINDOWS_N
    expected_six_hour = EXPECTED_COHORT_N * SIX_HOUR_WINDOWS_N

    if hourly_rows != expected_hourly:
        raise HeartRateExtractionError(
            f"Hourly table has {hourly_rows:,} rows; expected "
            f"{expected_hourly:,}."
        )

    if six_hour_rows != expected_six_hour:
        raise HeartRateExtractionError(
            f"Six-hour table has {six_hour_rows:,} rows; expected "
            f"{expected_six_hour:,}."
        )

    invalid_window_minutes = con.execute(
        """
        SELECT COUNT(*)
        FROM hr_temporal_summary
        WHERE eligible_minutes < -0.0001
           OR eligible_minutes > nominal_minutes + 0.0001
        """
    ).fetchone()[0]

    if invalid_window_minutes:
        raise HeartRateExtractionError(
            f"{invalid_window_minutes:,} patient-windows have invalid "
            "eligible duration."
        )

    invalid_support = con.execute(
        """
        SELECT COUNT(*)
        FROM hr_temporal_summary
        WHERE hr_locf_supported_minutes_60 < -0.0001
           OR hr_locf_supported_minutes_90 < -0.0001
           OR hr_locf_supported_minutes_60 > eligible_minutes + 0.0001
           OR hr_locf_supported_minutes_90 > eligible_minutes + 0.0001
           OR hr_locf_supported_minutes_60
              > hr_locf_supported_minutes_90 + 0.0001
           OR (
                hr_locf_supported_minutes_60 = 0
                AND hr_time_weighted_mean_60 IS NOT NULL
              )
           OR (
                hr_locf_supported_minutes_90 = 0
                AND hr_time_weighted_mean_90 IS NOT NULL
              )
           OR (
                hr_locf_supported_minutes_60 > 0
                AND (
                    hr_time_weighted_mean_60 IS NULL
                    OR hr_time_weighted_mean_60 <= 0
                    OR hr_time_weighted_mean_60 >= 300
                )
              )
           OR (
                hr_locf_supported_minutes_90 > 0
                AND (
                    hr_time_weighted_mean_90 IS NULL
                    OR hr_time_weighted_mean_90 <= 0
                    OR hr_time_weighted_mean_90 >= 300
                )
              )
        """
    ).fetchone()[0]

    if invalid_support:
        raise HeartRateExtractionError(
            f"{invalid_support:,} patient-windows violate LOCF invariants."
        )

    for table_name, carry_minutes in (
        ("hr_locf_segments_60", LOCF_SENSITIVITY_MINUTES),
        ("hr_locf_segments_90", LOCF_PRIMARY_MINUTES),
    ):
        invalid_segments = con.execute(
            f"""
            SELECT COUNT(*)
            FROM {table_name}
            WHERE segment_minutes <= 0
               OR segment_minutes > {carry_minutes} + 0.0001
               OR segment_end > observation_end_time
               OR segment_end > postoperative_24h_time
            """
        ).fetchone()[0]

        if invalid_segments:
            raise HeartRateExtractionError(
                f"{invalid_segments:,} segments in {table_name} violate "
                "duration or endpoint rules."
            )

        crossing_blockers = con.execute(
            f"""
            SELECT COUNT(*)
            FROM {table_name} AS s
            WHERE EXISTS (
                SELECT 1
                FROM hr_invalid_times AS i
                WHERE i.stay_id = s.stay_id
                  AND i.charttime > s.segment_start
                  AND i.charttime < s.segment_end
            )
            """
        ).fetchone()[0]

        if crossing_blockers:
            raise HeartRateExtractionError(
                f"{crossing_blockers:,} segments in {table_name} cross "
                "invalid HR blockers."
            )

    hourly_points = con.execute(
        """
        SELECT SUM(hr_points_n)
        FROM hr_temporal_summary
        WHERE resolution = 'HOURLY'
        """
    ).fetchone()[0] or 0

    six_hour_points = con.execute(
        """
        SELECT SUM(hr_points_n)
        FROM hr_temporal_summary
        WHERE resolution = 'SIX_HOUR'
        """
    ).fetchone()[0] or 0

    if hourly_points != valid_n or six_hour_points != valid_n:
        raise HeartRateExtractionError(
            "Hourly or six-hour windows do not reconstruct all valid HR "
            "points."
        )

    hourly_invalid = con.execute(
        """
        SELECT SUM(invalid_hr_timestamps_n)
        FROM hr_temporal_summary
        WHERE resolution = 'HOURLY'
        """
    ).fetchone()[0] or 0

    six_hour_invalid = con.execute(
        """
        SELECT SUM(invalid_hr_timestamps_n)
        FROM hr_temporal_summary
        WHERE resolution = 'SIX_HOUR'
        """
    ).fetchone()[0] or 0

    if hourly_invalid != invalid_n or six_hour_invalid != invalid_n:
        raise HeartRateExtractionError(
            "Hourly or six-hour windows do not reconstruct all invalid HR "
            "timestamps."
        )

    for suffix, segment_table in (
        ("60", "hr_locf_segments_60"),
        ("90", "hr_locf_segments_90"),
    ):
        segment_minutes = con.execute(
            f"SELECT SUM(segment_minutes) FROM {segment_table}"
        ).fetchone()[0] or 0.0

        hourly_minutes = con.execute(
            f"""
            SELECT SUM(hr_locf_supported_minutes_{suffix})
            FROM hr_temporal_summary
            WHERE resolution = 'HOURLY'
            """
        ).fetchone()[0] or 0.0

        six_hour_minutes = con.execute(
            f"""
            SELECT SUM(hr_locf_supported_minutes_{suffix})
            FROM hr_temporal_summary
            WHERE resolution = 'SIX_HOUR'
            """
        ).fetchone()[0] or 0.0

        if abs(hourly_minutes - segment_minutes) > 0.1:
            raise HeartRateExtractionError(
                f"Hourly {suffix}-minute LOCF windows do not reconstruct "
                "total segment duration."
            )

        if abs(six_hour_minutes - segment_minutes) > 0.1:
            raise HeartRateExtractionError(
                f"Six-hour {suffix}-minute LOCF windows do not reconstruct "
                "total segment duration."
            )

    audit_rows = con.execute(
        "SELECT COUNT(*) FROM hr_extraction_audit"
    ).fetchone()[0]

    expected_audit_rows = 1 + HOURLY_WINDOWS_N + SIX_HOUR_WINDOWS_N
    if audit_rows != expected_audit_rows:
        raise HeartRateExtractionError(
            f"Extraction audit has {audit_rows:,} rows; expected "
            f"{expected_audit_rows:,}."
        )


def canonical_temporal_select_sql() -> str:
    """Return the canonical column list for both trajectory outputs."""
    return """
        subject_id,
        hadm_id,
        stay_id,
        surgery_group,
        sex,
        age_at_icu_admission,
        postoperative_time_zero,
        observation_end_time,
        observation_end_type,
        window_index,
        window_label,
        start_hour,
        end_hour,
        nominal_minutes,
        window_start_time,
        nominal_window_end_time,
        eligible_window_end_time,
        eligible_minutes,
        observability_status,
        hr_points_n,
        hr_warning_points_n,
        invalid_hr_timestamps_n,
        non_positive_hr_timestamps_n,
        hr_ge300_timestamps_n,
        hr_arithmetic_mean_observed,
        hr_median_observed,
        hr_sd_observed,
        hr_min_observed,
        hr_max_observed,
        hr_first_observed,
        hr_last_observed,
        hr_delta_last_minus_first_observed,
        hr_first_charttime,
        hr_last_charttime,
        hr_first_point_offset_minutes,
        hr_last_point_to_eligible_end_minutes,
        hr_max_internal_point_gap_minutes,
        hr_time_weighted_mean_90 AS hr_time_weighted_mean,
        hr_locf_supported_minutes_90 AS hr_locf_supported_minutes,
        hr_locf_supported_pct_90 AS hr_locf_supported_pct_of_eligible,
        hr_locf_fragments_n_90 AS hr_locf_fragments_n,
        hr_fragments_ending_invalid_blocker_n_90
            AS hr_locf_fragments_ending_invalid_blocker_n,
        hr_fragments_ending_carry_limit_n_90
            AS hr_locf_fragments_ending_90min_n,
        hr_locf_support_class_90 AS hr_locf_support_class,
        hr_twa_minus_arithmetic_mean,
        hr_twa_minus_median_observed,
        hr_time_weighted_mean_60
            AS hr_time_weighted_mean_sensitivity_60,
        hr_locf_supported_minutes_60
            AS hr_locf_supported_minutes_sensitivity_60,
        hr_locf_supported_pct_60
            AS hr_locf_supported_pct_sensitivity_60,
        hr_supported_minutes_gain_90_vs_60,
        hr_twa_difference_90_minus_60,
        hr_twa_absolute_difference_60_vs_90,
        hr_locf_support_class_60
            AS hr_locf_support_class_sensitivity_60,
        hr_locf_support_class_changed_60_vs_90_flag,
        hr_available_only_with_90_flag,
        hr_twa_difference_band_60_vs_90
    """


def export_outputs(con: duckdb.DuckDBPyConnection) -> None:
    """Export only the two trajectories and the compact audit."""
    outputs = (
        HOURLY_OUTPUT_PATH,
        SIX_HOUR_OUTPUT_PATH,
        AUDIT_OUTPUT_PATH,
    )

    for path in outputs:
        if path.exists():
            path.unlink()

    hourly_output = sql_path(HOURLY_OUTPUT_PATH)
    six_hour_output = sql_path(SIX_HOUR_OUTPUT_PATH)
    audit_output = sql_path(AUDIT_OUTPUT_PATH)
    columns = canonical_temporal_select_sql()

    con.execute(
        f"""
        COPY (
            SELECT {columns}
            FROM hr_temporal_summary
            WHERE resolution = 'HOURLY'
            ORDER BY stay_id, window_index
        )
        TO '{hourly_output}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    con.execute(
        f"""
        COPY (
            SELECT {columns}
            FROM hr_temporal_summary
            WHERE resolution = 'SIX_HOUR'
            ORDER BY stay_id, window_index
        )
        TO '{six_hour_output}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    con.execute(
        f"""
        COPY (
            SELECT *
            FROM hr_extraction_audit
            ORDER BY audit_order, resolution, window_index
        )
        TO '{audit_output}'
        (HEADER, DELIMITER ',', QUOTE '"', ESCAPE '"', NULL '')
        """
    )


def print_final_summary(con: duckdb.DuckDBPyConnection) -> None:
    """Print the main extraction and trajectory results."""
    raw_n, valid_n, invalid_n, stays_n = con.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM hr_raw),
            (SELECT COUNT(*) FROM hr_valid),
            (SELECT COUNT(*) FROM hr_invalid_times),
            (SELECT COUNT(DISTINCT stay_id) FROM hr_valid)
        """
    ).fetchone()

    overall = con.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE eligible_minutes > 0),
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND hr_time_weighted_mean_60 IS NOT NULL
            ),
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND hr_time_weighted_mean_90 IS NOT NULL
            ),
            COUNT(*) FILTER (WHERE hr_available_only_with_90_flag = 1),
            COUNT(*) FILTER (
                WHERE hr_twa_absolute_difference_60_vs_90 > 2
            ),
            COUNT(*) FILTER (
                WHERE hr_twa_absolute_difference_60_vs_90 > 5
            )
        FROM hr_temporal_summary
        WHERE resolution = 'HOURLY'
        """
    ).fetchone()

    (
        eligible_hourly,
        twa60_n,
        twa90_n,
        gained_n,
        diff_gt2_n,
        diff_gt5_n,
    ) = overall

    hourly_n = con.execute(
        """
        SELECT COUNT(*) FROM hr_temporal_summary
        WHERE resolution = 'HOURLY'
        """
    ).fetchone()[0]

    six_hour_n = con.execute(
        """
        SELECT COUNT(*) FROM hr_temporal_summary
        WHERE resolution = 'SIX_HOUR'
        """
    ).fetchone()[0]

    print("=" * 140)
    print("FINAL HEART-RATE EXTRACTION AND TRAJECTORY")
    print("=" * 140)
    print(f"Frozen analytical cohort:                     {EXPECTED_COHORT_N:,}")
    print(f"Raw numeric itemid-220045 rows:                {raw_n:,}")
    print(f"Valid HR rows (0 < HR < 300):                  {valid_n:,}")
    print(f"Invalid HR timestamps retained as blockers:   {invalid_n:,}")
    print(f"Stays with valid HR:                           {stays_n:,}")
    print(f"Stays without valid HR:                        {EXPECTED_COHORT_N - stays_n:,}")
    print(f"Hourly trajectory rows:                        {hourly_n:,}")
    print(f"Six-hour summary rows:                         {six_hour_n:,}")
    print("Primary / sensitivity LOCF:                    90 / 60 minutes")
    print("No backward extrapolation or interpolation:    YES")

    print("\nHourly LOCF sensitivity")
    print("-" * 100)
    print(f"  Eligible hourly patient-windows:             {eligible_hourly:,}")
    print(f"  TWA available with 60 / 90 minutes:          {twa60_n:,} / {twa90_n:,}")
    print(f"  Available only with 90 minutes:              {gained_n:,}")
    print(f"  Absolute TWA differences >2 / >5 bpm:        {diff_gt2_n:,} / {diff_gt5_n:,}")

    print("\nCanonical outputs")
    print("-" * 100)
    print(f"  {HOURLY_OUTPUT_PATH}")
    print(f"  {SIX_HOUR_OUTPUT_PATH}")
    print(f"  {AUDIT_OUTPUT_PATH}")
    print("No raw, case-level, or intermediate HR object was exported.")
    print("=" * 140)


def main() -> int:
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        validate_inputs()

        con = duckdb.connect(":memory:")
        try:
            create_source_views(con)
            validate_cohort_and_dictionary(con)
            extract_and_classify_hr(con)
            create_temporal_windows(con)
            create_patient_windows(con)
            create_point_and_blocker_summaries(con)

            create_locf_segments(
                con,
                "hr_locf_segments_60",
                LOCF_SENSITIVITY_MINUTES,
                "MAX_60_MIN_CARRY",
            )
            create_locf_segments(
                con,
                "hr_locf_segments_90",
                LOCF_PRIMARY_MINUTES,
                "MAX_90_MIN_CARRY",
            )

            create_interval_locf_summary(
                con,
                "hr_locf_segments_60",
                "hr_locf_summary_60",
                "60",
                "MAX_60_MIN_CARRY",
            )
            create_interval_locf_summary(
                con,
                "hr_locf_segments_90",
                "hr_locf_summary_90",
                "90",
                "MAX_90_MIN_CARRY",
            )

            create_final_temporal_summary(con)
            create_extraction_audit(con)
            validate_outputs(con)
            export_outputs(con)
            print_final_summary(con)
        finally:
            con.close()

        return 0

    except HeartRateExtractionError as exc:
        print("=" * 118, file=sys.stderr)
        print("HEART-RATE EXTRACTION FAILED", file=sys.stderr)
        print(str(exc), file=sys.stderr)
        print("=" * 118, file=sys.stderr)
        return 1

    except Exception as exc:
        print("=" * 118, file=sys.stderr)
        print("UNEXPECTED ERROR", file=sys.stderr)
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        print("=" * 118, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
