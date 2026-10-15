#!/usr/bin/env python3
"""
Canonical postoperative MAP extractor and trajectory feature builder.

PURPOSE
-------
This autonomous production script reconstructs postoperative mean arterial
pressure (MAP) from raw MIMIC-IV data, applies the validated invasive/
non-invasive hierarchy, builds 24 hourly trajectory windows and four six-hour
descriptive windows, calculates 90-minute LOCF time-weighted MAP, retains the
validated 60-minute LOCF sensitivity variables, and quantifies hypotension
burden below 65, 60, and 55 mmHg.

VALIDATED MAP RULES
-------------------
1. Invasive MAP is preferred.
2. Non-invasive MAP is used only outside invasive monitoring episodes or as
   exact-time fallback for an excluded ambiguous invasive timestamp.
3. Raw MAP values must satisfy 0 < MAP < 300 mmHg.
4. Valid invasive itemids at the same charttime are consolidated by arithmetic
   mean.
5. Invasive monitoring episodes use all physiologically valid invasive
   timestamps and are separated by gaps >90 minutes.
6. If simultaneous invasive itemids differ by >=20 mmHg, the timestamp defines
   monitoring coverage but its invasive MAP value is excluded.
7. Invasive and non-invasive MAP values are never averaged together.
8. Clinically extreme but possible values inside 0 < MAP < 300 are retained.
9. The primary trajectory consists of 24 consecutive one-hour windows.
10. Four six-hour windows are retained as secondary descriptive summaries.
11. Primary time weighting uses right-continuous LOCF for at most 90 minutes.
12. LOCF stops at the next selected MAP, an unresolved ambiguous invasive
    timestamp, observation end, or 24 postoperative hours.
13. No backward extrapolation and no linear interpolation are used.
14. A 60-minute LOCF reconstruction is retained for sensitivity analysis.
15. Hypotension burden is calculated on 90-minute LOCF-supported time using
    strict thresholds MAP <65, <60, and <55 mmHg.

HYPOTENSION FEATURES
--------------------
For each threshold and temporal window:
- minutes below threshold;
- area below threshold in mmHg*minutes;
- percentage of LOCF-supported time below threshold;
- time-weighted deficit over all supported time;
- mean deficit during exposed time;
- any-exposure flag.

Burden values are NULL when the window is not observable or has no LOCF
support. A supported window with no exposure has burden equal to zero.

INPUT
-----
data/00_04_final_analytic_cohort.csv

RAW MIMIC-IV INPUTS
-------------------
MIMIC_ROOT/icu/chartevents.csv.gz
MIMIC_ROOT/icu/d_items.csv.gz

CANONICAL OUTPUTS
-----------------
data/01_00_map_hourly_trajectory.parquet
data/01_00_map_six_hour_summary.parquet
data/01_00_map_extraction_audit.csv

All raw, source-specific, hierarchical, quality-case, and sensitivity-detail
objects remain temporary DuckDB tables and are not exported.

Source item IDs: invasive MAP 220052 and 225312; non-invasive MAP 220181.
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

HOURLY_OUTPUT_PATH = DATA_DIR / "01_00_map_hourly_trajectory.parquet"
SIX_HOUR_OUTPUT_PATH = DATA_DIR / "01_00_map_six_hour_summary.parquet"
FINAL_AUDIT_OUTPUT_PATH = DATA_DIR / "01_00_map_extraction_audit.csv"

EXPECTED_COHORT_N = 9305

INVASIVE_ITEMIDS = (220052, 225312)
NONINVASIVE_ITEMIDS = (220181,)
ALL_MAP_ITEMIDS = INVASIVE_ITEMIDS + NONINVASIVE_ITEMIDS

VALID_MAP_LOWER_EXCLUSIVE = 0.0
VALID_MAP_UPPER_EXCLUSIVE = 300.0
INVASIVE_EPISODE_MAX_GAP_MINUTES = 90.0

# Invasive timestamps with a cross-item range at or above this threshold
# are ambiguous and are excluded from the hierarchical series.
INVASIVE_WIDE_DISAGREEMENT_MMHG = 20.0

# Maximum duration for right-continuous LOCF support from one selected MAP point.
# The same 90-minute limit is retained for comparison with prior pairwise validation.
TEMPORAL_SUPPORT_MAX_GAP_MINUTES = 90.0
LOCF_MAX_CARRY_MINUTES = 90.0
LOCF_SENSITIVITY_CARRY_MINUTES = 60.0

HYPOTENSION_THRESHOLDS_MMHG = (65.0, 60.0, 55.0)

HOURLY_WINDOWS_N = 24
SIX_HOUR_WINDOWS_N = 4

BIN_ROWS = (
    ("H00_H06", 0.0, 6.0, 1),
    ("H06_H12", 6.0, 12.0, 2),
    ("H12_H18", 12.0, 18.0, 3),
    ("H18_H24", 18.0, 24.0, 4),
    ("H00_H24", 0.0, 24.0, 5),
)


class MapHierarchyError(RuntimeError):
    """Raised when a mandatory extraction or hierarchy invariant fails."""


def sql_path(path: Path) -> str:
    """Return an absolute DuckDB-safe path."""
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def validate_inputs() -> None:
    """Check canonical cohort and raw MIMIC-IV inputs."""
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
        raise MapHierarchyError(
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


def validate_cohort_and_itemids(con: duckdb.DuckDBPyConnection) -> None:
    """Validate the definitive cohort and selected MAP itemids."""
    cohort_n = con.execute(
        "SELECT COUNT(*) FROM cohort"
    ).fetchone()[0]

    if cohort_n != EXPECTED_COHORT_N:
        raise MapHierarchyError(
            f"Cohort contains {cohort_n:,} rows; "
            f"expected {EXPECTED_COHORT_N:,}."
        )

    duplicate_stays = con.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT stay_id
            FROM cohort
            GROUP BY stay_id
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]

    if duplicate_stays:
        raise MapHierarchyError(
            f"{duplicate_stays:,} duplicated stay_id group(s) in cohort."
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
        raise MapHierarchyError(
            f"{invalid_times:,} cohort rows have invalid observation times."
        )

    itemids = ",".join(str(x) for x in ALL_MAP_ITEMIDS)

    observed_itemids = con.execute(
        f"""
        SELECT COUNT(DISTINCT itemid)
        FROM d_items
        WHERE itemid IN ({itemids})
        """
    ).fetchone()[0]

    if observed_itemids != len(ALL_MAP_ITEMIDS):
        raise MapHierarchyError(
            f"Found {observed_itemids} of "
            f"{len(ALL_MAP_ITEMIDS)} expected MAP itemids."
        )


def create_audit_bins(con: duckdb.DuckDBPyConnection) -> None:
    """Create four trajectory bins and an overall 0–24-hour audit row."""
    values_sql = ",\n".join(
        f"('{label}', {start_hour}, {end_hour}, {audit_order})"
        for label, start_hour, end_hour, audit_order in BIN_ROWS
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE audit_bins (
            audit_bin VARCHAR,
            start_hour DOUBLE,
            end_hour DOUBLE,
            audit_order INTEGER
        );

        INSERT INTO audit_bins VALUES
        {values_sql}
        """
    )


def extract_raw_map(con: duckdb.DuckDBPyConnection) -> None:
    """Extract every numeric MAP row in the observed first 24 hours."""
    invasive_itemids = ",".join(str(x) for x in INVASIVE_ITEMIDS)
    all_itemids = ",".join(str(x) for x in ALL_MAP_ITEMIDS)

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE map_raw AS
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

            CASE
                WHEN ce.itemid IN ({invasive_itemids})
                    THEN 'INVASIVE'
                WHEN ce.itemid = {NONINVASIVE_ITEMIDS[0]}
                    THEN 'NON_INVASIVE'
                ELSE 'UNKNOWN'
            END AS map_source,

            ce.value AS original_value,
            ce.valuenum AS map_value,
            ce.valueuom,
            ce.warning,

            DATE_DIFF(
                'second',
                c.postoperative_time_zero,
                ce.charttime
            ) / 3600.0 AS hours_from_time_zero,

            CASE
                WHEN ce.charttime <
                     c.postoperative_time_zero + INTERVAL '6 hours'
                    THEN 'H00_H06'
                WHEN ce.charttime <
                     c.postoperative_time_zero + INTERVAL '12 hours'
                    THEN 'H06_H12'
                WHEN ce.charttime <
                     c.postoperative_time_zero + INTERVAL '18 hours'
                    THEN 'H12_H18'
                ELSE 'H18_H24'
            END AS trajectory_bin,

            CASE
                WHEN ce.valuenum > {VALID_MAP_LOWER_EXCLUSIVE}
                 AND ce.valuenum < {VALID_MAP_UPPER_EXCLUSIVE}
                    THEN 1
                ELSE 0
            END AS raw_valid_value_flag,

            CASE
                WHEN ce.valuenum <= {VALID_MAP_LOWER_EXCLUSIVE}
                    THEN 'INVALID_NON_POSITIVE'
                WHEN ce.valuenum >= {VALID_MAP_UPPER_EXCLUSIVE}
                    THEN 'INVALID_GE300'
                WHEN ce.valuenum < 20
                    THEN 'VALID_LT20'
                WHEN ce.valuenum < 40
                    THEN 'VALID_20_TO_39_99'
                WHEN ce.valuenum < 150
                    THEN 'VALID_40_TO_149_99'
                WHEN ce.valuenum < 200
                    THEN 'VALID_150_TO_199_99'
                ELSE 'VALID_200_TO_299_99'
            END AS raw_value_band

        FROM cohort AS c
        INNER JOIN chartevents AS ce
            ON c.stay_id = ce.stay_id

        WHERE ce.itemid IN ({all_itemids})
          AND ce.valuenum IS NOT NULL
          AND ce.charttime >= c.postoperative_time_zero
          AND ce.charttime < c.postoperative_time_zero + INTERVAL '24 hours'
          AND ce.charttime <= c.observation_end_time

        ORDER BY c.stay_id, ce.charttime, ce.itemid
        """
    )

    raw_n = con.execute(
        "SELECT COUNT(*) FROM map_raw"
    ).fetchone()[0]

    if raw_n == 0:
        raise MapHierarchyError(
            "No numeric MAP rows were extracted."
        )

    invalid_window_rows = con.execute(
        """
        SELECT COUNT(*)
        FROM map_raw
        WHERE hours_from_time_zero < 0
           OR hours_from_time_zero >= 24
           OR charttime > observation_end_time
           OR trajectory_bin IS NULL
           OR map_source NOT IN ('INVASIVE', 'NON_INVASIVE')
        """
    ).fetchone()[0]

    if invalid_window_rows:
        raise MapHierarchyError(
            f"{invalid_window_rows:,} raw MAP rows violate extraction rules."
        )


def create_clean_source_series(con: duckdb.DuckDBPyConnection) -> None:
    """
    Remove certainly invalid raw values and consolidate each source/charttime.

    The arithmetic mean is calculated only from valid raw values.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE map_source_time_audit AS
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
            charttime,
            trajectory_bin,
            hours_from_time_zero,
            map_source,

            COUNT(*) AS raw_rows_n,

            COUNT(*) FILTER (
                WHERE raw_valid_value_flag = 1
            ) AS valid_raw_rows_n,

            COUNT(*) FILTER (
                WHERE raw_valid_value_flag = 0
            ) AS invalid_raw_rows_n,

            COUNT(DISTINCT itemid) AS all_itemids_n,

            COUNT(DISTINCT itemid) FILTER (
                WHERE raw_valid_value_flag = 1
            ) AS valid_itemids_n,

            COUNT(DISTINCT map_value) FILTER (
                WHERE raw_valid_value_flag = 1
            ) AS valid_distinct_values_n,

            MIN(map_value) FILTER (
                WHERE raw_valid_value_flag = 1
            ) AS valid_minimum_value,

            MAX(map_value) FILTER (
                WHERE raw_valid_value_flag = 1
            ) AS valid_maximum_value,

            AVG(map_value) FILTER (
                WHERE raw_valid_value_flag = 1
            ) AS consolidated_map_value,

            MAX(map_value) FILTER (
                WHERE raw_valid_value_flag = 1
            )
            -
            MIN(map_value) FILTER (
                WHERE raw_valid_value_flag = 1
            ) AS valid_value_range,

            COUNT(*) FILTER (
                WHERE COALESCE(warning, 0) <> 0
                  AND raw_valid_value_flag = 1
            ) AS valid_warning_rows_n,

            STRING_AGG(
                CAST(itemid AS VARCHAR)
                || ':'
                || CAST(map_value AS VARCHAR),
                ' | '
                ORDER BY itemid, map_value
            ) AS all_itemid_value_pairs,

            STRING_AGG(
                CAST(itemid AS VARCHAR)
                || ':'
                || CAST(map_value AS VARCHAR),
                ' | '
                ORDER BY itemid, map_value
            ) FILTER (
                WHERE raw_valid_value_flag = 1
            ) AS valid_itemid_value_pairs,

            STRING_AGG(
                DISTINCT CAST(itemid AS VARCHAR),
                ' | '
                ORDER BY CAST(itemid AS VARCHAR)
            ) FILTER (
                WHERE raw_valid_value_flag = 1
            ) AS valid_itemids_present,

            MIN(storetime) FILTER (
                WHERE raw_valid_value_flag = 1
            ) AS first_valid_storetime,

            MAX(storetime) FILTER (
                WHERE raw_valid_value_flag = 1
            ) AS last_valid_storetime

        FROM map_raw

        GROUP BY
            subject_id,
            hadm_id,
            stay_id,
            surgery_group,
            sex,
            age_at_icu_admission,
            postoperative_time_zero,
            observation_end_time,
            observation_end_type,
            charttime,
            trajectory_bin,
            hours_from_time_zero,
            map_source
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE map_clean_by_source_base AS
        SELECT
            *,

            CASE
                WHEN valid_raw_rows_n = 1
                    THEN 'SINGLE_VALID_VALUE'

                WHEN valid_raw_rows_n > 1
                 AND valid_itemids_n = 1
                 AND valid_distinct_values_n = 1
                    THEN 'EXACT_DUPLICATE_SAME_ITEM'

                WHEN valid_raw_rows_n > 1
                 AND valid_itemids_n = 1
                 AND valid_distinct_values_n > 1
                    THEN 'CONFLICT_SAME_ITEM'

                WHEN valid_raw_rows_n > 1
                 AND valid_itemids_n > 1
                 AND valid_distinct_values_n = 1
                    THEN 'CROSS_ITEM_IDENTICAL'

                WHEN valid_raw_rows_n > 1
                 AND valid_itemids_n > 1
                 AND valid_value_range <= 3
                    THEN 'CROSS_ITEM_CLOSE'

                WHEN valid_raw_rows_n > 1
                 AND valid_itemids_n > 1
                    THEN 'CROSS_ITEM_DISCORDANT'

                ELSE 'UNCLASSIFIED'
            END AS source_time_quality_class,

            CASE
                WHEN map_source = 'INVASIVE'
                 AND valid_raw_rows_n > 1
                 AND valid_value_range >= {INVASIVE_WIDE_DISAGREEMENT_MMHG}
                    THEN 1
                ELSE 0
            END AS wide_invasive_disagreement_flag

        FROM map_source_time_audit
        WHERE valid_raw_rows_n > 0
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE map_clean_by_source AS
        SELECT
            *,

            LAG(consolidated_map_value) OVER (
                PARTITION BY stay_id, map_source
                ORDER BY charttime
            ) AS previous_source_map_value,

            LEAD(consolidated_map_value) OVER (
                PARTITION BY stay_id, map_source
                ORDER BY charttime
            ) AS next_source_map_value,

            DATE_DIFF(
                'second',
                LAG(charttime) OVER (
                    PARTITION BY stay_id, map_source
                    ORDER BY charttime
                ),
                charttime
            ) / 60.0 AS previous_source_gap_minutes,

            DATE_DIFF(
                'second',
                charttime,
                LEAD(charttime) OVER (
                    PARTITION BY stay_id, map_source
                    ORDER BY charttime
                )
            ) / 60.0 AS next_source_gap_minutes

        FROM map_clean_by_source_base

        ORDER BY stay_id, charttime, map_source
        """
    )

    duplicate_keys = con.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT stay_id, charttime, map_source
            FROM map_clean_by_source
            GROUP BY stay_id, charttime, map_source
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]

    if duplicate_keys:
        raise MapHierarchyError(
            f"{duplicate_keys:,} duplicate source/timestamp keys remain."
        )


def create_invasive_episodes(con: duckdb.DuckDBPyConnection) -> None:
    """
    Construct invasive monitoring episodes from all physiologically valid
    invasive timestamps, including timestamps with cross-item disagreement.

    Monitoring coverage and value acceptance are deliberately separated:
    ambiguous timestamps define line presence but do not contribute a MAP value.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE invasive_monitoring_points_numbered AS
        WITH ordered AS (
            SELECT
                *,

                LAG(charttime) OVER (
                    PARTITION BY stay_id
                    ORDER BY charttime
                ) AS previous_invasive_charttime

            FROM map_clean_by_source
            WHERE map_source = 'INVASIVE'
        ),

        marked AS (
            SELECT
                *,

                CASE
                    WHEN previous_invasive_charttime IS NULL
                        THEN NULL
                    ELSE DATE_DIFF(
                        'second',
                        previous_invasive_charttime,
                        charttime
                    ) / 60.0
                END AS gap_from_previous_invasive_minutes,

                CASE
                    WHEN previous_invasive_charttime IS NULL
                        THEN 1
                    WHEN DATE_DIFF(
                            'second',
                            previous_invasive_charttime,
                            charttime
                         ) / 60.0
                         > {INVASIVE_EPISODE_MAX_GAP_MINUTES}
                        THEN 1
                    ELSE 0
                END AS new_invasive_episode_flag

            FROM ordered
        )

        SELECT
            *,

            SUM(new_invasive_episode_flag) OVER (
                PARTITION BY stay_id
                ORDER BY charttime
                ROWS BETWEEN UNBOUNDED PRECEDING
                         AND CURRENT ROW
            ) AS invasive_episode_id

        FROM marked
        ORDER BY stay_id, charttime
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE invasive_episode_bounds AS
        SELECT
            subject_id,
            hadm_id,
            stay_id,
            invasive_episode_id,
            MIN(charttime) AS invasive_episode_start,
            MAX(charttime) AS invasive_episode_end,
            COUNT(*) AS invasive_monitoring_points_n,
            COUNT(*) FILTER (
                WHERE wide_invasive_disagreement_flag = 0
            ) AS accepted_invasive_values_n,
            COUNT(*) FILTER (
                WHERE wide_invasive_disagreement_flag = 1
            ) AS ambiguous_invasive_timestamps_n,
            DATE_DIFF(
                'second',
                MIN(charttime),
                MAX(charttime)
            ) / 3600.0 AS invasive_episode_duration_hours,
            MAX(gap_from_previous_invasive_minutes) FILTER (
                WHERE new_invasive_episode_flag = 0
            ) AS maximum_internal_gap_minutes
        FROM invasive_monitoring_points_numbered
        GROUP BY
            subject_id,
            hadm_id,
            stay_id,
            invasive_episode_id
        ORDER BY stay_id, invasive_episode_id
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE invasive_values_accepted_numbered AS
        SELECT *
        FROM invasive_monitoring_points_numbered
        WHERE wide_invasive_disagreement_flag = 0
        ORDER BY stay_id, charttime
        """
    )

    invalid_internal_gap = con.execute(
        f"""
        SELECT COUNT(*)
        FROM invasive_monitoring_points_numbered
        WHERE new_invasive_episode_flag = 0
          AND gap_from_previous_invasive_minutes
              > {INVASIVE_EPISODE_MAX_GAP_MINUTES}
        """
    ).fetchone()[0]

    if invalid_internal_gap:
        raise MapHierarchyError(
            f"{invalid_internal_gap:,} invasive monitoring points violate "
            "episode rules."
        )

    ambiguous_value_retained = con.execute(
        """
        SELECT COUNT(*)
        FROM invasive_values_accepted_numbered
        WHERE wide_invasive_disagreement_flag = 1
        """
    ).fetchone()[0]

    if ambiguous_value_retained:
        raise MapHierarchyError(
            f"{ambiguous_value_retained:,} ambiguous invasive timestamps "
            "entered the accepted value series."
        )

    all_invasive_points_n = con.execute(
        """
        SELECT COUNT(*)
        FROM map_clean_by_source
        WHERE map_source = 'INVASIVE'
        """
    ).fetchone()[0]

    monitoring_points_n = con.execute(
        "SELECT COUNT(*) FROM invasive_monitoring_points_numbered"
    ).fetchone()[0]

    if monitoring_points_n != all_invasive_points_n:
        raise MapHierarchyError(
            f"Monitoring series has {monitoring_points_n:,} points; "
            f"expected {all_invasive_points_n:,}."
        )


def create_hierarchical_series(con: duckdb.DuckDBPyConnection) -> None:
    """Apply invasive preference using monitoring coverage and accepted values."""
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE nibp_hierarchy_audit AS
        WITH flagged AS (
            SELECT
                n.*,

                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM map_clean_by_source AS a
                        WHERE a.stay_id = n.stay_id
                          AND a.charttime = n.charttime
                          AND a.map_source = 'INVASIVE'
                          AND a.wide_invasive_disagreement_flag = 1
                    )
                        THEN 1
                    ELSE 0
                END AS same_time_ambiguous_invasive_flag,

                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM invasive_episode_bounds AS e
                        WHERE e.stay_id = n.stay_id
                          AND n.charttime >= e.invasive_episode_start
                          AND n.charttime <= e.invasive_episode_end
                    )
                        THEN 1
                    ELSE 0
                END AS inside_invasive_monitoring_episode_flag,

                (
                    SELECT MIN(e.invasive_episode_id)
                    FROM invasive_episode_bounds AS e
                    WHERE e.stay_id = n.stay_id
                      AND n.charttime >= e.invasive_episode_start
                      AND n.charttime <= e.invasive_episode_end
                ) AS covering_invasive_episode_id

            FROM map_clean_by_source AS n
            WHERE n.map_source = 'NON_INVASIVE'
        )

        SELECT
            *,

            CASE
                WHEN inside_invasive_monitoring_episode_flag = 1
                 AND same_time_ambiguous_invasive_flag = 0
                    THEN 1
                ELSE 0
            END AS excluded_inside_invasive_episode_flag,

            CASE
                WHEN same_time_ambiguous_invasive_flag = 1
                    THEN 'SAME_TIME_AMBIGUOUS_INVASIVE'
                WHEN inside_invasive_monitoring_episode_flag = 0
                    THEN 'OUTSIDE_INVASIVE_MONITORING_EPISODE'
                ELSE 'EXCLUDED_INSIDE_INVASIVE_MONITORING_EPISODE'
            END AS nibp_hierarchy_status

        FROM flagged
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE map_hierarchical_series AS
        SELECT
            i.subject_id,
            i.hadm_id,
            i.stay_id,
            i.surgery_group,
            i.sex,
            i.age_at_icu_admission,
            i.postoperative_time_zero,
            i.observation_end_time,
            i.observation_end_type,
            i.charttime,
            i.trajectory_bin,
            i.hours_from_time_zero,

            i.consolidated_map_value AS map_value,
            'INVASIVE' AS map_source_used,
            'ACCEPTED_INVASIVE' AS source_selection_reason,

            i.invasive_episode_id,
            i.gap_from_previous_invasive_minutes,

            i.raw_rows_n AS source_time_raw_rows_n,
            i.valid_raw_rows_n AS source_time_valid_rows_n,
            i.invalid_raw_rows_n AS source_time_invalid_rows_n,
            i.valid_itemids_present AS source_itemids_present,
            i.valid_value_range AS source_time_value_range,
            i.source_time_quality_class,
            i.wide_invasive_disagreement_flag,
            i.valid_warning_rows_n,

            0 AS nibp_fallback_flag,
            0 AS same_time_ambiguous_invasive_fallback_flag

        FROM invasive_values_accepted_numbered AS i

        UNION ALL

        SELECT
            n.subject_id,
            n.hadm_id,
            n.stay_id,
            n.surgery_group,
            n.sex,
            n.age_at_icu_admission,
            n.postoperative_time_zero,
            n.observation_end_time,
            n.observation_end_type,
            n.charttime,
            n.trajectory_bin,
            n.hours_from_time_zero,

            n.consolidated_map_value AS map_value,
            'NON_INVASIVE' AS map_source_used,

            CASE
                WHEN n.same_time_ambiguous_invasive_flag = 1
                    THEN 'NIBP_SAME_TIME_AMBIGUOUS_INVASIVE'
                ELSE 'NIBP_OUTSIDE_INVASIVE_MONITORING_EPISODE'
            END AS source_selection_reason,

            NULL::BIGINT AS invasive_episode_id,
            NULL::DOUBLE AS gap_from_previous_invasive_minutes,

            n.raw_rows_n AS source_time_raw_rows_n,
            n.valid_raw_rows_n AS source_time_valid_rows_n,
            n.invalid_raw_rows_n AS source_time_invalid_rows_n,
            n.valid_itemids_present AS source_itemids_present,
            n.valid_value_range AS source_time_value_range,
            n.source_time_quality_class,
            0 AS wide_invasive_disagreement_flag,
            n.valid_warning_rows_n,

            1 AS nibp_fallback_flag,
            n.same_time_ambiguous_invasive_flag
                AS same_time_ambiguous_invasive_fallback_flag

        FROM nibp_hierarchy_audit AS n
        WHERE n.excluded_inside_invasive_episode_flag = 0

        ORDER BY stay_id, charttime, map_source_used
        """
    )

    duplicate_hierarchy_times = con.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT stay_id, charttime
            FROM map_hierarchical_series
            GROUP BY stay_id, charttime
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]

    if duplicate_hierarchy_times:
        raise MapHierarchyError(
            f"{duplicate_hierarchy_times:,} hierarchical timestamps contain both sources."
        )

    invalid_values = con.execute(
        f"""
        SELECT COUNT(*)
        FROM map_hierarchical_series
        WHERE map_value <= {VALID_MAP_LOWER_EXCLUSIVE}
           OR map_value >= {VALID_MAP_UPPER_EXCLUSIVE}
        """
    ).fetchone()[0]

    if invalid_values:
        raise MapHierarchyError(
            f"{invalid_values:,} invalid MAP values remain in hierarchy."
        )

    ambiguous_invasive_retained = con.execute(
        """
        SELECT COUNT(*)
        FROM map_hierarchical_series AS h
        INNER JOIN map_clean_by_source AS a
            ON h.stay_id = a.stay_id
           AND h.charttime = a.charttime
        WHERE h.map_source_used = 'INVASIVE'
          AND a.map_source = 'INVASIVE'
          AND a.wide_invasive_disagreement_flag = 1
        """
    ).fetchone()[0]

    if ambiguous_invasive_retained:
        raise MapHierarchyError(
            f"{ambiguous_invasive_retained:,} ambiguous invasive points remain in hierarchy."
        )

def create_source_patterns(con: duckdb.DuckDBPyConnection) -> None:
    """Create one patient-level record describing the final source sequence."""
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE hierarchical_sequence AS
        SELECT
            h.*,

            LAG(map_source_used) OVER (
                PARTITION BY stay_id
                ORDER BY charttime
            ) AS previous_source_used

        FROM map_hierarchical_series AS h
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE map_source_patterns AS
        WITH sequence_aggregated AS (
            SELECT
                stay_id,

                COUNT(*) AS hierarchical_points_n,

                COUNT(*) FILTER (
                    WHERE map_source_used = 'INVASIVE'
                ) AS invasive_points_used_n,

                COUNT(*) FILTER (
                    WHERE map_source_used = 'NON_INVASIVE'
                ) AS nibp_points_used_n,

                COUNT(*) FILTER (
                    WHERE previous_source_used IS NOT NULL
                      AND previous_source_used <> map_source_used
                ) AS source_transition_count,

                MIN(charttime) FILTER (
                    WHERE map_source_used = 'INVASIVE'
                ) AS first_invasive_time,

                MAX(charttime) FILTER (
                    WHERE map_source_used = 'INVASIVE'
                ) AS last_invasive_time,

                MIN(charttime) FILTER (
                    WHERE map_source_used = 'NON_INVASIVE'
                ) AS first_nibp_time,

                MAX(charttime) FILTER (
                    WHERE map_source_used = 'NON_INVASIVE'
                ) AS last_nibp_time,

                ARG_MIN(map_source_used, charttime)
                    AS first_source_used,

                ARG_MAX(map_source_used, charttime)
                    AS last_source_used

            FROM hierarchical_sequence
            GROUP BY stay_id
        ),

        episode_aggregated AS (
            SELECT
                stay_id,
                COUNT(*) AS invasive_episodes_n
            FROM invasive_episode_bounds
            GROUP BY stay_id
        ),

        combined AS (
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

                COALESCE(s.hierarchical_points_n, 0)
                    AS hierarchical_points_n,

                COALESCE(s.invasive_points_used_n, 0)
                    AS invasive_points_used_n,

                COALESCE(s.nibp_points_used_n, 0)
                    AS nibp_points_used_n,

                COALESCE(s.source_transition_count, 0)
                    AS source_transition_count,

                s.first_invasive_time,
                s.last_invasive_time,
                s.first_nibp_time,
                s.last_nibp_time,
                s.first_source_used,
                s.last_source_used,

                COALESCE(e.invasive_episodes_n, 0)
                    AS invasive_episodes_n

            FROM cohort AS c
            LEFT JOIN sequence_aggregated AS s
                ON c.stay_id = s.stay_id
            LEFT JOIN episode_aggregated AS e
                ON c.stay_id = e.stay_id
        )

        SELECT
            *,

            CASE
                WHEN hierarchical_points_n = 0
                    THEN 'NO_MAP'

                WHEN invasive_points_used_n > 0
                 AND nibp_points_used_n = 0
                    THEN 'INVASIVE_ONLY'

                WHEN invasive_points_used_n = 0
                 AND nibp_points_used_n > 0
                    THEN 'NON_INVASIVE_ONLY'

                WHEN invasive_points_used_n > 0
                 AND nibp_points_used_n > 0
                 AND source_transition_count = 1
                 AND first_source_used = 'INVASIVE'
                 AND last_source_used = 'NON_INVASIVE'
                    THEN 'INVASIVE_THEN_NON_INVASIVE'

                WHEN invasive_points_used_n > 0
                 AND nibp_points_used_n > 0
                 AND source_transition_count = 1
                 AND first_source_used = 'NON_INVASIVE'
                 AND last_source_used = 'INVASIVE'
                    THEN 'NON_INVASIVE_THEN_INVASIVE'

                ELSE 'MIXED_MULTIPLE_TRANSITIONS'
            END AS hierarchical_source_pattern_0_24h,

            CASE
                WHEN observation_end_time >=
                     postoperative_time_zero + INTERVAL '24 hours'
                    THEN 1
                ELSE 0
            END AS fully_observable_24h_flag

        FROM combined
        ORDER BY stay_id
        """
    )

    n = con.execute(
        "SELECT COUNT(*) FROM map_source_patterns"
    ).fetchone()[0]

    if n != EXPECTED_COHORT_N:
        raise MapHierarchyError(
            f"Source pattern table has {n:,} rows; "
            f"expected {EXPECTED_COHORT_N:,}."
        )

def create_hierarchy_audit(con: duckdb.DuckDBPyConnection) -> None:
    """Audit coverage, cleaning, source use, and episode structure by bin."""
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE denominator AS
        SELECT
            b.audit_bin,
            b.start_hour,
            b.end_hour,
            b.audit_order,

            COUNT(*) FILTER (
                WHERE c.observation_end_time >
                      c.postoperative_time_zero
                      + b.start_hour * INTERVAL '1 hour'
            ) AS eligible_patients_n,

            COUNT(*) FILTER (
                WHERE c.observation_end_time >=
                      c.postoperative_time_zero
                      + b.end_hour * INTERVAL '1 hour'
            ) AS fully_observable_patients_n

        FROM audit_bins AS b
        CROSS JOIN cohort AS c

        GROUP BY
            b.audit_bin,
            b.start_hour,
            b.end_hour,
            b.audit_order
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE hierarchy_patient_bin AS
        SELECT
            b.audit_bin,
            b.audit_order,
            c.stay_id,

            COUNT(h.map_value) AS hierarchy_points_n,

            COUNT(h.map_value) FILTER (
                WHERE h.map_source_used = 'INVASIVE'
            ) AS invasive_points_used_n,

            COUNT(h.map_value) FILTER (
                WHERE h.map_source_used = 'NON_INVASIVE'
            ) AS nibp_points_used_n

        FROM audit_bins AS b
        INNER JOIN cohort AS c
            ON c.observation_end_time >
               c.postoperative_time_zero
               + b.start_hour * INTERVAL '1 hour'

        LEFT JOIN map_hierarchical_series AS h
            ON c.stay_id = h.stay_id
           AND h.hours_from_time_zero >= b.start_hour
           AND h.hours_from_time_zero < b.end_hour

        GROUP BY
            b.audit_bin,
            b.audit_order,
            c.stay_id
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE hierarchy_patient_summary AS
        SELECT
            audit_bin,
            audit_order,

            COUNT(*) FILTER (
                WHERE hierarchy_points_n > 0
            ) AS any_hierarchical_map_patients_n,

            COUNT(*) FILTER (
                WHERE invasive_points_used_n > 0
                  AND nibp_points_used_n = 0
            ) AS invasive_only_patients_n,

            COUNT(*) FILTER (
                WHERE invasive_points_used_n = 0
                  AND nibp_points_used_n > 0
            ) AS nibp_only_patients_n,

            COUNT(*) FILTER (
                WHERE invasive_points_used_n > 0
                  AND nibp_points_used_n > 0
            ) AS both_sources_used_patients_n,

            COUNT(*) FILTER (
                WHERE hierarchy_points_n = 0
            ) AS no_hierarchical_map_patients_n,

            MEDIAN(NULLIF(hierarchy_points_n, 0))
                AS hierarchy_points_per_patient_median,

            MEDIAN(NULLIF(invasive_points_used_n, 0))
                AS invasive_points_per_patient_median,

            MEDIAN(NULLIF(nibp_points_used_n, 0))
                AS nibp_points_per_patient_median

        FROM hierarchy_patient_bin

        GROUP BY audit_bin, audit_order
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE hierarchy_point_summary AS
        SELECT
            b.audit_bin,
            b.audit_order,

            COUNT(h.map_value) AS hierarchical_points_n,

            COUNT(h.map_value) FILTER (
                WHERE h.map_source_used = 'INVASIVE'
            ) AS invasive_points_used_n,

            COUNT(h.map_value) FILTER (
                WHERE h.map_source_used = 'NON_INVASIVE'
            ) AS nibp_points_used_n,

            COUNT(h.map_value) FILTER (
                WHERE h.valid_warning_rows_n > 0
            ) AS points_with_warning_n,

            MIN(h.map_value) AS hierarchical_map_min,
            QUANTILE_CONT(h.map_value, 0.01) AS hierarchical_map_p01,
            MEDIAN(h.map_value) AS hierarchical_map_median,
            QUANTILE_CONT(h.map_value, 0.99) AS hierarchical_map_p99,
            MAX(h.map_value) AS hierarchical_map_max

        FROM audit_bins AS b
        LEFT JOIN map_hierarchical_series AS h
            ON h.hours_from_time_zero >= b.start_hour
           AND h.hours_from_time_zero < b.end_hour

        GROUP BY b.audit_bin, b.audit_order
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE nibp_bin_summary AS
        SELECT
            b.audit_bin,
            b.audit_order,

            COUNT(n.consolidated_map_value)
                AS valid_nibp_candidate_points_n,

            COUNT(n.consolidated_map_value) FILTER (
                WHERE n.excluded_inside_invasive_episode_flag = 1
            ) AS nibp_points_excluded_inside_invasive_episode_n,

            COUNT(n.consolidated_map_value) FILTER (
                WHERE n.excluded_inside_invasive_episode_flag = 0
            ) AS nibp_points_retained_as_fallback_n,

            COUNT(DISTINCT n.stay_id) FILTER (
                WHERE n.excluded_inside_invasive_episode_flag = 0
            ) AS patients_with_nibp_fallback_n,

            COUNT(n.consolidated_map_value) FILTER (
                WHERE n.same_time_ambiguous_invasive_flag = 1
                  AND n.excluded_inside_invasive_episode_flag = 0
            ) AS nibp_same_time_ambiguous_rescue_n,

            COUNT(DISTINCT n.stay_id) FILTER (
                WHERE n.same_time_ambiguous_invasive_flag = 1
                  AND n.excluded_inside_invasive_episode_flag = 0
            ) AS patients_with_same_time_ambiguous_rescue_n

        FROM audit_bins AS b
        LEFT JOIN nibp_hierarchy_audit AS n
            ON n.hours_from_time_zero >= b.start_hour
           AND n.hours_from_time_zero < b.end_hour

        GROUP BY b.audit_bin, b.audit_order
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE ambiguous_invasive_bin_summary AS
        SELECT
            b.audit_bin,
            b.audit_order,

            COUNT(a.consolidated_map_value)
                AS ambiguous_invasive_timestamps_excluded_n,

            COUNT(DISTINCT a.stay_id)
                AS patients_with_ambiguous_invasive_timestamp_n,

            COUNT(a.consolidated_map_value) FILTER (
                WHERE EXISTS (
                    SELECT 1
                    FROM map_clean_by_source AS n
                    WHERE n.stay_id = a.stay_id
                      AND n.charttime = a.charttime
                      AND n.map_source = 'NON_INVASIVE'
                )
            ) AS ambiguous_invasive_with_same_time_nibp_n

        FROM audit_bins AS b
        LEFT JOIN map_clean_by_source AS a
            ON a.hours_from_time_zero >= b.start_hour
           AND a.hours_from_time_zero < b.end_hour
           AND a.map_source = 'INVASIVE'
           AND a.wide_invasive_disagreement_flag = 1

        GROUP BY b.audit_bin, b.audit_order
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE cleaning_bin_summary AS
        SELECT
            b.audit_bin,
            b.audit_order,

            COUNT(r.map_value) AS raw_rows_n,

            COUNT(r.map_value) FILTER (
                WHERE r.raw_valid_value_flag = 0
            ) AS invalid_raw_rows_removed_n,

            COUNT(r.map_value) FILTER (
                WHERE r.raw_valid_value_flag = 0
                  AND r.map_source = 'INVASIVE'
            ) AS invalid_invasive_raw_rows_removed_n,

            COUNT(r.map_value) FILTER (
                WHERE r.raw_valid_value_flag = 0
                  AND r.map_source = 'NON_INVASIVE'
            ) AS invalid_nibp_raw_rows_removed_n

        FROM audit_bins AS b
        LEFT JOIN map_raw AS r
            ON r.hours_from_time_zero >= b.start_hour
           AND r.hours_from_time_zero < b.end_hour

        GROUP BY b.audit_bin, b.audit_order
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE hierarchy_gap_summary AS
        WITH ordered AS (
            SELECT
                b.audit_bin,
                b.audit_order,
                h.stay_id,
                h.charttime,

                LAG(h.charttime) OVER (
                    PARTITION BY b.audit_bin, h.stay_id
                    ORDER BY h.charttime
                ) AS previous_charttime

            FROM audit_bins AS b
            INNER JOIN map_hierarchical_series AS h
                ON h.hours_from_time_zero >= b.start_hour
               AND h.hours_from_time_zero < b.end_hour
        ),

        gaps AS (
            SELECT
                audit_bin,
                audit_order,
                stay_id,

                DATE_DIFF(
                    'second',
                    previous_charttime,
                    charttime
                ) / 60.0 AS gap_minutes

            FROM ordered
            WHERE previous_charttime IS NOT NULL
              AND charttime > previous_charttime
        )

        SELECT
            audit_bin,
            audit_order,
            COUNT(*) AS positive_gaps_n,
            MEDIAN(gap_minutes) AS hierarchy_gap_median_minutes,
            QUANTILE_CONT(gap_minutes, 0.90)
                AS hierarchy_gap_p90_minutes,
            QUANTILE_CONT(gap_minutes, 0.95)
                AS hierarchy_gap_p95_minutes,
            QUANTILE_CONT(gap_minutes, 0.99)
                AS hierarchy_gap_p99_minutes,
            COUNT(*) FILTER (
                WHERE gap_minutes > 90
            ) AS hierarchy_gaps_gt90_minutes_n

        FROM gaps

        GROUP BY audit_bin, audit_order
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE map_hierarchy_audit AS
        SELECT
            d.audit_order,
            d.audit_bin,
            d.start_hour,
            d.end_hour,
            d.eligible_patients_n,
            d.fully_observable_patients_n,

            COALESCE(p.any_hierarchical_map_patients_n, 0)
                AS any_hierarchical_map_patients_n,

            ROUND(
                100.0
                * COALESCE(p.any_hierarchical_map_patients_n, 0)
                / NULLIF(d.eligible_patients_n, 0),
                2
            ) AS hierarchical_map_coverage_pct,

            COALESCE(p.invasive_only_patients_n, 0)
                AS invasive_only_patients_n,

            COALESCE(p.nibp_only_patients_n, 0)
                AS nibp_only_patients_n,

            COALESCE(p.both_sources_used_patients_n, 0)
                AS both_sources_used_patients_n,

            COALESCE(p.no_hierarchical_map_patients_n, 0)
                AS no_hierarchical_map_patients_n,

            p.hierarchy_points_per_patient_median,
            p.invasive_points_per_patient_median,
            p.nibp_points_per_patient_median,

            COALESCE(h.hierarchical_points_n, 0)
                AS hierarchical_points_n,

            COALESCE(h.invasive_points_used_n, 0)
                AS invasive_points_used_n,

            COALESCE(h.nibp_points_used_n, 0)
                AS nibp_points_used_n,

            COALESCE(n.valid_nibp_candidate_points_n, 0)
                AS valid_nibp_candidate_points_n,

            COALESCE(
                n.nibp_points_excluded_inside_invasive_episode_n,
                0
            ) AS nibp_points_excluded_inside_invasive_episode_n,

            COALESCE(n.nibp_points_retained_as_fallback_n, 0)
                AS nibp_points_retained_as_fallback_n,

            COALESCE(n.patients_with_nibp_fallback_n, 0)
                AS patients_with_nibp_fallback_n,

            COALESCE(n.nibp_same_time_ambiguous_rescue_n, 0)
                AS nibp_same_time_ambiguous_rescue_n,

            COALESCE(n.patients_with_same_time_ambiguous_rescue_n, 0)
                AS patients_with_same_time_ambiguous_rescue_n,

            COALESCE(a.ambiguous_invasive_timestamps_excluded_n, 0)
                AS ambiguous_invasive_timestamps_excluded_n,

            COALESCE(a.patients_with_ambiguous_invasive_timestamp_n, 0)
                AS patients_with_ambiguous_invasive_timestamp_n,

            COALESCE(a.ambiguous_invasive_with_same_time_nibp_n, 0)
                AS ambiguous_invasive_with_same_time_nibp_n,

            COALESCE(c.raw_rows_n, 0)
                AS raw_rows_n,

            COALESCE(c.invalid_raw_rows_removed_n, 0)
                AS invalid_raw_rows_removed_n,

            COALESCE(c.invalid_invasive_raw_rows_removed_n, 0)
                AS invalid_invasive_raw_rows_removed_n,

            COALESCE(c.invalid_nibp_raw_rows_removed_n, 0)
                AS invalid_nibp_raw_rows_removed_n,

            COALESCE(h.points_with_warning_n, 0)
                AS points_with_warning_n,

            h.hierarchical_map_min,
            h.hierarchical_map_p01,
            h.hierarchical_map_median,
            h.hierarchical_map_p99,
            h.hierarchical_map_max,

            g.positive_gaps_n,
            g.hierarchy_gap_median_minutes,
            g.hierarchy_gap_p90_minutes,
            g.hierarchy_gap_p95_minutes,
            g.hierarchy_gap_p99_minutes,
            g.hierarchy_gaps_gt90_minutes_n

        FROM denominator AS d
        LEFT JOIN hierarchy_patient_summary AS p
            USING (audit_bin, audit_order)
        LEFT JOIN hierarchy_point_summary AS h
            USING (audit_bin, audit_order)
        LEFT JOIN nibp_bin_summary AS n
            USING (audit_bin, audit_order)
        LEFT JOIN ambiguous_invasive_bin_summary AS a
            USING (audit_bin, audit_order)
        LEFT JOIN cleaning_bin_summary AS c
            USING (audit_bin, audit_order)
        LEFT JOIN hierarchy_gap_summary AS g
            USING (audit_bin, audit_order)

        ORDER BY d.audit_order
        """
    )


def create_quality_cases(con: duckdb.DuckDBPyConnection) -> None:
    """Create focused records documenting exclusions and fallback recovery."""
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE map_quality_cases AS
        SELECT
            'NO_VALID_MAP_PATIENT' AS record_type,
            p.subject_id,
            p.hadm_id,
            p.stay_id,
            p.surgery_group,
            NULL::VARCHAR AS map_source,
            NULL::TIMESTAMP AS charttime,
            NULL::VARCHAR AS trajectory_bin,
            NULL::BIGINT AS itemid,
            NULL::DOUBLE AS raw_map_value,
            NULL::DOUBLE AS consolidated_map_value,
            NULL::DOUBLE AS source_time_value_range,
            NULL::VARCHAR AS quality_class,
            'No valid hierarchical MAP remained in the observed 0–24-hour window.' AS details

        FROM map_source_patterns AS p
        WHERE p.hierarchical_source_pattern_0_24h = 'NO_MAP'

        UNION ALL

        SELECT
            'INVALID_RAW_VALUE_REMOVED' AS record_type,
            r.subject_id,
            r.hadm_id,
            r.stay_id,
            r.surgery_group,
            r.map_source,
            r.charttime,
            r.trajectory_bin,
            r.itemid,
            r.map_value AS raw_map_value,
            NULL::DOUBLE AS consolidated_map_value,
            NULL::DOUBLE AS source_time_value_range,
            r.raw_value_band AS quality_class,
            'Raw MAP removed because the accepted validity rule is 0 < MAP < 300 mmHg.' AS details

        FROM map_raw AS r
        WHERE r.raw_valid_value_flag = 0

        UNION ALL

        SELECT
            'AMBIGUOUS_INVASIVE_TIMESTAMP_EXCLUDED' AS record_type,
            d.subject_id,
            d.hadm_id,
            d.stay_id,
            d.surgery_group,
            d.map_source,
            d.charttime,
            d.trajectory_bin,
            NULL::BIGINT AS itemid,
            NULL::DOUBLE AS raw_map_value,
            d.consolidated_map_value,
            d.valid_value_range AS source_time_value_range,
            d.source_time_quality_class AS quality_class,
            d.valid_itemid_value_pairs AS details

        FROM map_clean_by_source AS d
        WHERE d.map_source = 'INVASIVE'
          AND d.wide_invasive_disagreement_flag = 1

        UNION ALL

        SELECT
            'NIBP_RESCUE_AT_AMBIGUOUS_INVASIVE_TIME' AS record_type,
            n.subject_id,
            n.hadm_id,
            n.stay_id,
            n.surgery_group,
            n.map_source,
            n.charttime,
            n.trajectory_bin,
            NULL::BIGINT AS itemid,
            NULL::DOUBLE AS raw_map_value,
            n.consolidated_map_value,
            n.valid_value_range AS source_time_value_range,
            n.source_time_quality_class AS quality_class,
            'Valid NIBP retained at the exact charttime of an excluded ambiguous invasive timestamp.' AS details

        FROM nibp_hierarchy_audit AS n
        WHERE n.same_time_ambiguous_invasive_flag = 1
          AND n.excluded_inside_invasive_episode_flag = 0

        ORDER BY record_type, stay_id, charttime
        """
    )


def create_temporal_windows(con: duckdb.DuckDBPyConnection) -> None:
    """Create the 24 hourly and four six-hour postoperative windows."""
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

        ORDER BY resolution, window_index
        """
    )

    rows_n = con.execute(
        "SELECT COUNT(*) FROM temporal_windows"
    ).fetchone()[0]

    expected_n = HOURLY_WINDOWS_N + SIX_HOUR_WINDOWS_N
    if rows_n != expected_n:
        raise MapHierarchyError(
            f"Temporal-window table has {rows_n:,} rows; expected {expected_n:,}."
        )


def create_patient_windows(con: duckdb.DuckDBPyConnection) -> None:
    """Create a complete patient-by-window grid with true observability."""
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE patient_temporal_windows AS
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


def create_temporal_support_segments(con: duckdb.DuckDBPyConnection) -> None:
    """
    Identify intervals supported by consecutive hierarchical MAP observations.

    A segment is support-eligible when its endpoints are no more than 90 minutes
    apart and it does not cross an excluded ambiguous invasive timestamp without
    a same-time NIBP rescue. Values are not interpolated.
    """
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE unresolved_ambiguous_invasive_times AS
        SELECT
            a.subject_id,
            a.hadm_id,
            a.stay_id,
            a.charttime
        FROM map_clean_by_source AS a
        WHERE a.map_source = 'INVASIVE'
          AND a.wide_invasive_disagreement_flag = 1
          AND NOT EXISTS (
              SELECT 1
              FROM map_hierarchical_series AS h
              WHERE h.stay_id = a.stay_id
                AND h.charttime = a.charttime
          )
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE map_temporal_support_segments AS
        WITH ordered AS (
            SELECT
                h.*,
                LEAD(h.charttime) OVER (
                    PARTITION BY h.stay_id
                    ORDER BY h.charttime
                ) AS next_charttime,
                LEAD(h.map_value) OVER (
                    PARTITION BY h.stay_id
                    ORDER BY h.charttime
                ) AS next_map_value,
                LEAD(h.map_source_used) OVER (
                    PARTITION BY h.stay_id
                    ORDER BY h.charttime
                ) AS next_map_source_used
            FROM map_hierarchical_series AS h
        ),

        classified AS (
            SELECT
                subject_id,
                hadm_id,
                stay_id,
                charttime AS segment_start_time,
                next_charttime AS segment_end_time,
                map_value AS segment_start_value,
                next_map_value AS segment_end_value,
                map_source_used AS segment_start_source,
                next_map_source_used AS segment_end_source,
                DATE_DIFF(
                    'second',
                    charttime,
                    next_charttime
                ) / 60.0 AS segment_gap_minutes,
                CASE
                    WHEN map_source_used <> next_map_source_used
                        THEN 1
                    ELSE 0
                END AS cross_source_segment_flag,
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM unresolved_ambiguous_invasive_times AS a
                        WHERE a.stay_id = ordered.stay_id
                          AND a.charttime > ordered.charttime
                          AND a.charttime < ordered.next_charttime
                    )
                        THEN 1
                    ELSE 0
                END AS unresolved_ambiguous_blocker_flag
            FROM ordered
            WHERE next_charttime IS NOT NULL
        )

        SELECT
            *,
            CASE
                WHEN segment_gap_minutes > 0
                 AND segment_gap_minutes <= {TEMPORAL_SUPPORT_MAX_GAP_MINUTES}
                 AND unresolved_ambiguous_blocker_flag = 0
                    THEN 1
                ELSE 0
            END AS support_eligible_flag
        FROM classified
        ORDER BY stay_id, segment_start_time
        """
    )



def create_locf_segments(con: duckdb.DuckDBPyConnection) -> None:
    """
    Build right-continuous LOCF segments from the validated hierarchical series.

    Each selected MAP point carries its value forward for no more than 90
    minutes. Support stops earlier at the next selected point, an unresolved
    ambiguous invasive timestamp, observation end, or 24 postoperative hours.
    """
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE map_locf_segments AS
        WITH ordered AS (
            SELECT
                h.*,

                LEAD(h.charttime) OVER (
                    PARTITION BY h.stay_id
                    ORDER BY h.charttime
                ) AS next_selected_charttime,

                LEAD(h.map_source_used) OVER (
                    PARTITION BY h.stay_id
                    ORDER BY h.charttime
                ) AS next_selected_source,

                (
                    SELECT MIN(a.charttime)
                    FROM unresolved_ambiguous_invasive_times AS a
                    WHERE a.stay_id = h.stay_id
                      AND a.charttime > h.charttime
                ) AS next_unresolved_ambiguous_time

            FROM map_hierarchical_series AS h
        ),

        bounded AS (
            SELECT
                *,
                charttime AS locf_segment_start,
                charttime
                    + {LOCF_MAX_CARRY_MINUTES} * INTERVAL '1 minute'
                    AS locf_carry_limit_time,
                postoperative_time_zero + INTERVAL '24 hours'
                    AS postoperative_24h_time,

                LEAST(
                    COALESCE(
                        next_selected_charttime,
                        TIMESTAMP '9999-12-31 23:59:59'
                    ),
                    COALESCE(
                        next_unresolved_ambiguous_time,
                        TIMESTAMP '9999-12-31 23:59:59'
                    ),
                    observation_end_time,
                    postoperative_time_zero + INTERVAL '24 hours',
                    charttime
                        + {LOCF_MAX_CARRY_MINUTES} * INTERVAL '1 minute'
                ) AS locf_segment_end

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
            map_value,
            map_source_used,
            source_selection_reason,
            same_time_ambiguous_invasive_fallback_flag,
            locf_segment_start,
            locf_segment_end,
            DATE_DIFF(
                'second',
                locf_segment_start,
                locf_segment_end
            ) / 60.0 AS locf_segment_minutes,
            next_selected_charttime,
            next_selected_source,
            next_unresolved_ambiguous_time,
            locf_carry_limit_time,
            postoperative_24h_time,

            CASE
                WHEN next_selected_charttime IS NOT NULL
                 AND locf_segment_end = next_selected_charttime
                    THEN 'NEXT_SELECTED_MAP'
                WHEN next_unresolved_ambiguous_time IS NOT NULL
                 AND locf_segment_end = next_unresolved_ambiguous_time
                    THEN 'UNRESOLVED_AMBIGUOUS_INVASIVE'
                WHEN locf_segment_end = observation_end_time
                    THEN 'OBSERVATION_END'
                WHEN locf_segment_end = postoperative_24h_time
                    THEN 'POSTOPERATIVE_24H_END'
                WHEN locf_segment_end = locf_carry_limit_time
                    THEN 'MAX_90_MIN_CARRY'
                ELSE 'OTHER_BOUNDARY'
            END AS locf_segment_end_reason,

            CASE
                WHEN next_selected_charttime IS NOT NULL
                 AND locf_segment_end = next_selected_charttime
                 AND next_selected_source <> map_source_used
                    THEN 1
                ELSE 0
            END AS cross_source_transition_at_segment_end_flag,

            CASE
                WHEN next_selected_charttime IS NULL
                  OR next_selected_charttime > locf_carry_limit_time
                    THEN 1
                ELSE 0
            END AS no_next_selected_within_90_minutes_flag

        FROM bounded
        WHERE locf_segment_end > locf_segment_start
        ORDER BY stay_id, locf_segment_start
        """
    )


def create_interval_point_summaries(con: duckdb.DuckDBPyConnection) -> None:
    """Summarize directly observed hierarchical MAP points within each window."""
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE points_by_temporal_window AS
        SELECT
            w.resolution,
            w.window_index,
            w.window_label,
            h.subject_id,
            h.hadm_id,
            h.stay_id,
            h.charttime,
            h.map_value,
            h.map_source_used,
            h.source_selection_reason,
            h.same_time_ambiguous_invasive_fallback_flag,
            h.valid_warning_rows_n
        FROM temporal_windows AS w
        INNER JOIN map_hierarchical_series AS h
            ON h.hours_from_time_zero >= w.start_hour
           AND h.hours_from_time_zero < w.end_hour
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE interval_point_gaps AS
        WITH ordered AS (
            SELECT
                *,
                LAG(charttime) OVER (
                    PARTITION BY resolution, window_index, stay_id
                    ORDER BY charttime
                ) AS previous_charttime
            FROM points_by_temporal_window
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
            ) AS map_max_internal_point_gap_minutes
        FROM ordered
        GROUP BY resolution, window_index, stay_id
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE interval_point_summary AS
        SELECT
            resolution,
            window_index,
            window_label,
            subject_id,
            hadm_id,
            stay_id,
            COUNT(*) AS map_points_n,
            COUNT(*) FILTER (
                WHERE map_source_used = 'INVASIVE'
            ) AS map_invasive_points_n,
            COUNT(*) FILTER (
                WHERE map_source_used = 'NON_INVASIVE'
            ) AS map_nibp_points_n,
            COUNT(*) FILTER (
                WHERE same_time_ambiguous_invasive_fallback_flag = 1
            ) AS map_nibp_ambiguous_rescue_points_n,
            COUNT(*) FILTER (
                WHERE valid_warning_rows_n > 0
            ) AS map_warning_points_n,
            AVG(map_value) AS map_arithmetic_mean_observed,
            STDDEV_SAMP(map_value) AS map_sd_observed,
            MIN(map_value) AS map_min_observed,
            MAX(map_value) AS map_max_observed,
            ARG_MIN(map_value, charttime) AS map_first_observed,
            ARG_MAX(map_value, charttime) AS map_last_observed,
            ARG_MAX(map_value, charttime)
                - ARG_MIN(map_value, charttime)
                AS map_delta_last_minus_first_observed,
            MIN(charttime) AS map_first_charttime,
            MAX(charttime) AS map_last_charttime
        FROM points_by_temporal_window
        GROUP BY
            resolution,
            window_index,
            window_label,
            subject_id,
            hadm_id,
            stay_id
        """
    )


def create_interval_support_summaries(con: duckdb.DuckDBPyConnection) -> None:
    """Clip support-eligible point-to-point segments to each patient window."""
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE interval_support_fragments AS
        SELECT
            pw.resolution,
            pw.window_index,
            pw.window_label,
            pw.stay_id,
            GREATEST(
                s.segment_start_time,
                pw.window_start_time
            ) AS supported_fragment_start,
            LEAST(
                s.segment_end_time,
                pw.eligible_window_end_time
            ) AS supported_fragment_end,
            s.cross_source_segment_flag
        FROM patient_temporal_windows AS pw
        INNER JOIN map_temporal_support_segments AS s
            ON pw.stay_id = s.stay_id
           AND s.support_eligible_flag = 1
           AND pw.eligible_minutes > 0
           AND s.segment_end_time > pw.window_start_time
           AND s.segment_start_time < pw.eligible_window_end_time
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE interval_support_summary AS
        SELECT
            resolution,
            window_index,
            window_label,
            stay_id,
            SUM(
                GREATEST(
                    0.0,
                    DATE_DIFF(
                        'second',
                        supported_fragment_start,
                        supported_fragment_end
                    ) / 60.0
                )
            ) AS map_supported_minutes,
            SUM(
                CASE
                    WHEN cross_source_segment_flag = 0
                    THEN GREATEST(
                        0.0,
                        DATE_DIFF(
                            'second',
                            supported_fragment_start,
                            supported_fragment_end
                        ) / 60.0
                    )
                    ELSE 0.0
                END
            ) AS map_same_source_supported_minutes,
            SUM(
                CASE
                    WHEN cross_source_segment_flag = 1
                    THEN GREATEST(
                        0.0,
                        DATE_DIFF(
                            'second',
                            supported_fragment_start,
                            supported_fragment_end
                        ) / 60.0
                    )
                    ELSE 0.0
                END
            ) AS map_cross_source_supported_minutes,
            COUNT(*) AS map_support_fragments_n,
            COUNT(*) FILTER (
                WHERE cross_source_segment_flag = 1
            ) AS map_cross_source_support_fragments_n
        FROM interval_support_fragments
        WHERE supported_fragment_end > supported_fragment_start
        GROUP BY resolution, window_index, window_label, stay_id
        """
    )



def create_interval_locf_summaries(con: duckdb.DuckDBPyConnection) -> None:
    """Clip LOCF segments to each patient window and calculate time-weighted MAP."""
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE interval_locf_fragments AS
        SELECT
            pw.resolution,
            pw.window_index,
            pw.window_label,
            pw.stay_id,
            s.map_value,
            s.map_source_used,
            s.locf_segment_end_reason,
            s.cross_source_transition_at_segment_end_flag,
            GREATEST(
                s.locf_segment_start,
                pw.window_start_time
            ) AS locf_fragment_start,
            LEAST(
                s.locf_segment_end,
                pw.eligible_window_end_time
            ) AS locf_fragment_end
        FROM patient_temporal_windows AS pw
        INNER JOIN map_locf_segments AS s
            ON pw.stay_id = s.stay_id
           AND pw.eligible_minutes > 0
           AND s.locf_segment_end > pw.window_start_time
           AND s.locf_segment_start < pw.eligible_window_end_time
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE interval_locf_summary AS
        WITH valid_fragments AS (
            SELECT
                *,
                DATE_DIFF(
                    'second',
                    locf_fragment_start,
                    locf_fragment_end
                ) / 60.0 AS locf_fragment_minutes
            FROM interval_locf_fragments
            WHERE locf_fragment_end > locf_fragment_start
        )

        SELECT
            resolution,
            window_index,
            window_label,
            stay_id,

            SUM(locf_fragment_minutes)
                AS map_locf_supported_minutes,

            SUM(map_value * locf_fragment_minutes)
                AS map_locf_weighted_sum_mmhg_minutes,

            SUM(map_value * locf_fragment_minutes)
            / NULLIF(SUM(locf_fragment_minutes), 0.0)
                AS map_time_weighted_mean,

            SUM(
                CASE
                    WHEN map_source_used = 'INVASIVE'
                        THEN locf_fragment_minutes
                    ELSE 0.0
                END
            ) AS map_locf_invasive_minutes,

            SUM(
                CASE
                    WHEN map_source_used = 'NON_INVASIVE'
                        THEN locf_fragment_minutes
                    ELSE 0.0
                END
            ) AS map_locf_nibp_minutes,

            COUNT(*) AS map_locf_fragments_n,

            COUNT(*) FILTER (
                WHERE locf_segment_end_reason = 'NEXT_SELECTED_MAP'
            ) AS map_locf_fragments_ending_next_map_n,

            COUNT(*) FILTER (
                WHERE locf_segment_end_reason = 'MAX_90_MIN_CARRY'
            ) AS map_locf_fragments_ending_90min_n,

            COUNT(*) FILTER (
                WHERE locf_segment_end_reason =
                      'UNRESOLVED_AMBIGUOUS_INVASIVE'
            ) AS map_locf_fragments_ending_ambiguous_n,

            COUNT(*) FILTER (
                WHERE locf_segment_end_reason = 'OBSERVATION_END'
            ) AS map_locf_fragments_ending_observation_n,

            COUNT(*) FILTER (
                WHERE locf_segment_end_reason = 'POSTOPERATIVE_24H_END'
            ) AS map_locf_fragments_ending_24h_n,

            COUNT(*) FILTER (
                WHERE cross_source_transition_at_segment_end_flag = 1
            ) AS map_locf_cross_source_transition_fragments_n

        FROM valid_fragments
        GROUP BY resolution, window_index, window_label, stay_id
        """
    )


def create_temporal_interval_summaries(con: duckdb.DuckDBPyConnection) -> None:
    """Build the complete hourly and six-hour patient-level datasets."""
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE ambiguous_invasive_by_window AS
        SELECT
            w.resolution,
            w.window_index,
            a.stay_id,
            COUNT(*) AS ambiguous_invasive_timestamps_n
        FROM temporal_windows AS w
        INNER JOIN map_clean_by_source AS a
            ON a.hours_from_time_zero >= w.start_hour
           AND a.hours_from_time_zero < w.end_hour
        WHERE a.map_source = 'INVASIVE'
          AND a.wide_invasive_disagreement_flag = 1
        GROUP BY w.resolution, w.window_index, a.stay_id
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE map_temporal_interval_summary AS
        SELECT
            pw.subject_id,
            pw.hadm_id,
            pw.stay_id,
            pw.surgery_group,
            pw.sex,
            pw.age_at_icu_admission,
            pw.postoperative_time_zero,
            pw.observation_end_time,
            pw.observation_end_type,
            pw.resolution,
            pw.window_index,
            pw.window_label,
            pw.start_hour,
            pw.end_hour,
            pw.nominal_minutes,
            pw.window_start_time,
            pw.nominal_window_end_time,
            pw.eligible_window_end_time,
            pw.eligible_minutes,
            pw.observability_status,

            COALESCE(p.map_points_n, 0) AS map_points_n,
            COALESCE(p.map_invasive_points_n, 0)
                AS map_invasive_points_n,
            COALESCE(p.map_nibp_points_n, 0)
                AS map_nibp_points_n,
            COALESCE(p.map_nibp_ambiguous_rescue_points_n, 0)
                AS map_nibp_ambiguous_rescue_points_n,
            COALESCE(p.map_warning_points_n, 0)
                AS map_warning_points_n,

            p.map_arithmetic_mean_observed,
            p.map_sd_observed,
            p.map_min_observed,
            p.map_max_observed,
            p.map_first_observed,
            p.map_last_observed,
            p.map_delta_last_minus_first_observed,
            p.map_first_charttime,
            p.map_last_charttime,
            g.map_max_internal_point_gap_minutes,

            CASE
                WHEN p.map_first_charttime IS NULL THEN NULL
                ELSE DATE_DIFF(
                    'second',
                    pw.window_start_time,
                    p.map_first_charttime
                ) / 60.0
            END AS map_first_point_offset_minutes,

            CASE
                WHEN p.map_last_charttime IS NULL THEN NULL
                ELSE DATE_DIFF(
                    'second',
                    p.map_last_charttime,
                    pw.eligible_window_end_time
                ) / 60.0
            END AS map_last_point_to_eligible_end_minutes,

            COALESCE(s.map_supported_minutes, 0.0)
                AS map_supported_minutes,
            COALESCE(s.map_same_source_supported_minutes, 0.0)
                AS map_same_source_supported_minutes,
            COALESCE(s.map_cross_source_supported_minutes, 0.0)
                AS map_cross_source_supported_minutes,
            COALESCE(s.map_support_fragments_n, 0)
                AS map_support_fragments_n,
            COALESCE(s.map_cross_source_support_fragments_n, 0)
                AS map_cross_source_support_fragments_n,

            CASE
                WHEN pw.eligible_minutes <= 0 THEN NULL
                ELSE LEAST(
                    100.0,
                    100.0 * COALESCE(s.map_supported_minutes, 0.0)
                    / pw.eligible_minutes
                )
            END AS map_supported_pct_of_eligible,

            COALESCE(a.ambiguous_invasive_timestamps_n, 0)
                AS ambiguous_invasive_timestamps_n,

            CASE
                WHEN pw.eligible_minutes <= 0
                    THEN 'NOT_OBSERVABLE'
                WHEN COALESCE(p.map_points_n, 0) = 0
                    THEN 'NO_SELECTED_MAP_POINT'
                WHEN COALESCE(s.map_supported_minutes, 0.0) = 0
                    THEN 'POINTS_WITHOUT_SUPPORTED_SPAN'
                WHEN 100.0 * COALESCE(s.map_supported_minutes, 0.0)
                     / pw.eligible_minutes >= 90.0
                    THEN 'SUPPORT_GE90_PCT'
                WHEN 100.0 * COALESCE(s.map_supported_minutes, 0.0)
                     / pw.eligible_minutes >= 75.0
                    THEN 'SUPPORT_75_TO_LT90_PCT'
                WHEN 100.0 * COALESCE(s.map_supported_minutes, 0.0)
                     / pw.eligible_minutes >= 50.0
                    THEN 'SUPPORT_50_TO_LT75_PCT'
                ELSE 'SUPPORT_GT0_TO_LT50_PCT'
            END AS map_support_class

        FROM patient_temporal_windows AS pw
        LEFT JOIN interval_point_summary AS p
            ON pw.resolution = p.resolution
           AND pw.window_index = p.window_index
           AND pw.stay_id = p.stay_id
        LEFT JOIN interval_point_gaps AS g
            ON pw.resolution = g.resolution
           AND pw.window_index = g.window_index
           AND pw.stay_id = g.stay_id
        LEFT JOIN interval_support_summary AS s
            ON pw.resolution = s.resolution
           AND pw.window_index = s.window_index
           AND pw.stay_id = s.stay_id
        LEFT JOIN ambiguous_invasive_by_window AS a
            ON pw.resolution = a.resolution
           AND pw.window_index = a.window_index
           AND pw.stay_id = a.stay_id

        ORDER BY pw.stay_id, pw.resolution, pw.window_index
        """
    )



def augment_temporal_intervals_with_locf(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Add LOCF coverage and time-weighted MAP to every patient-window row."""
    con.execute(
        """
        ALTER TABLE map_temporal_interval_summary
        RENAME TO map_temporal_interval_summary_pre_locf
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE map_temporal_interval_summary AS
        SELECT
            b.*,

            COALESCE(l.map_locf_supported_minutes, 0.0)
                AS map_locf_supported_minutes,

            COALESCE(l.map_locf_weighted_sum_mmhg_minutes, 0.0)
                AS map_locf_weighted_sum_mmhg_minutes,

            l.map_time_weighted_mean,

            COALESCE(l.map_locf_invasive_minutes, 0.0)
                AS map_locf_invasive_minutes,

            COALESCE(l.map_locf_nibp_minutes, 0.0)
                AS map_locf_nibp_minutes,

            COALESCE(l.map_locf_fragments_n, 0)
                AS map_locf_fragments_n,

            COALESCE(l.map_locf_fragments_ending_next_map_n, 0)
                AS map_locf_fragments_ending_next_map_n,

            COALESCE(l.map_locf_fragments_ending_90min_n, 0)
                AS map_locf_fragments_ending_90min_n,

            COALESCE(l.map_locf_fragments_ending_ambiguous_n, 0)
                AS map_locf_fragments_ending_ambiguous_n,

            COALESCE(l.map_locf_fragments_ending_observation_n, 0)
                AS map_locf_fragments_ending_observation_n,

            COALESCE(l.map_locf_fragments_ending_24h_n, 0)
                AS map_locf_fragments_ending_24h_n,

            COALESCE(
                l.map_locf_cross_source_transition_fragments_n,
                0
            ) AS map_locf_cross_source_transition_fragments_n,

            CASE
                WHEN b.eligible_minutes <= 0 THEN NULL
                ELSE LEAST(
                    100.0,
                    100.0
                    * COALESCE(l.map_locf_supported_minutes, 0.0)
                    / b.eligible_minutes
                )
            END AS map_locf_supported_pct_of_eligible,

            CASE
                WHEN b.eligible_minutes <= 0
                    THEN 'NOT_OBSERVABLE'
                WHEN COALESCE(l.map_locf_supported_minutes, 0.0) = 0
                    THEN 'NO_LOCF_SUPPORT'
                WHEN 100.0
                     * COALESCE(l.map_locf_supported_minutes, 0.0)
                     / b.eligible_minutes >= 90.0
                    THEN 'LOCF_SUPPORT_GE90_PCT'
                WHEN 100.0
                     * COALESCE(l.map_locf_supported_minutes, 0.0)
                     / b.eligible_minutes >= 75.0
                    THEN 'LOCF_SUPPORT_75_TO_LT90_PCT'
                WHEN 100.0
                     * COALESCE(l.map_locf_supported_minutes, 0.0)
                     / b.eligible_minutes >= 50.0
                    THEN 'LOCF_SUPPORT_50_TO_LT75_PCT'
                ELSE 'LOCF_SUPPORT_GT0_TO_LT50_PCT'
            END AS map_locf_support_class,

            CASE
                WHEN l.map_time_weighted_mean IS NULL
                  OR b.map_arithmetic_mean_observed IS NULL
                    THEN NULL
                ELSE l.map_time_weighted_mean
                     - b.map_arithmetic_mean_observed
            END AS map_twa_minus_arithmetic_mean

        FROM map_temporal_interval_summary_pre_locf AS b
        LEFT JOIN interval_locf_summary AS l
            ON b.resolution = l.resolution
           AND b.window_index = l.window_index
           AND b.stay_id = l.stay_id

        ORDER BY b.stay_id, b.resolution, b.window_index
        """
    )


def create_temporal_aggregation_audit(con: duckdb.DuckDBPyConnection) -> None:
    """Summarize point availability, LOCF coverage, and time-weighted MAP."""
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE map_temporal_aggregation_audit AS
        SELECT
            resolution,
            window_index,
            window_label,
            start_hour,
            end_hour,
            nominal_minutes,
            COUNT(*) AS cohort_rows_n,

            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
            ) AS eligible_patients_n,

            COUNT(*) FILTER (
                WHERE observability_status = 'FULLY_OBSERVABLE'
            ) AS fully_observable_patients_n,

            COUNT(*) FILTER (
                WHERE observability_status = 'PARTIALLY_OBSERVABLE'
            ) AS partially_observable_patients_n,

            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND map_points_n > 0
            ) AS patients_with_any_map_point_n,

            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND map_points_n = 0
            ) AS eligible_patients_without_map_point_n,

            ROUND(
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND map_points_n > 0
                )
                / NULLIF(
                    COUNT(*) FILTER (WHERE eligible_minutes > 0),
                    0
                ),
                2
            ) AS patients_with_any_map_point_pct,

            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND map_time_weighted_mean IS NOT NULL
            ) AS patients_with_time_weighted_map_n,

            ROUND(
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND map_time_weighted_mean IS NOT NULL
                )
                / NULLIF(
                    COUNT(*) FILTER (WHERE eligible_minutes > 0),
                    0
                ),
                2
            ) AS patients_with_time_weighted_map_pct,

            MEDIAN(map_points_n) FILTER (
                WHERE eligible_minutes > 0
            ) AS map_points_per_eligible_patient_median,

            MEDIAN(map_locf_supported_pct_of_eligible) FILTER (
                WHERE eligible_minutes > 0
            ) AS map_locf_supported_pct_median,

            QUANTILE_CONT(map_locf_supported_pct_of_eligible, 0.25)
                FILTER (WHERE eligible_minutes > 0)
                AS map_locf_supported_pct_p25,

            QUANTILE_CONT(map_locf_supported_pct_of_eligible, 0.75)
                FILTER (WHERE eligible_minutes > 0)
                AS map_locf_supported_pct_p75,

            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND map_locf_supported_pct_of_eligible >= 90
            ) AS locf_support_ge90_patients_n,

            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND map_locf_supported_pct_of_eligible >= 75
            ) AS locf_support_ge75_patients_n,

            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND map_locf_supported_pct_of_eligible >= 50
            ) AS locf_support_ge50_patients_n,

            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND map_locf_supported_minutes = 0
            ) AS zero_locf_support_patients_n,

            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND map_nibp_points_n > 0
            ) AS patients_using_nibp_points_n,

            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND map_locf_nibp_minutes > 0
            ) AS patients_with_nibp_supported_minutes_n,

            MEDIAN(map_time_weighted_mean) FILTER (
                WHERE eligible_minutes > 0
                  AND map_time_weighted_mean IS NOT NULL
            ) AS map_time_weighted_mean_median,

            QUANTILE_CONT(map_time_weighted_mean, 0.25) FILTER (
                WHERE eligible_minutes > 0
                  AND map_time_weighted_mean IS NOT NULL
            ) AS map_time_weighted_mean_p25,

            QUANTILE_CONT(map_time_weighted_mean, 0.75) FILTER (
                WHERE eligible_minutes > 0
                  AND map_time_weighted_mean IS NOT NULL
            ) AS map_time_weighted_mean_p75,

            MEDIAN(ABS(map_twa_minus_arithmetic_mean)) FILTER (
                WHERE eligible_minutes > 0
                  AND map_twa_minus_arithmetic_mean IS NOT NULL
            ) AS absolute_twa_arithmetic_difference_median,

            SUM(map_points_n) AS selected_map_points_n,
            SUM(map_invasive_points_n) AS invasive_points_n,
            SUM(map_nibp_points_n) AS nibp_points_n,
            SUM(map_locf_supported_minutes) AS locf_supported_minutes,
            SUM(map_locf_invasive_minutes) AS locf_invasive_minutes,
            SUM(map_locf_nibp_minutes) AS locf_nibp_minutes,
            SUM(map_locf_fragments_ending_90min_n)
                AS locf_fragments_ending_90min_n,
            SUM(map_locf_fragments_ending_ambiguous_n)
                AS locf_fragments_ending_ambiguous_n,
            SUM(ambiguous_invasive_timestamps_n)
                AS ambiguous_invasive_timestamps_n

        FROM map_temporal_interval_summary

        GROUP BY
            resolution,
            window_index,
            window_label,
            start_hour,
            end_hour,
            nominal_minutes

        ORDER BY resolution, window_index
        """
    )


def create_locf_segments_60(con: duckdb.DuckDBPyConnection) -> None:
    """Build the 60-minute LOCF sensitivity segments from the same MAP series."""
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE map_locf_segments_60 AS
        WITH ordered AS (
            SELECT
                h.*,

                LEAD(h.charttime) OVER (
                    PARTITION BY h.stay_id
                    ORDER BY h.charttime
                ) AS next_selected_charttime,

                LEAD(h.map_source_used) OVER (
                    PARTITION BY h.stay_id
                    ORDER BY h.charttime
                ) AS next_selected_source,

                (
                    SELECT MIN(a.charttime)
                    FROM unresolved_ambiguous_invasive_times AS a
                    WHERE a.stay_id = h.stay_id
                      AND a.charttime > h.charttime
                ) AS next_unresolved_ambiguous_time

            FROM map_hierarchical_series AS h
        ),

        bounded AS (
            SELECT
                *,
                charttime AS locf_segment_start,
                charttime
                    + {LOCF_SENSITIVITY_CARRY_MINUTES}
                      * INTERVAL '1 minute'
                    AS locf_carry_limit_time,
                postoperative_time_zero + INTERVAL '24 hours'
                    AS postoperative_24h_time,

                LEAST(
                    COALESCE(
                        next_selected_charttime,
                        TIMESTAMP '9999-12-31 23:59:59'
                    ),
                    COALESCE(
                        next_unresolved_ambiguous_time,
                        TIMESTAMP '9999-12-31 23:59:59'
                    ),
                    observation_end_time,
                    postoperative_time_zero + INTERVAL '24 hours',
                    charttime
                        + {LOCF_SENSITIVITY_CARRY_MINUTES}
                          * INTERVAL '1 minute'
                ) AS locf_segment_end

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
            map_value,
            map_source_used,
            source_selection_reason,
            same_time_ambiguous_invasive_fallback_flag,
            locf_segment_start,
            locf_segment_end,
            DATE_DIFF(
                'second',
                locf_segment_start,
                locf_segment_end
            ) / 60.0 AS locf_segment_minutes,
            next_selected_charttime,
            next_selected_source,
            next_unresolved_ambiguous_time,
            locf_carry_limit_time,
            postoperative_24h_time,

            CASE
                WHEN next_selected_charttime IS NOT NULL
                 AND locf_segment_end = next_selected_charttime
                    THEN 'NEXT_SELECTED_MAP'
                WHEN next_unresolved_ambiguous_time IS NOT NULL
                 AND locf_segment_end = next_unresolved_ambiguous_time
                    THEN 'UNRESOLVED_AMBIGUOUS_INVASIVE'
                WHEN locf_segment_end = observation_end_time
                    THEN 'OBSERVATION_END'
                WHEN locf_segment_end = postoperative_24h_time
                    THEN 'POSTOPERATIVE_24H_END'
                WHEN locf_segment_end = locf_carry_limit_time
                    THEN 'MAX_60_MIN_CARRY'
                ELSE 'OTHER_BOUNDARY'
            END AS locf_segment_end_reason,

            CASE
                WHEN next_selected_charttime IS NOT NULL
                 AND locf_segment_end = next_selected_charttime
                 AND next_selected_source <> map_source_used
                    THEN 1
                ELSE 0
            END AS cross_source_transition_at_segment_end_flag

        FROM bounded
        WHERE locf_segment_end > locf_segment_start
        ORDER BY stay_id, locf_segment_start
        """
    )


def create_interval_locf_summaries_60(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Clip 60-minute LOCF segments to hourly and six-hour windows."""
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE interval_locf_fragments_60 AS
        SELECT
            pw.resolution,
            pw.window_index,
            pw.window_label,
            pw.stay_id,
            s.map_value,
            s.map_source_used,
            s.locf_segment_end_reason,
            s.cross_source_transition_at_segment_end_flag,
            GREATEST(
                s.locf_segment_start,
                pw.window_start_time
            ) AS locf_fragment_start,
            LEAST(
                s.locf_segment_end,
                pw.eligible_window_end_time
            ) AS locf_fragment_end
        FROM patient_temporal_windows AS pw
        INNER JOIN map_locf_segments_60 AS s
            ON pw.stay_id = s.stay_id
           AND pw.eligible_minutes > 0
           AND s.locf_segment_end > pw.window_start_time
           AND s.locf_segment_start < pw.eligible_window_end_time
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE interval_locf_summary_60 AS
        WITH valid_fragments AS (
            SELECT
                *,
                DATE_DIFF(
                    'second',
                    locf_fragment_start,
                    locf_fragment_end
                ) / 60.0 AS locf_fragment_minutes
            FROM interval_locf_fragments_60
            WHERE locf_fragment_end > locf_fragment_start
        )

        SELECT
            resolution,
            window_index,
            window_label,
            stay_id,

            SUM(locf_fragment_minutes)
                AS map_locf_supported_minutes_60,

            SUM(map_value * locf_fragment_minutes)
                AS map_locf_weighted_sum_mmhg_minutes_60,

            SUM(map_value * locf_fragment_minutes)
            / NULLIF(SUM(locf_fragment_minutes), 0.0)
                AS map_time_weighted_mean_60,

            SUM(
                CASE
                    WHEN map_source_used = 'INVASIVE'
                        THEN locf_fragment_minutes
                    ELSE 0.0
                END
            ) AS map_locf_invasive_minutes_60,

            SUM(
                CASE
                    WHEN map_source_used = 'NON_INVASIVE'
                        THEN locf_fragment_minutes
                    ELSE 0.0
                END
            ) AS map_locf_nibp_minutes_60,

            COUNT(*) AS map_locf_fragments_n_60,

            COUNT(*) FILTER (
                WHERE locf_segment_end_reason = 'MAX_60_MIN_CARRY'
            ) AS map_locf_fragments_ending_60min_n

        FROM valid_fragments
        GROUP BY resolution, window_index, window_label, stay_id
        """
    )


def create_locf_sensitivity_comparison(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Create patient-window, by-window, and patient-level 60-vs-90 audits."""
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE map_locf_sensitivity_patient_windows AS
        WITH joined AS (
            SELECT
                b.subject_id,
                b.hadm_id,
                b.stay_id,
                b.surgery_group,
                b.sex,
                b.age_at_icu_admission,
                b.postoperative_time_zero,
                b.observation_end_time,
                b.observation_end_type,
                b.resolution,
                b.window_index,
                b.window_label,
                b.start_hour,
                b.end_hour,
                b.nominal_minutes,
                b.eligible_minutes,
                b.observability_status,
                b.map_points_n,
                b.map_invasive_points_n,
                b.map_nibp_points_n,

                COALESCE(s.map_locf_supported_minutes_60, 0.0)
                    AS map_locf_supported_minutes_60,
                s.map_time_weighted_mean_60,
                COALESCE(s.map_locf_invasive_minutes_60, 0.0)
                    AS map_locf_invasive_minutes_60,
                COALESCE(s.map_locf_nibp_minutes_60, 0.0)
                    AS map_locf_nibp_minutes_60,
                COALESCE(s.map_locf_fragments_n_60, 0)
                    AS map_locf_fragments_n_60,
                COALESCE(s.map_locf_fragments_ending_60min_n, 0)
                    AS map_locf_fragments_ending_60min_n,

                b.map_locf_supported_minutes
                    AS map_locf_supported_minutes_90,
                b.map_time_weighted_mean
                    AS map_time_weighted_mean_90,
                b.map_locf_invasive_minutes
                    AS map_locf_invasive_minutes_90,
                b.map_locf_nibp_minutes
                    AS map_locf_nibp_minutes_90,
                b.map_locf_fragments_n
                    AS map_locf_fragments_n_90,
                b.map_locf_fragments_ending_90min_n
                    AS map_locf_fragments_ending_90min_n

            FROM map_temporal_interval_summary AS b
            LEFT JOIN interval_locf_summary_60 AS s
                ON b.resolution = s.resolution
               AND b.window_index = s.window_index
               AND b.stay_id = s.stay_id
        ),

        calculated AS (
            SELECT
                *,

                CASE
                    WHEN eligible_minutes <= 0 THEN NULL
                    ELSE LEAST(
                        100.0,
                        100.0 * map_locf_supported_minutes_60
                        / eligible_minutes
                    )
                END AS map_locf_supported_pct_60,

                CASE
                    WHEN eligible_minutes <= 0 THEN NULL
                    ELSE LEAST(
                        100.0,
                        100.0 * map_locf_supported_minutes_90
                        / eligible_minutes
                    )
                END AS map_locf_supported_pct_90,

                map_locf_supported_minutes_90
                - map_locf_supported_minutes_60
                    AS map_supported_minutes_gain_90_vs_60,

                map_time_weighted_mean_90
                - map_time_weighted_mean_60
                    AS map_twa_difference_90_minus_60,

                ABS(
                    map_time_weighted_mean_90
                    - map_time_weighted_mean_60
                ) AS map_twa_absolute_difference,

                CASE
                    WHEN map_time_weighted_mean_60 IS NOT NULL THEN 1
                    ELSE 0
                END AS twa_available_60_flag,

                CASE
                    WHEN map_time_weighted_mean_90 IS NOT NULL THEN 1
                    ELSE 0
                END AS twa_available_90_flag

            FROM joined
        ),

        classified AS (
            SELECT
                *,

                CASE
                    WHEN eligible_minutes <= 0
                        THEN 'NOT_OBSERVABLE'
                    WHEN map_locf_supported_minutes_60 = 0
                        THEN 'NO_SUPPORT'
                    WHEN map_locf_supported_pct_60 >= 90
                        THEN 'SUPPORT_GE90_PCT'
                    WHEN map_locf_supported_pct_60 >= 75
                        THEN 'SUPPORT_75_TO_LT90_PCT'
                    WHEN map_locf_supported_pct_60 >= 50
                        THEN 'SUPPORT_50_TO_LT75_PCT'
                    ELSE 'SUPPORT_GT0_TO_LT50_PCT'
                END AS support_class_60,

                CASE
                    WHEN eligible_minutes <= 0
                        THEN 'NOT_OBSERVABLE'
                    WHEN map_locf_supported_minutes_90 = 0
                        THEN 'NO_SUPPORT'
                    WHEN map_locf_supported_pct_90 >= 90
                        THEN 'SUPPORT_GE90_PCT'
                    WHEN map_locf_supported_pct_90 >= 75
                        THEN 'SUPPORT_75_TO_LT90_PCT'
                    WHEN map_locf_supported_pct_90 >= 50
                        THEN 'SUPPORT_50_TO_LT75_PCT'
                    ELSE 'SUPPORT_GT0_TO_LT50_PCT'
                END AS support_class_90

            FROM calculated
        )

        SELECT
            *,

            CASE
                WHEN eligible_minutes <= 0 THEN 0
                WHEN support_class_60 <> support_class_90 THEN 1
                ELSE 0
            END AS support_class_changed_flag,

            CASE
                WHEN map_twa_absolute_difference IS NULL THEN NULL
                WHEN map_twa_absolute_difference > 5 THEN 'GT5_MMHG'
                WHEN map_twa_absolute_difference > 2 THEN 'GT2_TO_5_MMHG'
                WHEN map_twa_absolute_difference > 1 THEN 'GT1_TO_2_MMHG'
                WHEN map_twa_absolute_difference > 0.5 THEN 'GT0_5_TO_1_MMHG'
                ELSE 'LE0_5_MMHG'
            END AS twa_difference_band

        FROM classified
        ORDER BY stay_id, resolution, window_index
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE map_locf_sensitivity_by_window AS
        SELECT
            resolution,
            window_index,
            window_label,
            start_hour,
            end_hour,
            nominal_minutes,

            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
            ) AS eligible_patient_windows_n,

            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND twa_available_60_flag = 1
            ) AS twa_available_60_n,

            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND twa_available_90_flag = 1
            ) AS twa_available_90_n,

            ROUND(
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND twa_available_60_flag = 1
                )
                / NULLIF(COUNT(*) FILTER (WHERE eligible_minutes > 0), 0),
                2
            ) AS twa_available_60_pct,

            ROUND(
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND twa_available_90_flag = 1
                )
                / NULLIF(COUNT(*) FILTER (WHERE eligible_minutes > 0), 0),
                2
            ) AS twa_available_90_pct,

            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND twa_available_60_flag = 0
                  AND twa_available_90_flag = 1
            ) AS windows_available_only_with_90_n,

            MEDIAN(map_locf_supported_pct_60) FILTER (
                WHERE eligible_minutes > 0
            ) AS supported_pct_60_median,

            MEDIAN(map_locf_supported_pct_90) FILTER (
                WHERE eligible_minutes > 0
            ) AS supported_pct_90_median,

            QUANTILE_CONT(map_locf_supported_pct_60, 0.25) FILTER (
                WHERE eligible_minutes > 0
            ) AS supported_pct_60_p25,

            QUANTILE_CONT(map_locf_supported_pct_90, 0.25) FILTER (
                WHERE eligible_minutes > 0
            ) AS supported_pct_90_p25,

            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND map_locf_supported_pct_60 >= 90
            ) AS support_ge90_60_n,

            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND map_locf_supported_pct_90 >= 90
            ) AS support_ge90_90_n,

            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND map_locf_supported_minutes_60 = 0
            ) AS zero_support_60_n,

            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND map_locf_supported_minutes_90 = 0
            ) AS zero_support_90_n,

            COUNT(*) FILTER (
                WHERE map_twa_absolute_difference IS NOT NULL
            ) AS paired_twa_windows_n,

            MEDIAN(map_twa_absolute_difference) FILTER (
                WHERE map_twa_absolute_difference IS NOT NULL
            ) AS twa_absolute_difference_median,

            AVG(map_twa_absolute_difference) FILTER (
                WHERE map_twa_absolute_difference IS NOT NULL
            ) AS twa_absolute_difference_mean,

            QUANTILE_CONT(map_twa_absolute_difference, 0.90) FILTER (
                WHERE map_twa_absolute_difference IS NOT NULL
            ) AS twa_absolute_difference_p90,

            QUANTILE_CONT(map_twa_absolute_difference, 0.95) FILTER (
                WHERE map_twa_absolute_difference IS NOT NULL
            ) AS twa_absolute_difference_p95,

            QUANTILE_CONT(map_twa_absolute_difference, 0.99) FILTER (
                WHERE map_twa_absolute_difference IS NOT NULL
            ) AS twa_absolute_difference_p99,

            MAX(map_twa_absolute_difference) FILTER (
                WHERE map_twa_absolute_difference IS NOT NULL
            ) AS twa_absolute_difference_max,

            COUNT(*) FILTER (
                WHERE map_twa_absolute_difference > 0.5
            ) AS twa_difference_gt0_5_n,

            COUNT(*) FILTER (
                WHERE map_twa_absolute_difference > 1
            ) AS twa_difference_gt1_n,

            COUNT(*) FILTER (
                WHERE map_twa_absolute_difference > 2
            ) AS twa_difference_gt2_n,

            COUNT(*) FILTER (
                WHERE map_twa_absolute_difference > 5
            ) AS twa_difference_gt5_n,

            COUNT(*) FILTER (
                WHERE support_class_changed_flag = 1
            ) AS support_class_changed_n,

            MEDIAN(map_supported_minutes_gain_90_vs_60) FILTER (
                WHERE eligible_minutes > 0
            ) AS supported_minutes_gain_median,

            QUANTILE_CONT(map_supported_minutes_gain_90_vs_60, 0.95)
                FILTER (WHERE eligible_minutes > 0)
                AS supported_minutes_gain_p95,

            CORR(
                map_time_weighted_mean_60,
                map_time_weighted_mean_90
            ) FILTER (
                WHERE map_time_weighted_mean_60 IS NOT NULL
                  AND map_time_weighted_mean_90 IS NOT NULL
            ) AS twa_60_90_correlation,

            MEDIAN(map_time_weighted_mean_60) FILTER (
                WHERE map_time_weighted_mean_60 IS NOT NULL
            ) AS twa_60_median,

            MEDIAN(map_time_weighted_mean_90) FILTER (
                WHERE map_time_weighted_mean_90 IS NOT NULL
            ) AS twa_90_median

        FROM map_locf_sensitivity_patient_windows
        GROUP BY
            resolution,
            window_index,
            window_label,
            start_hour,
            end_hour,
            nominal_minutes
        ORDER BY resolution, window_index
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE map_locf_sensitivity_by_patient AS
        SELECT
            subject_id,
            hadm_id,
            stay_id,
            surgery_group,
            sex,
            age_at_icu_admission,

            COUNT(*) FILTER (
                WHERE resolution = 'HOURLY'
                  AND eligible_minutes > 0
            ) AS eligible_hourly_windows_n,

            COUNT(*) FILTER (
                WHERE resolution = 'HOURLY'
                  AND eligible_minutes > 0
                  AND twa_available_60_flag = 1
            ) AS hourly_twa_available_60_n,

            COUNT(*) FILTER (
                WHERE resolution = 'HOURLY'
                  AND eligible_minutes > 0
                  AND twa_available_90_flag = 1
            ) AS hourly_twa_available_90_n,

            COUNT(*) FILTER (
                WHERE resolution = 'HOURLY'
                  AND eligible_minutes > 0
                  AND twa_available_60_flag = 0
                  AND twa_available_90_flag = 1
            ) AS hourly_windows_gained_with_90_n,

            COUNT(*) FILTER (
                WHERE resolution = 'HOURLY'
                  AND eligible_minutes > 0
                  AND map_locf_supported_pct_60 >= 90
            ) AS hourly_support_ge90_60_n,

            COUNT(*) FILTER (
                WHERE resolution = 'HOURLY'
                  AND eligible_minutes > 0
                  AND map_locf_supported_pct_90 >= 90
            ) AS hourly_support_ge90_90_n,

            COUNT(*) FILTER (
                WHERE resolution = 'HOURLY'
                  AND support_class_changed_flag = 1
            ) AS hourly_support_class_changed_n,

            COUNT(*) FILTER (
                WHERE resolution = 'HOURLY'
                  AND map_twa_absolute_difference IS NOT NULL
            ) AS paired_hourly_twa_n,

            MEDIAN(map_twa_absolute_difference) FILTER (
                WHERE resolution = 'HOURLY'
                  AND map_twa_absolute_difference IS NOT NULL
            ) AS hourly_twa_absolute_difference_median,

            QUANTILE_CONT(map_twa_absolute_difference, 0.95) FILTER (
                WHERE resolution = 'HOURLY'
                  AND map_twa_absolute_difference IS NOT NULL
            ) AS hourly_twa_absolute_difference_p95,

            MAX(map_twa_absolute_difference) FILTER (
                WHERE resolution = 'HOURLY'
                  AND map_twa_absolute_difference IS NOT NULL
            ) AS hourly_twa_absolute_difference_max,

            COUNT(*) FILTER (
                WHERE resolution = 'HOURLY'
                  AND map_twa_absolute_difference > 2
            ) AS hourly_twa_difference_gt2_n,

            SUM(map_supported_minutes_gain_90_vs_60) FILTER (
                WHERE resolution = 'HOURLY'
                  AND eligible_minutes > 0
            ) AS total_hourly_supported_minutes_gain_90_vs_60

        FROM map_locf_sensitivity_patient_windows
        GROUP BY
            subject_id,
            hadm_id,
            stay_id,
            surgery_group,
            sex,
            age_at_icu_admission
        ORDER BY stay_id
        """
    )



def validate_outputs(con: duckdb.DuckDBPyConnection) -> None:
    """Validate the full canonical extraction, exclusions, fallback, and hierarchy."""
    raw_n = con.execute("SELECT COUNT(*) FROM map_raw").fetchone()[0]
    clean_n = con.execute("SELECT COUNT(*) FROM map_clean_by_source").fetchone()[0]
    hierarchy_n = con.execute("SELECT COUNT(*) FROM map_hierarchical_series").fetchone()[0]
    audit_n = con.execute("SELECT COUNT(*) FROM map_hierarchy_audit").fetchone()[0]
    pattern_n = con.execute("SELECT COUNT(*) FROM map_source_patterns").fetchone()[0]

    if raw_n == 0 or clean_n == 0 or hierarchy_n == 0:
        raise MapHierarchyError("One or more core canonical tables are empty.")

    if audit_n != len(BIN_ROWS):
        raise MapHierarchyError(
            f"Hierarchy audit has {audit_n:,} rows; expected {len(BIN_ROWS):,}."
        )

    if pattern_n != EXPECTED_COHORT_N:
        raise MapHierarchyError(
            f"Pattern output has {pattern_n:,} rows; expected {EXPECTED_COHORT_N:,}."
        )

    invalid_clean_values = con.execute(
        f"""
        SELECT COUNT(*)
        FROM map_clean_by_source
        WHERE consolidated_map_value <= {VALID_MAP_LOWER_EXCLUSIVE}
           OR consolidated_map_value >= {VALID_MAP_UPPER_EXCLUSIVE}
        """
    ).fetchone()[0]

    if invalid_clean_values:
        raise MapHierarchyError(
            f"{invalid_clean_values:,} invalid values remain after cleaning."
        )

    source_clean_expected = con.execute(
        """
        SELECT COUNT(*)
        FROM map_source_time_audit
        WHERE valid_raw_rows_n > 0
        """
    ).fetchone()[0]

    if clean_n != source_clean_expected:
        raise MapHierarchyError(
            f"Clean source series has {clean_n:,} rows; expected {source_clean_expected:,}."
        )

    expected_hierarchy_n = con.execute(
        """
        SELECT
            (SELECT COUNT(*)
             FROM map_clean_by_source
             WHERE map_source = 'INVASIVE'
               AND wide_invasive_disagreement_flag = 0)
            +
            (SELECT COUNT(*)
             FROM nibp_hierarchy_audit
             WHERE excluded_inside_invasive_episode_flag = 0)
        """
    ).fetchone()[0]

    if hierarchy_n != expected_hierarchy_n:
        raise MapHierarchyError(
            f"Hierarchy has {hierarchy_n:,} points; expected {expected_hierarchy_n:,}."
        )

    ambiguous_invasive_retained = con.execute(
        """
        SELECT COUNT(*)
        FROM map_hierarchical_series AS h
        INNER JOIN map_clean_by_source AS a
            ON h.stay_id = a.stay_id
           AND h.charttime = a.charttime
        WHERE h.map_source_used = 'INVASIVE'
          AND a.map_source = 'INVASIVE'
          AND a.wide_invasive_disagreement_flag = 1
        """
    ).fetchone()[0]

    if ambiguous_invasive_retained:
        raise MapHierarchyError(
            f"{ambiguous_invasive_retained:,} ambiguous invasive points remain."
        )

    invalid_nibp_inside_episode = con.execute(
        """
        SELECT COUNT(*)
        FROM map_hierarchical_series AS h
        WHERE h.map_source_used = 'NON_INVASIVE'
          AND h.same_time_ambiguous_invasive_fallback_flag = 0
          AND EXISTS (
              SELECT 1
              FROM invasive_episode_bounds AS e
              WHERE e.stay_id = h.stay_id
                AND h.charttime >= e.invasive_episode_start
                AND h.charttime <= e.invasive_episode_end
          )
        """
    ).fetchone()[0]

    if invalid_nibp_inside_episode:
        raise MapHierarchyError(
            f"{invalid_nibp_inside_episode:,} non-rescue NIBP points remain inside invasive monitoring episodes."
        )

    invalid_rescue_points = con.execute(
        """
        SELECT COUNT(*)
        FROM map_hierarchical_series AS h
        WHERE h.same_time_ambiguous_invasive_fallback_flag = 1
          AND NOT EXISTS (
              SELECT 1
              FROM map_clean_by_source AS a
              WHERE a.stay_id = h.stay_id
                AND a.charttime = h.charttime
                AND a.map_source = 'INVASIVE'
                AND a.wide_invasive_disagreement_flag = 1
          )
        """
    ).fetchone()[0]

    if invalid_rescue_points:
        raise MapHierarchyError(
            f"{invalid_rescue_points:,} NIBP rescue points lack an ambiguous invasive counterpart."
        )

    no_map_patterns = con.execute(
        """
        SELECT COUNT(*)
        FROM map_source_patterns
        WHERE hierarchical_source_pattern_0_24h = 'NO_MAP'
        """
    ).fetchone()[0]

    no_map_cases = con.execute(
        """
        SELECT COUNT(*)
        FROM map_quality_cases
        WHERE record_type = 'NO_VALID_MAP_PATIENT'
        """
    ).fetchone()[0]

    if no_map_patterns != no_map_cases:
        raise MapHierarchyError(
            "NO_MAP counts differ between pattern and quality-case tables."
        )


    hourly_rows_n = con.execute(
        """
        SELECT COUNT(*)
        FROM map_temporal_interval_summary
        WHERE resolution = 'HOURLY'
        """
    ).fetchone()[0]

    six_hour_rows_n = con.execute(
        """
        SELECT COUNT(*)
        FROM map_temporal_interval_summary
        WHERE resolution = 'SIX_HOUR'
        """
    ).fetchone()[0]

    expected_hourly_rows_n = EXPECTED_COHORT_N * HOURLY_WINDOWS_N
    expected_six_hour_rows_n = EXPECTED_COHORT_N * SIX_HOUR_WINDOWS_N

    if hourly_rows_n != expected_hourly_rows_n:
        raise MapHierarchyError(
            f"Hourly trajectory has {hourly_rows_n:,} rows; "
            f"expected {expected_hourly_rows_n:,}."
        )

    if six_hour_rows_n != expected_six_hour_rows_n:
        raise MapHierarchyError(
            f"Six-hour summary has {six_hour_rows_n:,} rows; "
            f"expected {expected_six_hour_rows_n:,}."
        )

    duplicate_interval_keys = con.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT stay_id, resolution, window_index
            FROM map_temporal_interval_summary
            GROUP BY stay_id, resolution, window_index
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]

    if duplicate_interval_keys:
        raise MapHierarchyError(
            f"{duplicate_interval_keys:,} duplicate temporal interval keys remain."
        )

    invalid_interval_minutes = con.execute(
        """
        SELECT COUNT(*)
        FROM map_temporal_interval_summary
        WHERE eligible_minutes < 0
           OR eligible_minutes > nominal_minutes + 0.0001
           OR map_supported_minutes < 0
           OR map_supported_minutes > eligible_minutes + 0.0001
        """
    ).fetchone()[0]

    if invalid_interval_minutes:
        raise MapHierarchyError(
            f"{invalid_interval_minutes:,} temporal intervals have invalid duration or support."
        )

    hierarchy_points_n = con.execute(
        "SELECT COUNT(*) FROM map_hierarchical_series"
    ).fetchone()[0]

    hourly_points_n = con.execute(
        """
        SELECT SUM(map_points_n)
        FROM map_temporal_interval_summary
        WHERE resolution = 'HOURLY'
        """
    ).fetchone()[0]

    six_hour_points_n = con.execute(
        """
        SELECT SUM(map_points_n)
        FROM map_temporal_interval_summary
        WHERE resolution = 'SIX_HOUR'
        """
    ).fetchone()[0]

    if hourly_points_n != hierarchy_points_n:
        raise MapHierarchyError(
            f"Hourly windows contain {hourly_points_n:,} points; "
            f"hierarchy contains {hierarchy_points_n:,}."
        )

    if six_hour_points_n != hierarchy_points_n:
        raise MapHierarchyError(
            f"Six-hour windows contain {six_hour_points_n:,} points; "
            f"hierarchy contains {hierarchy_points_n:,}."
        )

    temporal_audit_rows_n = con.execute(
        "SELECT COUNT(*) FROM map_temporal_aggregation_audit"
    ).fetchone()[0]

    expected_temporal_audit_rows_n = HOURLY_WINDOWS_N + SIX_HOUR_WINDOWS_N
    if temporal_audit_rows_n != expected_temporal_audit_rows_n:
        raise MapHierarchyError(
            f"Temporal audit has {temporal_audit_rows_n:,} rows; "
            f"expected {expected_temporal_audit_rows_n:,}."
        )


    locf_invalid_segments = con.execute(
        f"""
        SELECT COUNT(*)
        FROM map_locf_segments
        WHERE locf_segment_minutes <= 0
           OR locf_segment_minutes > {LOCF_MAX_CARRY_MINUTES} + 0.0001
           OR locf_segment_end > observation_end_time
           OR locf_segment_end > postoperative_24h_time
        """
    ).fetchone()[0]

    if locf_invalid_segments:
        raise MapHierarchyError(
            f"{locf_invalid_segments:,} LOCF segments violate duration or endpoint rules."
        )

    locf_crossing_blockers = con.execute(
        """
        SELECT COUNT(*)
        FROM map_locf_segments AS s
        WHERE EXISTS (
            SELECT 1
            FROM unresolved_ambiguous_invasive_times AS a
            WHERE a.stay_id = s.stay_id
              AND a.charttime > s.locf_segment_start
              AND a.charttime < s.locf_segment_end
        )
        """
    ).fetchone()[0]

    if locf_crossing_blockers:
        raise MapHierarchyError(
            f"{locf_crossing_blockers:,} LOCF segments cross unresolved ambiguous invasive timestamps."
        )

    invalid_locf_intervals = con.execute(
        """
        SELECT COUNT(*)
        FROM map_temporal_interval_summary
        WHERE map_locf_supported_minutes < 0
           OR map_locf_supported_minutes > eligible_minutes + 0.0001
           OR map_locf_invasive_minutes < 0
           OR map_locf_nibp_minutes < 0
           OR ABS(
                map_locf_supported_minutes
                - map_locf_invasive_minutes
                - map_locf_nibp_minutes
              ) > 0.001
           OR (
                map_locf_supported_minutes = 0
                AND map_time_weighted_mean IS NOT NULL
              )
           OR (
                map_locf_supported_minutes > 0
                AND (
                    map_time_weighted_mean IS NULL
                    OR map_time_weighted_mean <= 0
                    OR map_time_weighted_mean >= 300
                )
              )
        """
    ).fetchone()[0]

    if invalid_locf_intervals:
        raise MapHierarchyError(
            f"{invalid_locf_intervals:,} patient-windows have invalid LOCF coverage or TWA values."
        )

    locf_segment_minutes = con.execute(
        "SELECT SUM(locf_segment_minutes) FROM map_locf_segments"
    ).fetchone()[0] or 0.0

    hourly_locf_minutes = con.execute(
        """
        SELECT SUM(map_locf_supported_minutes)
        FROM map_temporal_interval_summary
        WHERE resolution = 'HOURLY'
        """
    ).fetchone()[0] or 0.0

    six_hour_locf_minutes = con.execute(
        """
        SELECT SUM(map_locf_supported_minutes)
        FROM map_temporal_interval_summary
        WHERE resolution = 'SIX_HOUR'
        """
    ).fetchone()[0] or 0.0

    if abs(hourly_locf_minutes - locf_segment_minutes) > 0.1:
        raise MapHierarchyError(
            "Hourly LOCF fragments do not reconstruct total LOCF segment duration."
        )

    if abs(six_hour_locf_minutes - locf_segment_minutes) > 0.1:
        raise MapHierarchyError(
            "Six-hour LOCF fragments do not reconstruct total LOCF segment duration."
        )


    sensitivity_rows_n = con.execute(
        "SELECT COUNT(*) FROM map_locf_sensitivity_patient_windows"
    ).fetchone()[0]
    expected_sensitivity_rows_n = EXPECTED_COHORT_N * (
        HOURLY_WINDOWS_N + SIX_HOUR_WINDOWS_N
    )
    if sensitivity_rows_n != expected_sensitivity_rows_n:
        raise MapHierarchyError(
            f"LOCF sensitivity table has {sensitivity_rows_n:,} rows; "
            f"expected {expected_sensitivity_rows_n:,}."
        )

    sensitivity_window_rows_n = con.execute(
        "SELECT COUNT(*) FROM map_locf_sensitivity_by_window"
    ).fetchone()[0]
    if sensitivity_window_rows_n != HOURLY_WINDOWS_N + SIX_HOUR_WINDOWS_N:
        raise MapHierarchyError(
            "LOCF sensitivity by-window audit has an unexpected row count."
        )

    sensitivity_patient_rows_n = con.execute(
        "SELECT COUNT(*) FROM map_locf_sensitivity_by_patient"
    ).fetchone()[0]
    if sensitivity_patient_rows_n != EXPECTED_COHORT_N:
        raise MapHierarchyError(
            f"LOCF sensitivity patient audit has {sensitivity_patient_rows_n:,} "
            f"rows; expected {EXPECTED_COHORT_N:,}."
        )

    invalid_locf_60_segments = con.execute(
        f"""
        SELECT COUNT(*)
        FROM map_locf_segments_60
        WHERE locf_segment_minutes <= 0
           OR locf_segment_minutes
              > {LOCF_SENSITIVITY_CARRY_MINUTES} + 0.0001
           OR locf_segment_end > observation_end_time
           OR locf_segment_end > postoperative_24h_time
        """
    ).fetchone()[0]
    if invalid_locf_60_segments:
        raise MapHierarchyError(
            f"{invalid_locf_60_segments:,} 60-minute LOCF segments violate "
            "duration or endpoint rules."
        )

    locf_60_crossing_blockers = con.execute(
        """
        SELECT COUNT(*)
        FROM map_locf_segments_60 AS s
        WHERE EXISTS (
            SELECT 1
            FROM unresolved_ambiguous_invasive_times AS a
            WHERE a.stay_id = s.stay_id
              AND a.charttime > s.locf_segment_start
              AND a.charttime < s.locf_segment_end
        )
        """
    ).fetchone()[0]
    if locf_60_crossing_blockers:
        raise MapHierarchyError(
            f"{locf_60_crossing_blockers:,} 60-minute LOCF segments cross "
            "unresolved ambiguous invasive timestamps."
        )

    invalid_sensitivity_windows = con.execute(
        """
        SELECT COUNT(*)
        FROM map_locf_sensitivity_patient_windows
        WHERE map_locf_supported_minutes_60 < 0
           OR map_locf_supported_minutes_90 < 0
           OR map_locf_supported_minutes_60
              > eligible_minutes + 0.0001
           OR map_locf_supported_minutes_90
              > eligible_minutes + 0.0001
           OR map_locf_supported_minutes_60
              > map_locf_supported_minutes_90 + 0.0001
           OR (
                map_locf_supported_minutes_60 = 0
                AND map_time_weighted_mean_60 IS NOT NULL
              )
           OR (
                map_locf_supported_minutes_60 > 0
                AND (
                    map_time_weighted_mean_60 IS NULL
                    OR map_time_weighted_mean_60 <= 0
                    OR map_time_weighted_mean_60 >= 300
                )
              )
           OR (
                twa_available_60_flag = 1
                AND twa_available_90_flag = 0
              )
        """
    ).fetchone()[0]
    if invalid_sensitivity_windows:
        raise MapHierarchyError(
            f"{invalid_sensitivity_windows:,} 60-vs-90 sensitivity windows "
            "violate support or TWA invariants."
        )

    locf_60_total_minutes = con.execute(
        "SELECT SUM(locf_segment_minutes) FROM map_locf_segments_60"
    ).fetchone()[0] or 0.0
    hourly_60_minutes = con.execute(
        """
        SELECT SUM(map_locf_supported_minutes_60)
        FROM map_locf_sensitivity_patient_windows
        WHERE resolution = 'HOURLY'
        """
    ).fetchone()[0] or 0.0
    six_hour_60_minutes = con.execute(
        """
        SELECT SUM(map_locf_supported_minutes_60)
        FROM map_locf_sensitivity_patient_windows
        WHERE resolution = 'SIX_HOUR'
        """
    ).fetchone()[0] or 0.0

    if abs(hourly_60_minutes - locf_60_total_minutes) > 0.1:
        raise MapHierarchyError(
            "Hourly 60-minute LOCF fragments do not reconstruct total duration."
        )
    if abs(six_hour_60_minutes - locf_60_total_minutes) > 0.1:
        raise MapHierarchyError(
            "Six-hour 60-minute LOCF fragments do not reconstruct total duration."
        )


def create_interval_hypotension_summaries(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """
    Quantify hypotension burden on primary 90-minute LOCF-supported fragments.

    LOCF fragments are piecewise constant. Duration below a threshold is the
    fragment duration when the carried MAP is strictly below that threshold.
    Area below threshold is (threshold - MAP) multiplied by fragment duration.
    """
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE interval_hypotension_summary AS
        WITH valid_fragments AS (
            SELECT
                resolution,
                window_index,
                window_label,
                stay_id,
                map_value,
                DATE_DIFF(
                    'second',
                    locf_fragment_start,
                    locf_fragment_end
                ) / 60.0 AS fragment_minutes
            FROM interval_locf_fragments
            WHERE locf_fragment_end > locf_fragment_start
        )

        SELECT
            resolution,
            window_index,
            window_label,
            stay_id,

            SUM(CASE WHEN map_value < 65
                     THEN fragment_minutes ELSE 0.0 END)
                AS map_minutes_below_65,

            SUM(CASE WHEN map_value < 65
                     THEN (65.0 - map_value) * fragment_minutes
                     ELSE 0.0 END)
                AS map_area_below_65_mmhg_min,

            SUM(CASE WHEN map_value < 60
                     THEN fragment_minutes ELSE 0.0 END)
                AS map_minutes_below_60,

            SUM(CASE WHEN map_value < 60
                     THEN (60.0 - map_value) * fragment_minutes
                     ELSE 0.0 END)
                AS map_area_below_60_mmhg_min,

            SUM(CASE WHEN map_value < 55
                     THEN fragment_minutes ELSE 0.0 END)
                AS map_minutes_below_55,

            SUM(CASE WHEN map_value < 55
                     THEN (55.0 - map_value) * fragment_minutes
                     ELSE 0.0 END)
                AS map_area_below_55_mmhg_min

        FROM valid_fragments
        GROUP BY resolution, window_index, window_label, stay_id
        """
    )


def augment_final_intervals_with_burden_and_sensitivity(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Add burden and compact 60-vs-90 sensitivity fields to final windows."""
    con.execute(
        """
        ALTER TABLE map_temporal_interval_summary
        RENAME TO map_temporal_interval_summary_pre_final
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE map_temporal_interval_summary AS
        SELECT
            b.*,

            s.map_locf_supported_minutes_60
                AS map_locf_supported_minutes_sensitivity_60,
            s.map_locf_supported_pct_60
                AS map_locf_supported_pct_sensitivity_60,
            s.map_time_weighted_mean_60
                AS map_time_weighted_mean_sensitivity_60,
            s.map_supported_minutes_gain_90_vs_60
                AS map_supported_minutes_gain_90_vs_60,
            s.map_twa_difference_90_minus_60
                AS map_twa_difference_90_minus_60,
            s.map_twa_absolute_difference
                AS map_twa_absolute_difference_60_vs_90,
            s.support_class_60
                AS map_locf_support_class_sensitivity_60,
            s.support_class_changed_flag
                AS map_locf_support_class_changed_60_vs_90_flag,
            s.twa_difference_band
                AS map_twa_difference_band_60_vs_90,

            CASE
                WHEN b.eligible_minutes <= 0
                  OR b.map_locf_supported_minutes <= 0 THEN NULL
                ELSE COALESCE(h.map_minutes_below_65, 0.0)
            END AS map_minutes_below_65,

            CASE
                WHEN b.eligible_minutes <= 0
                  OR b.map_locf_supported_minutes <= 0 THEN NULL
                ELSE COALESCE(h.map_area_below_65_mmhg_min, 0.0)
            END AS map_area_below_65_mmhg_min,

            CASE
                WHEN b.eligible_minutes <= 0
                  OR b.map_locf_supported_minutes <= 0 THEN NULL
                ELSE 100.0 * COALESCE(h.map_minutes_below_65, 0.0)
                     / b.map_locf_supported_minutes
            END AS map_pct_supported_time_below_65,

            CASE
                WHEN b.eligible_minutes <= 0
                  OR b.map_locf_supported_minutes <= 0 THEN NULL
                ELSE COALESCE(h.map_area_below_65_mmhg_min, 0.0)
                     / b.map_locf_supported_minutes
            END AS map_time_weighted_deficit_below_65,

            CASE
                WHEN COALESCE(h.map_minutes_below_65, 0.0) <= 0 THEN NULL
                ELSE h.map_area_below_65_mmhg_min
                     / h.map_minutes_below_65
            END AS map_mean_deficit_during_below_65,

            CASE
                WHEN b.eligible_minutes <= 0
                  OR b.map_locf_supported_minutes <= 0 THEN NULL
                WHEN COALESCE(h.map_minutes_below_65, 0.0) > 0 THEN 1
                ELSE 0
            END AS map_any_below_65_flag,

            CASE
                WHEN b.eligible_minutes <= 0
                  OR b.map_locf_supported_minutes <= 0 THEN NULL
                ELSE COALESCE(h.map_minutes_below_60, 0.0)
            END AS map_minutes_below_60,

            CASE
                WHEN b.eligible_minutes <= 0
                  OR b.map_locf_supported_minutes <= 0 THEN NULL
                ELSE COALESCE(h.map_area_below_60_mmhg_min, 0.0)
            END AS map_area_below_60_mmhg_min,

            CASE
                WHEN b.eligible_minutes <= 0
                  OR b.map_locf_supported_minutes <= 0 THEN NULL
                ELSE 100.0 * COALESCE(h.map_minutes_below_60, 0.0)
                     / b.map_locf_supported_minutes
            END AS map_pct_supported_time_below_60,

            CASE
                WHEN b.eligible_minutes <= 0
                  OR b.map_locf_supported_minutes <= 0 THEN NULL
                ELSE COALESCE(h.map_area_below_60_mmhg_min, 0.0)
                     / b.map_locf_supported_minutes
            END AS map_time_weighted_deficit_below_60,

            CASE
                WHEN COALESCE(h.map_minutes_below_60, 0.0) <= 0 THEN NULL
                ELSE h.map_area_below_60_mmhg_min
                     / h.map_minutes_below_60
            END AS map_mean_deficit_during_below_60,

            CASE
                WHEN b.eligible_minutes <= 0
                  OR b.map_locf_supported_minutes <= 0 THEN NULL
                WHEN COALESCE(h.map_minutes_below_60, 0.0) > 0 THEN 1
                ELSE 0
            END AS map_any_below_60_flag,

            CASE
                WHEN b.eligible_minutes <= 0
                  OR b.map_locf_supported_minutes <= 0 THEN NULL
                ELSE COALESCE(h.map_minutes_below_55, 0.0)
            END AS map_minutes_below_55,

            CASE
                WHEN b.eligible_minutes <= 0
                  OR b.map_locf_supported_minutes <= 0 THEN NULL
                ELSE COALESCE(h.map_area_below_55_mmhg_min, 0.0)
            END AS map_area_below_55_mmhg_min,

            CASE
                WHEN b.eligible_minutes <= 0
                  OR b.map_locf_supported_minutes <= 0 THEN NULL
                ELSE 100.0 * COALESCE(h.map_minutes_below_55, 0.0)
                     / b.map_locf_supported_minutes
            END AS map_pct_supported_time_below_55,

            CASE
                WHEN b.eligible_minutes <= 0
                  OR b.map_locf_supported_minutes <= 0 THEN NULL
                ELSE COALESCE(h.map_area_below_55_mmhg_min, 0.0)
                     / b.map_locf_supported_minutes
            END AS map_time_weighted_deficit_below_55,

            CASE
                WHEN COALESCE(h.map_minutes_below_55, 0.0) <= 0 THEN NULL
                ELSE h.map_area_below_55_mmhg_min
                     / h.map_minutes_below_55
            END AS map_mean_deficit_during_below_55,

            CASE
                WHEN b.eligible_minutes <= 0
                  OR b.map_locf_supported_minutes <= 0 THEN NULL
                WHEN COALESCE(h.map_minutes_below_55, 0.0) > 0 THEN 1
                ELSE 0
            END AS map_any_below_55_flag,

            CASE
                WHEN b.eligible_minutes <= 0 THEN NULL
                WHEN b.map_locf_supported_pct_of_eligible >= 90 THEN 1
                ELSE 0
            END AS map_burden_support_ge90_flag

        FROM map_temporal_interval_summary_pre_final AS b

        LEFT JOIN map_locf_sensitivity_patient_windows AS s
            ON b.resolution = s.resolution
           AND b.window_index = s.window_index
           AND b.stay_id = s.stay_id

        LEFT JOIN interval_hypotension_summary AS h
            ON b.resolution = h.resolution
           AND b.window_index = h.window_index
           AND b.stay_id = h.stay_id

        ORDER BY b.stay_id, b.resolution, b.window_index
        """
    )


def create_final_extraction_audit(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Create a compact combined extraction, temporal, sensitivity, and burden audit."""
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE final_threshold_window_audit AS
        SELECT
            resolution,
            window_index,
            window_label,
            COUNT(*) FILTER (
                WHERE map_any_below_65_flag = 1
            ) AS patients_below_65_n,
            COUNT(*) FILTER (
                WHERE map_any_below_60_flag = 1
            ) AS patients_below_60_n,
            COUNT(*) FILTER (
                WHERE map_any_below_55_flag = 1
            ) AS patients_below_55_n,
            MEDIAN(map_minutes_below_65) FILTER (
                WHERE map_any_below_65_flag = 1
            ) AS minutes_below_65_median_exposed,
            MEDIAN(map_minutes_below_60) FILTER (
                WHERE map_any_below_60_flag = 1
            ) AS minutes_below_60_median_exposed,
            MEDIAN(map_minutes_below_55) FILTER (
                WHERE map_any_below_55_flag = 1
            ) AS minutes_below_55_median_exposed,
            MEDIAN(map_area_below_65_mmhg_min) FILTER (
                WHERE map_any_below_65_flag = 1
            ) AS area_below_65_median_exposed,
            MEDIAN(map_area_below_60_mmhg_min) FILTER (
                WHERE map_any_below_60_flag = 1
            ) AS area_below_60_median_exposed,
            MEDIAN(map_area_below_55_mmhg_min) FILTER (
                WHERE map_any_below_55_flag = 1
            ) AS area_below_55_median_exposed
        FROM map_temporal_interval_summary
        GROUP BY resolution, window_index, window_label
        """
    )

    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE map_extraction_audit (
            audit_scope VARCHAR,
            resolution VARCHAR,
            window_index INTEGER,
            window_label VARCHAR,
            locf_primary_minutes DOUBLE,
            locf_sensitivity_minutes DOUBLE,
            threshold_65_mmhg DOUBLE,
            threshold_60_mmhg DOUBLE,
            threshold_55_mmhg DOUBLE,
            cohort_n BIGINT,
            raw_map_rows_n BIGINT,
            invalid_raw_rows_n BIGINT,
            clean_source_points_n BIGINT,
            ambiguous_invasive_timestamps_n BIGINT,
            accepted_invasive_points_n BIGINT,
            hierarchical_map_points_n BIGINT,
            invasive_monitoring_episodes_n BIGINT,
            patients_without_map_n BIGINT,
            eligible_patients_n BIGINT,
            fully_observable_patients_n BIGINT,
            twa90_available_n BIGINT,
            twa90_available_pct DOUBLE,
            twa90_median DOUBLE,
            locf90_supported_pct_median DOUBLE,
            locf90_support_ge90_n BIGINT,
            twa60_available_n BIGINT,
            twa60_available_pct DOUBLE,
            locf60_supported_pct_median DOUBLE,
            windows_available_only_with_90_n BIGINT,
            paired_twa_windows_n BIGINT,
            twa_abs_difference_median DOUBLE,
            twa_abs_difference_p95 DOUBLE,
            twa_difference_gt2_n BIGINT,
            twa_difference_gt5_n BIGINT,
            support_class_changed_n BIGINT,
            patients_below_65_n BIGINT,
            patients_below_60_n BIGINT,
            patients_below_55_n BIGINT,
            minutes_below_65_median_exposed DOUBLE,
            minutes_below_60_median_exposed DOUBLE,
            minutes_below_55_median_exposed DOUBLE,
            area_below_65_median_exposed DOUBLE,
            area_below_60_median_exposed DOUBLE,
            area_below_55_median_exposed DOUBLE,
            note VARCHAR
        )
        """
    )

    con.execute(
        """
        INSERT INTO map_extraction_audit
        SELECT
            'OVERALL_EXTRACTION',
            'OVERALL',
            NULL::INTEGER,
            'H00_H24',
            90.0,
            60.0,
            65.0,
            60.0,
            55.0,
            (SELECT COUNT(*) FROM cohort),
            (SELECT COUNT(*) FROM map_raw),
            (
                SELECT COUNT(*) FROM map_raw
                WHERE raw_valid_value_flag = 0
            ),
            (SELECT COUNT(*) FROM map_clean_by_source),
            (
                SELECT COUNT(*) FROM map_clean_by_source
                WHERE map_source = 'INVASIVE'
                  AND wide_invasive_disagreement_flag = 1
            ),
            (SELECT COUNT(*) FROM invasive_values_accepted_numbered),
            (SELECT COUNT(*) FROM map_hierarchical_series),
            (SELECT COUNT(*) FROM invasive_episode_bounds),
            (
                SELECT COUNT(*) FROM map_source_patterns
                WHERE hierarchical_source_pattern_0_24h = 'NO_MAP'
            ),
            (
                SELECT COUNT(*) FROM map_locf_sensitivity_patient_windows
                WHERE resolution = 'HOURLY'
                  AND eligible_minutes > 0
            ),
            NULL::BIGINT,
            (
                SELECT COUNT(*) FROM map_locf_sensitivity_patient_windows
                WHERE resolution = 'HOURLY'
                  AND eligible_minutes > 0
                  AND twa_available_90_flag = 1
            ),
            (
                SELECT 100.0 * COUNT(*) FILTER (
                    WHERE twa_available_90_flag = 1
                ) / NULLIF(COUNT(*), 0)
                FROM map_locf_sensitivity_patient_windows
                WHERE resolution = 'HOURLY'
                  AND eligible_minutes > 0
            ),
            NULL::DOUBLE,
            NULL::DOUBLE,
            NULL::BIGINT,
            (
                SELECT COUNT(*) FROM map_locf_sensitivity_patient_windows
                WHERE resolution = 'HOURLY'
                  AND eligible_minutes > 0
                  AND twa_available_60_flag = 1
            ),
            (
                SELECT 100.0 * COUNT(*) FILTER (
                    WHERE twa_available_60_flag = 1
                ) / NULLIF(COUNT(*), 0)
                FROM map_locf_sensitivity_patient_windows
                WHERE resolution = 'HOURLY'
                  AND eligible_minutes > 0
            ),
            NULL::DOUBLE,
            (
                SELECT COUNT(*) FROM map_locf_sensitivity_patient_windows
                WHERE resolution = 'HOURLY'
                  AND eligible_minutes > 0
                  AND twa_available_60_flag = 0
                  AND twa_available_90_flag = 1
            ),
            (
                SELECT COUNT(*) FROM map_locf_sensitivity_patient_windows
                WHERE resolution = 'HOURLY'
                  AND map_twa_absolute_difference IS NOT NULL
            ),
            (
                SELECT MEDIAN(map_twa_absolute_difference)
                FROM map_locf_sensitivity_patient_windows
                WHERE resolution = 'HOURLY'
                  AND map_twa_absolute_difference IS NOT NULL
            ),
            (
                SELECT QUANTILE_CONT(map_twa_absolute_difference, 0.95)
                FROM map_locf_sensitivity_patient_windows
                WHERE resolution = 'HOURLY'
                  AND map_twa_absolute_difference IS NOT NULL
            ),
            (
                SELECT COUNT(*) FROM map_locf_sensitivity_patient_windows
                WHERE resolution = 'HOURLY'
                  AND map_twa_absolute_difference > 2
            ),
            (
                SELECT COUNT(*) FROM map_locf_sensitivity_patient_windows
                WHERE resolution = 'HOURLY'
                  AND map_twa_absolute_difference > 5
            ),
            (
                SELECT COUNT(*) FROM map_locf_sensitivity_patient_windows
                WHERE resolution = 'HOURLY'
                  AND support_class_changed_flag = 1
            ),
            (
                SELECT COUNT(DISTINCT stay_id)
                FROM map_temporal_interval_summary
                WHERE resolution = 'HOURLY'
                  AND map_any_below_65_flag = 1
            ),
            (
                SELECT COUNT(DISTINCT stay_id)
                FROM map_temporal_interval_summary
                WHERE resolution = 'HOURLY'
                  AND map_any_below_60_flag = 1
            ),
            (
                SELECT COUNT(DISTINCT stay_id)
                FROM map_temporal_interval_summary
                WHERE resolution = 'HOURLY'
                  AND map_any_below_55_flag = 1
            ),
            NULL::DOUBLE,
            NULL::DOUBLE,
            NULL::DOUBLE,
            NULL::DOUBLE,
            NULL::DOUBLE,
            NULL::DOUBLE,
            'Primary hourly TWA and burden use 90-minute LOCF; 60-minute LOCF is retained in final output columns for sensitivity.'
        """
    )

    con.execute(
        """
        INSERT INTO map_extraction_audit
        SELECT
            'TEMPORAL_WINDOW',
            a.resolution,
            CAST(a.window_index AS INTEGER),
            a.window_label,
            90.0,
            60.0,
            65.0,
            60.0,
            55.0,
            NULL::BIGINT,
            NULL::BIGINT,
            NULL::BIGINT,
            NULL::BIGINT,
            NULL::BIGINT,
            NULL::BIGINT,
            NULL::BIGINT,
            NULL::BIGINT,
            NULL::BIGINT,
            a.eligible_patients_n,
            a.fully_observable_patients_n,
            a.patients_with_time_weighted_map_n,
            a.patients_with_time_weighted_map_pct,
            a.map_time_weighted_mean_median,
            a.map_locf_supported_pct_median,
            a.locf_support_ge90_patients_n,
            s.twa_available_60_n,
            s.twa_available_60_pct,
            s.supported_pct_60_median,
            s.windows_available_only_with_90_n,
            s.paired_twa_windows_n,
            s.twa_absolute_difference_median,
            s.twa_absolute_difference_p95,
            s.twa_difference_gt2_n,
            s.twa_difference_gt5_n,
            s.support_class_changed_n,
            t.patients_below_65_n,
            t.patients_below_60_n,
            t.patients_below_55_n,
            t.minutes_below_65_median_exposed,
            t.minutes_below_60_median_exposed,
            t.minutes_below_55_median_exposed,
            t.area_below_65_median_exposed,
            t.area_below_60_median_exposed,
            t.area_below_55_median_exposed,
            'Burden is evaluated only during primary 90-minute LOCF-supported time.'
        FROM map_temporal_aggregation_audit AS a
        LEFT JOIN map_locf_sensitivity_by_window AS s
            ON a.resolution = s.resolution
           AND a.window_index = s.window_index
        LEFT JOIN final_threshold_window_audit AS t
            ON a.resolution = t.resolution
           AND a.window_index = t.window_index
        """
    )


def validate_final_outputs(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Validate canonical row counts, burden ordering, NULL semantics, and reconstruction."""
    hourly_expected = EXPECTED_COHORT_N * HOURLY_WINDOWS_N
    six_hour_expected = EXPECTED_COHORT_N * SIX_HOUR_WINDOWS_N

    hourly_n = con.execute(
        """
        SELECT COUNT(*) FROM map_temporal_interval_summary
        WHERE resolution = 'HOURLY'
        """
    ).fetchone()[0]
    six_hour_n = con.execute(
        """
        SELECT COUNT(*) FROM map_temporal_interval_summary
        WHERE resolution = 'SIX_HOUR'
        """
    ).fetchone()[0]

    if hourly_n != hourly_expected:
        raise MapHierarchyError(
            f"Final hourly table has {hourly_n:,} rows; expected {hourly_expected:,}."
        )
    if six_hour_n != six_hour_expected:
        raise MapHierarchyError(
            f"Final six-hour table has {six_hour_n:,} rows; expected {six_hour_expected:,}."
        )

    invalid_burden = con.execute(
        """
        SELECT COUNT(*)
        FROM map_temporal_interval_summary
        WHERE
            map_minutes_below_65 < -0.0001
         OR map_minutes_below_60 < -0.0001
         OR map_minutes_below_55 < -0.0001
         OR map_area_below_65_mmhg_min < -0.0001
         OR map_area_below_60_mmhg_min < -0.0001
         OR map_area_below_55_mmhg_min < -0.0001
         OR map_minutes_below_55 > map_minutes_below_60 + 0.0001
         OR map_minutes_below_60 > map_minutes_below_65 + 0.0001
         OR map_minutes_below_65 > map_locf_supported_minutes + 0.0001
         OR map_area_below_55_mmhg_min
              > map_area_below_60_mmhg_min + 0.0001
         OR map_area_below_60_mmhg_min
              > map_area_below_65_mmhg_min + 0.0001
         OR map_pct_supported_time_below_65 < -0.0001
         OR map_pct_supported_time_below_65 > 100.0001
         OR map_pct_supported_time_below_60 < -0.0001
         OR map_pct_supported_time_below_60 > 100.0001
         OR map_pct_supported_time_below_55 < -0.0001
         OR map_pct_supported_time_below_55 > 100.0001
        """
    ).fetchone()[0]

    if invalid_burden:
        raise MapHierarchyError(
            f"{invalid_burden:,} patient-windows violate burden invariants."
        )

    invalid_null_semantics = con.execute(
        """
        SELECT COUNT(*)
        FROM map_temporal_interval_summary
        WHERE
            (
                (eligible_minutes <= 0 OR map_locf_supported_minutes <= 0)
                AND (
                    map_minutes_below_65 IS NOT NULL
                    OR map_minutes_below_60 IS NOT NULL
                    OR map_minutes_below_55 IS NOT NULL
                )
            )
         OR (
                eligible_minutes > 0
                AND map_locf_supported_minutes > 0
                AND (
                    map_minutes_below_65 IS NULL
                    OR map_minutes_below_60 IS NULL
                    OR map_minutes_below_55 IS NULL
                )
            )
        """
    ).fetchone()[0]

    if invalid_null_semantics:
        raise MapHierarchyError(
            f"{invalid_null_semantics:,} patient-windows violate burden NULL semantics."
        )

    for metric in (
        "map_minutes_below_65",
        "map_minutes_below_60",
        "map_minutes_below_55",
        "map_area_below_65_mmhg_min",
        "map_area_below_60_mmhg_min",
        "map_area_below_55_mmhg_min",
    ):
        hourly_total = con.execute(
            f"""
            SELECT SUM({metric})
            FROM map_temporal_interval_summary
            WHERE resolution = 'HOURLY'
            """
        ).fetchone()[0] or 0.0
        six_hour_total = con.execute(
            f"""
            SELECT SUM({metric})
            FROM map_temporal_interval_summary
            WHERE resolution = 'SIX_HOUR'
            """
        ).fetchone()[0] or 0.0

        if abs(hourly_total - six_hour_total) > 0.1:
            raise MapHierarchyError(
                f"Hourly and six-hour totals do not reconstruct for {metric}: "
                f"{hourly_total:.4f} vs {six_hour_total:.4f}."
            )

    audit_n = con.execute(
        "SELECT COUNT(*) FROM map_extraction_audit"
    ).fetchone()[0]
    expected_audit_n = 1 + HOURLY_WINDOWS_N + SIX_HOUR_WINDOWS_N

    if audit_n != expected_audit_n:
        raise MapHierarchyError(
            f"Final audit has {audit_n:,} rows; expected {expected_audit_n:,}."
        )


def canonical_temporal_select_sql() -> str:
    """Return the final canonical column list for hourly and six-hour outputs."""
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
        map_points_n,
        map_invasive_points_n,
        map_nibp_points_n,
        map_nibp_ambiguous_rescue_points_n,
        map_warning_points_n,
        ambiguous_invasive_timestamps_n,
        map_arithmetic_mean_observed,
        map_sd_observed,
        map_min_observed,
        map_max_observed,
        map_first_observed,
        map_last_observed,
        map_delta_last_minus_first_observed,
        map_first_charttime,
        map_last_charttime,
        map_first_point_offset_minutes,
        map_last_point_to_eligible_end_minutes,
        map_max_internal_point_gap_minutes,
        map_time_weighted_mean,
        map_locf_supported_minutes,
        map_locf_supported_pct_of_eligible,
        map_locf_invasive_minutes,
        map_locf_nibp_minutes,
        map_locf_support_class,
        map_twa_minus_arithmetic_mean,
        map_time_weighted_mean_sensitivity_60,
        map_locf_supported_minutes_sensitivity_60,
        map_locf_supported_pct_sensitivity_60,
        map_supported_minutes_gain_90_vs_60,
        map_twa_difference_90_minus_60,
        map_twa_absolute_difference_60_vs_90,
        map_locf_support_class_sensitivity_60,
        map_locf_support_class_changed_60_vs_90_flag,
        map_twa_difference_band_60_vs_90,
        map_minutes_below_65,
        map_area_below_65_mmhg_min,
        map_pct_supported_time_below_65,
        map_time_weighted_deficit_below_65,
        map_mean_deficit_during_below_65,
        map_any_below_65_flag,
        map_minutes_below_60,
        map_area_below_60_mmhg_min,
        map_pct_supported_time_below_60,
        map_time_weighted_deficit_below_60,
        map_mean_deficit_during_below_60,
        map_any_below_60_flag,
        map_minutes_below_55,
        map_area_below_55_mmhg_min,
        map_pct_supported_time_below_55,
        map_time_weighted_deficit_below_55,
        map_mean_deficit_during_below_55,
        map_any_below_55_flag,
        map_burden_support_ge90_flag
    """


def export_outputs(con: duckdb.DuckDBPyConnection) -> None:
    """Export only the three declared canonical outputs."""
    paths = (
        HOURLY_OUTPUT_PATH,
        SIX_HOUR_OUTPUT_PATH,
        FINAL_AUDIT_OUTPUT_PATH,
    )

    for path in paths:
        if path.exists():
            path.unlink()

    hourly_output = sql_path(HOURLY_OUTPUT_PATH)
    six_hour_output = sql_path(SIX_HOUR_OUTPUT_PATH)
    audit_output = sql_path(FINAL_AUDIT_OUTPUT_PATH)
    columns = canonical_temporal_select_sql()

    con.execute(
        f"""
        COPY (
            SELECT {columns}
            FROM map_temporal_interval_summary
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
            FROM map_temporal_interval_summary
            WHERE resolution = 'SIX_HOUR'
            ORDER BY stay_id, window_index
        )
        TO '{six_hour_output}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    con.execute(
        f"""
        COPY map_extraction_audit
        TO '{audit_output}'
        (HEADER, DELIMITER ',', QUOTE '"', ESCAPE '"', NULL '')
        """
    )

def print_final_summary(con: duckdb.DuckDBPyConnection) -> None:
    """Print the canonical extraction, sensitivity, and burden summary."""
    raw_n = con.execute("SELECT COUNT(*) FROM map_raw").fetchone()[0]
    invalid_n = con.execute(
        "SELECT COUNT(*) FROM map_raw WHERE raw_valid_value_flag = 0"
    ).fetchone()[0]
    hierarchy_n = con.execute(
        "SELECT COUNT(*) FROM map_hierarchical_series"
    ).fetchone()[0]
    no_map_n = con.execute(
        """
        SELECT COUNT(*) FROM map_source_patterns
        WHERE hierarchical_source_pattern_0_24h = 'NO_MAP'
        """
    ).fetchone()[0]
    hourly_n = con.execute(
        """
        SELECT COUNT(*) FROM map_temporal_interval_summary
        WHERE resolution = 'HOURLY'
        """
    ).fetchone()[0]
    six_hour_n = con.execute(
        """
        SELECT COUNT(*) FROM map_temporal_interval_summary
        WHERE resolution = 'SIX_HOUR'
        """
    ).fetchone()[0]

    burden = con.execute(
        """
        SELECT
            COUNT(DISTINCT stay_id) FILTER (
                WHERE map_any_below_65_flag = 1
            ),
            COUNT(DISTINCT stay_id) FILTER (
                WHERE map_any_below_60_flag = 1
            ),
            COUNT(DISTINCT stay_id) FILTER (
                WHERE map_any_below_55_flag = 1
            ),
            SUM(map_minutes_below_65),
            SUM(map_minutes_below_60),
            SUM(map_minutes_below_55),
            SUM(map_area_below_65_mmhg_min),
            SUM(map_area_below_60_mmhg_min),
            SUM(map_area_below_55_mmhg_min)
        FROM map_temporal_interval_summary
        WHERE resolution = 'HOURLY'
        """
    ).fetchone()

    sensitivity = con.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE eligible_minutes > 0),
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND twa_available_60_flag = 1
            ),
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND twa_available_90_flag = 1
            ),
            COUNT(*) FILTER (WHERE map_twa_absolute_difference > 2),
            COUNT(*) FILTER (WHERE map_twa_absolute_difference > 5)
        FROM map_locf_sensitivity_patient_windows
        WHERE resolution = 'HOURLY'
        """
    ).fetchone()

    (
        patients_below_65,
        patients_below_60,
        patients_below_55,
        minutes_below_65,
        minutes_below_60,
        minutes_below_55,
        area_below_65,
        area_below_60,
        area_below_55,
    ) = burden

    eligible_hourly, twa60_n, twa90_n, diff_gt2_n, diff_gt5_n = sensitivity

    print("=" * 150)
    print("FINAL MAP EXTRACTION, TRAJECTORY, AND HYPOTENSION BURDEN")
    print("=" * 150)
    print(f"Definitive analytical cohort:                  {EXPECTED_COHORT_N:,}")
    print(f"Raw numeric MAP rows:                          {raw_n:,}")
    print(f"Invalid raw rows removed:                      {invalid_n:,}")
    print(f"Final hierarchical MAP points:                 {hierarchy_n:,}")
    print(f"Patients without valid MAP:                    {no_map_n:,}")
    print(f"Hourly trajectory rows:                        {hourly_n:,}")
    print(f"Six-hour summary rows:                         {six_hour_n:,}")
    print("Primary / sensitivity LOCF:                    90 / 60 minutes")
    print("Hypotension thresholds:                        MAP <65, <60, <55 mmHg")

    print("\nLOCF sensitivity")
    print("-" * 100)
    print(f"  Eligible hourly windows:                     {eligible_hourly:,}")
    print(f"  TWA available with 60 / 90 min:             {twa60_n:,} / {twa90_n:,}")
    print(f"  Hourly absolute TWA differences >2 / >5:    {diff_gt2_n:,} / {diff_gt5_n:,}")

    print("\nHypotension burden during primary LOCF-supported time")
    print("-" * 100)
    print(f"  Patients with any MAP <65:                  {patients_below_65:,}")
    print(f"  Patients with any MAP <60:                  {patients_below_60:,}")
    print(f"  Patients with any MAP <55:                  {patients_below_55:,}")
    print(
        f"  Total minutes below 65 / 60 / 55:           "
        f"{minutes_below_65:,.1f} / {minutes_below_60:,.1f} / "
        f"{minutes_below_55:,.1f}"
    )
    print(
        f"  Total area below 65 / 60 / 55:              "
        f"{area_below_65:,.1f} / {area_below_60:,.1f} / "
        f"{area_below_55:,.1f} mmHg*min"
    )

    print("\nCanonical outputs")
    print("-" * 100)
    print(f"  {HOURLY_OUTPUT_PATH}")
    print(f"  {SIX_HOUR_OUTPUT_PATH}")
    print(f"  {FINAL_AUDIT_OUTPUT_PATH}")
    print("No raw or intermediate MAP objects were exported.")
    print("=" * 150)


def main() -> int:
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        validate_inputs()

        con = duckdb.connect(":memory:")

        try:
            create_source_views(con)
            validate_cohort_and_itemids(con)
            create_audit_bins(con)
            extract_raw_map(con)
            create_clean_source_series(con)
            create_invasive_episodes(con)
            create_hierarchical_series(con)
            create_source_patterns(con)
            create_hierarchy_audit(con)
            create_quality_cases(con)

            create_temporal_windows(con)
            create_patient_windows(con)
            create_temporal_support_segments(con)

            create_locf_segments(con)
            create_interval_point_summaries(con)
            create_interval_support_summaries(con)
            create_interval_locf_summaries(con)
            create_temporal_interval_summaries(con)
            augment_temporal_intervals_with_locf(con)
            create_temporal_aggregation_audit(con)

            create_locf_segments_60(con)
            create_interval_locf_summaries_60(con)
            create_locf_sensitivity_comparison(con)

            create_interval_hypotension_summaries(con)
            augment_final_intervals_with_burden_and_sensitivity(con)
            create_final_extraction_audit(con)

            validate_outputs(con)
            validate_final_outputs(con)
            export_outputs(con)
            print_final_summary(con)

        finally:
            con.close()

        return 0

    except MapHierarchyError as exc:
        print("=" * 118, file=sys.stderr)
        print("CANONICAL MAP EXTRACTION FAILED", file=sys.stderr)
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
