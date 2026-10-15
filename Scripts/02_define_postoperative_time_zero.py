#!/usr/bin/env python3
"""
Create the definitive postoperative time-zero dataset.

FINAL SCIENTIFIC RULE
---------------------
For each eligible cardiac-surgery admission, postoperative time zero is the
first charttime of the sustained core-monitoring episode associated with
postoperative CVICU arrival.

A monitoring gap >3 hours starts a new episode.

A sustained episode requires, during its first 2 hours:
- at least 2 distinct charttimes;
- at least 2 monitoring domains;
- at least 4 charted rows.

Core monitoring domains:
- heart rate;
- mean arterial pressure;
- respiratory rate;
- peripheral oxygen saturation.

OR Received (itemid 225469) validates episode selection but never replaces the
first monitor charttime.

Candidates earlier than icustays.intime - 1 hour are rejected.

Definitive selection hierarchy:
1. OR-compatible sustained monitoring episode;
2. sustained episode after a monitoring gap >3 hours;
3. first sustained monitoring episode;
4. first available monitoring episode;
5. first core-monitor event;
6. icustays.intime as final fallback.

FINAL COHORT CORRECTION
-----------------------
Alive observations shorter than 6 hours are excluded only when a different ICU
stay_id in the same hadm_id begins within 3 hours of the recorded ICU endpoint.
This reproducible rule identifies ambiguous ICU-stay segmentation, not a
clinical readmission. In MIMIC-IV 3.1 it excludes exactly 8 admissions.

REPRODUCIBILITY
---------------
- Reads only raw MIMIC-IV 3.1 files and the retained canonical starting cohort.
- Produces one canonical dataset.
- Does not create handovers.
- Never deletes, renames, or moves unrelated files.
- May replace only its own declared canonical output when rerun.
- Can be rerun against unchanged inputs to reproduce the same result.

INPUTS
------
data/00_cardiac_surgery_starting_cohort.csv
MIMIC_ROOT/icu/chartevents.csv.gz
MIMIC_ROOT/icu/procedureevents.csv.gz
MIMIC_ROOT/icu/icustays.csv.gz

OUTPUT
------
data/00_03_postoperative_time_zero.csv
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

COHORT_PATH = DATA_DIR / "00_cardiac_surgery_starting_cohort.csv"
OUTPUT_PATH = DATA_DIR / "00_03_postoperative_time_zero.csv"

# Set this path to your local MIMIC-IV 3.1 directory (containing hosp/ and icu/).
MIMIC_ROOT = Path("path/to/mimic-iv-3.1")
CHARTEVENTS_PATH = MIMIC_ROOT / "icu" / "chartevents.csv.gz"
PROCEDUREEVENTS_PATH = MIMIC_ROOT / "icu" / "procedureevents.csv.gz"
ICUSTAYS_PATH = MIMIC_ROOT / "icu" / "icustays.csv.gz"

CVICU_NAME = "Cardiac Vascular Intensive Care Unit (CVICU)"

MONITORING_GAP_HOURS = 3.0
SUSTAINED_WINDOW_HOURS = 2.0
MIN_TIMEPOINTS_FIRST_2H = 2
MIN_DOMAINS_FIRST_2H = 2
MIN_ROWS_FIRST_2H = 4

EARLIEST_ALLOWED_HOURS_BEFORE_ICU_INTIME = 1.0
# Search ends strictly before 36 hours after midnight of the operation date.
MAX_SEARCH_HOURS_AFTER_OPERATION_DATE = 36.0

OR_RECEIVED_ITEMID = 225469
OR_COMPATIBLE_BEFORE_HOURS = 1.0
OR_COMPATIBLE_AFTER_HOURS = 2.0

SHORT_ALIVE_STAY_HOURS = 6.0
AMBIGUOUS_NEXT_ICU_WINDOW_HOURS = 3.0

# Fixed reproduction checks for the validated MIMIC-IV 3.1 analysis.
EXPECTED_VALID_SAME_DAY_CVICU_N = 9313
EXPECTED_AMBIGUOUS_SEGMENTATION_N = 8
EXPECTED_FINAL_COHORT_N = 9305

CORE_ITEMIDS = (
    220045,              # Heart rate
    220052, 225312,      # Invasive MAP
    220181,              # Non-invasive MAP
    220210, 224690,      # Respiratory rate
    220277,              # SpO2
)


class TimeZeroError(RuntimeError):
    """Raised when a mandatory invariant is violated."""


def sql_path(path: Path) -> str:
    """Return an absolute DuckDB-safe path."""
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def validate_inputs() -> None:
    required = {
        "canonical starting cohort": COHORT_PATH,
        "chartevents": CHARTEVENTS_PATH,
        "procedureevents": PROCEDUREEVENTS_PATH,
        "icustays": ICUSTAYS_PATH,
    }
    missing = [
        f"{name}: {path}"
        for name, path in required.items()
        if not path.is_file()
    ]
    if missing:
        raise TimeZeroError(
            "Required file(s) not found:\n- " + "\n- ".join(missing)
        )


def create_source_views(con: duckdb.DuckDBPyConnection) -> None:
    cohort = sql_path(COHORT_PATH)
    chartevents = sql_path(CHARTEVENTS_PATH)
    procedureevents = sql_path(PROCEDUREEVENTS_PATH)
    icustays = sql_path(ICUSTAYS_PATH)

    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW cohort_source AS
        SELECT
            CAST(subject_id AS BIGINT) AS subject_id,
            CAST(hadm_id AS BIGINT) AS hadm_id,
            CAST(stay_id AS BIGINT) AS stay_id,
            CAST(index_operation_date AS DATE) AS index_operation_date,
            CAST(first_careunit AS VARCHAR) AS first_careunit,
            CAST(time_zero_icu AS TIMESTAMP) AS current_icu_intime,
            CAST(icu_outtime AS TIMESTAMP) AS current_icu_outtime,
            CAST(hospital_deathtime AS TIMESTAMP) AS hospital_deathtime
        FROM read_csv_auto(
            '{cohort}',
            header = true,
            sample_size = -1
        )
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW chartevents_source AS
        SELECT
            subject_id,
            hadm_id,
            stay_id,
            charttime,
            itemid,
            valuenum
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

    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW procedureevents_source AS
        SELECT
            CAST(subject_id AS BIGINT) AS subject_id,
            CAST(hadm_id AS BIGINT) AS hadm_id,
            CAST(stay_id AS BIGINT) AS stay_id,
            CAST(starttime AS TIMESTAMP) AS starttime,
            CAST(storetime AS TIMESTAMP) AS storetime,
            CAST(itemid AS BIGINT) AS itemid
        FROM read_csv_auto(
            '{procedureevents}',
            header = true,
            all_varchar = true,
            sample_size = 10000,
            strict_mode = false,
            null_padding = true,
            escape = '"'
        )
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW icustays_source AS
        SELECT
            CAST(subject_id AS BIGINT) AS subject_id,
            CAST(hadm_id AS BIGINT) AS hadm_id,
            CAST(stay_id AS BIGINT) AS stay_id,
            CAST(first_careunit AS VARCHAR) AS first_careunit,
            CAST(last_careunit AS VARCHAR) AS last_careunit,
            CAST(intime AS TIMESTAMP) AS intime,
            CAST(outtime AS TIMESTAMP) AS outtime
        FROM read_csv_auto(
            '{icustays}',
            header = true,
            all_varchar = true,
            sample_size = -1
        )
        """
    )


def create_valid_cohort(con: duckdb.DuckDBPyConnection) -> None:
    # Retain initial CVICU stays whose ICU admission date equals the operation date.
    # Observation ends at hospital death if it falls within the recorded ICU
    # interval (inclusive); otherwise it ends at the recorded ICU discharge.
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE valid_cohort AS
        SELECT
            subject_id,
            hadm_id,
            stay_id,
            index_operation_date,
            current_icu_intime,
            current_icu_outtime,
            hospital_deathtime,

            CASE
                WHEN hospital_deathtime IS NOT NULL
                 AND hospital_deathtime >= current_icu_intime
                 AND hospital_deathtime <= current_icu_outtime
                    THEN hospital_deathtime
                ELSE current_icu_outtime
            END AS observation_end_time,

            CASE
                WHEN hospital_deathtime IS NOT NULL
                 AND hospital_deathtime >= current_icu_intime
                 AND hospital_deathtime <= current_icu_outtime
                    THEN 'DIED_IN_ICU'
                ELSE 'DISCHARGED_ALIVE_FROM_ICU'
            END AS observation_end_type

        FROM cohort_source
        WHERE first_careunit = ?
          AND CAST(current_icu_intime AS DATE) = index_operation_date
        """,
        [CVICU_NAME],
    )

    n = con.execute("SELECT COUNT(*) FROM valid_cohort").fetchone()[0]
    if n != EXPECTED_VALID_SAME_DAY_CVICU_N:
        raise TimeZeroError(
            f"Valid same-day CVICU cohort has {n:,} rows; "
            f"expected {EXPECTED_VALID_SAME_DAY_CVICU_N:,}."
        )

    duplicates = con.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT stay_id
            FROM valid_cohort
            GROUP BY stay_id
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    if duplicates:
        raise TimeZeroError(
            f"{duplicates:,} duplicated stay_id group(s) in valid cohort."
        )


def extract_core_monitoring(con: duckdb.DuckDBPyConnection) -> None:
    itemids = ",".join(str(x) for x in CORE_ITEMIDS)

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE core_monitor_rows AS
        SELECT
            c.subject_id,
            c.hadm_id,
            c.stay_id,
            c.index_operation_date,
            c.current_icu_intime,
            c.observation_end_time,
            ce.charttime,
            ce.itemid,

            CASE
                WHEN ce.itemid = 220045
                    THEN 'HEART_RATE'
                WHEN ce.itemid IN (220052, 225312, 220181)
                    THEN 'MAP'
                WHEN ce.itemid IN (220210, 224690)
                    THEN 'RESPIRATORY_RATE'
                WHEN ce.itemid = 220277
                    THEN 'SPO2'
                ELSE 'OTHER'
            END AS monitoring_domain

        FROM chartevents_source AS ce
        INNER JOIN valid_cohort AS c
            ON ce.stay_id = c.stay_id
        WHERE ce.itemid IN ({itemids})
          AND ce.valuenum IS NOT NULL
          AND ce.charttime >=
              c.current_icu_intime
              - INTERVAL '{EARLIEST_ALLOWED_HOURS_BEFORE_ICU_INTIME} hours'
          AND ce.charttime <= c.observation_end_time
          AND ce.charttime <
              CAST(c.index_operation_date AS TIMESTAMP)
              + INTERVAL '{MAX_SEARCH_HOURS_AFTER_OPERATION_DATE} hours'
        """
    )

    n = con.execute(
        "SELECT COUNT(*) FROM core_monitor_rows"
    ).fetchone()[0]
    if n == 0:
        raise TimeZeroError("No core-monitoring rows were extracted.")


def build_monitoring_episodes(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE numbered_timepoints AS
        WITH timepoints AS (
            SELECT
                subject_id,
                hadm_id,
                stay_id,
                index_operation_date,
                current_icu_intime,
                observation_end_time,
                charttime
            FROM core_monitor_rows
            GROUP BY
                subject_id,
                hadm_id,
                stay_id,
                index_operation_date,
                current_icu_intime,
                observation_end_time,
                charttime
        ),

        gaps AS (
            SELECT
                *,
                LAG(charttime) OVER (
                    PARTITION BY stay_id
                    ORDER BY charttime
                ) AS previous_charttime
            FROM timepoints
        ),

        marked AS (
            SELECT
                *,

                CASE
                    WHEN previous_charttime IS NULL
                        THEN NULL
                    ELSE DATE_DIFF(
                        'second',
                        previous_charttime,
                        charttime
                    ) / 3600.0
                END AS gap_from_previous_hours,

                CASE
                    WHEN previous_charttime IS NULL
                        THEN 1
                    WHEN DATE_DIFF(
                            'second',
                            previous_charttime,
                            charttime
                         ) / 3600.0 > {MONITORING_GAP_HOURS}
                        THEN 1
                    ELSE 0
                END AS episode_start_flag

            FROM gaps
        )

        SELECT
            *,

            SUM(episode_start_flag) OVER (
                PARTITION BY stay_id
                ORDER BY charttime
                ROWS BETWEEN UNBOUNDED PRECEDING
                         AND CURRENT ROW
            ) AS episode_number

        FROM marked
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE monitoring_episodes AS
        WITH bounds AS (
            SELECT
                subject_id,
                hadm_id,
                stay_id,
                index_operation_date,
                current_icu_intime,
                observation_end_time,
                episode_number,
                MIN(charttime) AS episode_start,
                MAX(charttime) AS episode_end,
                MAX(
                    CASE
                        WHEN episode_start_flag = 1
                            THEN gap_from_previous_hours
                        ELSE NULL
                    END
                ) AS preceding_gap_hours
            FROM numbered_timepoints
            GROUP BY
                subject_id,
                hadm_id,
                stay_id,
                index_operation_date,
                current_icu_intime,
                observation_end_time,
                episode_number
        )

        SELECT
            b.subject_id,
            b.hadm_id,
            b.stay_id,
            b.index_operation_date,
            b.current_icu_intime,
            b.observation_end_time,
            b.episode_number,
            b.episode_start,
            b.episode_end,
            b.preceding_gap_hours,

            COUNT(DISTINCT r.charttime)
                AS distinct_charttimes_total,

            COUNT(DISTINCT r.charttime) FILTER (
                WHERE r.charttime <
                      b.episode_start
                      + INTERVAL '{SUSTAINED_WINDOW_HOURS} hours'
            ) AS distinct_charttimes_first_2h,

            COUNT(*) FILTER (
                WHERE r.charttime <
                      b.episode_start
                      + INTERVAL '{SUSTAINED_WINDOW_HOURS} hours'
            ) AS charted_rows_first_2h,

            COUNT(DISTINCT r.monitoring_domain) FILTER (
                WHERE r.charttime <
                      b.episode_start
                      + INTERVAL '{SUSTAINED_WINDOW_HOURS} hours'
            ) AS domains_first_2h,

            CASE
                WHEN COUNT(DISTINCT r.charttime) FILTER (
                        WHERE r.charttime <
                              b.episode_start
                              + INTERVAL '{SUSTAINED_WINDOW_HOURS} hours'
                     ) >= {MIN_TIMEPOINTS_FIRST_2H}
                 AND COUNT(*) FILTER (
                        WHERE r.charttime <
                              b.episode_start
                              + INTERVAL '{SUSTAINED_WINDOW_HOURS} hours'
                     ) >= {MIN_ROWS_FIRST_2H}
                 AND COUNT(DISTINCT r.monitoring_domain) FILTER (
                        WHERE r.charttime <
                              b.episode_start
                              + INTERVAL '{SUSTAINED_WINDOW_HOURS} hours'
                     ) >= {MIN_DOMAINS_FIRST_2H}
                    THEN 1
                ELSE 0
            END AS sustained_episode_flag

        FROM bounds AS b
        INNER JOIN numbered_timepoints AS n
            ON b.stay_id = n.stay_id
           AND b.episode_number = n.episode_number
        INNER JOIN core_monitor_rows AS r
            ON n.stay_id = r.stay_id
           AND n.charttime = r.charttime
        GROUP BY
            b.subject_id,
            b.hadm_id,
            b.stay_id,
            b.index_operation_date,
            b.current_icu_intime,
            b.observation_end_time,
            b.episode_number,
            b.episode_start,
            b.episode_end,
            b.preceding_gap_hours
        """
    )


def extract_first_or_received(con: duckdb.DuckDBPyConnection) -> None:
    # Use the earliest eligible OR Received starttime, with storetime breaking ties.
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE first_or_received AS
        WITH ranked AS (
            SELECT
                c.stay_id,
                p.starttime AS or_received_time,

                ROW_NUMBER() OVER (
                    PARTITION BY c.stay_id
                    ORDER BY p.starttime, p.storetime
                ) AS row_number

            FROM valid_cohort AS c
            INNER JOIN procedureevents_source AS p
                ON c.stay_id = p.stay_id
            WHERE p.itemid = {OR_RECEIVED_ITEMID}
              AND p.starttime >=
                  c.current_icu_intime
                  - INTERVAL '{EARLIEST_ALLOWED_HOURS_BEFORE_ICU_INTIME} hours'
              AND p.starttime <= c.observation_end_time
              AND p.starttime <
                  CAST(c.index_operation_date AS TIMESTAMP)
                  + INTERVAL '{MAX_SEARCH_HOURS_AFTER_OPERATION_DATE} hours'
        )

        SELECT stay_id, or_received_time
        FROM ranked
        WHERE row_number = 1
        """
    )


def assign_time_zero(con: duckdb.DuckDBPyConnection) -> None:
    # Prefer sustained episodes starting from 1 hour before to 2 hours after the
    # selected OR event, ranked by proximity to it; then apply the documented
    # gap/sustained/available hierarchy, breaking remaining ties by start and number.
    # The selected monitor start remains time zero; the OR timestamp does not replace it.
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE assigned_time_zero AS
        WITH candidate_episodes AS (
            SELECT
                e.*,
                o.or_received_time,

                CASE
                    WHEN o.or_received_time IS NOT NULL
                     AND e.sustained_episode_flag = 1
                     AND e.episode_start >=
                         o.or_received_time
                         - INTERVAL '{OR_COMPATIBLE_BEFORE_HOURS} hours'
                     AND e.episode_start <=
                         o.or_received_time
                         + INTERVAL '{OR_COMPATIBLE_AFTER_HOURS} hours'
                        THEN 1

                    WHEN e.sustained_episode_flag = 1
                     AND e.preceding_gap_hours > {MONITORING_GAP_HOURS}
                        THEN 2

                    WHEN e.sustained_episode_flag = 1
                        THEN 3

                    ELSE 4
                END AS selection_priority,

                CASE
                    WHEN o.or_received_time IS NULL
                        THEN NULL
                    ELSE ABS(
                        DATE_DIFF(
                            'second',
                            o.or_received_time,
                            e.episode_start
                        ) / 3600.0
                    )
                END AS absolute_or_distance_hours

            FROM monitoring_episodes AS e
            LEFT JOIN first_or_received AS o
                USING (stay_id)
        ),

        ranked AS (
            SELECT
                *,

                ROW_NUMBER() OVER (
                    PARTITION BY stay_id
                    ORDER BY
                        selection_priority,
                        CASE
                            WHEN selection_priority = 1
                                THEN absolute_or_distance_hours
                            ELSE NULL
                        END,
                        episode_start,
                        episode_number
                ) AS selection_rank

            FROM candidate_episodes
        ),

        chosen AS (
            SELECT *
            FROM ranked
            WHERE selection_rank = 1
        ),

        first_core_event AS (
            SELECT
                c.stay_id,
                MIN(r.charttime) AS first_core_monitor_time
            FROM valid_cohort AS c
            LEFT JOIN core_monitor_rows AS r
                ON c.stay_id = r.stay_id
            GROUP BY c.stay_id
        )

        SELECT
            c.subject_id,
            c.hadm_id,
            c.stay_id,
            c.current_icu_intime,
            c.observation_end_time,
            c.observation_end_type,

            COALESCE(
                e.episode_start,
                f.first_core_monitor_time,
                c.current_icu_intime
            ) AS postoperative_time_zero,

            CASE
                WHEN e.selection_priority = 1
                    THEN 'OR_VALIDATED_SUSTAINED_MONITORING'
                WHEN e.selection_priority = 2
                    THEN 'SUSTAINED_MONITORING_AFTER_GAP'
                WHEN e.selection_priority = 3
                    THEN 'FIRST_SUSTAINED_MONITORING'
                WHEN e.episode_start IS NOT NULL
                    THEN 'FIRST_AVAILABLE_MONITORING_EPISODE'
                WHEN f.first_core_monitor_time IS NOT NULL
                    THEN 'FIRST_CORE_MONITOR_EVENT'
                ELSE 'ICUSTAY_INTIME_FALLBACK'
            END AS internal_method

        FROM valid_cohort AS c
        LEFT JOIN chosen AS e
            ON c.stay_id = e.stay_id
        LEFT JOIN first_core_event AS f
            ON c.stay_id = f.stay_id
        """
    )


def identify_ambiguous_segmentation(
    con: duckdb.DuckDBPyConnection,
) -> None:
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE ambiguous_segmentation AS
        SELECT DISTINCT
            t.subject_id,
            t.hadm_id,
            t.stay_id,
            i.stay_id AS subsequent_icu_stay_id,
            i.intime AS subsequent_icu_intime,

            DATE_DIFF(
                'second',
                t.observation_end_time,
                i.intime
            ) / 3600.0 AS hours_to_subsequent_icu

        FROM assigned_time_zero AS t
        INNER JOIN icustays_source AS i
            ON t.subject_id = i.subject_id
           AND t.hadm_id = i.hadm_id
           AND t.stay_id <> i.stay_id
           AND i.intime >= t.observation_end_time
           AND i.intime <=
               t.observation_end_time
               + INTERVAL '{AMBIGUOUS_NEXT_ICU_WINDOW_HOURS} hours'

        WHERE t.observation_end_type = 'DISCHARGED_ALIVE_FROM_ICU'
          AND DATE_DIFF(
                'second',
                t.postoperative_time_zero,
                t.observation_end_time
              ) / 3600.0 < {SHORT_ALIVE_STAY_HOURS}
        """
    )

    excluded_n = con.execute(
        "SELECT COUNT(DISTINCT stay_id) FROM ambiguous_segmentation"
    ).fetchone()[0]

    if excluded_n != EXPECTED_AMBIGUOUS_SEGMENTATION_N:
        raise TimeZeroError(
            f"Ambiguous segmentation rule identified {excluded_n:,} cases; "
            f"expected {EXPECTED_AMBIGUOUS_SEGMENTATION_N:,}."
        )


def create_final_output(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE final_time_zero AS
        SELECT
            t.subject_id,
            t.hadm_id,
            t.stay_id,
            t.postoperative_time_zero,
            t.observation_end_time,
            t.observation_end_type

        FROM assigned_time_zero AS t
        LEFT JOIN ambiguous_segmentation AS x
            ON t.stay_id = x.stay_id
        WHERE x.stay_id IS NULL
        ORDER BY t.stay_id
        """
    )


def validate_final_output(con: duckdb.DuckDBPyConnection) -> None:
    n = con.execute("SELECT COUNT(*) FROM final_time_zero").fetchone()[0]
    if n != EXPECTED_FINAL_COHORT_N:
        raise TimeZeroError(
            f"Final cohort has {n:,} rows; "
            f"expected {EXPECTED_FINAL_COHORT_N:,}."
        )

    duplicate_stays = con.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT stay_id
            FROM final_time_zero
            GROUP BY stay_id
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    if duplicate_stays:
        raise TimeZeroError(
            f"{duplicate_stays:,} duplicated stay_id group(s)."
        )

    missing = con.execute(
        """
        SELECT COUNT(*)
        FROM final_time_zero
        WHERE subject_id IS NULL
           OR hadm_id IS NULL
           OR stay_id IS NULL
           OR postoperative_time_zero IS NULL
           OR observation_end_time IS NULL
           OR observation_end_type IS NULL
        """
    ).fetchone()[0]
    if missing:
        raise TimeZeroError(
            f"{missing:,} rows contain mandatory missing values."
        )

    invalid_order = con.execute(
        """
        SELECT COUNT(*)
        FROM final_time_zero
        WHERE postoperative_time_zero > observation_end_time
        """
    ).fetchone()[0]
    if invalid_order:
        raise TimeZeroError(
            f"{invalid_order:,} time-zero values occur after observation end."
        )

    too_early = con.execute(
        f"""
        SELECT COUNT(*)
        FROM assigned_time_zero
        WHERE postoperative_time_zero <
              current_icu_intime
              - INTERVAL '{EARLIEST_ALLOWED_HOURS_BEFORE_ICU_INTIME} hours'
        """
    ).fetchone()[0]
    if too_early:
        raise TimeZeroError(
            f"{too_early:,} time-zero values precede the accepted limit."
        )


def export_final_output(con: duckdb.DuckDBPyConnection) -> None:
    # Replacing the script's own declared output is required for reproducible
    # reruns. No other file is modified.
    if OUTPUT_PATH.exists():
        OUTPUT_PATH.unlink()

    output = sql_path(OUTPUT_PATH)

    con.execute(
        f"""
        COPY final_time_zero
        TO '{output}'
        (
            HEADER,
            DELIMITER ',',
            QUOTE '"',
            ESCAPE '"',
            NULL ''
        )
        """
    )


def print_summary(con: duckdb.DuckDBPyConnection) -> None:
    n = con.execute("SELECT COUNT(*) FROM final_time_zero").fetchone()[0]
    excluded = con.execute(
        "SELECT COUNT(DISTINCT stay_id) FROM ambiguous_segmentation"
    ).fetchone()[0]
    deaths = con.execute(
        """
        SELECT COUNT(*)
        FROM final_time_zero
        WHERE observation_end_type = 'DIED_IN_ICU'
        """
    ).fetchone()[0]
    full_24h = con.execute(
        """
        SELECT COUNT(*)
        FROM final_time_zero
        WHERE observation_end_time >=
              postoperative_time_zero + INTERVAL '24 hours'
        """
    ).fetchone()[0]
    methods = con.execute(
        """
        SELECT internal_method, COUNT(*)
        FROM assigned_time_zero AS a
        LEFT JOIN ambiguous_segmentation AS x
            ON a.stay_id = x.stay_id
        WHERE x.stay_id IS NULL
        GROUP BY internal_method
        ORDER BY COUNT(*) DESC, internal_method
        """
    ).fetchall()

    print("=" * 92)
    print("DEFINITIVE POSTOPERATIVE TIME-ZERO DATASET")
    print("=" * 92)
    print(f"Valid same-day CVICU cohort:       {EXPECTED_VALID_SAME_DAY_CVICU_N:,}")
    print(f"Ambiguous ICU segments excluded:   {excluded:,}")
    print(f"Final analytical cohort:           {n:,}")
    print(f"ICU deaths retained:               {deaths:,}")
    print(f"Observable for at least 24 hours:  {full_24h:,}")
    print("\nAssignment methods")
    for method, count in methods:
        print(f"  {method:<44} {count:>7,}")
    print(f"\nOutput: {OUTPUT_PATH}")
    print("=" * 92)


def main() -> int:
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        validate_inputs()

        con = duckdb.connect(":memory:")
        try:
            create_source_views(con)
            create_valid_cohort(con)
            extract_core_monitoring(con)
            build_monitoring_episodes(con)
            extract_first_or_received(con)
            assign_time_zero(con)
            identify_ambiguous_segmentation(con)
            create_final_output(con)
            validate_final_output(con)
            export_final_output(con)
            print_summary(con)
        finally:
            con.close()

        return 0

    except TimeZeroError as exc:
        print("=" * 92, file=sys.stderr)
        print("DEFINITIVE TIME-ZERO CREATION FAILED", file=sys.stderr)
        print(str(exc), file=sys.stderr)
        print("=" * 92, file=sys.stderr)
        return 1

    except Exception as exc:
        print("=" * 92, file=sys.stderr)
        print("UNEXPECTED ERROR", file=sys.stderr)
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        print("=" * 92, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
