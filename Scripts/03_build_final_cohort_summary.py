#!/usr/bin/env python3
"""
Create the definitive admission/ICU-stay analytical cohort and population summary.

INPUTS
------
data/00_cardiac_surgery_starting_cohort.csv
data/00_03_postoperative_time_zero.csv

OUTPUTS
-------
data/00_04_final_analytic_cohort.csv
data/00_04_final_cohort_summary.csv

Records represent hospital admissions and their selected ICU stays, not
necessarily unique patients. The dataset contains the variables required to
describe the population and to anchor future trajectories.

The summary contains overall and surgery-group values for:
- cohort size and proportion;
- age mean and standard deviation;
- male/female distribution;
- ICU deaths;
- in-hospital deaths;
- postoperative ICU length of stay from the definitive time zero:
  mean, standard deviation, median, first quartile, and third quartile.

REPRODUCIBILITY
---------------
The script reads only retained canonical datasets, creates only its two declared
outputs, never modifies unrelated files, and may replace only its own two declared outputs when rerun.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb


SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = (
    SCRIPT_PATH.parent.parent
    if SCRIPT_PATH.parent.name.lower() == "scripts"
    else SCRIPT_PATH.parent
)
DATA_DIR = PROJECT_ROOT / "data"

STARTING_COHORT_PATH = (
    DATA_DIR / "00_cardiac_surgery_starting_cohort.csv"
)
TIME_ZERO_PATH = (
    DATA_DIR / "00_03_postoperative_time_zero.csv"
)

FINAL_COHORT_PATH = (
    DATA_DIR / "00_04_final_analytic_cohort.csv"
)
SUMMARY_PATH = (
    DATA_DIR / "00_04_final_cohort_summary.csv"
)

EXPECTED_FINAL_COHORT_N = 9305


class CohortSummaryError(RuntimeError):
    """Raised when a mandatory cohort-summary invariant fails."""


def sql_path(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def validate_inputs() -> None:
    missing = [
        str(path)
        for path in (STARTING_COHORT_PATH, TIME_ZERO_PATH)
        if not path.is_file()
    ]
    if missing:
        raise CohortSummaryError(
            "Required canonical input(s) not found:\n- "
            + "\n- ".join(missing)
        )


def create_views(con: duckdb.DuckDBPyConnection) -> None:
    cohort = sql_path(STARTING_COHORT_PATH)
    time_zero = sql_path(TIME_ZERO_PATH)

    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW starting_cohort AS
        SELECT *
        FROM read_csv_auto(
            '{cohort}',
            header = true,
            sample_size = -1
        )
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW time_zero AS
        SELECT
            CAST(subject_id AS BIGINT) AS subject_id,
            CAST(hadm_id AS BIGINT) AS hadm_id,
            CAST(stay_id AS BIGINT) AS stay_id,
            CAST(postoperative_time_zero AS TIMESTAMP)
                AS postoperative_time_zero,
            CAST(observation_end_time AS TIMESTAMP)
                AS observation_end_time,
            CAST(observation_end_type AS VARCHAR)
                AS observation_end_type
        FROM read_csv_auto(
            '{time_zero}',
            header = true,
            sample_size = -1
        )
        """
    )


def create_final_cohort(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE final_cohort AS
        SELECT
            CAST(c.subject_id AS BIGINT) AS subject_id,
            CAST(c.hadm_id AS BIGINT) AS hadm_id,
            CAST(c.stay_id AS BIGINT) AS stay_id,

            CAST(c.index_operation_date AS DATE)
                AS index_operation_date,

            CAST(c.surgery_group AS VARCHAR)
                AS surgery_group,

            CAST(c.cabg_flag AS INTEGER)
                AS cabg_flag,
            CAST(c.valve_flag AS INTEGER)
                AS valve_flag,
            CAST(c.ascending_aorta_flag AS INTEGER)
                AS ascending_aorta_flag,

            CAST(c.sex AS VARCHAR)
                AS sex,

            CAST(c.age_at_icu_admission AS DOUBLE)
                AS age_at_icu_admission,

            CAST(c.age_over_89_deidentified_flag AS INTEGER)
                AS age_over_89_deidentified_flag,

            t.postoperative_time_zero,
            t.observation_end_time,
            t.observation_end_type,

            ROUND(
                DATE_DIFF(
                    'second',
                    t.postoperative_time_zero,
                    t.observation_end_time
                ) / 3600.0,
                3
            ) AS postoperative_icu_los_hours,

            ROUND(
                DATE_DIFF(
                    'second',
                    t.postoperative_time_zero,
                    t.observation_end_time
                ) / 86400.0,
                3
            ) AS postoperative_icu_los_days,

            CAST(c.hospital_expire_flag AS INTEGER)
                AS hospital_expire_flag,

            CAST(c.hospital_deathtime AS TIMESTAMP)
                AS hospital_deathtime,

            CAST(c.hospital_discharge_status AS VARCHAR)
                AS hospital_discharge_status,

            CAST(c.hospital_length_of_stay_days AS DOUBLE)
                AS hospital_length_of_stay_days

        FROM starting_cohort AS c
        INNER JOIN time_zero AS t
            ON CAST(c.subject_id AS BIGINT) = t.subject_id
           AND CAST(c.hadm_id AS BIGINT) = t.hadm_id
           AND CAST(c.stay_id AS BIGINT) = t.stay_id

        ORDER BY index_operation_date, subject_id, hadm_id
        """
    )


def validate_final_cohort(con: duckdb.DuckDBPyConnection) -> None:
    n = con.execute("SELECT COUNT(*) FROM final_cohort").fetchone()[0]
    if n != EXPECTED_FINAL_COHORT_N:
        raise CohortSummaryError(
            f"Final cohort has {n:,} rows; "
            f"expected {EXPECTED_FINAL_COHORT_N:,}."
        )

    duplicates = con.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT hadm_id
            FROM final_cohort
            GROUP BY hadm_id
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    if duplicates:
        raise CohortSummaryError(
            f"{duplicates:,} duplicated hadm_id group(s)."
        )

    missing = con.execute(
        """
        SELECT COUNT(*)
        FROM final_cohort
        WHERE subject_id IS NULL
           OR hadm_id IS NULL
           OR stay_id IS NULL
           OR surgery_group IS NULL
           OR sex IS NULL
           OR age_at_icu_admission IS NULL
           OR postoperative_time_zero IS NULL
           OR observation_end_time IS NULL
           OR postoperative_icu_los_hours IS NULL
        """
    ).fetchone()[0]
    if missing:
        raise CohortSummaryError(
            f"{missing:,} rows contain mandatory missing values."
        )

    invalid_los = con.execute(
        """
        SELECT COUNT(*)
        FROM final_cohort
        WHERE postoperative_icu_los_hours < 0
        """
    ).fetchone()[0]
    if invalid_los:
        raise CohortSummaryError(
            f"{invalid_los:,} rows have negative postoperative ICU LOS."
        )


def create_summary(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE final_summary AS
        WITH expanded AS (
            SELECT
                0 AS summary_order,
                'OVERALL' AS summary_group,
                *
            FROM final_cohort

            UNION ALL

            SELECT
                1 AS summary_order,
                surgery_group AS summary_group,
                *
            FROM final_cohort
        ),

        totals AS (
            SELECT COUNT(*) AS total_n
            FROM final_cohort
        )

        SELECT
            e.summary_order,
            e.summary_group,

            COUNT(*) AS n,

            ROUND(
                100.0 * COUNT(*) / MAX(t.total_n),
                2
            ) AS proportion_of_total_pct,

            ROUND(AVG(age_at_icu_admission), 2)
                AS age_mean_years,

            ROUND(STDDEV_SAMP(age_at_icu_admission), 2)
                AS age_sd_years,

            COUNT(*) FILTER (
                WHERE UPPER(TRIM(sex)) = 'M'
            ) AS male_n,

            ROUND(
                100.0 * COUNT(*) FILTER (
                    WHERE UPPER(TRIM(sex)) = 'M'
                ) / COUNT(*),
                2
            ) AS male_pct,

            COUNT(*) FILTER (
                WHERE UPPER(TRIM(sex)) = 'F'
            ) AS female_n,

            ROUND(
                100.0 * COUNT(*) FILTER (
                    WHERE UPPER(TRIM(sex)) = 'F'
                ) / COUNT(*),
                2
            ) AS female_pct,

            COUNT(*) FILTER (
                WHERE observation_end_type = 'DIED_IN_ICU'
            ) AS icu_deaths_n,

            ROUND(
                100.0 * COUNT(*) FILTER (
                    WHERE observation_end_type = 'DIED_IN_ICU'
                ) / COUNT(*),
                2
            ) AS icu_mortality_pct,

            SUM(hospital_expire_flag)
                AS in_hospital_deaths_n,

            ROUND(
                100.0 * SUM(hospital_expire_flag) / COUNT(*),
                2
            ) AS in_hospital_mortality_pct,

            ROUND(AVG(postoperative_icu_los_hours), 2)
                AS postoperative_icu_los_mean_hours,

            ROUND(STDDEV_SAMP(postoperative_icu_los_hours), 2)
                AS postoperative_icu_los_sd_hours,

            ROUND(MEDIAN(postoperative_icu_los_hours), 2)
                AS postoperative_icu_los_median_hours,

            ROUND(
                QUANTILE_CONT(postoperative_icu_los_hours, 0.25),
                2
            ) AS postoperative_icu_los_q1_hours,

            ROUND(
                QUANTILE_CONT(postoperative_icu_los_hours, 0.75),
                2
            ) AS postoperative_icu_los_q3_hours

        FROM expanded AS e
        CROSS JOIN totals AS t
        GROUP BY
            e.summary_order,
            e.summary_group
        ORDER BY
            e.summary_order,
            n DESC,
            e.summary_group
        """
    )


def export_outputs(con: duckdb.DuckDBPyConnection) -> None:
    # Replacing only the script's own declared outputs makes reruns
    # deterministic without introducing project-cleanup side effects.
    for path in (FINAL_COHORT_PATH, SUMMARY_PATH):
        if path.exists():
            path.unlink()

    final_cohort = sql_path(FINAL_COHORT_PATH)
    summary = sql_path(SUMMARY_PATH)

    con.execute(
        f"""
        COPY final_cohort
        TO '{final_cohort}'
        (
            HEADER,
            DELIMITER ',',
            QUOTE '"',
            ESCAPE '"',
            NULL ''
        )
        """
    )

    con.execute(
        f"""
        COPY final_summary
        TO '{summary}'
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
    rows = con.execute(
        """
        SELECT
            summary_group,
            n,
            age_mean_years,
            male_n,
            female_n,
            icu_deaths_n,
            in_hospital_deaths_n,
            postoperative_icu_los_mean_hours,
            postoperative_icu_los_median_hours,
            postoperative_icu_los_q1_hours,
            postoperative_icu_los_q3_hours
        FROM final_summary
        ORDER BY summary_order, n DESC, summary_group
        """
    ).fetchall()

    print("=" * 142)
    print("DEFINITIVE CARDIAC-SURGERY COHORT SUMMARY")
    print("=" * 142)
    print(
        f"{'GROUP':<38} {'N':>7} {'AGE':>8} {'M':>7} {'F':>7} "
        f"{'ICU D':>7} {'HOSP D':>8} {'MEAN LOS':>11} "
        f"{'MEDIAN LOS [Q1-Q3]':>25}"
    )
    for row in rows:
        (
            group,
            n,
            age,
            male,
            female,
            icu_deaths,
            hospital_deaths,
            mean_los,
            median_los,
            q1,
            q3,
        ) = row

        print(
            f"{str(group):<38} {n:>7,} {age:>8.2f} "
            f"{male:>7,} {female:>7,} {icu_deaths:>7,} "
            f"{hospital_deaths:>8,} {mean_los:>10.2f}h "
            f"{median_los:>7.2f} [{q1:.2f}-{q3:.2f}]"
        )

    print("\nOutputs")
    print(f"  {FINAL_COHORT_PATH}")
    print(f"  {SUMMARY_PATH}")
    print("=" * 142)


def main() -> int:
    try:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        validate_inputs()

        con = duckdb.connect(":memory:")
        try:
            create_views(con)
            create_final_cohort(con)
            validate_final_cohort(con)
            create_summary(con)
            export_outputs(con)
            print_summary(con)
        finally:
            con.close()

        return 0

    except CohortSummaryError as exc:
        print("=" * 100, file=sys.stderr)
        print("FINAL COHORT SUMMARY CREATION FAILED", file=sys.stderr)
        print(str(exc), file=sys.stderr)
        print("=" * 100, file=sys.stderr)
        return 1

    except Exception as exc:
        print("=" * 100, file=sys.stderr)
        print("UNEXPECTED ERROR", file=sys.stderr)
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        print("=" * 100, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
