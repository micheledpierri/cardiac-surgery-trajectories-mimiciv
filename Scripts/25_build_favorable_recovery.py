#!/usr/bin/env python3
"""
TRAJECTORIES — Study 1
Phase 02_05 — Canonical favorable-recovery definition

Study:
"Early usual-care and favorable-recovery trajectories after adult cardiac surgery:
a multidomain reference study"

FROZEN PRIMARY DEFINITION
-------------------------
Favorable recovery requires all of the following:

1. Alive at hospital discharge.
2. Discharged by postoperative day 10.
3. Effective discharge disposition.

Included effective-discharge destinations:
- HOME
- HOME HEALTH CARE
- SKILLED NURSING FACILITY
- REHAB
- OTHER FACILITY

Excluded destinations:
- CHRONIC/LONG TERM ACUTE CARE
- ACUTE HOSPITAL
- HOSPICE
- AGAINST ADVICE
- missing discharge disposition

Hospital death and postoperative LOS >10 days are non-favorable.

RATIONALE
---------
The primary favorable-recovery definition intentionally excludes early
physiological trajectory variables and domain-specific complications from the
classification rule, thereby minimizing circularity when those trajectories are
described subsequently.

Postoperative LOS is calculated from midnight of index_operation_date to
hospital dischtime because the frozen Study-1 cohort stores operation date at
day-level resolution.

SENSITIVITY DEFINITIONS
-----------------------
The same effective-discharge rule is also evaluated at POD7 and POD14.

INPUTS
------
data/00_04_final_analytic_cohort.csv
data/01_14_respiratory_outcomes.parquet
data/01_09_creatinine_renal_outcomes.parquet
data/01_17_revision_for_bleeding_outcomes.parquet
data/01_10_fluid_balance_hourly_trajectory.parquet
MIMIC_ROOT/hosp/admissions.csv.gz

OUTPUTS
-------
data/02_05_favorable_recovery_patient_level.csv
data/02_05_favorable_recovery_summary.csv
data/02_05_favorable_recovery_disposition_audit.csv
data/02_05_favorable_recovery_construct_validation.csv
data/02_05_favorable_recovery_strata.csv

Classification and construct validation
--------------------------------------
Records represent cohort admissions/ICU stays, keyed by stay_id, rather than
necessarily unique patients. Favorable-recovery classification uses survival,
postoperative hospital length of stay, and discharge destination only.
Respiratory, creatinine, revision-for-bleeding, and fluid-derived RRT inputs
are required for execution and provide construct-validation context. Their
complication flags do not enter the primary or sensitivity classification
criteria or exclusion rules.
"""

from __future__ import annotations

import sys
from pathlib import Path

try:
    import duckdb
except ImportError as exc:
    raise SystemExit("Missing dependency 'duckdb'.") from exc


EXPECTED_COHORT_N = 9305
EXPECTED_FAVORABLE_N = 7835

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = (
    SCRIPT_PATH.parent.parent
    if SCRIPT_PATH.parent.name.lower() == "scripts"
    else SCRIPT_PATH.parent
)
DATA_DIR = PROJECT_ROOT / "data"
MIMIC_ROOT = Path("path/to/mimic-iv-3.1")

COHORT = DATA_DIR / "00_04_final_analytic_cohort.csv"
RESP = DATA_DIR / "01_14_respiratory_outcomes.parquet"
CREAT = DATA_DIR / "01_09_creatinine_renal_outcomes.parquet"
REVISION = DATA_DIR / "01_17_revision_for_bleeding_outcomes.parquet"
FLUID_H = DATA_DIR / "01_10_fluid_balance_hourly_trajectory.parquet"
ADMISSIONS = MIMIC_ROOT / "hosp" / "admissions.csv.gz"

OUT_PATIENT = DATA_DIR / "02_05_favorable_recovery_patient_level.csv"
OUT_SUMMARY = DATA_DIR / "02_05_favorable_recovery_summary.csv"
OUT_DISP = DATA_DIR / "02_05_favorable_recovery_disposition_audit.csv"
OUT_VALID = DATA_DIR / "02_05_favorable_recovery_construct_validation.csv"
OUT_STRATA = DATA_DIR / "02_05_favorable_recovery_strata.csv"

INCLUDED_DISPOSITIONS = (
    "HOME",
    "HOME HEALTH CARE",
    "SKILLED NURSING FACILITY",
    "REHAB",
    "OTHER FACILITY",
)


def qpath(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def require_inputs() -> None:
    required = (COHORT, RESP, CREAT, REVISION, FLUID_H, ADMISSIONS)
    missing = [str(p) for p in required if not p.is_file()]
    if missing:
        raise FileNotFoundError(
            "Missing required input(s):\n- " + "\n- ".join(missing)
        )


def get_columns(con, table: str) -> set[str]:
    return {
        row[1]
        for row in con.execute(
            f"PRAGMA table_info('{table}')"
        ).fetchall()
    }


def require_columns(
    con,
    table: str,
    required: set[str],
) -> None:
    present = get_columns(con, table)
    missing = sorted(required - present)
    if missing:
        raise RuntimeError(
            f"{table} missing required columns: {missing}"
        )


def main() -> int:
    require_inputs()

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA preserve_insertion_order=false")

    con.execute(
        f"""
        CREATE VIEW cohort AS
        SELECT *
        FROM read_csv_auto(
            '{qpath(COHORT)}',
            header=true,
            all_varchar=false
        )
        """
    )

    con.execute(
        f"CREATE VIEW resp AS "
        f"SELECT * FROM read_parquet('{qpath(RESP)}')"
    )
    con.execute(
        f"CREATE VIEW creat AS "
        f"SELECT * FROM read_parquet('{qpath(CREAT)}')"
    )
    con.execute(
        f"CREATE VIEW revision AS "
        f"SELECT * FROM read_parquet('{qpath(REVISION)}')"
    )
    con.execute(
        f"CREATE VIEW fluid_h AS "
        f"SELECT * FROM read_parquet('{qpath(FLUID_H)}')"
    )

    con.execute(
        f"""
        CREATE VIEW admissions AS
        SELECT
            try_cast(hadm_id AS BIGINT) AS hadm_id,
            try_cast(admittime AS TIMESTAMP) AS admittime,
            try_cast(dischtime AS TIMESTAMP) AS dischtime,
            try_cast(deathtime AS TIMESTAMP) AS deathtime,
            discharge_location
        FROM read_csv_auto(
            '{qpath(ADMISSIONS)}',
            header=true,
            all_varchar=true
        )
        WHERE try_cast(hadm_id AS BIGINT) IS NOT NULL
        """
    )

    require_columns(
        con,
        "cohort",
        {
            "stay_id",
            "hadm_id",
            "index_operation_date",
            "hospital_length_of_stay_days",
            "hospital_expire_flag",
            "hospital_discharge_status",
            "age_at_icu_admission",
            "sex",
            "surgery_group",
        },
    )

    require_columns(
        con,
        "resp",
        {
            "stay_id",
            "reintubation_within_48h_flag",
        },
    )

    require_columns(
        con,
        "creat",
        {
            "stay_id",
            "creatinine_outcome_evaluable_flag",
            "creatinine_aki_stage_7d_creatinine_only",
        },
    )

    require_columns(
        con,
        "revision",
        {
            "stay_id",
            "revision_for_bleeding_early_flag",
        },
    )

    require_columns(
        con,
        "fluid_h",
        {
            "stay_id",
            "fb_rrt_net_removal_ml",
        },
    )

    cohort_rows, cohort_stays = con.execute(
        """
        SELECT
            COUNT(*),
            COUNT(DISTINCT stay_id)
        FROM cohort
        """
    ).fetchone()

    if (
        cohort_rows != EXPECTED_COHORT_N
        or cohort_stays != EXPECTED_COHORT_N
    ):
        raise RuntimeError(
            "Unexpected cohort dimensions: "
            f"{cohort_rows}/{cohort_stays}; "
            f"expected {EXPECTED_COHORT_N}/{EXPECTED_COHORT_N}."
        )

    duplicate_hadm_n = con.execute(
        """
        SELECT COUNT(*)
        FROM (
            SELECT hadm_id
            FROM admissions
            GROUP BY hadm_id
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]

    if duplicate_hadm_n:
        raise RuntimeError(
            "Raw admissions contains duplicated hadm_id rows: "
            f"{duplicate_hadm_n}"
        )

    con.execute(
        """
        CREATE TABLE rrt_context AS
        SELECT
            stay_id,
            max(
                CASE
                    WHEN coalesce(
                        fb_rrt_net_removal_ml,
                        0.0
                    ) <> 0
                    THEN 1
                    ELSE 0
                END
            ) AS rrt_activity_0_24_flag
        FROM fluid_h
        GROUP BY stay_id
        """
    )

    included_sql = ", ".join(
        "'" + value.replace("'", "''") + "'"
        for value in INCLUDED_DISPOSITIONS
    )

    con.execute(
        f"""
        CREATE TABLE favorable_recovery AS
        SELECT
            c.stay_id,
            c.hadm_id,

            c.index_operation_date::DATE
                AS index_operation_date,

            a.admittime,
            a.dischtime,

            coalesce(
                a.discharge_location,
                '<MISSING>'
            ) AS discharge_location,

            c.hospital_length_of_stay_days::DOUBLE
                AS total_hospital_los_days,

            date_diff(
                'second',
                CAST(
                    c.index_operation_date
                    AS TIMESTAMP
                ),
                a.dischtime
            ) / 86400.0
                AS postoperative_los_days,

            c.hospital_expire_flag,
            c.hospital_discharge_status,

            c.age_at_icu_admission::DOUBLE
                AS age,

            c.sex,
            c.surgery_group,

            CASE
                WHEN
                    c.hospital_expire_flag = 0
                    AND date_diff(
                        'second',
                        CAST(
                            c.index_operation_date
                            AS TIMESTAMP
                        ),
                        a.dischtime
                    ) / 86400.0 <= 10
                    AND coalesce(
                        a.discharge_location,
                        '<MISSING>'
                    ) IN ({included_sql})
                THEN 1
                ELSE 0
            END AS favorable_recovery_flag,

            CASE
                WHEN
                    c.hospital_expire_flag = 0
                    AND date_diff(
                        'second',
                        CAST(
                            c.index_operation_date
                            AS TIMESTAMP
                        ),
                        a.dischtime
                    ) / 86400.0 <= 7
                    AND coalesce(
                        a.discharge_location,
                        '<MISSING>'
                    ) IN ({included_sql})
                THEN 1
                ELSE 0
            END AS favorable_recovery_pod7_sensitivity_flag,

            CASE
                WHEN
                    c.hospital_expire_flag = 0
                    AND date_diff(
                        'second',
                        CAST(
                            c.index_operation_date
                            AS TIMESTAMP
                        ),
                        a.dischtime
                    ) / 86400.0 <= 14
                    AND coalesce(
                        a.discharge_location,
                        '<MISSING>'
                    ) IN ({included_sql})
                THEN 1
                ELSE 0
            END AS favorable_recovery_pod14_sensitivity_flag,

            CASE
                WHEN c.hospital_expire_flag = 1
                    THEN 'HOSPITAL_DEATH'

                WHEN date_diff(
                    'second',
                    CAST(
                        c.index_operation_date
                        AS TIMESTAMP
                    ),
                    a.dischtime
                ) / 86400.0 > 10
                    THEN 'POSTOP_LOS_GT10'

                WHEN coalesce(
                    a.discharge_location,
                    '<MISSING>'
                ) NOT IN ({included_sql})
                    THEN 'NON_EFFECTIVE_DISCHARGE_DISPOSITION'

                ELSE 'FAVORABLE_RECOVERY'
            END AS favorable_recovery_reason,

            k.creatinine_outcome_evaluable_flag,
            k.creatinine_aki_stage_7d_creatinine_only,

            b.revision_for_bleeding_early_flag,

            r.reintubation_within_48h_flag,

            x.rrt_activity_0_24_flag

        FROM cohort c

        LEFT JOIN admissions a
            USING (hadm_id)

        LEFT JOIN resp r
            USING (stay_id)

        LEFT JOIN creat k
            USING (stay_id)

        LEFT JOIN revision b
            USING (stay_id)

        LEFT JOIN rrt_context x
            USING (stay_id)
        """
    )

    missing_admittime_n, missing_dischtime_n, negative_postop_n = (
        con.execute(
            """
            SELECT
                COUNT(*) FILTER (
                    WHERE admittime IS NULL
                ),
                COUNT(*) FILTER (
                    WHERE dischtime IS NULL
                ),
                COUNT(*) FILTER (
                    WHERE postoperative_los_days IS NOT NULL
                      AND postoperative_los_days < 0
                )
            FROM favorable_recovery
            """
        ).fetchone()
    )

    if (
        missing_admittime_n
        or missing_dischtime_n
        or negative_postop_n
    ):
        raise RuntimeError(
            "Timing/join QC failed: "
            f"missing_admittime={missing_admittime_n}, "
            f"missing_dischtime={missing_dischtime_n}, "
            f"negative_postop_los={negative_postop_n}."
        )

    # The corrected 01_10 fluid file is used only to derive an RRT
    # context flag for construct validation. It must not affect the
    # favorable-recovery classification rule.
    rrt_context_rows, rrt_context_stays, rrt_positive_n = con.execute(
        """
        SELECT
            COUNT(*),
            COUNT(DISTINCT stay_id),
            COUNT(*) FILTER (WHERE rrt_activity_0_24_flag = 1)
        FROM rrt_context
        """
    ).fetchone()

    if rrt_context_rows != rrt_context_stays:
        raise RuntimeError(
            "RRT context is not one row per stay: "
            f"{rrt_context_rows}/{rrt_context_stays}."
        )

    favorable_n = con.execute(
        """
        SELECT COUNT(*)
        FROM favorable_recovery
        WHERE favorable_recovery_flag = 1
        """
    ).fetchone()[0]

    favorable_rule_mismatch_n = con.execute(
        f"""
        SELECT COUNT(*)
        FROM favorable_recovery
        WHERE favorable_recovery_flag <>
            CASE
                WHEN
                    hospital_expire_flag = 0
                    AND postoperative_los_days <= 10
                    AND discharge_location IN ({included_sql})
                THEN 1
                ELSE 0
            END
        """
    ).fetchone()[0]

    if favorable_rule_mismatch_n:
        raise RuntimeError(
            "Favorable-recovery rule reconstruction mismatch in "
            f"{favorable_rule_mismatch_n} patient(s)."
        )

    if favorable_n != EXPECTED_FAVORABLE_N:
        raise RuntimeError(
            "Canonical favorable-recovery closure failed: "
            f"{favorable_n}, "
            f"expected {EXPECTED_FAVORABLE_N}."
        )

    con.execute(
        """
        CREATE TABLE summary AS
        SELECT
            COUNT(*) AS cohort_n,

            COUNT(*) FILTER (
                WHERE favorable_recovery_flag = 1
            ) AS favorable_n,

            COUNT(*) FILTER (
                WHERE favorable_recovery_flag = 0
            ) AS nonfavorable_n,

            100.0
                * COUNT(*) FILTER (
                    WHERE favorable_recovery_flag = 1
                )
                / COUNT(*)
                AS favorable_pct,

            COUNT(*) FILTER (
                WHERE
                    favorable_recovery_pod7_sensitivity_flag = 1
            ) AS favorable_pod7_n,

            COUNT(*) FILTER (
                WHERE
                    favorable_recovery_pod14_sensitivity_flag = 1
            ) AS favorable_pod14_n,

            median(postoperative_los_days)
                AS postop_los_median_days,

            quantile_cont(
                postoperative_los_days,
                0.25
            ) AS postop_los_p25_days,

            quantile_cont(
                postoperative_los_days,
                0.75
            ) AS postop_los_p75_days

        FROM favorable_recovery
        """
    )

    con.execute(
        f"""
        CREATE TABLE disposition_audit AS
        SELECT
            discharge_location,

            COUNT(*) AS cohort_n,

            COUNT(*) FILTER (
                WHERE
                    hospital_expire_flag = 0
                    AND postoperative_los_days <= 10
            ) AS alive_pod10_n,

            CASE
                WHEN discharge_location
                    IN ({included_sql})
                THEN 1
                ELSE 0
            END AS included_effective_discharge

        FROM favorable_recovery

        GROUP BY discharge_location
        """
    )

    con.execute(
        """
        CREATE TABLE construct_validation AS
        SELECT
            favorable_recovery_flag,

            COUNT(*) AS n,

            COUNT(*) FILTER (
                WHERE
                    creatinine_outcome_evaluable_flag = 1
                    AND creatinine_aki_stage_7d_creatinine_only
                        IN (2, 3)
            ) AS aki_stage_2_3_n,

            100.0
                * COUNT(*) FILTER (
                    WHERE
                        creatinine_outcome_evaluable_flag = 1
                        AND creatinine_aki_stage_7d_creatinine_only
                            IN (2, 3)
                )
                / COUNT(*)
                AS aki_stage_2_3_pct,

            COUNT(*) FILTER (
                WHERE revision_for_bleeding_early_flag = 1
            ) AS revision_for_bleeding_n,

            100.0
                * COUNT(*) FILTER (
                    WHERE revision_for_bleeding_early_flag = 1
                )
                / COUNT(*)
                AS revision_for_bleeding_pct,

            COUNT(*) FILTER (
                WHERE reintubation_within_48h_flag = 1
            ) AS reintubation_48h_n,

            100.0
                * COUNT(*) FILTER (
                    WHERE reintubation_within_48h_flag = 1
                )
                / COUNT(*)
                AS reintubation_48h_pct,

            COUNT(*) FILTER (
                WHERE rrt_activity_0_24_flag = 1
            ) AS rrt_activity_0_24_n,

            100.0
                * COUNT(*) FILTER (
                    WHERE rrt_activity_0_24_flag = 1
                )
                / COUNT(*)
                AS rrt_activity_0_24_pct

        FROM favorable_recovery

        GROUP BY favorable_recovery_flag
        """
    )

    con.execute(
        """
        CREATE TABLE strata AS
        WITH base AS (
            SELECT
                *,
                CASE
                    WHEN age < 60 THEN '<60'
                    WHEN age < 70 THEN '60-69'
                    WHEN age < 80 THEN '70-79'
                    ELSE '>=80'
                END AS age_group
            FROM favorable_recovery
        )

        SELECT
            'AGE' AS stratum_type,
            age_group AS stratum,
            COUNT(*) AS n,

            COUNT(*) FILTER (
                WHERE favorable_recovery_flag = 1
            ) AS favorable_n,

            100.0
                * COUNT(*) FILTER (
                    WHERE favorable_recovery_flag = 1
                )
                / COUNT(*)
                AS favorable_pct

        FROM base
        GROUP BY age_group

        UNION ALL

        SELECT
            'SURGERY_GROUP',
            surgery_group,
            COUNT(*),

            COUNT(*) FILTER (
                WHERE favorable_recovery_flag = 1
            ),

            100.0
                * COUNT(*) FILTER (
                    WHERE favorable_recovery_flag = 1
                )
                / COUNT(*)

        FROM base
        GROUP BY surgery_group
        """
    )

    for path in (
        OUT_PATIENT,
        OUT_SUMMARY,
        OUT_DISP,
        OUT_VALID,
        OUT_STRATA,
    ):
        if path.exists():
            path.unlink()

    con.execute(
        f"""
        COPY (
            SELECT *
            FROM favorable_recovery
            ORDER BY stay_id
        )
        TO '{qpath(OUT_PATIENT)}'
        (FORMAT CSV, HEADER TRUE)
        """
    )

    con.execute(
        f"""
        COPY summary
        TO '{qpath(OUT_SUMMARY)}'
        (FORMAT CSV, HEADER TRUE)
        """
    )

    con.execute(
        f"""
        COPY (
            SELECT *
            FROM disposition_audit
            ORDER BY cohort_n DESC
        )
        TO '{qpath(OUT_DISP)}'
        (FORMAT CSV, HEADER TRUE)
        """
    )

    con.execute(
        f"""
        COPY (
            SELECT *
            FROM construct_validation
            ORDER BY favorable_recovery_flag DESC
        )
        TO '{qpath(OUT_VALID)}'
        (FORMAT CSV, HEADER TRUE)
        """
    )

    con.execute(
        f"""
        COPY (
            SELECT *
            FROM strata
            ORDER BY stratum_type, stratum
        )
        TO '{qpath(OUT_STRATA)}'
        (FORMAT CSV, HEADER TRUE)
        """
    )

    summary_row = con.execute(
        "SELECT * FROM summary"
    ).fetchone()

    print("=" * 112)
    print(
        "TRAJECTORIES — Study 1 Phase 02_05 "
        "candidate favorable-recovery definition complete"
    )
    print("=" * 112)
    print("PRIMARY FAVORABLE-RECOVERY DEFINITION UNCHANGED.")
    print(
        "Favorable recovery = alive + discharged by POD10 "
        "+ effective discharge disposition."
    )
    print()

    print("SOURCE / TIMING QC")
    print("------------------")
    print(
        f"Cohort rows / unique stays : "
        f"{cohort_rows} / {cohort_stays}"
    )
    print(
        f"Missing admittime          : "
        f"{missing_admittime_n}"
    )
    print(
        f"Missing dischtime          : "
        f"{missing_dischtime_n}"
    )
    print(
        f"Negative postoperative LOS : "
        f"{negative_postop_n}"
    )
    print(
        f"RRT context rows/stays      : "
        f"{rrt_context_rows}/{rrt_context_stays}"
    )
    print(
        f"RRT activity 0-24 h         : "
        f"{rrt_positive_n}"
    )
    print(
        f"Recovery-rule mismatches    : "
        f"{favorable_rule_mismatch_n}"
    )
    print()

    print("PRIMARY DEFINITION")
    print("------------------")
    print(
        f"Favorable     : "
        f"{summary_row[1]}/{summary_row[0]} "
        f"({summary_row[3]:.2f}%)"
    )
    print(
        f"Non-favorable : "
        f"{summary_row[2]}/{summary_row[0]} "
        f"({100.0*summary_row[2]/summary_row[0]:.2f}%)"
    )
    print()

    print("SENSITIVITY")
    print("-----------")
    print(
        f"POD7 + effective disposition : "
        f"{summary_row[4]}/{summary_row[0]} "
        f"({100.0*summary_row[4]/summary_row[0]:.2f}%)"
    )
    print(
        f"POD14 + effective disposition: "
        f"{summary_row[5]}/{summary_row[0]} "
        f"({100.0*summary_row[5]/summary_row[0]:.2f}%)"
    )
    print()

    print("POSTOPERATIVE LOS")
    print("-----------------")
    print(
        f"Median [P25-P75]: "
        f"{summary_row[6]:.2f} "
        f"[{summary_row[7]:.2f}-{summary_row[8]:.2f}] days"
    )
    print()

    print("CONSTRUCT VALIDATION")
    print("--------------------")
    for row in con.execute(
        """
        SELECT
            favorable_recovery_flag,
            n,
            aki_stage_2_3_n,
            aki_stage_2_3_pct,
            revision_for_bleeding_n,
            revision_for_bleeding_pct,
            reintubation_48h_n,
            reintubation_48h_pct,
            rrt_activity_0_24_n,
            rrt_activity_0_24_pct
        FROM construct_validation
        ORDER BY favorable_recovery_flag DESC
        """
    ).fetchall():
        label = (
            "FAVORABLE"
            if row[0] == 1
            else "NON-FAVORABLE"
        )

        print(
            f"{label}: "
            f"n={row[1]}, "
            f"AKI2-3={row[2]} ({row[3]:.2f}%), "
            f"revision={row[4]} ({row[5]:.2f}%), "
            f"reintub={row[6]} ({row[7]:.2f}%), "
            f"RRT={row[8]} ({row[9]:.2f}%)"
        )

    print()
    print("CANDIDATE OUTPUTS")
    print("-------")

    for path in (
        OUT_PATIENT,
        OUT_SUMMARY,
        OUT_DISP,
        OUT_VALID,
        OUT_STRATA,
    ):
        print(path)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
