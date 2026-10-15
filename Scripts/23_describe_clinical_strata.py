#!/usr/bin/env python3
"""
02_03_describe_clinical_strata.py

TRAJECTORIES — Study 1
Early usual-care and favorable-recovery trajectories after adult cardiac surgery:
a multidomain reference study

PHASE 02_03 — Clinical stratification of usual-care trajectories

Purpose
-------
Describe six-hour usual-care trajectories across prespecified baseline clinical
strata already present in the frozen cohort:
    - surgery group analysis:
        CABG
        VALVE
        CABG+VALVE
        ASCENDING_AORTA ± CABG/VALVE
    - sex (used exactly as frozen upstream);
    - age group: <60, 60-69, 70-79, >=80 years.

The original frozen surgery_group is retained unchanged. The analytical surgery
group only collapses sparse ascending-aorta combinations for descriptive
stratification.

This phase is descriptive only:
    - no outcome information;
    - no favorable-recovery definition;
    - no clustering;
    - no imputation;
    - no hypothesis-test p-values.

Inputs
------
data/00_04_final_analytic_cohort.csv
data/02_01_study1_six_hour.parquet
data/01_18_vasoactive_inotrope_six_hour_trajectory.parquet
data/01_08_diuretic_exposure_six_hour_summary.parquet

Outputs
-------
data/02_03_stratum_counts.csv
data/02_03_continuous_trajectories_by_stratum.csv
data/02_03_respiratory_states_by_stratum.csv
data/02_03_treatment_context_by_stratum.csv

Important
---------
Urgency/elective status is NOT reconstructed in this version. The script audits
whether an urgency-like field is already present in the frozen cohort and reports
its presence/absence to terminal, but does not derive a new variable from raw
MIMIC-IV data.

Statistical summaries
---------------------
Within each clinical stratum and six-hour window, eligible nonmissing values
contribute the median, quartiles, and 10th/90th percentiles. Stratum size,
eligible stays, and available stays are reported separately; availability is
100 times available stays divided by eligible stays for the variable/window.
"""

from __future__ import annotations

import sys
from pathlib import Path

try:
    import duckdb
except ImportError as exc:
    raise SystemExit(
        "Missing dependency 'duckdb'. Install it in the project environment."
    ) from exc


EXPECTED_COHORT_N = 9305

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = (
    SCRIPT_PATH.parent.parent
    if SCRIPT_PATH.parent.name.lower() == "scripts"
    else SCRIPT_PATH.parent
)
DATA_DIR = PROJECT_ROOT / "data"

COHORT = DATA_DIR / "00_04_final_analytic_cohort.csv"
SIX = DATA_DIR / "02_01_study1_six_hour.parquet"
VASO_SIX = DATA_DIR / "01_18_vasoactive_inotrope_six_hour_trajectory.parquet"
DIURETIC_SIX = DATA_DIR / "01_08_diuretic_exposure_six_hour_summary.parquet"

OUT_COUNTS = DATA_DIR / "02_03_stratum_counts.csv"
OUT_CONT = DATA_DIR / "02_03_continuous_trajectories_by_stratum.csv"
OUT_RESP = DATA_DIR / "02_03_respiratory_states_by_stratum.csv"
OUT_TREAT = DATA_DIR / "02_03_treatment_context_by_stratum.csv"


CONTINUOUS_VARIABLES = [
    ("MAP", "map_twa_mmhg", "CORE", "eligible_minutes > 0"),
    ("Heart rate", "hr_twa_bpm", "CORE", "eligible_minutes > 0"),
    (
        "Lactate",
        "lactate_median_mmol_l",
        "CORE_CONDITIONAL",
        "eligible_minutes > 0",
    ),
    ("pH", "ph_median", "CORE_CONDITIONAL", "eligible_minutes > 0"),
    (
        "Base excess",
        "base_excess_median_meq_l",
        "SUPPORTIVE",
        "eligible_minutes > 0",
    ),
    (
        "Glucose",
        "glucose_median_mg_dl",
        "SUPPORTIVE",
        "eligible_minutes > 0",
    ),
    (
        "Urine output",
        "urine_output_ml_kg_h",
        "CORE",
        "eligible_minutes > 0",
    ),
    (
        "Fluid balance cumulative",
        "fluid_balance_cumulative_ml",
        "CORE",
        "eligible_minutes > 0",
    ),
    (
        "Fluid accumulation cumulative",
        "fluid_accumulation_cumulative_pct_weight",
        "CORE",
        "eligible_minutes > 0",
    ),
    (
        "Invasive ventilation fraction",
        "invasive_ventilation_fraction",
        "CORE",
        "eligible_minutes > 0",
    ),
    (
        "P/F ratio",
        "pf_invasive_median",
        "CORE_CONDITIONAL",
        "eligible_minutes > 0 AND invasive_ventilation_fraction > 0",
    ),
    ("VIS", "vis_twa", "CORE_TREATMENT", "eligible_minutes > 0"),
    ("NEE", "nee_twa", "SUPPORTIVE_TREATMENT", "eligible_minutes > 0"),
    (
        "Chest drainage",
        "bleeding_recorded_ml",
        "CORE_EARLY",
        "eligible_minutes > 0 AND start_hour < 12 "
        "AND bleeding_window_fully_observable_flag = 1 "
        "AND bleeding_source_documented_flag = 1",
    ),
]


def qpath(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def source_columns(con, relation: str) -> set[str]:
    return {str(r[0]).lower() for r in con.execute(f"DESCRIBE {relation}").fetchall()}


def require_files() -> None:
    missing = [
        str(p)
        for p in (COHORT, SIX, VASO_SIX, DIURETIC_SIX)
        if not p.is_file()
    ]
    if missing:
        raise FileNotFoundError("Missing required input(s):\n- " + "\n- ".join(missing))


def validate_cohort(con) -> None:
    cols = source_columns(con, "cohort_raw")
    required = {"stay_id", "surgery_group", "sex", "age_at_icu_admission"}
    missing = sorted(required - cols)
    if missing:
        raise RuntimeError(
            "Frozen cohort is missing required columns: " + ", ".join(missing)
        )

    n, unique_stays = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM cohort_raw"
    ).fetchone()
    if n != EXPECTED_COHORT_N or unique_stays != EXPECTED_COHORT_N:
        raise RuntimeError(
            f"Unexpected frozen cohort size: rows={n}, stays={unique_stays}; "
            f"expected {EXPECTED_COHORT_N}."
        )

    urgency_candidates = sorted(
        c
        for c in cols
        if any(token in c for token in ("urgency", "urgent", "elective", "emergency"))
    )
    print(
        "Urgency-like frozen cohort fields: "
        + (", ".join(urgency_candidates) if urgency_candidates else "NONE")
    )


def build_cohort_strata(con) -> None:
    con.execute(
        """
        CREATE TABLE cohort_strata AS
        SELECT
            stay_id::BIGINT AS stay_id,
            surgery_group::VARCHAR AS surgery_group_original,
            CASE
                WHEN surgery_group = 'CABG'
                    THEN 'CABG'
                WHEN surgery_group = 'VALVE'
                    THEN 'VALVE'
                WHEN surgery_group = 'CABG+VALVE'
                    THEN 'CABG+VALVE'
                WHEN surgery_group IN (
                    'ASCENDING_AORTA',
                    'CABG+ASCENDING_AORTA',
                    'VALVE+ASCENDING_AORTA',
                    'CABG+VALVE+ASCENDING_AORTA'
                )
                    THEN 'ASCENDING_AORTA ± CABG/VALVE'
                ELSE NULL
            END AS surgery_group_analysis,
            sex::VARCHAR AS sex,
            age_at_icu_admission::DOUBLE AS age_at_icu_admission,
            CASE
                WHEN age_at_icu_admission IS NULL THEN NULL
                WHEN age_at_icu_admission < 60 THEN '<60'
                WHEN age_at_icu_admission < 70 THEN '60-69'
                WHEN age_at_icu_admission < 80 THEN '70-79'
                ELSE '>=80'
            END AS age_group
        FROM cohort_raw
        """
    )

    for col in ("surgery_group_analysis", "sex", "age_group"):
        missing = con.execute(
            f"SELECT COUNT(*) FROM cohort_strata WHERE {col} IS NULL"
        ).fetchone()[0]
        if missing:
            print(f"WARNING: {col} missing in {missing} stays.")

    unmapped = con.execute(
        """
        SELECT DISTINCT surgery_group_original
        FROM cohort_strata
        WHERE surgery_group_analysis IS NULL
        ORDER BY surgery_group_original
        """
    ).fetchall()
    if unmapped:
        raise RuntimeError(
            "Unmapped frozen surgery_group values: "
            + ", ".join(str(x[0]) for x in unmapped)
        )


def standardize_six(con) -> None:
    cols = source_columns(con, "six_raw")

    required = {
        "stay_id",
        "window_index",
        "window_label",
        "start_hour",
        "eligible_minutes",
        "observability_status",
        "respiratory_state",
        "invasive_ventilation_fraction",
        "pf_invasive_median",
        "bleeding_recorded_ml",
    }
    required.update(col for _, col, _, _ in CONTINUOUS_VARIABLES)
    missing = sorted(required - cols)
    if missing:
        raise RuntimeError(
            "Phase 02_01 six-hour dataset missing columns: " + ", ".join(missing)
        )

    if "bleeding_primary_12h_available_flag" in cols:
        bleed_source = (
            "CASE WHEN bleeding_primary_12h_available_flag = 1 THEN 1 ELSE 0 END"
        )
    elif "bleeding_interval_contribution_flag" in cols:
        bleed_source = (
            "CASE WHEN bleeding_interval_contribution_flag IS NOT NULL THEN 1 ELSE 0 END"
        )
    else:
        raise RuntimeError(
            "Unable to identify frozen bleeding source-documentation field."
        )

    con.execute(
        f"""
        CREATE VIEW six AS
        SELECT
            *,
            CASE
                WHEN start_hour < 12
                 AND observability_status = 'FULLY_OBSERVABLE'
                THEN 1 ELSE 0
            END AS bleeding_window_fully_observable_flag,
            {bleed_source} AS bleeding_source_documented_flag
        FROM six_raw
        """
    )

    rows, stays = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM six"
    ).fetchone()
    if rows != EXPECTED_COHORT_N * 4 or stays != EXPECTED_COHORT_N:
        raise RuntimeError(
            f"Unexpected six-hour master dimensions: rows={rows}, stays={stays}."
        )


def create_long_strata(con) -> None:
    con.execute(
        """
        CREATE TABLE strata_long AS
        SELECT
            stay_id,
            'SURGERY_GROUP_ANALYSIS' AS stratifier,
            surgery_group_analysis AS stratum
        FROM cohort_strata
        WHERE surgery_group_analysis IS NOT NULL

        UNION ALL

        SELECT stay_id, 'SEX', sex
        FROM cohort_strata
        WHERE sex IS NOT NULL

        UNION ALL

        SELECT stay_id, 'AGE_GROUP', age_group
        FROM cohort_strata
        WHERE age_group IS NOT NULL
        """
    )


def build_counts(con) -> None:
    con.execute(
        """
        CREATE TABLE stratum_counts AS
        SELECT
            stratifier,
            stratum,
            COUNT(DISTINCT stay_id) AS stays_n,
            100.0 * COUNT(DISTINCT stay_id) / 9305.0 AS cohort_pct
        FROM strata_long
        GROUP BY stratifier, stratum
        """
    )


def build_continuous(con) -> None:
    con.execute(
        """
        CREATE TABLE continuous_by_stratum (
            stratifier VARCHAR,
            stratum VARCHAR,
            variable VARCHAR,
            study1_role VARCHAR,
            window_index INTEGER,
            window_label VARCHAR,
            stratum_stays_n BIGINT,
            eligible_stays_n BIGINT,
            available_stays_n BIGINT,
            availability_pct DOUBLE,
            median DOUBLE,
            p25 DOUBLE,
            p75 DOUBLE,
            p10 DOUBLE,
            p90 DOUBLE
        )
        """
    )

    for variable, column, role, eligibility in CONTINUOUS_VARIABLES:
        safe_var = variable.replace("'", "''")
        safe_role = role.replace("'", "''")
        con.execute(
            f"""
            INSERT INTO continuous_by_stratum
            SELECT
                sl.stratifier,
                sl.stratum,
                '{safe_var}',
                '{safe_role}',
                s.window_index,
                s.window_label,
                COUNT(DISTINCT sl.stay_id) AS stratum_stays_n,
                COUNT(*) FILTER (WHERE {eligibility}) AS eligible_stays_n,
                COUNT(*) FILTER (
                    WHERE {eligibility} AND s.{column} IS NOT NULL
                ) AS available_stays_n,
                CASE
                    WHEN COUNT(*) FILTER (WHERE {eligibility}) > 0
                    THEN 100.0 * COUNT(*) FILTER (
                        WHERE {eligibility} AND s.{column} IS NOT NULL
                    ) / COUNT(*) FILTER (WHERE {eligibility})
                    ELSE NULL
                END AS availability_pct,
                median(s.{column}) FILTER (
                    WHERE {eligibility} AND s.{column} IS NOT NULL
                ) AS median,
                quantile_cont(s.{column}, 0.25) FILTER (
                    WHERE {eligibility} AND s.{column} IS NOT NULL
                ) AS p25,
                quantile_cont(s.{column}, 0.75) FILTER (
                    WHERE {eligibility} AND s.{column} IS NOT NULL
                ) AS p75,
                quantile_cont(s.{column}, 0.10) FILTER (
                    WHERE {eligibility} AND s.{column} IS NOT NULL
                ) AS p10,
                quantile_cont(s.{column}, 0.90) FILTER (
                    WHERE {eligibility} AND s.{column} IS NOT NULL
                ) AS p90
            FROM six s
            INNER JOIN strata_long sl USING (stay_id)
            GROUP BY
                sl.stratifier,
                sl.stratum,
                s.window_index,
                s.window_label
            """
        )


def validate_corrected_renal_fluid_strata(con) -> None:
    """
    02_03 is descriptive only. Verify that the stratified renal/fluid rows are
    exact reductions of the corrected 02_01 master for every stratum/window.
    """
    checks = [
        ("Urine output", "urine_output_ml_kg_h"),
        ("Fluid balance cumulative", "fluid_balance_cumulative_ml"),
        (
            "Fluid accumulation cumulative",
            "fluid_accumulation_cumulative_pct_weight",
        ),
    ]

    for variable, column in checks:
        mismatch_n = con.execute(
            f"""
            WITH direct AS (
                SELECT
                    sl.stratifier,
                    sl.stratum,
                    s.window_index,
                    COUNT(*) FILTER (
                        WHERE s.eligible_minutes > 0
                    ) AS eligible_stays_n,
                    COUNT(*) FILTER (
                        WHERE s.eligible_minutes > 0
                          AND s.{column} IS NOT NULL
                    ) AS available_stays_n,
                    median(s.{column}) FILTER (
                        WHERE s.eligible_minutes > 0
                          AND s.{column} IS NOT NULL
                    ) AS median_value
                FROM six s
                INNER JOIN strata_long sl USING (stay_id)
                GROUP BY
                    sl.stratifier,
                    sl.stratum,
                    s.window_index
            )
            SELECT COUNT(*)
            FROM continuous_by_stratum c
            INNER JOIN direct d
                ON c.stratifier = d.stratifier
               AND c.stratum = d.stratum
               AND c.window_index = d.window_index
            WHERE c.variable = ?
              AND (
                    c.eligible_stays_n <> d.eligible_stays_n
                 OR c.available_stays_n <> d.available_stays_n
                 OR (c.median IS NULL) <> (d.median_value IS NULL)
                 OR (
                        c.median IS NOT NULL
                    AND d.median_value IS NOT NULL
                    AND abs(c.median - d.median_value) > 1e-9
                 )
              )
            """,
            [variable],
        ).fetchone()[0]

        if mismatch_n:
            raise RuntimeError(
                f"{variable}: {mismatch_n} stratified summary mismatches."
            )


def build_respiratory(con) -> None:
    con.execute(
        """
        CREATE TABLE respiratory_by_stratum AS
        WITH counts AS (
            SELECT
                sl.stratifier,
                sl.stratum,
                s.window_index,
                s.window_label,
                s.respiratory_state,
                COUNT(*) AS stays_n
            FROM six s
            INNER JOIN strata_long sl USING (stay_id)
            WHERE s.eligible_minutes > 0
              AND s.respiratory_state IS NOT NULL
            GROUP BY
                sl.stratifier,
                sl.stratum,
                s.window_index,
                s.window_label,
                s.respiratory_state
        )
        SELECT
            *,
            SUM(stays_n) OVER (
                PARTITION BY stratifier, stratum, window_index
            ) AS documented_state_windows_n,
            100.0 * stays_n / SUM(stays_n) OVER (
                PARTITION BY stratifier, stratum, window_index
            ) AS percent
        FROM counts
        """
    )


def build_treatment(con) -> None:
    vaso_cols = source_columns(con, "vaso")
    diur_cols = source_columns(con, "diur")

    vaso_req = {
        "stay_id",
        "window_index",
        "window_label",
        "observable_minutes",
        "vis_exposed_flag",
        "nee_exposed_flag",
        "inotrope_inodilator_exposed_flag",
        "any_candidate_exposed_flag",
    }
    diur_req = {
        "stay_id",
        "window_index",
        "window_label",
        "eligible_minutes",
        "furosemide_quantitative_exposure_flag",
        "furosemide_all_route_total_mg",
    }

    if vaso_req - vaso_cols:
        raise RuntimeError(
            "Vasoactive source missing: " + ", ".join(sorted(vaso_req - vaso_cols))
        )
    if diur_req - diur_cols:
        raise RuntimeError(
            "Diuretic source missing: " + ", ".join(sorted(diur_req - diur_cols))
        )

    con.execute(
        """
        CREATE TABLE treatment_by_stratum (
            stratifier VARCHAR,
            stratum VARCHAR,
            treatment_domain VARCHAR,
            metric VARCHAR,
            window_index INTEGER,
            window_label VARCHAR,
            eligible_stays_n BIGINT,
            exposed_stays_n BIGINT,
            exposed_pct DOUBLE,
            dose_median_among_exposed DOUBLE,
            dose_p25_among_exposed DOUBLE,
            dose_p75_among_exposed DOUBLE,
            dose_unit VARCHAR
        )
        """
    )

    metrics = [
        ("VIS exposure", "vis_exposed_flag"),
        ("NEE exposure", "nee_exposed_flag"),
        ("Inotrope/inodilator exposure", "inotrope_inodilator_exposed_flag"),
        ("Any vasoactive/inotrope exposure", "any_candidate_exposed_flag"),
    ]

    for label, flag in metrics:
        safe = label.replace("'", "''")
        con.execute(
            f"""
            INSERT INTO treatment_by_stratum
            SELECT
                sl.stratifier,
                sl.stratum,
                'VASOACTIVE_INOTROPE',
                '{safe}',
                v.window_index,
                v.window_label,
                COUNT(*) FILTER (WHERE v.observable_minutes > 0),
                COUNT(*) FILTER (
                    WHERE v.observable_minutes > 0 AND v.{flag} = 1
                ),
                CASE
                    WHEN COUNT(*) FILTER (WHERE v.observable_minutes > 0) > 0
                    THEN 100.0 * COUNT(*) FILTER (
                        WHERE v.observable_minutes > 0 AND v.{flag} = 1
                    ) / COUNT(*) FILTER (WHERE v.observable_minutes > 0)
                    ELSE NULL
                END,
                NULL, NULL, NULL, NULL
            FROM vaso v
            INNER JOIN strata_long sl USING (stay_id)
            GROUP BY
                sl.stratifier,
                sl.stratum,
                v.window_index,
                v.window_label
            """
        )

    con.execute(
        """
        INSERT INTO treatment_by_stratum
        SELECT
            sl.stratifier,
            sl.stratum,
            'DIURETIC',
            'Furosemide quantitative exposure',
            d.window_index,
            d.window_label,
            COUNT(*) FILTER (WHERE d.eligible_minutes > 0),
            COUNT(*) FILTER (
                WHERE d.eligible_minutes > 0
                  AND d.furosemide_quantitative_exposure_flag = 1
            ),
            CASE
                WHEN COUNT(*) FILTER (WHERE d.eligible_minutes > 0) > 0
                THEN 100.0 * COUNT(*) FILTER (
                    WHERE d.eligible_minutes > 0
                      AND d.furosemide_quantitative_exposure_flag = 1
                ) / COUNT(*) FILTER (WHERE d.eligible_minutes > 0)
                ELSE NULL
            END,
            median(d.furosemide_all_route_total_mg) FILTER (
                WHERE d.eligible_minutes > 0
                  AND d.furosemide_quantitative_exposure_flag = 1
                  AND d.furosemide_all_route_total_mg > 0
            ),
            quantile_cont(d.furosemide_all_route_total_mg, 0.25) FILTER (
                WHERE d.eligible_minutes > 0
                  AND d.furosemide_quantitative_exposure_flag = 1
                  AND d.furosemide_all_route_total_mg > 0
            ),
            quantile_cont(d.furosemide_all_route_total_mg, 0.75) FILTER (
                WHERE d.eligible_minutes > 0
                  AND d.furosemide_quantitative_exposure_flag = 1
                  AND d.furosemide_all_route_total_mg > 0
            ),
            'mg'
        FROM diur d
        INNER JOIN strata_long sl USING (stay_id)
        GROUP BY
            sl.stratifier,
            sl.stratum,
            d.window_index,
            d.window_label
        """
    )


def export(con) -> None:
    outputs = (
        OUT_COUNTS,
        OUT_CONT,
        OUT_RESP,
        OUT_TREAT,
    )
    for p in outputs:
        if p.exists():
            p.unlink()

    con.execute(
        f"""
        COPY (
            SELECT * FROM stratum_counts
            ORDER BY stratifier, stratum
        )
        TO '{qpath(OUT_COUNTS)}'
        (FORMAT CSV, HEADER TRUE)
        """
    )
    con.execute(
        f"""
        COPY (
            SELECT * FROM continuous_by_stratum
            ORDER BY stratifier, stratum, variable, window_index
        )
        TO '{qpath(OUT_CONT)}'
        (FORMAT CSV, HEADER TRUE)
        """
    )
    con.execute(
        f"""
        COPY (
            SELECT * FROM respiratory_by_stratum
            ORDER BY stratifier, stratum, window_index, respiratory_state
        )
        TO '{qpath(OUT_RESP)}'
        (FORMAT CSV, HEADER TRUE)
        """
    )
    con.execute(
        f"""
        COPY (
            SELECT * FROM treatment_by_stratum
            ORDER BY stratifier, stratum, treatment_domain, metric, window_index
        )
        TO '{qpath(OUT_TREAT)}'
        (FORMAT CSV, HEADER TRUE)
        """
    )


def print_summary(con) -> None:
    print("=" * 92)
    print("TRAJECTORIES — Study 1 Phase 02_03 candidate complete")
    print("=" * 92)
    print("Descriptive clinical stratification only (collapsed surgery groups v2).")
    print("No imputation. No favorable-recovery definition. No outcomes. No p-values.")
    print()

    print("Surgery-group analytical mapping")
    print("--------------------------------")
    for row in con.execute(
        """
        SELECT
            surgery_group_original,
            surgery_group_analysis,
            COUNT(*) AS stays_n
        FROM cohort_strata
        GROUP BY surgery_group_original, surgery_group_analysis
        ORDER BY surgery_group_analysis, surgery_group_original
        """
    ).fetchall():
        print("\t".join(str(x) for x in row))

    print()
    print("Stratum counts")
    print("--------------")
    for row in con.execute(
        """
        SELECT
            stratifier,
            stratum,
            stays_n,
            round(cohort_pct, 2)
        FROM stratum_counts
        ORDER BY stratifier, stratum
        """
    ).fetchall():
        print("\t".join(str(x) for x in row))

    print()
    print("Selected H00_H06 / H18_H24 trajectory medians")
    print("---------------------------------------------")
    for row in con.execute(
        """
        SELECT
            stratifier,
            stratum,
            variable,
            window_label,
            available_stays_n,
            round(median, 3)
        FROM continuous_by_stratum
        WHERE variable IN (
            'MAP',
            'Lactate',
            'Urine output',
            'Fluid balance cumulative',
            'Fluid accumulation cumulative',
            'Invasive ventilation fraction',
            'VIS'
        )
          AND window_index IN (0, 3)
        ORDER BY stratifier, stratum, variable, window_index
        """
    ).fetchall():
        print("\t".join("<NULL>" if x is None else str(x) for x in row))

    print()
    print("Outputs:")
    for p in (OUT_COUNTS, OUT_CONT, OUT_RESP, OUT_TREAT):
        print(f"  {p}")


def main() -> int:
    require_files()

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA preserve_insertion_order=false")

    con.execute(
        f"""
        CREATE VIEW cohort_raw AS
        SELECT * FROM read_csv_auto('{qpath(COHORT)}', header=true)
        """
    )
    con.execute(
        f"CREATE VIEW six_raw AS SELECT * FROM read_parquet('{qpath(SIX)}')"
    )
    con.execute(
        f"CREATE VIEW vaso AS SELECT * FROM read_parquet('{qpath(VASO_SIX)}')"
    )
    con.execute(
        f"CREATE VIEW diur AS SELECT * FROM read_parquet('{qpath(DIURETIC_SIX)}')"
    )

    validate_cohort(con)
    build_cohort_strata(con)
    standardize_six(con)
    create_long_strata(con)
    build_counts(con)
    build_continuous(con)
    validate_corrected_renal_fluid_strata(con)
    build_respiratory(con)
    build_treatment(con)
    export(con)
    print_summary(con)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Interrupted by user.", file=sys.stderr)
        raise SystemExit(130)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
