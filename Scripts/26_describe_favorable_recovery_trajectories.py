#!/usr/bin/env python3
"""
TRAJECTORIES — Study 1
Phase 02_06 — Favorable-recovery trajectories

Study:
"Early usual-care and favorable-recovery trajectories after adult cardiac
surgery: a multidomain reference study"

PURPOSE
-------
Describe the observed early postoperative trajectories among patients meeting
the frozen Phase 02_05 favorable-recovery definition.

The analytical structure deliberately mirrors Phase 02_02 so that the
favorable-recovery reference can be compared descriptively with the overall
usual-care population reference.

PRINCIPLES
----------
- frozen favorable-recovery cohort only (expected N=7,835);
- no imputation;
- no clustering or phenotype assignment;
- no prognostic modeling;
- no hypothesis-test p-values;
- same variable-specific eligibility as Phase 02_02;
- P/F availability is conditional on invasive ventilation;
- chest drainage remains restricted to 0-12 h;
- treatment/support context remains separate from physiology;
- comparison with usual-care is descriptive only; the full cohort contains
  the favorable cohort and is not an independent control group.

INPUTS
------
data/02_01_study1_hourly.parquet
data/02_01_study1_six_hour.parquet
data/01_18_vasoactive_inotrope_six_hour_trajectory.parquet
data/01_08_diuretic_exposure_six_hour_summary.parquet
data/02_05_favorable_recovery_patient_level.csv

Optional comparison inputs (canonical Phase 02_02):
data/02_02_continuous_trajectories.csv
data/02_02_treatment_context_trajectories.csv

OUTPUTS
-------
data/02_06_favorable_continuous_trajectories.csv
data/02_06_favorable_paired_change.csv
data/02_06_favorable_respiratory_states.csv
data/02_06_favorable_availability.csv
data/02_06_favorable_treatment_context.csv
data/02_06_favorable_vs_usual_care_reference.csv

PRODUCTION STATUS
-----------------
Frozen analytical workflow using canonical renal/fluid trajectories.

Optional comparison behavior
----------------------------
The two usual-care comparison files are checked independently. When a file
is absent, the corresponding comparison rows are omitted without stopping
the favorable-trajectory calculations. If both files are absent, the exported
comparison table is empty. Required-input checks are unchanged by this policy.
"""

from __future__ import annotations

import sys
from pathlib import Path

try:
    import duckdb
except ImportError as exc:
    raise SystemExit("Missing dependency 'duckdb'.") from exc


EXPECTED_FULL_COHORT_N = 9305
EXPECTED_FAVORABLE_N = 7835

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = (
    SCRIPT_PATH.parent.parent
    if SCRIPT_PATH.parent.name.lower() == "scripts"
    else SCRIPT_PATH.parent
)
DATA_DIR = PROJECT_ROOT / "data"

HOURLY = DATA_DIR / "02_01_study1_hourly.parquet"
SIX = DATA_DIR / "02_01_study1_six_hour.parquet"
VASO_SIX = DATA_DIR / "01_18_vasoactive_inotrope_six_hour_trajectory.parquet"
DIURETIC_SIX = DATA_DIR / "01_08_diuretic_exposure_six_hour_summary.parquet"
FAVORABLE = DATA_DIR / "02_05_favorable_recovery_patient_level.csv"

USUAL_CONT = DATA_DIR / "02_02_continuous_trajectories.csv"
USUAL_TREAT = DATA_DIR / "02_02_treatment_context_trajectories.csv"

OUT_CONT = DATA_DIR / "02_06_favorable_continuous_trajectories.csv"
OUT_DELTA = DATA_DIR / "02_06_favorable_paired_change.csv"
OUT_RESP = DATA_DIR / "02_06_favorable_respiratory_states.csv"
OUT_AVAIL = DATA_DIR / "02_06_favorable_availability.csv"
OUT_TREAT = DATA_DIR / "02_06_favorable_treatment_context.csv"
OUT_COMPARE = DATA_DIR / "02_06_favorable_vs_usual_care_reference.csv"


CONTINUOUS_VARIABLES = [
    ("MAP", "map_twa_mmhg", "CORE", "eligible_minutes > 0"),
    ("Heart rate", "hr_twa_bpm", "CORE", "eligible_minutes > 0"),
    ("Lactate", "lactate_median_mmol_l", "CORE_CONDITIONAL", "eligible_minutes > 0"),
    ("pH", "ph_median", "CORE_CONDITIONAL", "eligible_minutes > 0"),
    ("Base excess", "base_excess_median_meq_l", "SUPPORTIVE", "eligible_minutes > 0"),
    ("Glucose", "glucose_median_mg_dl", "SUPPORTIVE", "eligible_minutes > 0"),
    ("Urine output", "urine_output_ml_kg_h", "CORE", "eligible_minutes > 0"),
    ("Fluid balance cumulative", "fluid_balance_cumulative_ml", "CORE", "eligible_minutes > 0"),
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
        "eligible_minutes > 0 AND pf_invasive_eligible_flag = 1",
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

PAIRED_DELTA_VARIABLES = [
    ("MAP", "map_twa_mmhg"),
    ("Heart rate", "hr_twa_bpm"),
    ("Lactate", "lactate_median_mmol_l"),
    ("pH", "ph_median"),
    ("Base excess", "base_excess_median_meq_l"),
    ("Glucose", "glucose_median_mg_dl"),
    ("Urine output", "urine_output_ml_kg_h"),
    ("P/F ratio", "pf_invasive_median"),
    ("VIS", "vis_twa"),
    ("NEE", "nee_twa"),
]


def qpath(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def source_columns(con, table: str) -> set[str]:
    return {r[0].lower() for r in con.execute(f"DESCRIBE {table}").fetchall()}


def require_inputs() -> None:
    required = (HOURLY, SIX, VASO_SIX, DIURETIC_SIX, FAVORABLE)
    missing = [str(p) for p in required if not p.is_file()]
    if missing:
        raise FileNotFoundError("Missing required input(s):\n- " + "\n- ".join(missing))


def validate_dimensions(con) -> None:
    h = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM hourly_raw"
    ).fetchone()
    s = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM six_raw"
    ).fetchone()

    if h != (EXPECTED_FULL_COHORT_N * 24, EXPECTED_FULL_COHORT_N):
        raise RuntimeError(f"Unexpected hourly dimensions: {h}")
    if s != (EXPECTED_FULL_COHORT_N * 4, EXPECTED_FULL_COHORT_N):
        raise RuntimeError(f"Unexpected six-hour dimensions: {s}")

    f = con.execute(
        """
        SELECT
            COUNT(*),
            COUNT(DISTINCT stay_id),
            COUNT(*) FILTER (WHERE favorable_recovery_flag=1)
        FROM favorable_raw
        """
    ).fetchone()

    if f[0] != EXPECTED_FULL_COHORT_N or f[1] != EXPECTED_FULL_COHORT_N:
        raise RuntimeError(f"Unexpected Phase 02_05 patient-level dimensions: {f[:2]}")
    if f[2] != EXPECTED_FAVORABLE_N:
        raise RuntimeError(
            f"Favorable-recovery closure failed: {f[2]}, expected {EXPECTED_FAVORABLE_N}"
        )


def create_standardized_master_views(con) -> None:
    for source_table, final_table in (
        ("hourly_raw", "hourly_all"),
        ("six_raw", "six_all"),
    ):
        cols = source_columns(con, source_table)

        if "pf_observed_flag" not in cols:
            raise RuntimeError(
                f"{source_table} is missing pf_observed_flag from Phase 02_01."
            )

        pf_eligible_expr = (
            "CASE WHEN invasive_ventilation_fraction IS NOT NULL "
            "AND invasive_ventilation_fraction > 0 THEN 1 ELSE 0 END"
        )

        bleeding_full_expr = (
            "CASE WHEN start_hour < 12 "
            "AND observability_status = 'FULLY_OBSERVABLE' THEN 1 ELSE 0 END"
        )

        if "bleeding_primary_12h_available_flag" in cols:
            bleeding_source_expr = (
                "CASE WHEN bleeding_primary_12h_available_flag = 1 THEN 1 ELSE 0 END"
            )
        elif "bleeding_interval_contribution_flag" in cols:
            bleeding_source_expr = (
                "CASE WHEN bleeding_interval_contribution_flag IS NOT NULL THEN 1 ELSE 0 END"
            )
        else:
            raise RuntimeError(
                f"{source_table} lacks bleeding source-documentation fields."
            )

        con.execute(
            f"""
            CREATE VIEW {final_table} AS
            SELECT
                *,
                {pf_eligible_expr} AS pf_invasive_eligible_flag,
                {bleeding_full_expr} AS bleeding_window_fully_observable_flag,
                {bleeding_source_expr} AS bleeding_source_documented_flag
            FROM {source_table}
            """
        )

    con.execute(
        """
        CREATE VIEW favorable_stays AS
        SELECT stay_id
        FROM favorable_raw
        WHERE favorable_recovery_flag = 1
        """
    )

    con.execute(
        """
        CREATE VIEW hourly AS
        SELECT h.*
        FROM hourly_all h
        INNER JOIN favorable_stays f USING (stay_id)
        """
    )

    con.execute(
        """
        CREATE VIEW six AS
        SELECT s.*
        FROM six_all s
        INNER JOIN favorable_stays f USING (stay_id)
        """
    )

    con.execute(
        """
        CREATE VIEW vaso_fav AS
        SELECT v.*
        FROM vaso_raw v
        INNER JOIN favorable_stays f USING (stay_id)
        """
    )

    con.execute(
        """
        CREATE VIEW diuretic_fav AS
        SELECT d.*
        FROM diuretic_raw d
        INNER JOIN favorable_stays f USING (stay_id)
        """
    )

    h = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM hourly"
    ).fetchone()
    s = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM six"
    ).fetchone()

    if h != (EXPECTED_FAVORABLE_N * 24, EXPECTED_FAVORABLE_N):
        raise RuntimeError(f"Unexpected favorable hourly dimensions: {h}")
    if s != (EXPECTED_FAVORABLE_N * 4, EXPECTED_FAVORABLE_N):
        raise RuntimeError(f"Unexpected favorable six-hour dimensions: {s}")


def validate_schema(con) -> None:
    required = {
        "stay_id",
        "window_index",
        "window_label",
        "start_hour",
        "end_hour",
        "eligible_minutes",
        "observability_status",
        "respiratory_state",
        "invasive_ventilation_fraction",
        "pf_invasive_eligible_flag",
        "bleeding_window_fully_observable_flag",
        "bleeding_source_documented_flag",
    }
    required.update(col for _, col, _, _ in CONTINUOUS_VARIABLES)

    for table in ("hourly", "six"):
        cols = source_columns(con, table)
        missing = sorted(required - cols)
        if missing:
            raise RuntimeError(
                f"{table} is missing required Phase 02_01 columns: "
                + ", ".join(missing)
            )


def build_continuous_summary(con) -> None:
    con.execute(
        """
        CREATE TABLE continuous_summary (
            resolution VARCHAR,
            variable VARCHAR,
            study1_role VARCHAR,
            window_index INTEGER,
            window_label VARCHAR,
            favorable_cohort_n BIGINT,
            eligible_stays_n BIGINT,
            available_stays_n BIGINT,
            availability_pct DOUBLE,
            median DOUBLE,
            p25 DOUBLE,
            p75 DOUBLE,
            p10 DOUBLE,
            p90 DOUBLE,
            mean DOUBLE,
            sd DOUBLE
        )
        """
    )

    for resolution, table in (("HOURLY", "hourly"), ("SIX_HOUR", "six")):
        for variable, column, role, eligibility in CONTINUOUS_VARIABLES:
            safe_variable = variable.replace("'", "''")
            safe_role = role.replace("'", "''")
            con.execute(
                f"""
                INSERT INTO continuous_summary
                SELECT
                    '{resolution}',
                    '{safe_variable}',
                    '{safe_role}',
                    window_index,
                    window_label,
                    COUNT(*) AS favorable_cohort_n,
                    COUNT(*) FILTER (WHERE {eligibility}) AS eligible_stays_n,
                    COUNT(*) FILTER (
                        WHERE {eligibility} AND {column} IS NOT NULL
                    ) AS available_stays_n,
                    CASE
                        WHEN COUNT(*) FILTER (WHERE {eligibility}) > 0
                        THEN 100.0 * COUNT(*) FILTER (
                            WHERE {eligibility} AND {column} IS NOT NULL
                        ) / COUNT(*) FILTER (WHERE {eligibility})
                        ELSE NULL
                    END AS availability_pct,
                    median({column}) FILTER (
                        WHERE {eligibility} AND {column} IS NOT NULL
                    ),
                    quantile_cont({column},0.25) FILTER (
                        WHERE {eligibility} AND {column} IS NOT NULL
                    ),
                    quantile_cont({column},0.75) FILTER (
                        WHERE {eligibility} AND {column} IS NOT NULL
                    ),
                    quantile_cont({column},0.10) FILTER (
                        WHERE {eligibility} AND {column} IS NOT NULL
                    ),
                    quantile_cont({column},0.90) FILTER (
                        WHERE {eligibility} AND {column} IS NOT NULL
                    ),
                    avg({column}) FILTER (
                        WHERE {eligibility} AND {column} IS NOT NULL
                    ),
                    stddev_samp({column}) FILTER (
                        WHERE {eligibility} AND {column} IS NOT NULL
                    )
                FROM {table}
                GROUP BY window_index, window_label
                """
            )


def validate_corrected_renal_fluid_favorable_summary(con) -> None:
    """
    Verify that favorable-recovery renal/fluid six-hour summaries are exact
    descriptive reductions of the corrected 02_01 master after restricting to
    the frozen 02_05 favorable cohort.
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
                    window_index,
                    COUNT(*) FILTER (
                        WHERE eligible_minutes > 0
                    ) AS eligible_stays_n,
                    COUNT(*) FILTER (
                        WHERE eligible_minutes > 0
                          AND {column} IS NOT NULL
                    ) AS available_stays_n,
                    median({column}) FILTER (
                        WHERE eligible_minutes > 0
                          AND {column} IS NOT NULL
                    ) AS median_value
                FROM six
                GROUP BY window_index
            )
            SELECT COUNT(*)
            FROM continuous_summary c
            INNER JOIN direct d USING (window_index)
            WHERE c.resolution='SIX_HOUR'
              AND c.variable=?
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
                f"{variable}: {mismatch_n} favorable renal/fluid "
                "summary mismatches."
            )


def build_paired_delta(con) -> None:
    con.execute(
        """
        CREATE TABLE paired_delta_summary (
            variable VARCHAR,
            window_index INTEGER,
            window_label VARCHAR,
            paired_stays_n BIGINT,
            delta_median DOUBLE,
            delta_p25 DOUBLE,
            delta_p75 DOUBLE,
            delta_p10 DOUBLE,
            delta_p90 DOUBLE
        )
        """
    )

    for variable, column in PAIRED_DELTA_VARIABLES:
        safe_variable = variable.replace("'", "''")
        con.execute(
            f"""
            INSERT INTO paired_delta_summary
            WITH baseline AS (
                SELECT stay_id, {column} AS baseline_value
                FROM six
                WHERE window_index=0
                  AND {column} IS NOT NULL
            ),
            paired AS (
                SELECT
                    s.stay_id,
                    s.window_index,
                    s.window_label,
                    s.{column} - b.baseline_value AS delta
                FROM six s
                INNER JOIN baseline b USING (stay_id)
                WHERE s.{column} IS NOT NULL
            )
            SELECT
                '{safe_variable}',
                window_index,
                window_label,
                COUNT(*),
                median(delta),
                quantile_cont(delta,0.25),
                quantile_cont(delta,0.75),
                quantile_cont(delta,0.10),
                quantile_cont(delta,0.90)
            FROM paired
            GROUP BY window_index,window_label
            """
        )


def build_respiratory_distribution(con) -> None:
    con.execute(
        """
        CREATE TABLE respiratory_distribution AS
        SELECT
            'SIX_HOUR' AS resolution,
            window_index,
            window_label,
            respiratory_state,
            COUNT(*) AS stays_n,
            SUM(COUNT(*)) OVER (
                PARTITION BY window_index
            ) AS states_total_n,
            100.0 * COUNT(*) / SUM(COUNT(*)) OVER (
                PARTITION BY window_index
            ) AS percent_of_state_documented_windows
        FROM six
        WHERE eligible_minutes > 0
          AND respiratory_state IS NOT NULL
        GROUP BY window_index,window_label,respiratory_state
        """
    )


def build_availability(con) -> None:
    con.execute(
        """
        CREATE TABLE availability_summary AS
        SELECT
            resolution,
            variable,
            study1_role,
            window_index,
            window_label,
            eligible_stays_n,
            available_stays_n,
            availability_pct
        FROM continuous_summary

        UNION ALL

        SELECT
            'SIX_HOUR',
            'Respiratory support',
            'CORE',
            window_index,
            window_label,
            COUNT(*) FILTER (WHERE eligible_minutes > 0),
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0 AND respiratory_state IS NOT NULL
            ),
            CASE
                WHEN COUNT(*) FILTER (WHERE eligible_minutes > 0) > 0
                THEN 100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0 AND respiratory_state IS NOT NULL
                ) / COUNT(*) FILTER (WHERE eligible_minutes > 0)
                ELSE NULL
            END
        FROM six
        GROUP BY window_index,window_label
        """
    )


def build_treatment_context(con) -> None:
    vaso_required = {
        "stay_id",
        "window_index",
        "window_label",
        "observable_minutes",
        "vis_exposed_flag",
        "nee_exposed_flag",
        "inotrope_inodilator_exposed_flag",
        "any_candidate_exposed_flag",
    }
    diuretic_required = {
        "stay_id",
        "window_index",
        "window_label",
        "eligible_minutes",
        "furosemide_quantitative_exposure_flag",
        "furosemide_all_route_total_mg",
    }

    missing_vaso = sorted(vaso_required - source_columns(con, "vaso_fav"))
    missing_diuretic = sorted(diuretic_required - source_columns(con, "diuretic_fav"))
    if missing_vaso:
        raise RuntimeError(
            "vaso_fav is missing required columns: " + ", ".join(missing_vaso)
        )
    if missing_diuretic:
        raise RuntimeError(
            "diuretic_fav is missing required columns: " + ", ".join(missing_diuretic)
        )

    con.execute(
        """
        CREATE TABLE treatment_context AS

        SELECT
            'VASOACTIVE_INOTROPE' AS treatment_domain,
            window_index,
            window_label,
            'VIS exposure' AS metric,
            COUNT(*) FILTER (WHERE observable_minutes > 0) AS eligible_stays_n,
            COUNT(*) FILTER (
                WHERE observable_minutes > 0 AND vis_exposed_flag=1
            ) AS exposed_stays_n,
            100.0*COUNT(*) FILTER (
                WHERE observable_minutes > 0 AND vis_exposed_flag=1
            )/NULLIF(COUNT(*) FILTER (WHERE observable_minutes > 0),0) AS exposed_pct,
            NULL::DOUBLE AS dose_median_among_exposed,
            NULL::DOUBLE AS dose_p25_among_exposed,
            NULL::DOUBLE AS dose_p75_among_exposed,
            NULL::VARCHAR AS dose_unit
        FROM vaso_fav
        GROUP BY window_index,window_label

        UNION ALL

        SELECT
            'VASOACTIVE_INOTROPE',
            window_index,
            window_label,
            'NEE exposure',
            COUNT(*) FILTER (WHERE observable_minutes > 0),
            COUNT(*) FILTER (
                WHERE observable_minutes > 0 AND nee_exposed_flag=1
            ),
            100.0*COUNT(*) FILTER (
                WHERE observable_minutes > 0 AND nee_exposed_flag=1
            )/NULLIF(COUNT(*) FILTER (WHERE observable_minutes > 0),0),
            NULL::DOUBLE,NULL::DOUBLE,NULL::DOUBLE,NULL::VARCHAR
        FROM vaso_fav
        GROUP BY window_index,window_label

        UNION ALL

        SELECT
            'VASOACTIVE_INOTROPE',
            window_index,
            window_label,
            'Inotrope/inodilator exposure',
            COUNT(*) FILTER (WHERE observable_minutes > 0),
            COUNT(*) FILTER (
                WHERE observable_minutes > 0
                  AND inotrope_inodilator_exposed_flag=1
            ),
            100.0*COUNT(*) FILTER (
                WHERE observable_minutes > 0
                  AND inotrope_inodilator_exposed_flag=1
            )/NULLIF(COUNT(*) FILTER (WHERE observable_minutes > 0),0),
            NULL::DOUBLE,NULL::DOUBLE,NULL::DOUBLE,NULL::VARCHAR
        FROM vaso_fav
        GROUP BY window_index,window_label

        UNION ALL

        SELECT
            'VASOACTIVE_INOTROPE',
            window_index,
            window_label,
            'Any vasoactive/inotrope exposure',
            COUNT(*) FILTER (WHERE observable_minutes > 0),
            COUNT(*) FILTER (
                WHERE observable_minutes > 0
                  AND any_candidate_exposed_flag=1
            ),
            100.0*COUNT(*) FILTER (
                WHERE observable_minutes > 0
                  AND any_candidate_exposed_flag=1
            )/NULLIF(COUNT(*) FILTER (WHERE observable_minutes > 0),0),
            NULL::DOUBLE,NULL::DOUBLE,NULL::DOUBLE,NULL::VARCHAR
        FROM vaso_fav
        GROUP BY window_index,window_label

        UNION ALL

        SELECT
            'DIURETIC',
            window_index,
            window_label,
            'Furosemide quantitative exposure',
            COUNT(*) FILTER (WHERE eligible_minutes > 0),
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND furosemide_quantitative_exposure_flag=1
            ),
            100.0*COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND furosemide_quantitative_exposure_flag=1
            )/NULLIF(COUNT(*) FILTER (WHERE eligible_minutes > 0),0),
            median(furosemide_all_route_total_mg) FILTER (
                WHERE eligible_minutes > 0
                  AND furosemide_quantitative_exposure_flag=1
                  AND furosemide_all_route_total_mg > 0
            ),
            quantile_cont(furosemide_all_route_total_mg,0.25) FILTER (
                WHERE eligible_minutes > 0
                  AND furosemide_quantitative_exposure_flag=1
                  AND furosemide_all_route_total_mg > 0
            ),
            quantile_cont(furosemide_all_route_total_mg,0.75) FILTER (
                WHERE eligible_minutes > 0
                  AND furosemide_quantitative_exposure_flag=1
                  AND furosemide_all_route_total_mg > 0
            ),
            'mg'
        FROM diuretic_fav
        GROUP BY window_index,window_label
        """
    )


def build_reference_comparison(con) -> None:
    """
    Descriptive comparison only.
    The overall usual-care cohort includes the favorable cohort and is not an
    independent comparator. No hypothesis tests are generated.
    """
    con.execute(
        """
        CREATE TABLE reference_comparison (
            domain VARCHAR,
            variable_or_metric VARCHAR,
            window_index INTEGER,
            window_label VARCHAR,
            favorable_value DOUBLE,
            usual_care_value DOUBLE,
            favorable_minus_usual_care DOUBLE,
            favorable_n BIGINT,
            usual_care_n BIGINT,
            comparison_note VARCHAR
        )
        """
    )

    if USUAL_CONT.is_file():
        con.execute(
            f"""
            CREATE VIEW usual_cont AS
            SELECT *
            FROM read_csv_auto(
                '{qpath(USUAL_CONT)}',
                header=true,
                all_varchar=false
            )
            """
        )

        con.execute(
            """
            INSERT INTO reference_comparison
            SELECT
                'CONTINUOUS_MEDIAN',
                f.variable,
                f.window_index,
                f.window_label,
                f.median,
                u.median,
                f.median-u.median,
                f.available_stays_n,
                u.available_stays_n,
                'Descriptive difference only; overall usual-care cohort contains favorable patients.'
            FROM continuous_summary f
            INNER JOIN usual_cont u
              ON f.resolution=u.resolution
             AND f.variable=u.variable
             AND f.window_index=u.window_index
            WHERE f.resolution='SIX_HOUR'
            """
        )

    if USUAL_TREAT.is_file():
        con.execute(
            f"""
            CREATE VIEW usual_treat AS
            SELECT *
            FROM read_csv_auto(
                '{qpath(USUAL_TREAT)}',
                header=true,
                all_varchar=false
            )
            """
        )

        con.execute(
            """
            INSERT INTO reference_comparison
            SELECT
                'TREATMENT_EXPOSURE_PCT',
                f.metric,
                f.window_index,
                f.window_label,
                f.exposed_pct,
                u.exposed_pct,
                f.exposed_pct-u.exposed_pct,
                f.exposed_stays_n,
                u.exposed_stays_n,
                'Percentage-point difference only; overall usual-care cohort contains favorable patients.'
            FROM treatment_context f
            INNER JOIN usual_treat u
              ON f.treatment_domain=u.treatment_domain
             AND f.metric=u.metric
             AND f.window_index=u.window_index
            """
        )


def export(con) -> None:
    outputs = (OUT_CONT, OUT_DELTA, OUT_RESP, OUT_AVAIL, OUT_TREAT, OUT_COMPARE)
    for path in outputs:
        if path.exists():
            path.unlink()

    con.execute(
        f"""
        COPY (
            SELECT * FROM continuous_summary
            ORDER BY
                CASE resolution WHEN 'SIX_HOUR' THEN 1 ELSE 2 END,
                variable,
                window_index
        )
        TO '{qpath(OUT_CONT)}'
        (FORMAT CSV, HEADER TRUE)
        """
    )

    con.execute(
        f"""
        COPY (
            SELECT * FROM paired_delta_summary
            ORDER BY variable,window_index
        )
        TO '{qpath(OUT_DELTA)}'
        (FORMAT CSV, HEADER TRUE)
        """
    )

    con.execute(
        f"""
        COPY (
            SELECT * FROM respiratory_distribution
            ORDER BY window_index,respiratory_state
        )
        TO '{qpath(OUT_RESP)}'
        (FORMAT CSV, HEADER TRUE)
        """
    )

    con.execute(
        f"""
        COPY (
            SELECT * FROM availability_summary
            ORDER BY
                CASE resolution WHEN 'SIX_HOUR' THEN 1 ELSE 2 END,
                variable,
                window_index
        )
        TO '{qpath(OUT_AVAIL)}'
        (FORMAT CSV, HEADER TRUE)
        """
    )

    con.execute(
        f"""
        COPY (
            SELECT * FROM treatment_context
            ORDER BY treatment_domain,metric,window_index
        )
        TO '{qpath(OUT_TREAT)}'
        (FORMAT CSV, HEADER TRUE)
        """
    )

    con.execute(
        f"""
        COPY (
            SELECT * FROM reference_comparison
            ORDER BY domain,variable_or_metric,window_index
        )
        TO '{qpath(OUT_COMPARE)}'
        (FORMAT CSV, HEADER TRUE)
        """
    )


def print_summary(con) -> None:
    print("=" * 104)
    print("TRAJECTORIES — Study 1 Phase 02_06 favorable-recovery trajectories candidate complete")
    print("=" * 104)
    print("PHASE 02_06 CANDIDATE — validation run.")
    print("Descriptive favorable-recovery reference only.")
    print("No imputation. No clustering. No hypothesis tests. No prognostic modeling.")
    print(f"Frozen favorable cohort: {EXPECTED_FAVORABLE_N}/{EXPECTED_FULL_COHORT_N} (84.20%)")
    print()

    print("SIX-HOUR FAVORABLE-RECOVERY TRAJECTORIES")
    print("----------------------------------------")
    rows = con.execute(
        """
        SELECT
            variable,
            window_label,
            available_stays_n,
            round(availability_pct,1),
            round(median,3),
            round(p25,3),
            round(p75,3)
        FROM continuous_summary
        WHERE resolution='SIX_HOUR'
        ORDER BY variable,window_index
        """
    ).fetchall()
    for row in rows:
        print("\t".join("<NULL>" if x is None else str(x) for x in row))

    print()
    print("CORRECTED RENAL/FLUID FAVORABLE-RECOVERY SUMMARY")
    print("------------------------------------------------")
    rows = con.execute(
        """
        SELECT
            variable,
            window_label,
            available_stays_n,
            round(availability_pct,2),
            round(median,4),
            round(p25,4),
            round(p75,4)
        FROM continuous_summary
        WHERE resolution='SIX_HOUR'
          AND variable IN (
              'Urine output',
              'Fluid balance cumulative',
              'Fluid accumulation cumulative'
          )
        ORDER BY
            CASE variable
                WHEN 'Urine output' THEN 1
                WHEN 'Fluid balance cumulative' THEN 2
                WHEN 'Fluid accumulation cumulative' THEN 3
                ELSE 4
            END,
            window_index
        """
    ).fetchall()
    for row in rows:
        print("\t".join("<NULL>" if x is None else str(x) for x in row))

    print()
    print("SIX-HOUR TREATMENT CONTEXT")
    print("--------------------------")
    rows = con.execute(
        """
        SELECT
            metric,
            window_label,
            eligible_stays_n,
            exposed_stays_n,
            round(exposed_pct,2),
            round(dose_median_among_exposed,3),
            round(dose_p25_among_exposed,3),
            round(dose_p75_among_exposed,3),
            dose_unit
        FROM treatment_context
        ORDER BY treatment_domain,metric,window_index
        """
    ).fetchall()
    for row in rows:
        print("\t".join("<NULL>" if x is None else str(x) for x in row))

    print()
    print("PAIRED CHANGE FROM H00-H06")
    print("---------------------------")
    rows = con.execute(
        """
        SELECT
            variable,
            window_label,
            paired_stays_n,
            round(delta_median,3),
            round(delta_p25,3),
            round(delta_p75,3)
        FROM paired_delta_summary
        WHERE window_index > 0
        ORDER BY variable,window_index
        """
    ).fetchall()
    for row in rows:
        print("\t".join("<NULL>" if x is None else str(x) for x in row))

    print()
    print("RESPIRATORY STATE DISTRIBUTION")
    print("------------------------------")
    rows = con.execute(
        """
        SELECT
            window_label,
            respiratory_state,
            stays_n,
            round(percent_of_state_documented_windows,2)
        FROM respiratory_distribution
        ORDER BY window_index, stays_n DESC, respiratory_state
        """
    ).fetchall()
    for row in rows:
        print("\t".join("<NULL>" if x is None else str(x) for x in row))

    print()
    print("SELECTIVE-MEASUREMENT AVAILABILITY — SIX-HOUR")
    print("---------------------------------------------")
    rows = con.execute(
        """
        SELECT
            variable,
            window_label,
            eligible_stays_n,
            available_stays_n,
            round(availability_pct,1)
        FROM availability_summary
        WHERE resolution='SIX_HOUR'
          AND variable IN ('Lactate','pH','Base excess','P/F ratio')
        ORDER BY variable,window_index
        """
    ).fetchall()
    for row in rows:
        print("\t".join("<NULL>" if x is None else str(x) for x in row))

    print()
    print("DESCRIPTIVE DIFFERENCE FROM USUAL-CARE REFERENCE")
    print("------------------------------------------------")
    n_comp = con.execute("SELECT COUNT(*) FROM reference_comparison").fetchone()[0]
    if n_comp == 0:
        print(
            "No comparison table created because canonical Phase 02_02 "
            "reference outputs were not found."
        )
    else:
        rows = con.execute(
            """
            SELECT
                domain,
                variable_or_metric,
                window_label,
                round(favorable_value,3),
                round(usual_care_value,3),
                round(favorable_minus_usual_care,3)
            FROM reference_comparison
            ORDER BY domain,variable_or_metric,window_index
            """
        ).fetchall()
        for row in rows:
            print("\t".join("<NULL>" if x is None else str(x) for x in row))

    print()
    print("CANDIDATE OUTPUTS")
    print("-------")
    for path in (OUT_CONT, OUT_DELTA, OUT_RESP, OUT_AVAIL, OUT_TREAT, OUT_COMPARE):
        print(path)


def main() -> int:
    require_inputs()

    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA preserve_insertion_order=false")

    con.execute(
        f"CREATE VIEW hourly_raw AS SELECT * FROM read_parquet('{qpath(HOURLY)}')"
    )
    con.execute(
        f"CREATE VIEW six_raw AS SELECT * FROM read_parquet('{qpath(SIX)}')"
    )
    con.execute(
        f"CREATE VIEW vaso_raw AS SELECT * FROM read_parquet('{qpath(VASO_SIX)}')"
    )
    con.execute(
        f"CREATE VIEW diuretic_raw AS SELECT * FROM read_parquet('{qpath(DIURETIC_SIX)}')"
    )
    con.execute(
        f"""
        CREATE VIEW favorable_raw AS
        SELECT *
        FROM read_csv_auto(
            '{qpath(FAVORABLE)}',
            header=true,
            all_varchar=false
        )
        """
    )

    validate_dimensions(con)
    create_standardized_master_views(con)
    validate_schema(con)
    build_continuous_summary(con)
    validate_corrected_renal_fluid_favorable_summary(con)
    build_paired_delta(con)
    build_respiratory_distribution(con)
    build_availability(con)
    build_treatment_context(con)
    build_reference_comparison(con)
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
