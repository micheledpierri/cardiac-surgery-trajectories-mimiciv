#!/usr/bin/env python3
"""
02_02_describe_usual_care_trajectories.py

TRAJECTORIES — Study 1
"Early usual-care and favorable-recovery trajectories after adult cardiac
surgery: a multidomain reference study"

PHASE 02_02 — Population-level usual-care trajectories

Purpose
-------
Describe the observed population-level postoperative course before defining
favorable recovery, phenotypes, or outcomes.

Inputs
------
data/02_01_study1_hourly.parquet
data/02_01_study1_six_hour.parquet
data/01_18_vasoactive_inotrope_six_hour_trajectory.parquet
data/01_08_diuretic_exposure_six_hour_summary.parquet

Outputs
-------
data/02_02_continuous_trajectories.csv
data/02_02_paired_change_from_first_window.csv
data/02_02_respiratory_state_distribution.csv
data/02_02_availability_summary.csv
data/02_02_treatment_context_trajectories.csv

Principles
----------
- no imputation;
- no outcome information;
- no favorable-recovery definition;
- no clustering;
- variable-specific eligibility is preserved;
- VIS/NEE zeros are valid absence-of-treatment values;
- P/F availability is conditional on invasive eligibility already encoded
  upstream;
- bleeding is restricted to its frozen 0-12 h horizon;
- paired change is calculated within stay, not as a difference between
  population medians.

Statistical summaries
---------------------
Within each window, eligible nonmissing observations contribute the median,
quartiles, 10th/90th percentiles, mean, and sample standard deviation. Availability
is 100 times available stays divided by eligible stays for that variable/window.
Six-hour paired changes subtract the same stay's observed H00_H06 value;
only stays with both observed values contribute to the change distribution.
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


SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = (
    SCRIPT_PATH.parent.parent
    if SCRIPT_PATH.parent.name.lower() == "scripts"
    else SCRIPT_PATH.parent
)
DATA_DIR = PROJECT_ROOT / "data"

HOURLY = DATA_DIR / "02_01_study1_hourly.parquet"
SIX_HOUR = DATA_DIR / "02_01_study1_six_hour.parquet"
VASO_SIX = DATA_DIR / "01_18_vasoactive_inotrope_six_hour_trajectory.parquet"
DIURETIC_SIX = DATA_DIR / "01_08_diuretic_exposure_six_hour_summary.parquet"

OUT_CONT = DATA_DIR / "02_02_continuous_trajectories.csv"
OUT_DELTA = DATA_DIR / "02_02_paired_change_from_first_window.csv"
OUT_RESP = DATA_DIR / "02_02_respiratory_state_distribution.csv"
OUT_AVAIL = DATA_DIR / "02_02_availability_summary.csv"
OUT_TREATMENT = DATA_DIR / "02_02_treatment_context_trajectories.csv"

EXPECTED_COHORT_N = 9305


# variable_name, column_name, role, eligibility expression
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
        "eligible_minutes > 0 AND pf_invasive_eligible_flag = 1",
    ),
    ("VIS", "vis_twa", "CORE_TREATMENT", "eligible_minutes > 0"),
    (
        "NEE",
        "nee_twa",
        "SUPPORTIVE_TREATMENT",
        "eligible_minutes > 0",
    ),
    (
        "Chest drainage",
        "bleeding_recorded_ml",
        "CORE_EARLY",
        "eligible_minutes > 0 AND start_hour < 12 "
        "AND bleeding_window_fully_observable_flag = 1 "
        "AND bleeding_source_documented_flag = 1",
    ),
]

# Paired delta is meaningful only for variables where "change from baseline"
# has an interpretable sign and where the first six-hour window is an
# appropriate reference. Cumulative balance variables are excluded because
# they are mechanically cumulative.
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


def require_inputs() -> None:
    missing = [
        str(p)
        for p in (HOURLY, SIX_HOUR, VASO_SIX, DIURETIC_SIX)
        if not p.is_file()
    ]
    if missing:
        raise FileNotFoundError(
            "Missing Phase 02_01 input(s):\n- " + "\n- ".join(missing)
        )


def source_columns(con, table: str) -> set[str]:
    return {r[0].lower() for r in con.execute(f"DESCRIBE {table}").fetchall()}


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


def validate_dimensions(con) -> None:
    h = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM hourly"
    ).fetchone()
    s = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM six"
    ).fetchone()

    if h != (EXPECTED_COHORT_N * 24, EXPECTED_COHORT_N):
        raise RuntimeError(f"Unexpected hourly dimensions: {h}")
    if s != (EXPECTED_COHORT_N * 4, EXPECTED_COHORT_N):
        raise RuntimeError(f"Unexpected six-hour dimensions: {s}")


def build_continuous_summary(con) -> None:
    con.execute(
        """
        CREATE TABLE continuous_summary (
            resolution VARCHAR,
            variable VARCHAR,
            study1_role VARCHAR,
            window_index INTEGER,
            window_label VARCHAR,
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
                    COUNT(*) FILTER (
                        WHERE {eligibility}
                    ) AS eligible_stays_n,
                    COUNT(*) FILTER (
                        WHERE {eligibility}
                          AND {column} IS NOT NULL
                    ) AS available_stays_n,
                    CASE
                        WHEN COUNT(*) FILTER (WHERE {eligibility}) > 0
                        THEN 100.0 * COUNT(*) FILTER (
                            WHERE {eligibility}
                              AND {column} IS NOT NULL
                        ) / COUNT(*) FILTER (WHERE {eligibility})
                        ELSE NULL
                    END AS availability_pct,
                    median({column}) FILTER (
                        WHERE {eligibility}
                          AND {column} IS NOT NULL
                    ) AS median,
                    quantile_cont({column}, 0.25) FILTER (
                        WHERE {eligibility}
                          AND {column} IS NOT NULL
                    ) AS p25,
                    quantile_cont({column}, 0.75) FILTER (
                        WHERE {eligibility}
                          AND {column} IS NOT NULL
                    ) AS p75,
                    quantile_cont({column}, 0.10) FILTER (
                        WHERE {eligibility}
                          AND {column} IS NOT NULL
                    ) AS p10,
                    quantile_cont({column}, 0.90) FILTER (
                        WHERE {eligibility}
                          AND {column} IS NOT NULL
                    ) AS p90,
                    avg({column}) FILTER (
                        WHERE {eligibility}
                          AND {column} IS NOT NULL
                    ) AS mean,
                    stddev_samp({column}) FILTER (
                        WHERE {eligibility}
                          AND {column} IS NOT NULL
                    ) AS sd
                FROM {table}
                GROUP BY window_index, window_label
                """
            )


def validate_corrected_renal_fluid_summary(con) -> None:
    """
    Verify that six-hour renal/fluid summaries in 02_02 are exact descriptive
    reductions of the corrected 02_01 master dataset.
    """
    checks = [
        ("Urine output", "urine_output_ml_kg_h", "eligible_minutes > 0"),
        (
            "Fluid balance cumulative",
            "fluid_balance_cumulative_ml",
            "eligible_minutes > 0",
        ),
        (
            "Fluid accumulation cumulative",
            "fluid_accumulation_cumulative_pct_weight",
            "eligible_minutes > 0",
        ),
    ]

    for variable, column, eligibility in checks:
        summary_rows = con.execute(
            """
            SELECT
                window_index,
                eligible_stays_n,
                available_stays_n,
                median
            FROM continuous_summary
            WHERE resolution = 'SIX_HOUR'
              AND variable = ?
            ORDER BY window_index
            """,
            [variable],
        ).fetchall()

        direct_rows = con.execute(
            f"""
            SELECT
                window_index,
                COUNT(*) FILTER (WHERE {eligibility}) AS eligible_n,
                COUNT(*) FILTER (
                    WHERE {eligibility} AND {column} IS NOT NULL
                ) AS available_n,
                median({column}) FILTER (
                    WHERE {eligibility} AND {column} IS NOT NULL
                ) AS median_value
            FROM six
            GROUP BY window_index
            ORDER BY window_index
            """
        ).fetchall()

        if len(summary_rows) != len(direct_rows):
            raise RuntimeError(
                f"{variable}: summary/direct row-count mismatch."
            )

        for summary, direct in zip(summary_rows, direct_rows):
            if summary[0] != direct[0]:
                raise RuntimeError(
                    f"{variable}: window-index mismatch "
                    f"{summary[0]} vs {direct[0]}."
                )
            if summary[1] != direct[1] or summary[2] != direct[2]:
                raise RuntimeError(
                    f"{variable} window {summary[0]}: "
                    "eligibility/availability mismatch."
                )

            s_med, d_med = summary[3], direct[3]
            if (s_med is None) != (d_med is None):
                raise RuntimeError(
                    f"{variable} window {summary[0]}: median null mismatch."
                )
            if s_med is not None and abs(s_med - d_med) > 1e-9:
                raise RuntimeError(
                    f"{variable} window {summary[0]}: "
                    f"median mismatch {s_med} vs {d_med}."
                )


def build_paired_delta_summary(con) -> None:
    """
    For each six-hour window, compare a patient's value with that same
    patient's H00_H06 value. Only paired observed values contribute.
    """
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
                SELECT
                    stay_id,
                    {column} AS baseline_value
                FROM six
                WHERE window_index = 0
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
                COUNT(*) AS paired_stays_n,
                median(delta) AS delta_median,
                quantile_cont(delta, 0.25) AS delta_p25,
                quantile_cont(delta, 0.75) AS delta_p75,
                quantile_cont(delta, 0.10) AS delta_p10,
                quantile_cont(delta, 0.90) AS delta_p90
            FROM paired
            GROUP BY window_index, window_label
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
        GROUP BY window_index, window_label, respiratory_state
        ORDER BY window_index, respiratory_state
        """
    )


def build_availability_summary(con) -> None:
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
                WHERE eligible_minutes > 0
                  AND respiratory_state IS NOT NULL
            ),
            CASE
                WHEN COUNT(*) FILTER (WHERE eligible_minutes > 0) > 0
                THEN 100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND respiratory_state IS NOT NULL
                ) / COUNT(*) FILTER (WHERE eligible_minutes > 0)
                ELSE NULL
            END
        FROM six
        GROUP BY window_index, window_label
        """
    )



def build_treatment_context(con) -> None:
    """
    Describe treatment-process trajectories using frozen upstream exposure
    definitions. Denominator = observable/eligible windows in the canonical
    source component. No treatment exposure in an observable window is zero,
    not missing.
    """
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

    vaso_cols = source_columns(con, "vaso_six")
    diuretic_cols = source_columns(con, "diuretic_six")

    missing_vaso = sorted(vaso_required - vaso_cols)
    missing_diuretic = sorted(diuretic_required - diuretic_cols)
    if missing_vaso:
        raise RuntimeError(
            "vaso_six is missing required columns: " + ", ".join(missing_vaso)
        )
    if missing_diuretic:
        raise RuntimeError(
            "diuretic_six is missing required columns: "
            + ", ".join(missing_diuretic)
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
                WHERE observable_minutes > 0 AND vis_exposed_flag = 1
            ) AS exposed_stays_n,
            100.0 * COUNT(*) FILTER (
                WHERE observable_minutes > 0 AND vis_exposed_flag = 1
            ) / NULLIF(
                COUNT(*) FILTER (WHERE observable_minutes > 0), 0
            ) AS exposed_pct,
            NULL::DOUBLE AS dose_median_among_exposed,
            NULL::DOUBLE AS dose_p25_among_exposed,
            NULL::DOUBLE AS dose_p75_among_exposed,
            NULL::VARCHAR AS dose_unit
        FROM vaso_six
        GROUP BY window_index, window_label

        UNION ALL

        SELECT
            'VASOACTIVE_INOTROPE',
            window_index,
            window_label,
            'NEE exposure',
            COUNT(*) FILTER (WHERE observable_minutes > 0),
            COUNT(*) FILTER (
                WHERE observable_minutes > 0 AND nee_exposed_flag = 1
            ),
            100.0 * COUNT(*) FILTER (
                WHERE observable_minutes > 0 AND nee_exposed_flag = 1
            ) / NULLIF(
                COUNT(*) FILTER (WHERE observable_minutes > 0), 0
            ),
            NULL::DOUBLE,
            NULL::DOUBLE,
            NULL::DOUBLE,
            NULL::VARCHAR
        FROM vaso_six
        GROUP BY window_index, window_label

        UNION ALL

        SELECT
            'VASOACTIVE_INOTROPE',
            window_index,
            window_label,
            'Inotrope/inodilator exposure',
            COUNT(*) FILTER (WHERE observable_minutes > 0),
            COUNT(*) FILTER (
                WHERE observable_minutes > 0
                  AND inotrope_inodilator_exposed_flag = 1
            ),
            100.0 * COUNT(*) FILTER (
                WHERE observable_minutes > 0
                  AND inotrope_inodilator_exposed_flag = 1
            ) / NULLIF(
                COUNT(*) FILTER (WHERE observable_minutes > 0), 0
            ),
            NULL::DOUBLE,
            NULL::DOUBLE,
            NULL::DOUBLE,
            NULL::VARCHAR
        FROM vaso_six
        GROUP BY window_index, window_label

        UNION ALL

        SELECT
            'VASOACTIVE_INOTROPE',
            window_index,
            window_label,
            'Any vasoactive/inotrope exposure',
            COUNT(*) FILTER (WHERE observable_minutes > 0),
            COUNT(*) FILTER (
                WHERE observable_minutes > 0
                  AND any_candidate_exposed_flag = 1
            ),
            100.0 * COUNT(*) FILTER (
                WHERE observable_minutes > 0
                  AND any_candidate_exposed_flag = 1
            ) / NULLIF(
                COUNT(*) FILTER (WHERE observable_minutes > 0), 0
            ),
            NULL::DOUBLE,
            NULL::DOUBLE,
            NULL::DOUBLE,
            NULL::VARCHAR
        FROM vaso_six
        GROUP BY window_index, window_label

        UNION ALL

        SELECT
            'DIURETIC',
            window_index,
            window_label,
            'Furosemide quantitative exposure',
            COUNT(*) FILTER (WHERE eligible_minutes > 0),
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND furosemide_quantitative_exposure_flag = 1
            ),
            100.0 * COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND furosemide_quantitative_exposure_flag = 1
            ) / NULLIF(
                COUNT(*) FILTER (WHERE eligible_minutes > 0), 0
            ),
            median(furosemide_all_route_total_mg) FILTER (
                WHERE eligible_minutes > 0
                  AND furosemide_quantitative_exposure_flag = 1
                  AND furosemide_all_route_total_mg > 0
            ),
            quantile_cont(furosemide_all_route_total_mg, 0.25) FILTER (
                WHERE eligible_minutes > 0
                  AND furosemide_quantitative_exposure_flag = 1
                  AND furosemide_all_route_total_mg > 0
            ),
            quantile_cont(furosemide_all_route_total_mg, 0.75) FILTER (
                WHERE eligible_minutes > 0
                  AND furosemide_quantitative_exposure_flag = 1
                  AND furosemide_all_route_total_mg > 0
            ),
            'mg'
        FROM diuretic_six
        GROUP BY window_index, window_label
        """
    )


def export(con) -> None:
    for path in (OUT_CONT, OUT_DELTA, OUT_RESP, OUT_AVAIL, OUT_TREATMENT):
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
            ORDER BY variable, window_index
        )
        TO '{qpath(OUT_DELTA)}'
        (FORMAT CSV, HEADER TRUE)
        """
    )

    con.execute(
        f"""
        COPY (
            SELECT * FROM respiratory_distribution
            ORDER BY window_index, respiratory_state
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
            ORDER BY treatment_domain, metric, window_index
        )
        TO '{qpath(OUT_TREATMENT)}'
        (FORMAT CSV, HEADER TRUE)
        """
    )


def print_summary(con) -> None:
    print("=" * 88)
    print("TRAJECTORIES — Study 1 Phase 02_02 candidate complete")
    print("=" * 88)
    print("Population-level usual-care trajectories only (treatment-context v3).")
    print("No imputation. No favorable-recovery definition. No outcomes.")
    print()

    print("Six-hour availability and median")
    print("--------------------------------")
    rows = con.execute(
        """
        SELECT
            variable,
            window_label,
            available_stays_n,
            round(availability_pct, 1) AS availability_pct,
            round(median, 3) AS median,
            round(p25, 3) AS p25,
            round(p75, 3) AS p75
        FROM continuous_summary
        WHERE resolution = 'SIX_HOUR'
        ORDER BY variable, window_index
        """
    ).fetchall()

    for row in rows:
        print("\t".join("<NULL>" if x is None else str(x) for x in row))

    print()
    print("Corrected renal/fluid six-hour summary")
    print("--------------------------------------")
    rows = con.execute(
        """
        SELECT
            variable,
            window_label,
            available_stays_n,
            round(availability_pct, 2) AS availability_pct,
            round(median, 4) AS median,
            round(p25, 4) AS p25,
            round(p75, 4) AS p75
        FROM continuous_summary
        WHERE resolution = 'SIX_HOUR'
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
    print("Six-hour treatment context")
    print("--------------------------")
    rows = con.execute(
        """
        SELECT
            metric,
            window_label,
            eligible_stays_n,
            exposed_stays_n,
            round(exposed_pct, 2) AS exposed_pct,
            round(dose_median_among_exposed, 3) AS dose_median,
            round(dose_p25_among_exposed, 3) AS dose_p25,
            round(dose_p75_among_exposed, 3) AS dose_p75,
            dose_unit
        FROM treatment_context
        ORDER BY treatment_domain, metric, window_index
        """
    ).fetchall()
    for row in rows:
        print("\t".join("<NULL>" if x is None else str(x) for x in row))

    print()
    print("Outputs:")
    for p in (OUT_CONT, OUT_DELTA, OUT_RESP, OUT_AVAIL, OUT_TREATMENT):
        print(f"  {p}")


def main() -> int:
    require_inputs()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA preserve_insertion_order=false")

    con.execute(
        f"CREATE VIEW hourly_raw AS SELECT * FROM read_parquet('{qpath(HOURLY)}')"
    )
    con.execute(
        f"CREATE VIEW six_raw AS SELECT * FROM read_parquet('{qpath(SIX_HOUR)}')"
    )
    con.execute(
        f"CREATE VIEW vaso_six AS SELECT * FROM read_parquet('{qpath(VASO_SIX)}')"
    )
    con.execute(
        f"CREATE VIEW diuretic_six AS SELECT * FROM read_parquet('{qpath(DIURETIC_SIX)}')"
    )

    # Standardize corrected conditional-eligibility/source-documentation fields
    # from the Phase 02_01 master datasets.
    for source_table, final_table in (("hourly_raw", "hourly"), ("six_raw", "six")):
        columns = source_columns(con, source_table)

        if "pf_observed_flag" not in columns:
            raise RuntimeError(
                f"{source_table} is missing pf_observed_flag from Phase 02_01."
            )

        pf_eligible_expr = (
            "CASE WHEN invasive_ventilation_fraction IS NOT NULL "
            "AND invasive_ventilation_fraction > 0 THEN 1 ELSE 0 END"
        )

        bleeding_full_expr = (
            "CASE WHEN start_hour < 12 AND observability_status = 'FULLY_OBSERVABLE' "
            "THEN 1 ELSE 0 END"
        )

        # Prefer explicit source-documentation field if present; otherwise use
        # the frozen 01_15 availability flag carried into Phase 02_01.
        if "bleeding_primary_12h_available_flag" in columns:
            bleeding_source_expr = (
                "CASE WHEN bleeding_primary_12h_available_flag = 1 "
                "THEN 1 ELSE 0 END"
            )
        elif "bleeding_interval_contribution_flag" in columns:
            bleeding_source_expr = (
                "CASE WHEN bleeding_interval_contribution_flag IS NOT NULL "
                "THEN 1 ELSE 0 END"
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

    validate_schema(con)
    validate_dimensions(con)
    build_continuous_summary(con)
    validate_corrected_renal_fluid_summary(con)
    build_paired_delta_summary(con)
    build_respiratory_distribution(con)
    build_availability_summary(con)
    build_treatment_context(con)
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
