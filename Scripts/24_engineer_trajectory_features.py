#!/usr/bin/env python3
"""
02_04_engineer_trajectory_features.py

TRAJECTORIES — Study 1
"Early usual-care and favorable-recovery trajectories after adult cardiac surgery:
a multidomain reference study"

PHASE 02_04 — Patient-level trajectory feature engineering

Purpose
-------
Derive parsimonious patient-level descriptors of postoperative trajectory shape
and treatment/support burden from the frozen Phase 02_01 master datasets.

Principles
----------
- no outcome information;
- no favorable-recovery definition;
- no clustering;
- no imputation;
- preserve variable-specific observability and semantics;
- six-hour summaries are used for physiological shape descriptors;
- hourly data are used for duration/burden features where time resolution matters;
- cumulative variables are not treated like ordinary baseline-to-late variables;
- selective measurements (lactate, pH, Base Excess, P/F) carry explicit
  observation-count/context features and are never silently imputed;
- P/F is interpreted only within invasive-eligible windows;
- chest drainage is restricted to the frozen 0-12 h horizon;
- treatment-context features are exported separately from physiological features.

Inputs
------
data/02_01_study1_hourly.parquet
data/02_01_study1_six_hour.parquet

Outputs
-------
data/02_04_patient_trajectory_features.csv
data/02_04_patient_treatment_features.csv
data/02_04_feature_manifest.csv
data/02_04_feature_audit.csv

Production status
-----------------
Frozen analytical workflow using canonical urine-output and fluid-balance inputs.

Required source and feature semantics
-------------------------------------
Additional required input: data/01_08_diuretic_exposure_hourly_trajectory.parquet.
Furosemide features use this canonical hourly source directly; vasoactive
features use the hourly master. Records are keyed by cohort stay_id and do not
necessarily represent unique patients.
MAP and heart-rate slopes use linear regression against six-hour window
midpoints, requiring at least three observed windows. Selectively measured
variables use observed first-to-last changes only when at least two windows
are observed; these are not imputed baseline-to-24-hour changes.
"""

from __future__ import annotations

import sys
from pathlib import Path

try:
    import duckdb
except ImportError as exc:
    raise SystemExit("Missing dependency 'duckdb'.") from exc

EXPECTED_COHORT_N = 9305

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = (
    SCRIPT_PATH.parent.parent
    if SCRIPT_PATH.parent.name.lower() == "scripts"
    else SCRIPT_PATH.parent
)
DATA_DIR = PROJECT_ROOT / "data"

HOURLY = DATA_DIR / "02_01_study1_hourly.parquet"
SIX = DATA_DIR / "02_01_study1_six_hour.parquet"
DIURETIC_HOURLY = DATA_DIR / "01_08_diuretic_exposure_hourly_trajectory.parquet"

EXPECTED_FURO_QUANT_EXPOSED_N = 6907
EXPECTED_FURO_SIX_HOUR_EXPOSED = {0: 62, 6: 360, 12: 3078, 18: 4786}

OUT_TRAJ = DATA_DIR / "02_04_patient_trajectory_features.csv"
OUT_TREAT = DATA_DIR / "02_04_patient_treatment_features.csv"
OUT_MANIFEST = DATA_DIR / "02_04_feature_manifest.csv"
OUT_AUDIT = DATA_DIR / "02_04_feature_audit.csv"


def qpath(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def require_inputs() -> None:
    missing = [str(p) for p in (HOURLY, SIX, DIURETIC_HOURLY) if not p.is_file()]
    if missing:
        raise FileNotFoundError("Missing required input(s):\n- " + "\n- ".join(missing))


def create_sources(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(f"CREATE VIEW hourly AS SELECT * FROM read_parquet('{qpath(HOURLY)}')")
    con.execute(f"CREATE VIEW six AS SELECT * FROM read_parquet('{qpath(SIX)}')")
    con.execute(f"CREATE VIEW diuretic_hourly AS SELECT * FROM read_parquet('{qpath(DIURETIC_HOURLY)}')")

    for table, expected_rows in (
        ("hourly", EXPECTED_COHORT_N * 24),
        ("six", EXPECTED_COHORT_N * 4),
    ):
        rows, stays = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM {table}"
        ).fetchone()
        if (rows, stays) != (expected_rows, EXPECTED_COHORT_N):
            raise RuntimeError(
                f"{table}: unexpected dimensions rows={rows}, stays={stays}"
            )


def create_trajectory_features(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE trajectory_features AS
        WITH s AS (
            SELECT *, start_hour + 3.0 AS midpoint_hour
            FROM six
        ),
        base AS (
            SELECT
                stay_id,
                any_value(subject_id) AS subject_id,
                any_value(hadm_id) AS hadm_id,
                any_value(surgery_group) AS surgery_group,
                any_value(sex) AS sex,
                any_value(age_at_icu_admission) AS age_at_icu_admission,

                /* MAP: high-observability physiological shape */
                max(map_twa_mmhg) FILTER (WHERE window_index=0) AS map_h00_06,
                max(map_twa_mmhg) FILTER (WHERE window_index=3) AS map_h18_24,
                max(map_twa_mmhg) FILTER (WHERE window_index=3)
                  - max(map_twa_mmhg) FILTER (WHERE window_index=0) AS map_delta_h18_24_minus_h00_06,
                min(map_twa_mmhg) AS map_min_0_24,
                max(map_twa_mmhg) AS map_max_0_24,
                avg(map_twa_mmhg) AS map_mean_0_24,
                CASE WHEN count(map_twa_mmhg) >= 3
                     THEN regr_slope(map_twa_mmhg, midpoint_hour)
                     ELSE NULL END AS map_slope_per_hour,
                count(map_twa_mmhg) AS map_observed_6h_windows_n,

                /* Heart rate */
                max(hr_twa_bpm) FILTER (WHERE window_index=0) AS hr_h00_06,
                max(hr_twa_bpm) FILTER (WHERE window_index=3) AS hr_h18_24,
                max(hr_twa_bpm) FILTER (WHERE window_index=3)
                  - max(hr_twa_bpm) FILTER (WHERE window_index=0) AS hr_delta_h18_24_minus_h00_06,
                min(hr_twa_bpm) AS hr_min_0_24,
                max(hr_twa_bpm) AS hr_max_0_24,
                avg(hr_twa_bpm) AS hr_mean_0_24,
                CASE WHEN count(hr_twa_bpm) >= 3
                     THEN regr_slope(hr_twa_bpm, midpoint_hour)
                     ELSE NULL END AS hr_slope_per_hour,
                count(hr_twa_bpm) AS hr_observed_6h_windows_n,

                /* Selectively measured metabolic / acid-base variables.
                   First/last are observed values, not imputed baseline/24-h values.
                   Change is defined only when >=2 six-hour windows are observed,
                   preventing structural zero deltas when first == last because only
                   one window was measured. Window indices retain the temporal span. */
                arg_min(lactate_median_mmol_l, window_index)
                    FILTER (WHERE lactate_median_mmol_l IS NOT NULL) AS lactate_first_observed,
                arg_max(lactate_median_mmol_l, window_index)
                    FILTER (WHERE lactate_median_mmol_l IS NOT NULL) AS lactate_last_observed,
                CASE WHEN count(lactate_median_mmol_l) >= 2 THEN
                    arg_max(lactate_median_mmol_l, window_index)
                        FILTER (WHERE lactate_median_mmol_l IS NOT NULL)
                  - arg_min(lactate_median_mmol_l, window_index)
                        FILTER (WHERE lactate_median_mmol_l IS NOT NULL)
                ELSE NULL END AS lactate_last_minus_first,
                min(lactate_median_mmol_l) AS lactate_min_observed,
                max(lactate_median_mmol_l) AS lactate_max_observed,
                count(lactate_median_mmol_l) AS lactate_observed_6h_windows_n,
                min(window_index) FILTER (WHERE lactate_median_mmol_l IS NOT NULL)
                    AS lactate_first_observed_window_index,
                max(window_index) FILTER (WHERE lactate_median_mmol_l IS NOT NULL)
                    AS lactate_last_observed_window_index,

                arg_min(ph_median, window_index)
                    FILTER (WHERE ph_median IS NOT NULL) AS ph_first_observed,
                arg_max(ph_median, window_index)
                    FILTER (WHERE ph_median IS NOT NULL) AS ph_last_observed,
                CASE WHEN count(ph_median) >= 2 THEN
                    arg_max(ph_median, window_index) FILTER (WHERE ph_median IS NOT NULL)
                  - arg_min(ph_median, window_index) FILTER (WHERE ph_median IS NOT NULL)
                ELSE NULL END AS ph_last_minus_first,
                min(ph_median) AS ph_min_observed,
                max(ph_median) AS ph_max_observed,
                count(ph_median) AS ph_observed_6h_windows_n,
                min(window_index) FILTER (WHERE ph_median IS NOT NULL)
                    AS ph_first_observed_window_index,
                max(window_index) FILTER (WHERE ph_median IS NOT NULL)
                    AS ph_last_observed_window_index,

                arg_min(base_excess_median_meq_l, window_index)
                    FILTER (WHERE base_excess_median_meq_l IS NOT NULL) AS be_first_observed,
                arg_max(base_excess_median_meq_l, window_index)
                    FILTER (WHERE base_excess_median_meq_l IS NOT NULL) AS be_last_observed,
                CASE WHEN count(base_excess_median_meq_l) >= 2 THEN
                    arg_max(base_excess_median_meq_l, window_index)
                        FILTER (WHERE base_excess_median_meq_l IS NOT NULL)
                  - arg_min(base_excess_median_meq_l, window_index)
                        FILTER (WHERE base_excess_median_meq_l IS NOT NULL)
                ELSE NULL END AS be_last_minus_first,
                min(base_excess_median_meq_l) AS be_min_observed,
                max(base_excess_median_meq_l) AS be_max_observed,
                count(base_excess_median_meq_l) AS be_observed_6h_windows_n,
                min(window_index) FILTER (WHERE base_excess_median_meq_l IS NOT NULL)
                    AS be_first_observed_window_index,
                max(window_index) FILTER (WHERE base_excess_median_meq_l IS NOT NULL)
                    AS be_last_observed_window_index,

                arg_min(glucose_median_mg_dl, window_index)
                    FILTER (WHERE glucose_median_mg_dl IS NOT NULL) AS glucose_first_observed,
                arg_max(glucose_median_mg_dl, window_index)
                    FILTER (WHERE glucose_median_mg_dl IS NOT NULL) AS glucose_last_observed,
                CASE WHEN count(glucose_median_mg_dl) >= 2 THEN
                    arg_max(glucose_median_mg_dl, window_index)
                        FILTER (WHERE glucose_median_mg_dl IS NOT NULL)
                  - arg_min(glucose_median_mg_dl, window_index)
                        FILTER (WHERE glucose_median_mg_dl IS NOT NULL)
                ELSE NULL END AS glucose_last_minus_first,
                min(glucose_median_mg_dl) AS glucose_min_observed,
                max(glucose_median_mg_dl) AS glucose_max_observed,
                count(glucose_median_mg_dl) AS glucose_observed_6h_windows_n,
                min(window_index) FILTER (WHERE glucose_median_mg_dl IS NOT NULL)
                    AS glucose_first_observed_window_index,
                max(window_index) FILTER (WHERE glucose_median_mg_dl IS NOT NULL)
                    AS glucose_last_observed_window_index,

                /* Renal */
                max(urine_output_ml_kg_h) FILTER (WHERE window_index=0) AS urine_output_h00_06,
                max(urine_output_ml_kg_h) FILTER (WHERE window_index=3) AS urine_output_h18_24,
                max(urine_output_ml_kg_h) FILTER (WHERE window_index=3)
                  - max(urine_output_ml_kg_h) FILTER (WHERE window_index=0)
                    AS urine_output_delta_h18_24_minus_h00_06,
                min(urine_output_ml_kg_h) AS urine_output_nadir_0_24,
                avg(urine_output_ml_kg_h) AS urine_output_mean_0_24,
                count(urine_output_ml_kg_h) AS urine_output_observed_6h_windows_n,

                /* Cumulative fluid variables: retain level/peak, not ordinary baseline delta. */
                max(fluid_balance_cumulative_ml) AS fluid_balance_peak_cumulative_ml_0_24,
                arg_max(fluid_balance_cumulative_ml, window_index)
                    FILTER (WHERE fluid_balance_cumulative_ml IS NOT NULL)
                    AS fluid_balance_last_observed_cumulative_ml,
                max(fluid_accumulation_cumulative_pct_weight)
                    AS fluid_accumulation_peak_pct_weight_0_24,
                arg_max(fluid_accumulation_cumulative_pct_weight, window_index)
                    FILTER (WHERE fluid_accumulation_cumulative_pct_weight IS NOT NULL)
                    AS fluid_accumulation_last_observed_pct_weight,
                count(fluid_balance_cumulative_ml) AS fluid_balance_observed_6h_windows_n,

                /* P/F: values only when actually observed; conditioning is retained explicitly.
                   Change requires >=2 observed P/F windows. */
                arg_min(pf_invasive_median, window_index)
                    FILTER (WHERE pf_invasive_median IS NOT NULL) AS pf_first_observed,
                arg_max(pf_invasive_median, window_index)
                    FILTER (WHERE pf_invasive_median IS NOT NULL) AS pf_last_observed,
                CASE WHEN count(pf_invasive_median) >= 2 THEN
                    arg_max(pf_invasive_median, window_index)
                        FILTER (WHERE pf_invasive_median IS NOT NULL)
                  - arg_min(pf_invasive_median, window_index)
                        FILTER (WHERE pf_invasive_median IS NOT NULL)
                ELSE NULL END AS pf_last_minus_first,
                min(pf_invasive_median) AS pf_min_observed,
                count(pf_invasive_median) AS pf_observed_6h_windows_n,
                min(window_index) FILTER (WHERE pf_invasive_median IS NOT NULL)
                    AS pf_first_observed_window_index,
                max(window_index) FILTER (WHERE pf_invasive_median IS NOT NULL)
                    AS pf_last_observed_window_index,
                count(*) FILTER (WHERE invasive_ventilation_fraction > 0)
                    AS pf_invasive_eligible_6h_windows_n,

                /* Bleeding: only source-documented/full-observable windows are included. */
                max(bleeding_recorded_ml) FILTER (
                    WHERE window_index=0
                      AND start_hour < 12
                      AND observability_status='FULLY_OBSERVABLE'
                      AND bleeding_primary_12h_available_flag=1
                ) AS chest_drainage_h00_06_ml,
                max(bleeding_recorded_ml) FILTER (
                    WHERE window_index=1
                      AND start_hour < 12
                      AND observability_status='FULLY_OBSERVABLE'
                      AND bleeding_primary_12h_available_flag=1
                ) AS chest_drainage_h06_12_ml,
                sum(bleeding_recorded_ml) FILTER (
                    WHERE window_index IN (0,1)
                      AND start_hour < 12
                      AND observability_status='FULLY_OBSERVABLE'
                      AND bleeding_primary_12h_available_flag=1
                ) AS chest_drainage_sum_available_0_12_ml,
                min(bleeding_primary_12h_available_flag) FILTER (WHERE window_index IN (0,1))
                    AS chest_drainage_primary_12h_available_flag

            FROM s
            GROUP BY stay_id
        ),
        resp AS (
            SELECT
                stay_id,
                sum(
                    CASE WHEN eligible_minutes > 0
                         THEN coalesce(invasive_ventilation_fraction,0.0)
                              * eligible_minutes / 60.0
                         ELSE 0.0 END
                ) AS invasive_ventilation_equivalent_hours_0_24,
                max(CASE WHEN window_index >= 12 AND invasive_ventilation_fraction > 0
                         THEN 1 ELSE 0 END) AS invasive_ventilation_after_12h_flag,
                max(CASE WHEN window_index >= 18 AND invasive_ventilation_fraction > 0
                         THEN 1 ELSE 0 END) AS invasive_ventilation_after_18h_flag,
                sum(CASE WHEN eligible_minutes>0 THEN eligible_minutes ELSE 0 END)/60.0
                    AS icu_observable_hours_0_24
            FROM hourly
            GROUP BY stay_id
        ),
        bleed_hourly AS (
            SELECT
                stay_id,
                max(bleeding_recorded_ml) FILTER (
                    WHERE window_index < 12
                      AND observability_status='FULLY_OBSERVABLE'
                      AND bleeding_primary_12h_available_flag=1
                ) AS chest_drainage_max_hourly_ml_0_12
            FROM hourly
            GROUP BY stay_id
        )
        SELECT b.*, r.invasive_ventilation_equivalent_hours_0_24,
               r.invasive_ventilation_after_12h_flag,
               r.invasive_ventilation_after_18h_flag,
               r.icu_observable_hours_0_24,
               bh.chest_drainage_max_hourly_ml_0_12
        FROM base b
        LEFT JOIN resp r USING (stay_id)
        LEFT JOIN bleed_hourly bh USING (stay_id)
        ORDER BY stay_id
        """
    )



def create_treatment_features(con: duckdb.DuckDBPyConnection) -> None:
    """Create patient-level support/treatment features.

    Vasoactive/inotrope features come from the Phase 02_01 hourly master.
    Furosemide features intentionally come directly from the frozen 01_08
    canonical hourly source, avoiding the ambiguous generic alias in 02_01.
    """
    # Verify required frozen diuretic columns before deriving anything.
    diur_cols = {r[0] for r in con.execute("DESCRIBE diuretic_hourly").fetchall()}
    required = {
        "stay_id", "window_index", "eligible_minutes",
        "furosemide_quantitative_exposure_flag",
        "furosemide_any_documented_exposure_flag",
        "furosemide_iv_total_mg",
        "furosemide_all_route_total_mg",
        "furosemide_iv_total_24h_mg",
        "furosemide_all_route_total_24h_mg",
        "furosemide_quantitative_exposure_24h_flag",
        "furosemide_any_documented_exposure_24h_flag",
    }
    missing = sorted(required - diur_cols)
    if missing:
        raise RuntimeError(
            "Canonical 01_08 diuretic hourly source missing required columns: "
            + ", ".join(missing)
        )

    con.execute(
        """
        CREATE TABLE vasoactive_patient AS
        SELECT
            stay_id,
            any_value(subject_id) AS subject_id,
            any_value(hadm_id) AS hadm_id,
            sum(CASE WHEN eligible_minutes>0 AND vasoactive_any_exposed_flag=1
                     THEN eligible_minutes/60.0 ELSE 0 END)
                AS vasoactive_any_exposed_hours_0_24,
            sum(CASE WHEN eligible_minutes>0 AND inotrope_exposed_flag=1
                     THEN eligible_minutes/60.0 ELSE 0 END)
                AS inotrope_exposed_hours_0_24,
            sum(CASE WHEN eligible_minutes>0 AND vis_twa IS NOT NULL
                     THEN vis_twa * eligible_minutes/60.0 ELSE 0 END)
                AS vis_burden_score_hours_0_24,
            max(vis_peak) AS vis_peak_0_24,
            sum(CASE WHEN eligible_minutes>0 AND vis_twa > 0
                     THEN eligible_minutes/60.0 ELSE 0 END)
                AS vis_positive_hours_0_24,
            max(CASE WHEN window_index>=18 AND vis_twa>0 THEN 1 ELSE 0 END)
                AS vis_exposure_after_18h_flag,
            sum(CASE WHEN eligible_minutes>0 AND nee_twa IS NOT NULL
                     THEN nee_twa * eligible_minutes/60.0 ELSE 0 END)
                AS nee_burden_equivalent_hours_0_24,
            max(nee_peak) AS nee_peak_0_24,
            sum(CASE WHEN eligible_minutes>0 AND nee_twa > 0
                     THEN eligible_minutes/60.0 ELSE 0 END)
                AS nee_positive_hours_0_24,
            max(CASE WHEN window_index>=18 AND nee_twa>0 THEN 1 ELSE 0 END)
                AS nee_exposure_after_18h_flag,
            sum(CASE WHEN eligible_minutes>0 THEN eligible_minutes ELSE 0 END)/60.0
                AS icu_observable_hours_0_24
        FROM hourly
        GROUP BY stay_id
        """
    )

    con.execute(
        """
        CREATE TABLE furosemide_patient AS
        SELECT
            stay_id,
            min(window_index) FILTER (
                WHERE eligible_minutes>0
                  AND furosemide_quantitative_exposure_flag=1
            ) AS first_furosemide_quantitative_exposure_hour_index,
            count(*) FILTER (
                WHERE eligible_minutes>0
                  AND furosemide_quantitative_exposure_flag=1
            ) AS furosemide_exposed_hourly_windows_n,
            max(furosemide_quantitative_exposure_24h_flag)
                AS furosemide_quantitative_exposure_0_24_flag,
            max(furosemide_any_documented_exposure_24h_flag)
                AS furosemide_any_documented_exposure_0_24_flag,
            max(furosemide_iv_total_24h_mg)
                AS furosemide_iv_total_mg_0_24,
            max(furosemide_all_route_total_24h_mg)
                AS furosemide_all_route_total_mg_0_24,
            sum(CASE WHEN eligible_minutes>0
                     THEN coalesce(furosemide_iv_total_mg,0.0)
                     ELSE 0 END)
                AS furosemide_iv_hourly_sum_mg_0_24,
            sum(CASE WHEN eligible_minutes>0
                     THEN coalesce(furosemide_all_route_total_mg,0.0)
                     ELSE 0 END)
                AS furosemide_all_route_hourly_sum_mg_0_24,
            max(CASE
                WHEN window_index>=18 AND eligible_minutes>0
                 AND furosemide_quantitative_exposure_flag=1
                THEN 1 ELSE 0 END
            ) AS furosemide_exposure_after_18h_flag,
            sum(CASE WHEN eligible_minutes>0 THEN eligible_minutes ELSE 0 END)/60.0
                AS diuretic_observable_hours_0_24
        FROM diuretic_hourly
        GROUP BY stay_id
        """
    )

    con.execute(
        """
        CREATE TABLE treatment_features AS
        SELECT
            v.stay_id, v.subject_id, v.hadm_id,
            v.vasoactive_any_exposed_hours_0_24,
            v.inotrope_exposed_hours_0_24,
            v.vis_burden_score_hours_0_24,
            v.vis_peak_0_24,
            v.vis_positive_hours_0_24,
            v.vis_exposure_after_18h_flag,
            v.nee_burden_equivalent_hours_0_24,
            v.nee_peak_0_24,
            v.nee_positive_hours_0_24,
            v.nee_exposure_after_18h_flag,
            f.first_furosemide_quantitative_exposure_hour_index,
            f.furosemide_exposed_hourly_windows_n,
            f.furosemide_quantitative_exposure_0_24_flag,
            f.furosemide_any_documented_exposure_0_24_flag,
            f.furosemide_iv_total_mg_0_24,
            f.furosemide_all_route_total_mg_0_24,
            f.furosemide_iv_hourly_sum_mg_0_24,
            f.furosemide_all_route_hourly_sum_mg_0_24,
            f.furosemide_exposure_after_18h_flag,
            v.icu_observable_hours_0_24,
            f.diuretic_observable_hours_0_24
        FROM vasoactive_patient v
        LEFT JOIN furosemide_patient f USING (stay_id)
        ORDER BY v.stay_id
        """
    )


def create_manifest(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE feature_manifest(
            output_table VARCHAR,
            domain VARCHAR,
            feature_family VARCHAR,
            feature_pattern VARCHAR,
            interpretation VARCHAR,
            observability_rule VARCHAR,
            modeling_note VARCHAR
        )
        """
    )
    rows = [
        ("trajectory", "Hemodynamic", "level/change/extreme/slope", "map_*", "MAP trajectory shape from 6-h windows", "Observed six-hour MAP only", "Slope requires >=3 observed windows"),
        ("trajectory", "Hemodynamic", "level/change/extreme/slope", "hr_*", "Heart-rate trajectory shape from 6-h windows", "Observed six-hour HR only", "Slope requires >=3 observed windows"),
        ("trajectory", "Metabolic", "first/last/extreme/measurement", "lactate_*", "Observed lactate evolution", "No imputation; selectively measured", "Delta requires >=2 observed windows; observation count/window indices must accompany interpretation"),
        ("trajectory", "Acid-base", "first/last/extreme/measurement", "ph_* / be_*", "Observed acid-base evolution", "No imputation; selectively measured", "Delta requires >=2 observed windows; later measurements are care-selective"),
        ("trajectory", "Metabolic", "first/last/extreme/measurement", "glucose_*", "Observed glucose evolution", "No temporal imputation", "Delta requires >=2 observed windows; supportive feature family"),
        ("trajectory", "Renal", "level/change/nadir", "urine_output_*", "Urine-output trajectory", "Observed window values only", "Coverage remains explicit upstream"),
        ("trajectory", "Fluid", "peak/last cumulative", "fluid_balance_* / fluid_accumulation_*", "Cumulative fluid burden", "Observed cumulative values only", "No ordinary baseline-to-late delta"),
        ("trajectory", "Respiratory", "duration/persistence", "invasive_ventilation_*", "Invasive-support burden", "Fraction x eligible time", "Not equivalent to adjudicated extubation outcome"),
        ("trajectory", "Respiratory", "first/last/nadir/measurement", "pf_*", "Observed invasive P/F context", "Only observed while invasive-eligible", "Delta requires >=2 observed windows; strong selection at later times"),
        ("trajectory", "Bleeding", "early amount/maximum", "chest_drainage_*", "Early 0-12 h drainage burden", "Frozen 0-12 h, documented/full observable", "No extension beyond 12 h"),
        ("treatment", "Circulatory support", "duration/burden/peak/persistence", "vis_* / nee_* / vasoactive_* / inotrope_*", "Treatment/support burden", "Hourly observable windows", "Zeros are valid no-treatment values"),
        ("treatment", "Diuretic context", "timing/exposure/dose", "furosemide_*", "Quantitative furosemide treatment context from frozen 01_08 source", "Canonical 01_08 hourly eligible windows; zero=no treatment; non-observable=missing upstream", "IV dose is primary; all-route dose is companion sensitivity; not physiology"),
    ]
    con.executemany("INSERT INTO feature_manifest VALUES (?,?,?,?,?,?,?)", rows)


def add_audit_value(con, level, feature, metric, value_num=None, value_text=None):
    con.execute(
        "INSERT INTO feature_audit VALUES (?,?,?,?,?)",
        [level, feature, metric, value_num, value_text],
    )


def add_distribution_audit(con, table: str, feature: str) -> None:
    vals = con.execute(
        f"""
        SELECT
            count({feature}),
            count(*) FILTER (WHERE {feature}=0),
            min({feature}),
            quantile_cont({feature},0.10),
            quantile_cont({feature},0.25),
            median({feature}),
            quantile_cont({feature},0.75),
            quantile_cont({feature},0.90),
            quantile_cont({feature},0.99),
            max({feature})
        FROM {table}
        WHERE {feature} IS NOT NULL
        """
    ).fetchone()
    labels = ["nonmissing_n", "zero_n", "min", "p10", "p25", "median", "p75", "p90", "p99", "max"]
    for lab, val in zip(labels, vals):
        add_audit_value(con, "DISTRIBUTION", feature, lab, val)
    if vals[0]:
        add_audit_value(con, "DISTRIBUTION", feature, "zero_pct_nonmissing", 100.0 * vals[1] / vals[0])


def create_audit(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE feature_audit(
            audit_level VARCHAR,
            feature VARCHAR,
            metric VARCHAR,
            value_num DOUBLE,
            value_text VARCHAR
        )
        """
    )

    for table in ("trajectory_features", "treatment_features"):
        rows, stays = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM {table}"
        ).fetchone()
        add_audit_value(con, "STRUCTURE", table, "rows", rows)
        add_audit_value(con, "STRUCTURE", table, "unique_stays_n", stays)

    availability = [
        ("map_h00_06", "map_h00_06 IS NOT NULL"),
        ("map_h18_24", "map_h18_24 IS NOT NULL"),
        ("hr_h00_06", "hr_h00_06 IS NOT NULL"),
        ("hr_h18_24", "hr_h18_24 IS NOT NULL"),
        ("lactate_first_observed", "lactate_first_observed IS NOT NULL"),
        ("ph_first_observed", "ph_first_observed IS NOT NULL"),
        ("be_first_observed", "be_first_observed IS NOT NULL"),
        ("glucose_first_observed", "glucose_first_observed IS NOT NULL"),
        ("urine_output_h00_06", "urine_output_h00_06 IS NOT NULL"),
        ("urine_output_h18_24", "urine_output_h18_24 IS NOT NULL"),
        ("fluid_balance_peak_cumulative_ml_0_24", "fluid_balance_peak_cumulative_ml_0_24 IS NOT NULL"),
        ("pf_first_observed", "pf_first_observed IS NOT NULL"),
        ("chest_drainage_primary_12h_available_flag", "chest_drainage_primary_12h_available_flag=1"),
    ]
    for feature, expr in availability:
        n = con.execute(f"SELECT COUNT(*) FROM trajectory_features WHERE {expr}").fetchone()[0]
        add_audit_value(con, "AVAILABILITY", feature, "stays_n", n)
        add_audit_value(con, "AVAILABILITY", feature, "pct_of_cohort", 100.0*n/EXPECTED_COHORT_N)

    # Measurement-intensity distributions are themselves important features.
    for feature in [
        "lactate_observed_6h_windows_n", "ph_observed_6h_windows_n",
        "be_observed_6h_windows_n", "glucose_observed_6h_windows_n",
        "pf_observed_6h_windows_n", "pf_invasive_eligible_6h_windows_n",
    ]:
        add_distribution_audit(con, "trajectory_features", feature)

    # Delta validity: selectively measured first-to-last changes are defined only
    # when at least two six-hour windows contain an observed value.
    for prefix in ("lactate", "ph", "be", "glucose", "pf"):
        n_delta = con.execute(
            f"SELECT COUNT(*) FROM trajectory_features WHERE {prefix}_last_minus_first IS NOT NULL"
        ).fetchone()[0]
        add_audit_value(con, "DELTA_VALIDITY", prefix, "stays_with_valid_delta_n", n_delta)
        add_audit_value(con, "DELTA_VALIDITY", prefix, "pct_of_cohort", 100.0*n_delta/EXPECTED_COHORT_N)
        span_n = con.execute(
            f"""SELECT COUNT(*) FROM trajectory_features
                 WHERE {prefix}_first_observed_window_index IS NOT NULL
                   AND {prefix}_last_observed_window_index IS NOT NULL
                   AND {prefix}_last_observed_window_index > {prefix}_first_observed_window_index"""
        ).fetchone()[0]
        add_audit_value(con, "DELTA_VALIDITY", prefix, "stays_with_positive_window_span_n", span_n)

    trajectory_distribution_features = [
        "map_h00_06", "map_h18_24", "map_delta_h18_24_minus_h00_06", "map_slope_per_hour",
        "hr_h00_06", "hr_h18_24", "hr_delta_h18_24_minus_h00_06", "hr_slope_per_hour",
        "lactate_first_observed", "lactate_last_observed", "lactate_last_minus_first",
        "ph_first_observed", "ph_last_observed", "ph_last_minus_first",
        "be_first_observed", "be_last_observed", "be_last_minus_first",
        "glucose_first_observed", "glucose_last_observed", "glucose_last_minus_first",
        "urine_output_h00_06", "urine_output_h18_24", "urine_output_delta_h18_24_minus_h00_06", "urine_output_nadir_0_24",
        "fluid_balance_peak_cumulative_ml_0_24", "fluid_balance_last_observed_cumulative_ml",
        "fluid_accumulation_peak_pct_weight_0_24", "fluid_accumulation_last_observed_pct_weight",
        "invasive_ventilation_equivalent_hours_0_24",
        "pf_first_observed", "pf_last_observed", "pf_last_minus_first", "pf_min_observed",
        "chest_drainage_sum_available_0_12_ml", "chest_drainage_max_hourly_ml_0_12",
    ]
    for feature in trajectory_distribution_features:
        add_distribution_audit(con, "trajectory_features", feature)

    treatment_distribution_features = [
        "vasoactive_any_exposed_hours_0_24", "inotrope_exposed_hours_0_24",
        "vis_burden_score_hours_0_24", "vis_peak_0_24", "vis_positive_hours_0_24",
        "nee_burden_equivalent_hours_0_24", "nee_peak_0_24", "nee_positive_hours_0_24",
        "first_furosemide_quantitative_exposure_hour_index",
        "furosemide_exposed_hourly_windows_n",
        "furosemide_iv_total_mg_0_24", "furosemide_all_route_total_mg_0_24",
    ]
    for feature in treatment_distribution_features:
        add_distribution_audit(con, "treatment_features", feature)

    flags = [
        ("trajectory_features", "invasive_ventilation_after_12h_flag"),
        ("trajectory_features", "invasive_ventilation_after_18h_flag"),
        ("treatment_features", "vis_exposure_after_18h_flag"),
        ("treatment_features", "nee_exposure_after_18h_flag"),
        ("treatment_features", "furosemide_quantitative_exposure_0_24_flag"),
        ("treatment_features", "furosemide_any_documented_exposure_0_24_flag"),
        ("treatment_features", "furosemide_exposure_after_18h_flag"),
    ]
    for table, feature in flags:
        n = con.execute(f"SELECT COUNT(*) FROM {table} WHERE {feature}=1").fetchone()[0]
        add_audit_value(con, "PREVALENCE", feature, "positive_stays_n", n)
        add_audit_value(con, "PREVALENCE", feature, "pct_of_cohort", 100.0*n/EXPECTED_COHORT_N)

    # Frozen six-hour furosemide exposure audit from the canonical 01_08 source.
    for start_hour in (0, 6, 12, 18):
        eligible_n, exposed_n = con.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE eligible_minutes>0),
                COUNT(*) FILTER (
                    WHERE eligible_minutes>0
                      AND furosemide_quantitative_exposure_flag=1
                )
            FROM diuretic_hourly
            WHERE window_index>=? AND window_index<?
            """,
            [start_hour, start_hour+6],
        ).fetchone()
        # Patient count, not exposed hourly-window count.
        exposed_patients = con.execute(
            """
            SELECT COUNT(DISTINCT stay_id)
            FROM diuretic_hourly
            WHERE window_index>=? AND window_index<?
              AND eligible_minutes>0
              AND furosemide_quantitative_exposure_flag=1
            """,
            [start_hour, start_hour+6],
        ).fetchone()[0]
        eligible_patients = con.execute(
            """
            SELECT COUNT(DISTINCT stay_id)
            FROM diuretic_hourly
            WHERE window_index>=? AND window_index<? AND eligible_minutes>0
            """,
            [start_hour, start_hour+6],
        ).fetchone()[0]
        label=f"H{start_hour:02d}_H{start_hour+6:02d}"
        add_audit_value(con, "SOURCE_CLOSURE", f"furosemide_{label}", "eligible_stays_n", eligible_patients)
        add_audit_value(con, "SOURCE_CLOSURE", f"furosemide_{label}", "quantitative_exposed_stays_n", exposed_patients)
        add_audit_value(con, "SOURCE_CLOSURE", f"furosemide_{label}", "quantitative_exposed_pct", 100.0*exposed_patients/eligible_patients)


def validate_outputs(con: duckdb.DuckDBPyConnection) -> None:
    for table in ("trajectory_features", "treatment_features"):
        rows, stays = con.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM {table}"
        ).fetchone()
        if (rows, stays) != (EXPECTED_COHORT_N, EXPECTED_COHORT_N):
            raise RuntimeError(f"{table}: expected one row per stay, got {rows}/{stays}")

    bad = con.execute(
        """
        SELECT COUNT(*) FROM trajectory_features
        WHERE invasive_ventilation_equivalent_hours_0_24 < 0
           OR invasive_ventilation_equivalent_hours_0_24 > icu_observable_hours_0_24 + 1e-6
        """
    ).fetchone()[0]
    if bad:
        raise RuntimeError(f"Invalid invasive ventilation burden in {bad} stay(s)")

    bad = con.execute(
        """
        SELECT COUNT(*) FROM treatment_features
        WHERE vasoactive_any_exposed_hours_0_24 < 0
           OR vasoactive_any_exposed_hours_0_24 > icu_observable_hours_0_24 + 1e-6
           OR inotrope_exposed_hours_0_24 < 0
           OR inotrope_exposed_hours_0_24 > icu_observable_hours_0_24 + 1e-6
        """
    ).fetchone()[0]
    if bad:
        raise RuntimeError(f"Invalid treatment exposure duration in {bad} stay(s)")

    # Frozen 01_08 source closure. Fail loudly if the source does not reproduce
    # its canonical patient counts and six-hour exposure pattern.
    exposed_n = con.execute(
        "SELECT COUNT(*) FROM treatment_features WHERE furosemide_quantitative_exposure_0_24_flag=1"
    ).fetchone()[0]
    if exposed_n != EXPECTED_FURO_QUANT_EXPOSED_N:
        raise RuntimeError(
            f"Furosemide source closure failed: quantitative exposed n={exposed_n}, "
            f"expected {EXPECTED_FURO_QUANT_EXPOSED_N}."
        )

    # Internal furosemide-only conservation: the canonical patient-level 24 h
    # totals embedded in 01_08 must equal the sum of the hourly furosemide
    # components. Do NOT compare against the 223,474.25 mg overall diuretic
    # conservation figure, which includes non-furosemide bolus drugs.
    bad_furo_dose = con.execute(
        """
        SELECT COUNT(*)
        FROM treatment_features
        WHERE abs(furosemide_iv_total_mg_0_24 - furosemide_iv_hourly_sum_mg_0_24) > 1e-6
           OR abs(furosemide_all_route_total_mg_0_24 - furosemide_all_route_hourly_sum_mg_0_24) > 1e-6
        """
    ).fetchone()[0]
    if bad_furo_dose:
        raise RuntimeError(
            f"Furosemide hourly-to-24h conservation failed in {bad_furo_dose} stay(s)."
        )

    for start_hour, expected_n in EXPECTED_FURO_SIX_HOUR_EXPOSED.items():
        n = con.execute(
            """
            SELECT COUNT(DISTINCT stay_id)
            FROM diuretic_hourly
            WHERE window_index>=? AND window_index<?
              AND eligible_minutes>0
              AND furosemide_quantitative_exposure_flag=1
            """,
            [start_hour, start_hour+6],
        ).fetchone()[0]
        if n != expected_n:
            raise RuntimeError(
                f"Furosemide six-hour source closure failed H{start_hour:02d}-H{start_hour+6:02d}: "
                f"n={n}, expected {expected_n}."
            )


def validate_corrected_renal_fluid_features(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """
    Verify that patient-level renal/fluid features are exact deterministic
    reductions of the corrected 02_01 six-hour master.
    """
    mismatch_n = con.execute(
        """
        WITH direct AS (
            SELECT
                stay_id,
                max(urine_output_ml_kg_h) FILTER (WHERE window_index=0)
                    AS uo_h00_06,
                max(urine_output_ml_kg_h) FILTER (WHERE window_index=3)
                    AS uo_h18_24,
                min(urine_output_ml_kg_h)
                    AS uo_nadir,
                avg(urine_output_ml_kg_h)
                    AS uo_mean,
                max(fluid_balance_cumulative_ml)
                    AS fb_peak,
                arg_max(fluid_balance_cumulative_ml, window_index)
                    FILTER (WHERE fluid_balance_cumulative_ml IS NOT NULL)
                    AS fb_last,
                max(fluid_accumulation_cumulative_pct_weight)
                    AS fa_peak,
                arg_max(
                    fluid_accumulation_cumulative_pct_weight,
                    window_index
                )
                    FILTER (
                        WHERE fluid_accumulation_cumulative_pct_weight
                              IS NOT NULL
                    )
                    AS fa_last
            FROM six
            GROUP BY stay_id
        )
        SELECT COUNT(*)
        FROM trajectory_features t
        INNER JOIN direct d USING (stay_id)
        WHERE
            (t.urine_output_h00_06 IS NULL) <> (d.uo_h00_06 IS NULL)
         OR (
                t.urine_output_h00_06 IS NOT NULL
            AND abs(t.urine_output_h00_06 - d.uo_h00_06) > 1e-9
         )
         OR (t.urine_output_h18_24 IS NULL) <> (d.uo_h18_24 IS NULL)
         OR (
                t.urine_output_h18_24 IS NOT NULL
            AND abs(t.urine_output_h18_24 - d.uo_h18_24) > 1e-9
         )
         OR (t.urine_output_nadir_0_24 IS NULL) <> (d.uo_nadir IS NULL)
         OR (
                t.urine_output_nadir_0_24 IS NOT NULL
            AND abs(t.urine_output_nadir_0_24 - d.uo_nadir) > 1e-9
         )
         OR (t.urine_output_mean_0_24 IS NULL) <> (d.uo_mean IS NULL)
         OR (
                t.urine_output_mean_0_24 IS NOT NULL
            AND abs(t.urine_output_mean_0_24 - d.uo_mean) > 1e-9
         )
         OR (
                t.fluid_balance_peak_cumulative_ml_0_24 IS NULL
            ) <> (d.fb_peak IS NULL)
         OR (
                t.fluid_balance_peak_cumulative_ml_0_24 IS NOT NULL
            AND abs(
                t.fluid_balance_peak_cumulative_ml_0_24 - d.fb_peak
            ) > 1e-9
         )
         OR (
                t.fluid_balance_last_observed_cumulative_ml IS NULL
            ) <> (d.fb_last IS NULL)
         OR (
                t.fluid_balance_last_observed_cumulative_ml IS NOT NULL
            AND abs(
                t.fluid_balance_last_observed_cumulative_ml - d.fb_last
            ) > 1e-9
         )
         OR (
                t.fluid_accumulation_peak_pct_weight_0_24 IS NULL
            ) <> (d.fa_peak IS NULL)
         OR (
                t.fluid_accumulation_peak_pct_weight_0_24 IS NOT NULL
            AND abs(
                t.fluid_accumulation_peak_pct_weight_0_24 - d.fa_peak
            ) > 1e-9
         )
         OR (
                t.fluid_accumulation_last_observed_pct_weight IS NULL
            ) <> (d.fa_last IS NULL)
         OR (
                t.fluid_accumulation_last_observed_pct_weight IS NOT NULL
            AND abs(
                t.fluid_accumulation_last_observed_pct_weight - d.fa_last
            ) > 1e-9
         )
        """
    ).fetchone()[0]

    if mismatch_n:
        raise RuntimeError(
            f"Corrected renal/fluid feature reduction failed in "
            f"{mismatch_n} stay(s)."
        )


def export(con: duckdb.DuckDBPyConnection) -> None:
    for p in (OUT_TRAJ, OUT_TREAT, OUT_MANIFEST, OUT_AUDIT):
        if p.exists():
            p.unlink()
    con.execute(f"COPY (SELECT * FROM trajectory_features ORDER BY stay_id) TO '{qpath(OUT_TRAJ)}' (FORMAT CSV, HEADER TRUE)")
    con.execute(f"COPY (SELECT * FROM treatment_features ORDER BY stay_id) TO '{qpath(OUT_TREAT)}' (FORMAT CSV, HEADER TRUE)")
    con.execute(f"COPY (SELECT * FROM feature_manifest ORDER BY output_table,domain,feature_family) TO '{qpath(OUT_MANIFEST)}' (FORMAT CSV, HEADER TRUE)")
    con.execute(f"COPY (SELECT * FROM feature_audit ORDER BY audit_level,feature,metric) TO '{qpath(OUT_AUDIT)}' (FORMAT CSV, HEADER TRUE)")


def fmt(x, digits=2):
    if x is None:
        return "NA"
    return f"{x:.{digits}f}"


def print_dist(con, table, feature, digits=2):
    r = con.execute(
        f"""
        SELECT count({feature}), quantile_cont({feature},0.25), median({feature}),
               quantile_cont({feature},0.75), quantile_cont({feature},0.90),
               quantile_cont({feature},0.99), min({feature}), max({feature})
        FROM {table} WHERE {feature} IS NOT NULL
        """
    ).fetchone()
    print(f"{feature}: n={r[0]}  median={fmt(r[2],digits)} [{fmt(r[1],digits)}-{fmt(r[3],digits)}]  "
          f"p90={fmt(r[4],digits)}  p99={fmt(r[5],digits)}  range={fmt(r[6],digits)}-{fmt(r[7],digits)}")


def print_summary(con: duckdb.DuckDBPyConnection) -> None:
    print("=" * 108)
    print("TRAJECTORIES — Study 1 Phase 02_04 feature engineering candidate complete")
    print("=" * 108)
    print("No outcomes. No favorable-recovery definition. No clustering. No imputation.")
    print("Physiological trajectory features and treatment/support burden remain separated.")
    print("Furosemide features read directly from frozen canonical 01_08 hourly source.")
    print()

    print("STRUCTURE")
    print("---------")
    print("Trajectory feature rows:", con.execute("SELECT COUNT(*) FROM trajectory_features").fetchone()[0])
    print("Treatment feature rows :", con.execute("SELECT COUNT(*) FROM treatment_features").fetchone()[0])
    print()

    print("KEY PHYSIOLOGICAL TRAJECTORY FEATURES")
    print("-------------------------------------")
    for f,d in [
        ("map_h00_06",2),("map_h18_24",2),("map_delta_h18_24_minus_h00_06",2),("map_slope_per_hour",3),
        ("hr_h00_06",1),("hr_h18_24",1),("hr_delta_h18_24_minus_h00_06",1),
        ("lactate_first_observed",2),("lactate_last_observed",2),("lactate_last_minus_first",2),
        ("ph_first_observed",3),("ph_last_observed",3),("ph_last_minus_first",3),
        ("be_first_observed",1),("be_last_observed",1),("be_last_minus_first",1),
        ("glucose_first_observed",1),("glucose_last_observed",1),("glucose_last_minus_first",1),
        ("urine_output_h00_06",2),("urine_output_h18_24",2),("urine_output_nadir_0_24",2),
        ("urine_output_mean_0_24",2),
        ("fluid_balance_peak_cumulative_ml_0_24",1),
        ("fluid_balance_last_observed_cumulative_ml",1),
        ("fluid_accumulation_peak_pct_weight_0_24",2),
        ("fluid_accumulation_last_observed_pct_weight",2),
        ("invasive_ventilation_equivalent_hours_0_24",2),
        ("pf_first_observed",1),("pf_last_observed",1),("pf_last_minus_first",1),("pf_min_observed",1),
        ("chest_drainage_sum_available_0_12_ml",1),("chest_drainage_max_hourly_ml_0_12",1),
    ]:
        print_dist(con,"trajectory_features",f,d)
    print()

    print("MEASUREMENT / OBSERVABILITY CONTEXT")
    print("-----------------------------------")
    for f in ["lactate_observed_6h_windows_n","ph_observed_6h_windows_n","be_observed_6h_windows_n","glucose_observed_6h_windows_n","pf_observed_6h_windows_n","pf_invasive_eligible_6h_windows_n"]:
        r=con.execute(f"SELECT median({f}), quantile_cont({f},0.25), quantile_cont({f},0.75), max({f}) FROM trajectory_features").fetchone()
        print(f"{f}: median={fmt(r[0],1)} [{fmt(r[1],1)}-{fmt(r[2],1)}], max={fmt(r[3],0)}")
    print()

    print("SELECTIVELY MEASURED CHANGE — VALID DELTAS ONLY (>=2 OBSERVED 6-h WINDOWS)")
    print("--------------------------------------------------------------------------")
    for prefix in ["lactate","ph","be","glucose","pf"]:
        r=con.execute(
            f"""SELECT COUNT({prefix}_last_minus_first),
                       median({prefix}_last_observed_window_index-{prefix}_first_observed_window_index),
                       quantile_cont({prefix}_last_observed_window_index-{prefix}_first_observed_window_index,0.25),
                       quantile_cont({prefix}_last_observed_window_index-{prefix}_first_observed_window_index,0.75)
                FROM trajectory_features
                WHERE {prefix}_last_minus_first IS NOT NULL"""
        ).fetchone()
        print(f"{prefix}: valid delta n={r[0]}/{EXPECTED_COHORT_N} ({100*r[0]/EXPECTED_COHORT_N:.2f}%); "
              f"temporal span median={fmt(6*r[1],1)} h [{fmt(6*r[2],1)}-{fmt(6*r[3],1)}]")
    print()

    print("RESPIRATORY PERSISTENCE")
    print("-----------------------")
    for f in ["invasive_ventilation_after_12h_flag","invasive_ventilation_after_18h_flag"]:
        n=con.execute(f"SELECT COUNT(*) FROM trajectory_features WHERE {f}=1").fetchone()[0]
        print(f"{f}: {n}/{EXPECTED_COHORT_N} ({100*n/EXPECTED_COHORT_N:.2f}%)")
    print()

    print("VASOACTIVE / INOTROPE SUPPORT")
    print("-----------------------------")
    for f,d in [
        ("vasoactive_any_exposed_hours_0_24",1),("inotrope_exposed_hours_0_24",1),
        ("vis_burden_score_hours_0_24",2),("vis_peak_0_24",2),
        ("nee_burden_equivalent_hours_0_24",3),("nee_peak_0_24",3),
    ]:
        print_dist(con,"treatment_features",f,d)
    for f in ["vis_exposure_after_18h_flag","nee_exposure_after_18h_flag"]:
        n=con.execute(f"SELECT COUNT(*) FROM treatment_features WHERE {f}=1").fetchone()[0]
        print(f"{f}: {n}/{EXPECTED_COHORT_N} ({100*n/EXPECTED_COHORT_N:.2f}%)")
    print()

    print("FUROSEMIDE TREATMENT CONTEXT — CANONICAL 01_08 SOURCE")
    print("--------------------------------------------------")
    qn=con.execute("SELECT COUNT(*) FROM treatment_features WHERE furosemide_quantitative_exposure_0_24_flag=1").fetchone()[0]
    dn=con.execute("SELECT COUNT(*) FROM treatment_features WHERE furosemide_any_documented_exposure_0_24_flag=1").fetchone()[0]
    print(f"Quantitative exposure 0-24 h: {qn}/{EXPECTED_COHORT_N} ({100*qn/EXPECTED_COHORT_N:.2f}%)")
    print(f"Any documented exposure 0-24 h: {dn}/{EXPECTED_COHORT_N} ({100*dn/EXPECTED_COHORT_N:.2f}%)")
    print_dist(con,"treatment_features","furosemide_iv_total_mg_0_24",1)
    print_dist(con,"treatment_features","furosemide_all_route_total_mg_0_24",1)
    print("Six-hour quantitative exposure:")
    for start_hour in (0,6,12,18):
        eligible_n=con.execute("SELECT COUNT(DISTINCT stay_id) FROM diuretic_hourly WHERE window_index>=? AND window_index<? AND eligible_minutes>0",[start_hour,start_hour+6]).fetchone()[0]
        exposed_n=con.execute("SELECT COUNT(DISTINCT stay_id) FROM diuretic_hourly WHERE window_index>=? AND window_index<? AND eligible_minutes>0 AND furosemide_quantitative_exposure_flag=1",[start_hour,start_hour+6]).fetchone()[0]
        print(f"  H{start_hour:02d}-H{start_hour+6:02d}: {exposed_n}/{eligible_n} ({100*exposed_n/eligible_n:.2f}%)")
    late=con.execute("SELECT COUNT(*) FROM treatment_features WHERE furosemide_exposure_after_18h_flag=1").fetchone()[0]
    print(f"Exposure after 18 h: {late}/{EXPECTED_COHORT_N} ({100*late/EXPECTED_COHORT_N:.2f}%)")
    print()

    print("QC / SOURCE CLOSURE")
    print("-------------------")
    print(f"PASS  one row per stay: {EXPECTED_COHORT_N} trajectory + {EXPECTED_COHORT_N} treatment")
    print(f"PASS  furosemide quantitative exposed patients: {qn} (expected {EXPECTED_FURO_QUANT_EXPOSED_N})")
    total=con.execute("SELECT SUM(furosemide_all_route_total_mg_0_24) FROM treatment_features").fetchone()[0]
    print(f"PASS  furosemide-only all-route dose conserved hourly -> canonical 24 h: {total:.3f} mg")
    print("PASS  frozen six-hour furosemide exposure counts reproduced")
    print("NOTE  223,474.25 mg is the frozen overall diuretic conservation total (all bolus diuretics + furosemide infusion), not furosemide-only dose.")
    print()

    # Extreme tails were already source-audited in frozen canonical component 01_18.
    # Primary VIS/NEE remain uncapped; p99.9-capped versions exist only as sensitivity.
    vismax=con.execute("SELECT max(vis_peak_0_24) FROM treatment_features").fetchone()[0]
    neemax=con.execute("SELECT max(nee_peak_0_24) FROM treatment_features").fetchone()[0]
    print("VIS / NEE EXTREME-TAIL QC — FROZEN 01_18 SOURCE AUDIT")
    print("----------------------------------------------------")
    print(f"Observed Study-1 VIS peak max: {vismax:.3f}")
    print(f"Observed Study-1 NEE peak max: {neemax:.3f}")
    print("PASS  canonical 01_18 already audited true dose values above drug-specific p99.9.")
    print("PASS  67 stays / 109 hourly windows were affected by true above-p99.9 source doses in 01_18.")
    print("PASS  primary 01_18 trajectories are intentionally uncapped/unwinsorized; p99.9-capped fields are sensitivity only.")
    print("NOTE  documented source extremes include phenylephrine 833.333 mcg/kg/min (1 min),")
    print("      norepinephrine 5.72258 mcg/kg/min (10 min), dopamine 92.3077 mcg/kg/min (10 min),")
    print("      and vasopressin 83.8713 units/hour (62 min). No automatic deletion is applied here.")
    print()
    print("QC WARNINGS")
    print("-----------")
    print("None requiring Phase 02_04 source re-adjudication. Extreme VIS/NEE tails remain flagged for sensitivity-aware modeling.")
    print()

    print("Candidate outputs:")
    for p in (OUT_TRAJ, OUT_TREAT, OUT_MANIFEST, OUT_AUDIT):
        print(" ", p)
    print()
    print("Phase 02_04 candidate outputs generated.")


def main() -> int:
    require_inputs()
    con = duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA preserve_insertion_order=false")
    create_sources(con)
    create_trajectory_features(con)
    create_treatment_features(con)
    create_manifest(con)
    create_audit(con)
    validate_outputs(con)
    validate_corrected_renal_fluid_features(con)
    export(con)
    print_summary(con)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
