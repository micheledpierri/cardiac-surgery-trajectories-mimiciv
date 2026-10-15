#!/usr/bin/env python3
"""
02_01_build_study1_dataset.py

TRAJECTORIES — Study 1
Early usual-care and favorable-recovery trajectories after adult cardiac surgery:
a multidomain reference study

PHASE 1 — Analytical dataset assembly

Reads frozen canonical trajectory outputs and creates common hourly and six-hour
patient-window datasets. It performs no imputation, no favorable-recovery
classification, and uses no outcomes.

Assembly and observation unit
-----------------------------
Records represent cohort ICU stays and windows, not necessarily unique patients.
The grid contains 24 one-hour or four six-hour windows from postoperative time
zero. Eligible time is clipped at observation_end_time; windows remain in the
grid after observation ends with zero eligible minutes. Bleeding contributes
only within its frozen 0-12-hour horizon.
Components are left-joined by stay_id and window_index. Optional source aliases
are selected when available and otherwise represented as NULL; required
renal/fluid fields are checked separately. No values are imputed.

Inputs
------
data/00_04_final_analytic_cohort.csv
data/01_00_map_hourly_trajectory.parquet
data/01_00_map_six_hour_summary.parquet
data/01_01_heart_rate_hourly_trajectory.parquet
data/01_01_heart_rate_six_hour_summary.parquet
data/01_02_lactate_hourly_trajectory.parquet
data/01_02_lactate_six_hour_summary.parquet
data/01_03_ph_hourly_trajectory.parquet
data/01_03_ph_six_hour_summary.parquet
data/01_04_glucose_hourly_trajectory.parquet
data/01_04_glucose_six_hour_summary.parquet
data/01_06_base_excess_hourly_trajectory.parquet
data/01_06_base_excess_six_hour_summary.parquet
data/01_07_urine_output_hourly_trajectory.parquet
data/01_07_urine_output_six_hour_summary.parquet
data/01_08_diuretic_exposure_hourly_trajectory.parquet
data/01_08_diuretic_exposure_six_hour_summary.parquet
data/01_10_fluid_balance_hourly_trajectory.parquet
data/01_10_fluid_balance_six_hour_summary.parquet
data/01_11_respiratory_support_hourly.parquet
data/01_11_respiratory_support_six_hour.parquet
data/01_13_pf_hourly.parquet
data/01_13_pf_six_hour.parquet
data/01_15_bleeding_hourly_trajectory.parquet
data/01_15_bleeding_six_hour_summary.parquet
data/01_18_vasoactive_inotrope_hourly_trajectory.parquet
data/01_18_vasoactive_inotrope_six_hour_trajectory.parquet

Outputs
-------
data/02_01_study1_hourly.parquet
data/02_01_study1_six_hour.parquet
data/02_01_study1_variable_manifest.csv
data/02_01_study1_observability_qc.csv
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
PROJECT_ROOT = SCRIPT_PATH.parent.parent if SCRIPT_PATH.parent.name.lower() == "scripts" else SCRIPT_PATH.parent
DATA_DIR = PROJECT_ROOT / "data"

COHORT = DATA_DIR / "00_04_final_analytic_cohort.csv"
FILES = {
    "map_h": "01_00_map_hourly_trajectory.parquet", "map_6": "01_00_map_six_hour_summary.parquet",
    "hr_h": "01_01_heart_rate_hourly_trajectory.parquet", "hr_6": "01_01_heart_rate_six_hour_summary.parquet",
    "lactate_h": "01_02_lactate_hourly_trajectory.parquet", "lactate_6": "01_02_lactate_six_hour_summary.parquet",
    "ph_h": "01_03_ph_hourly_trajectory.parquet", "ph_6": "01_03_ph_six_hour_summary.parquet",
    "glucose_h": "01_04_glucose_hourly_trajectory.parquet", "glucose_6": "01_04_glucose_six_hour_summary.parquet",
    "be_h": "01_06_base_excess_hourly_trajectory.parquet", "be_6": "01_06_base_excess_six_hour_summary.parquet",
    "uo_h": "01_07_urine_output_hourly_trajectory.parquet", "uo_6": "01_07_urine_output_six_hour_summary.parquet",
    "diuretic_h": "01_08_diuretic_exposure_hourly_trajectory.parquet", "diuretic_6": "01_08_diuretic_exposure_six_hour_summary.parquet",
    "fb_h": "01_10_fluid_balance_hourly_trajectory.parquet", "fb_6": "01_10_fluid_balance_six_hour_summary.parquet",
    "resp_h": "01_11_respiratory_support_hourly.parquet", "resp_6": "01_11_respiratory_support_six_hour.parquet",
    "pf_h": "01_13_pf_hourly.parquet", "pf_6": "01_13_pf_six_hour.parquet",
    "bleeding_h": "01_15_bleeding_hourly_trajectory.parquet", "bleeding_6": "01_15_bleeding_six_hour_summary.parquet",
    "vaso_h": "01_18_vasoactive_inotrope_hourly_trajectory.parquet", "vaso_6": "01_18_vasoactive_inotrope_six_hour_trajectory.parquet",
}
FILES = {k: DATA_DIR / v for k, v in FILES.items()}

OUT_H = DATA_DIR / "02_01_study1_hourly.parquet"
OUT_6 = DATA_DIR / "02_01_study1_six_hour.parquet"
OUT_MANIFEST = DATA_DIR / "02_01_study1_variable_manifest.csv"
OUT_QC = DATA_DIR / "02_01_study1_observability_qc.csv"

def qp(p: Path) -> str:
    return str(p.resolve()).replace("\\", "/").replace("'", "''")

def require_files():
    req = [COHORT, *FILES.values()]
    missing = [str(p) for p in req if not p.is_file()]
    if missing:
        raise FileNotFoundError("Missing canonical file(s):\n- " + "\n- ".join(missing))

def columns(con, table):
    return {r[0].lower() for r in con.execute(f"DESCRIBE {table}").fetchall()}

def opt(con, table, alias, candidates, out):
    available = columns(con, table)
    for c in candidates:
        if c.lower() in available:
            return f"{alias}.{c.lower()} AS {out}"
    return f"NULL AS {out}"

def create_sources(con):
    con.execute(f"""
        CREATE TABLE cohort AS
        SELECT subject_id::BIGINT subject_id, hadm_id::BIGINT hadm_id,
               stay_id::BIGINT stay_id, surgery_group, sex,
               age_at_icu_admission::DOUBLE age_at_icu_admission,
               postoperative_time_zero::TIMESTAMP postoperative_time_zero,
               observation_end_time::TIMESTAMP observation_end_time,
               observation_end_type
        FROM read_csv_auto('{qp(COHORT)}', header=true)
    """)
    chk = con.execute("SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM cohort").fetchone()
    if chk != (EXPECTED_COHORT_N, EXPECTED_COHORT_N):
        raise RuntimeError(f"Unexpected cohort dimensions: {chk}")
    for name, p in FILES.items():
        con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{qp(p)}')")

def validate_corrected_source_schema(con):
    """Fail closed if the corrected canonical renal/fluid fields are not present."""
    required = {
        "uo_h": {
            "uo_rate_ml_per_kg_per_hour_observed",
            "uo_interval_coverage_fraction",
            "uo_primary_available_flag",
        },
        "uo_6": {
            "uo_rate_ml_per_kg_per_hour_observed",
            "uo_interval_coverage_fraction",
            "uo_primary_available_flag",
        },
        "fb_h": {
            "fb_net_balance_ml",
            "fb_cumulative_net_balance_ml",
            "fb_cumulative_fluid_accumulation_pct_weight",
            "fb_window_available_flag",
        },
        "fb_6": {
            "fb_net_balance_ml",
            "fb_cumulative_net_balance_ml",
            "fb_cumulative_fluid_accumulation_pct_weight",
            "fb_window_available_flag",
        },
    }
    for table, needed in required.items():
        available = columns(con, table)
        missing = sorted(needed - available)
        if missing:
            raise RuntimeError(
                f"{table}: missing corrected canonical field(s): {missing}"
            )


def validate_grids(con):
    for name in [k for k in FILES if k.endswith("_h") and k != "bleeding_h"]:
        n, s = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM {name}").fetchone()
        if (n, s) != (EXPECTED_COHORT_N*24, EXPECTED_COHORT_N):
            raise RuntimeError(f"{name}: unexpected dimensions {n}/{s}")
    for name in [k for k in FILES if k.endswith("_6") and k != "bleeding_6"]:
        n, s = con.execute(f"SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM {name}").fetchone()
        if (n, s) != (EXPECTED_COHORT_N*4, EXPECTED_COHORT_N):
            raise RuntimeError(f"{name}: unexpected dimensions {n}/{s}")
    if con.execute("SELECT COUNT(*) FROM bleeding_h").fetchone()[0] != EXPECTED_COHORT_N*12:
        raise RuntimeError("bleeding_h dimensions changed")
    if con.execute("SELECT COUNT(*) FROM bleeding_6").fetchone()[0] != EXPECTED_COHORT_N*2:
        raise RuntimeError("bleeding_6 dimensions changed")

def create_grid(con, six=False):
    name = "grid_6" if six else "grid_h"
    windows = """
        SELECT * FROM (VALUES
        (0,'H00_H06',0.0,6.0),(1,'H06_H12',6.0,12.0),
        (2,'H12_H18',12.0,18.0),(3,'H18_H24',18.0,24.0))
        t(window_index,window_label,start_hour,end_hour)
    """ if six else """
        SELECT i::INTEGER window_index,
               printf('H%02d_H%02d',i,i+1) window_label,
               i::DOUBLE start_hour,(i+1)::DOUBLE end_hour
        FROM range(24) r(i)
    """
    con.execute(f"""
        CREATE TABLE {name} AS
        WITH w AS ({windows})
        SELECT c.*, w.window_index,w.window_label,w.start_hour,w.end_hour,
               c.postoperative_time_zero + w.start_hour*INTERVAL '1 hour' window_start_time,
               least(c.observation_end_time,c.postoperative_time_zero+w.end_hour*INTERVAL '1 hour') eligible_window_end_time,
               greatest(0.0,date_diff('second',
                   c.postoperative_time_zero+w.start_hour*INTERVAL '1 hour',
                   least(c.observation_end_time,c.postoperative_time_zero+w.end_hour*INTERVAL '1 hour'))/60.0) eligible_minutes,
               CASE
                 WHEN c.observation_end_time <= c.postoperative_time_zero+w.start_hour*INTERVAL '1 hour' THEN 'NOT_OBSERVABLE'
                 WHEN c.observation_end_time < c.postoperative_time_zero+w.end_hour*INTERVAL '1 hour' THEN 'PARTIALLY_OBSERVABLE'
                 ELSE 'FULLY_OBSERVABLE'
               END observability_status
        FROM cohort c CROSS JOIN w
    """)

def build_master(con, six=False):
    suf = "6" if six else "h"
    grid = "grid_6" if six else "grid_h"
    out = "study1_six" if six else "study1_hourly"
    T = {a: f"{b}_{suf}" for a,b in {
        "m":"map","hr":"hr","lac":"lactate","ph":"ph","glu":"glucose","be":"be",
        "uo":"uo","diu":"diuretic","fb":"fb","rs":"resp","pf":"pf","bl":"bleeding","va":"vaso"
    }.items()}
    f = [
        opt(con,T["m"],"m",("map_time_weighted_mean",),"map_twa_mmhg"),
        opt(con,T["m"],"m",("map_locf_supported_pct_of_eligible",),"map_supported_pct"),
        opt(con,T["hr"],"hr",("hr_time_weighted_mean",),"hr_twa_bpm"),
        opt(con,T["hr"],"hr",("hr_locf_supported_pct_of_eligible",),"hr_supported_pct"),
        opt(con,T["lac"],"lac",("lactate_median_observed",),"lactate_median_mmol_l"),
        opt(con,T["lac"],"lac",("lactate_points_n",),"lactate_points_n"),
        opt(con,T["ph"],"ph",("ph_median_observed","ph_arterial_median_observed"),"ph_median"),
        opt(con,T["ph"],"ph",("ph_points_n","ph_arterial_points_n"),"ph_points_n"),
        opt(con,T["be"],"be",("be_median_observed",),"base_excess_median_meq_l"),
        opt(con,T["be"],"be",("be_points_n",),"base_excess_points_n"),
        opt(con,T["glu"],"glu",("glucose_median_observed",),"glucose_median_mg_dl"),
        opt(con,T["glu"],"glu",("glucose_points_n",),"glucose_points_n"),
        opt(con,T["uo"],"uo",("uo_rate_ml_per_kg_per_hour_observed",),"urine_output_ml_kg_h"),
        opt(con,T["uo"],"uo",("uo_interval_coverage_fraction",),"urine_output_coverage_fraction"),
        opt(con,T["uo"],"uo",("uo_primary_available_flag",),"urine_output_available_flag"),
        opt(con,T["fb"],"fb",("fb_net_balance_ml",),"fluid_balance_window_ml"),
        opt(con,T["fb"],"fb",("fb_cumulative_net_balance_ml",),"fluid_balance_cumulative_ml"),
        opt(con,T["fb"],"fb",("fb_cumulative_fluid_accumulation_pct_weight",),"fluid_accumulation_cumulative_pct_weight"),
        opt(con,T["fb"],"fb",("fb_window_available_flag",),"fluid_balance_available_flag"),
        opt(con,T["rs"],"rs",("dominant_respiratory_state",),"respiratory_state"),
        opt(con,T["rs"],"rs",("invasive_ventilation_fraction",),"invasive_ventilation_fraction"),
        opt(con,T["rs"],"rs",("respiratory_coverage_fraction",),"respiratory_coverage_fraction"),
        opt(con,T["pf"],"pf",("invasive_pf_median","invasive_pf_median_observed","pf_median_invasive"),"pf_invasive_median"),
        opt(con,T["pf"],"pf",("invasive_pf_events_n",),"pf_invasive_events_n"),
        opt(con,T["pf"],"pf",("invasive_pf_observed_flag",),"pf_observed_flag"),
        opt(con,T["va"],"va",("vis_twa",),"vis_twa"),
        opt(con,T["va"],"va",("vis_peak",),"vis_peak"),
        opt(con,T["va"],"va",("nee_twa",),"nee_twa"),
        opt(con,T["va"],"va",("nee_peak",),"nee_peak"),
        opt(con,T["va"],"va",("any_candidate_exposed_flag",),"vasoactive_any_exposed_flag"),
        opt(con,T["va"],"va",("inotrope_inodilator_exposed_flag",),"inotrope_exposed_flag"),
        opt(con,T["bl"],"bl",("bleeding_recorded_ml",),"bleeding_recorded_ml"),
        opt(con,T["bl"],"bl",("bleeding_recorded_ml_per_kg",),"bleeding_recorded_ml_per_kg"),
        opt(con,T["bl"],"bl",("bleeding_primary_12h_available_flag",),"bleeding_primary_12h_available_flag"),
        opt(con,T["diu"],"diu",("furosemide_integrated_mg","furosemide_best_available_mg","furosemide_total_mg"),"furosemide_context_mg"),
        opt(con,T["diu"],"diu",("any_diuretic_exposure_flag","any_diuretic_exposed_flag","diuretic_any_exposed_flag"),"any_diuretic_context_flag"),
    ]
    joins = " ".join(
        f"LEFT JOIN {table} {alias} ON g.stay_id={alias}.stay_id AND g.window_index={alias}.window_index"
        for alias, table in T.items()
    )
    con.execute(f"CREATE TABLE {out} AS SELECT g.*, {','.join(f)} FROM {grid} g {joins} ORDER BY g.stay_id,g.window_index")
    expected = EXPECTED_COHORT_N*(4 if six else 24)
    n,s = con.execute(f"SELECT COUNT(*),COUNT(DISTINCT stay_id) FROM {out}").fetchone()
    if (n,s)!=(expected,EXPECTED_COHORT_N):
        raise RuntimeError(f"{out}: unexpected dimensions {n}/{s}")

def validate_renal_fluid_passthrough(con):
    """
    02_01 is an assembly step only. Renal/fluid values must pass through
    unchanged from corrected canonical 01_07 / 01_10 sources.
    """
    checks = [
        (
            "hourly urine",
            "study1_hourly", "uo_h",
            "urine_output_ml_kg_h", "uo_rate_ml_per_kg_per_hour_observed",
        ),
        (
            "six-hour urine",
            "study1_six", "uo_6",
            "urine_output_ml_kg_h", "uo_rate_ml_per_kg_per_hour_observed",
        ),
        (
            "hourly fluid balance",
            "study1_hourly", "fb_h",
            "fluid_balance_cumulative_ml", "fb_cumulative_net_balance_ml",
        ),
        (
            "six-hour fluid balance",
            "study1_six", "fb_6",
            "fluid_balance_cumulative_ml", "fb_cumulative_net_balance_ml",
        ),
        (
            "hourly fluid accumulation",
            "study1_hourly", "fb_h",
            "fluid_accumulation_cumulative_pct_weight",
            "fb_cumulative_fluid_accumulation_pct_weight",
        ),
        (
            "six-hour fluid accumulation",
            "study1_six", "fb_6",
            "fluid_accumulation_cumulative_pct_weight",
            "fb_cumulative_fluid_accumulation_pct_weight",
        ),
    ]

    for label, master, source, master_col, source_col in checks:
        mismatch_n = con.execute(f"""
            SELECT COUNT(*)
            FROM {master} m
            INNER JOIN {source} s
                ON m.stay_id = s.stay_id
               AND m.window_index = s.window_index
            WHERE
                (m.{master_col} IS NULL) <> (s.{source_col} IS NULL)
                OR (
                    m.{master_col} IS NOT NULL
                    AND s.{source_col} IS NOT NULL
                    AND abs(m.{master_col} - s.{source_col}) > 1e-9
                )
        """).fetchone()[0]
        if mismatch_n:
            raise RuntimeError(
                f"{label}: {mismatch_n} renal/fluid passthrough mismatches."
            )


def create_manifest(con):
    rows = [
      ("MAP","Hemodynamic","CORE","map_twa_mmhg","90-min LOCF time-weighted MAP","0-24 h","continuous"),
      ("Heart rate","Hemodynamic","CORE","hr_twa_bpm","90-min LOCF time-weighted HR","0-24 h","continuous"),
      ("Lactate","Metabolic","CORE_CONDITIONAL","lactate_median_mmol_l","observed median; no imputation","0-24 h","measurement selective"),
      ("pH","Acid-base","CORE","ph_median","observed arterial median","0-24 h","no temporal imputation"),
      ("Base excess","Acid-base","SUPPORTIVE","base_excess_median_meq_l","observed arterial median","0-24 h","supportive to pH"),
      ("Glucose","Metabolic","SUPPORTIVE","glucose_median_mg_dl","integrated LAB/POC median","0-24 h","no temporal imputation"),
      ("Urine output","Renal","CORE","urine_output_ml_kg_h","interval-derived observed rate","0-24 h","coverage explicit"),
      ("Fluid balance","Fluid","CORE","fluid_balance_cumulative_ml","ICU-recorded cumulative balance","0-24 h","not total perioperative balance"),
      ("Respiratory support","Respiratory","CORE","respiratory_state","dominant reconstructed support state","0-24 h","6-h persistence cap"),
      ("P/F ratio","Respiratory","CORE_CONDITIONAL","pf_invasive_median","observed invasive P/F median","0-24 h","invasive-eligible only"),
      ("VIS","Circulatory support","CORE_TREATMENT","vis_twa","standard VIS TWA","0-24 h","treatment/support"),
      ("NEE","Circulatory support","SUPPORTIVE_TREATMENT","nee_twa","NEE TWA","0-24 h","supportive treatment metric"),
      ("Chest drainage","Bleeding","CORE","bleeding_recorded_ml","interval-allocated drainage","0-12 h","frozen early horizon"),
      ("Diuretic exposure","Treatment context","CONTEXT","furosemide_context_mg","best-available administration context","0-24 h","not physiology"),
    ]
    con.execute("CREATE TABLE manifest(variable VARCHAR,domain VARCHAR,study1_role VARCHAR,master_column VARCHAR,representation VARCHAR,horizon VARCHAR,note VARCHAR)")
    con.executemany("INSERT INTO manifest VALUES (?,?,?,?,?,?,?)",rows)

def create_qc(con):
    """Create observability QC with variable-appropriate denominators."""
    con.execute("""
        CREATE TABLE observability_qc(
            resolution VARCHAR,
            window_index INTEGER,
            window_label VARCHAR,
            variable VARCHAR,
            denominator_type VARCHAR,
            denominator_stays_n BIGINT,
            available_stays_n BIGINT,
            available_pct DOUBLE,
            note VARCHAR
        )
    """)

    # General variables: denominator = ICU-observable patient-window.
    vars_ = [
      ("MAP","map_twa_mmhg IS NOT NULL"),
      ("Heart rate","hr_twa_bpm IS NOT NULL"),
      ("Lactate","lactate_median_mmol_l IS NOT NULL"),
      ("pH","ph_median IS NOT NULL"),
      ("Base excess","base_excess_median_meq_l IS NOT NULL"),
      ("Glucose","glucose_median_mg_dl IS NOT NULL"),
      ("Urine output","urine_output_ml_kg_h IS NOT NULL"),
      ("Fluid balance","fluid_balance_available_flag=1"),
      ("Respiratory support","respiratory_state IS NOT NULL"),
      ("VIS","vis_twa IS NOT NULL"),
    ]

    for res,table in [("HOURLY","study1_hourly"),("SIX_HOUR","study1_six")]:
        for var,expr in vars_:
            con.execute(f"""
              INSERT INTO observability_qc
              SELECT
                  '{res}',
                  window_index,
                  window_label,
                  '{var}',
                  'ICU_OBSERVABLE',
                  COUNT(*) FILTER(WHERE eligible_minutes>0),
                  COUNT(*) FILTER(WHERE eligible_minutes>0 AND ({expr})),
                  100.0*COUNT(*) FILTER(WHERE eligible_minutes>0 AND ({expr}))
                    /NULLIF(COUNT(*) FILTER(WHERE eligible_minutes>0),0),
                  CASE
                    WHEN '{var}'='VIS' THEN
                      'Observable untreated windows are valid zero-treatment windows, not missing measurements.'
                    ELSE NULL
                  END
              FROM {table}
              GROUP BY window_index,window_label
            """)

    # P/F is conditionally defined only during windows with invasive ventilation.
    # The canonical 01_13 extractor explicitly separates structural ineligibility
    # from measurement missingness.
    for res,pf_table in [("HOURLY","pf_h"),("SIX_HOUR","pf_6")]:
        con.execute(f"""
          INSERT INTO observability_qc
          SELECT
              '{res}',
              window_index,
              window_label,
              'P/F ratio',
              'INVASIVE_ELIGIBLE',
              COUNT(*) FILTER(WHERE invasive_window_eligible_flag=1),
              COUNT(*) FILTER(WHERE invasive_pf_observed_flag=1),
              100.0*COUNT(*) FILTER(WHERE invasive_pf_observed_flag=1)
                /NULLIF(COUNT(*) FILTER(WHERE invasive_window_eligible_flag=1),0),
              'Availability is measured among windows with FULL_INVASIVE or PARTIAL_INVASIVE respiratory eligibility.'
          FROM {pf_table}
          GROUP BY window_index,window_label
        """)

    # Bleeding is a frozen 0-12 h trajectory. A window can be ICU-observable yet
    # lack reliable chest-drain source documentation. The canonical 01_15 analysis
    # uses full window observability + any core source documentation for summaries.
    for res,bl_table in [("HOURLY","bleeding_h"),("SIX_HOUR","bleeding_6")]:
        con.execute(f"""
          INSERT INTO observability_qc
          SELECT
              '{res}',
              window_index,
              window_label,
              'Chest drainage',
              'BLEEDING_WINDOW_FULL_OBSERVABLE',
              COUNT(*) FILTER(WHERE bleeding_window_full_observable_flag=1),
              COUNT(*) FILTER(
                  WHERE bleeding_window_full_observable_flag=1
                    AND bleeding_any_core_source_documentation_flag=1
              ),
              100.0*COUNT(*) FILTER(
                  WHERE bleeding_window_full_observable_flag=1
                    AND bleeding_any_core_source_documentation_flag=1
              ) / NULLIF(
                  COUNT(*) FILTER(WHERE bleeding_window_full_observable_flag=1),0
              ),
              'Frozen bleeding horizon 0-12 h; availability requires chest-drain source documentation.'
          FROM {bl_table}
          GROUP BY window_index,window_label
        """)

def export(con):
    for p in (OUT_H,OUT_6,OUT_MANIFEST,OUT_QC):
        if p.exists(): p.unlink()
    con.execute(f"COPY (SELECT * FROM study1_hourly ORDER BY stay_id,window_index) TO '{qp(OUT_H)}' (FORMAT PARQUET,COMPRESSION ZSTD)")
    con.execute(f"COPY (SELECT * FROM study1_six ORDER BY stay_id,window_index) TO '{qp(OUT_6)}' (FORMAT PARQUET,COMPRESSION ZSTD)")
    con.execute(f"COPY (SELECT * FROM manifest ORDER BY domain,study1_role,variable) TO '{qp(OUT_MANIFEST)}' (HEADER,DELIMITER ',')")
    con.execute(f"COPY (SELECT * FROM observability_qc ORDER BY resolution,variable,window_index) TO '{qp(OUT_QC)}' (HEADER,DELIMITER ',')")

def main():
    require_files()
    con=duckdb.connect(":memory:")
    con.execute("PRAGMA threads=4")
    con.execute("PRAGMA preserve_insertion_order=false")
    create_sources(con)
    validate_corrected_source_schema(con)
    validate_grids(con)
    create_grid(con,False); create_grid(con,True)
    build_master(con,False); build_master(con,True)
    validate_renal_fluid_passthrough(con)
    create_manifest(con); create_qc(con); export(con)
    print("TRAJECTORIES — Study 1 Phase 02_01 candidate complete")
    print(f"Hourly rows: {con.execute('SELECT COUNT(*) FROM study1_hourly').fetchone()[0]:,}")
    print(f"Six-hour rows: {con.execute('SELECT COUNT(*) FROM study1_six').fetchone()[0]:,}")
    print("No imputation. No favorable-recovery definition. No outcomes.")
    print("QC: P/F uses invasive-eligible denominator; bleeding requires full-window observability + source documentation.")
    print()
    print("Corrected renal/fluid six-hour passthrough")
    print("------------------------------------------")
    print(
        con.execute(
            """
            SELECT
                window_label,
                round(median(urine_output_ml_kg_h), 4) AS median_uo_ml_kg_h,
                round(median(fluid_balance_cumulative_ml), 1) AS median_cumulative_balance_ml,
                round(
                    median(fluid_accumulation_cumulative_pct_weight),
                    4
                ) AS median_fluid_accumulation_pct
            FROM study1_six
            GROUP BY window_index, window_label
            ORDER BY window_index
            """
        ).fetchdf().to_string(index=False)
    )
    print()
    print("Candidate outputs")
    print("-----------------")
    print(OUT_H); print(OUT_6); print(OUT_MANIFEST); print(OUT_QC)

if __name__=="__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
