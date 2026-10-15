#!/usr/bin/env python3
"""
02_07_cross_domain_observability.py

TRAJECTORIES — Study 1
"Early usual-care and favorable-recovery trajectories after adult cardiac surgery:
a multidomain reference study"

PHASE 02_07 — Cross-domain structure, observability, and physiology-support context

Frozen analytical workflow using canonical renal/fluid trajectories.

Scientific scope
----------------
Descriptive Study-1 analysis only:
- no outcome information;
- no favorable/non-favorable discrimination;
- no prognostic modeling;
- no hypothesis-test p-values;
- no causal interpretation;
- no data-driven feature screening.

Primary objectives
------------------
1. Formalize six-hour observability trajectories from the frozen Phase 02_02 output.
2. Quantify a prespecified set of clinically interpretable cross-domain relationships.
3. Describe achieved physiology across treatment/support burden strata.

Inputs
------
data/02_04_patient_trajectory_features.csv
data/02_04_patient_treatment_features.csv
data/02_02_availability_summary.csv

Outputs
-------
data/02_07_analysis_manifest.csv
data/02_07_observability_trajectories.csv
data/02_07_cross_domain_correlations.csv
data/02_07_support_burden_profiles.csv
data/02_07_support_stratified_observability.csv
data/02_07_report.txt

Statistics
----------
- Spearman rank correlation.
- Pairwise-complete observations.
- Percentile bootstrap 95% CI with deterministic seed.
- No p-values.
- Treatment variables with a mass at zero are additionally summarized as:
  NONE + tertiles among patients with positive burden.

Bootstrap and temporal observability
------------------------------------
The percentile bootstrap uses 2,000 replicates with seed 20260821, resampling
paired observations within each pairwise-complete feature comparison.
Observability summaries use SIX_HOUR rows from the availability input.
Chest-drainage windows at or beyond H12 are excluded because they lie outside
the frozen 0-12-hour bleeding horizon; they are not reported as 0/0 availability.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

try:
    from scipy.stats import spearmanr
except ImportError as exc:
    raise SystemExit(
        "Missing dependency 'scipy'. Install scipy in the project environment."
    ) from exc


EXPECTED_COHORT_N = 9305
BOOTSTRAP_REPS = 2000
BOOTSTRAP_SEED = 20260821

SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = (
    SCRIPT_PATH.parent.parent
    if SCRIPT_PATH.parent.name.lower() == "scripts"
    else SCRIPT_PATH.parent
)
DATA_DIR = PROJECT_ROOT / "data"

TRAJ = DATA_DIR / "02_04_patient_trajectory_features.csv"
TREAT = DATA_DIR / "02_04_patient_treatment_features.csv"
AVAIL = DATA_DIR / "02_02_availability_summary.csv"

OUT_MANIFEST = DATA_DIR / "02_07_analysis_manifest.csv"
OUT_OBS = DATA_DIR / "02_07_observability_trajectories.csv"
OUT_CORR = DATA_DIR / "02_07_cross_domain_correlations.csv"
OUT_PROFILE = DATA_DIR / "02_07_support_burden_profiles.csv"
OUT_STRAT_OBS = DATA_DIR / "02_07_support_stratified_observability.csv"
OUT_REPORT = DATA_DIR / "02_07_report.txt"


# =============================================================================
# Prespecified analysis set
# =============================================================================

CORRELATION_PAIRS = [
    # Cardiovascular support vs achieved physiology / metabolic burden
    dict(
        analysis_id="CD01",
        x="vis_burden_score_hours_0_24",
        y="map_mean_0_24",
        x_label="VIS burden 0-24 h",
        y_label="Mean MAP 0-24 h",
        domain_pair="circulatory support ↔ hemodynamics",
        rationale="Achieved MAP relative to overall vasoactive/inotropic support burden.",
    ),
    dict(
        analysis_id="CD02",
        x="nee_burden_equivalent_hours_0_24",
        y="map_mean_0_24",
        x_label="NEE burden 0-24 h",
        y_label="Mean MAP 0-24 h",
        domain_pair="vasopressor support ↔ hemodynamics",
        rationale="Achieved MAP relative to vasopressor intensity.",
    ),
    dict(
        analysis_id="CD03",
        x="vis_burden_score_hours_0_24",
        y="lactate_max_observed",
        x_label="VIS burden 0-24 h",
        y_label="Maximum observed lactate 0-24 h",
        domain_pair="circulatory support ↔ metabolism",
        rationale="Cardiovascular support burden in relation to observed metabolic/perfusion burden.",
    ),
    dict(
        analysis_id="CD04",
        x="nee_burden_equivalent_hours_0_24",
        y="lactate_max_observed",
        x_label="NEE burden 0-24 h",
        y_label="Maximum observed lactate 0-24 h",
        domain_pair="vasopressor support ↔ metabolism",
        rationale="Vasopressor burden in relation to observed metabolic/perfusion burden.",
    ),

    # Renal / fluid
    dict(
        analysis_id="CD05",
        x="urine_output_nadir_0_24",
        y="fluid_accumulation_peak_pct_weight_0_24",
        x_label="Urine-output nadir 0-24 h",
        y_label="Peak fluid accumulation 0-24 h",
        domain_pair="renal ↔ fluid",
        rationale="Low urine-output burden in relation to fluid accumulation.",
    ),
    dict(
        analysis_id="CD06",
        x="furosemide_iv_total_mg_0_24",
        y="fluid_accumulation_peak_pct_weight_0_24",
        x_label="IV furosemide dose 0-24 h",
        y_label="Peak fluid accumulation 0-24 h",
        domain_pair="diuretic treatment ↔ fluid",
        rationale="Diuretic treatment context in relation to accumulated fluid burden.",
    ),
    dict(
        analysis_id="CD07",
        x="furosemide_iv_total_mg_0_24",
        y="urine_output_h18_24",
        x_label="IV furosemide dose 0-24 h",
        y_label="Urine output H18-24",
        domain_pair="diuretic treatment ↔ renal",
        rationale="Diuretic treatment context in relation to late first-day urine output.",
    ),

    # Respiratory / fluid
    dict(
        analysis_id="CD08",
        x="invasive_ventilation_equivalent_hours_0_24",
        y="fluid_accumulation_peak_pct_weight_0_24",
        x_label="Invasive ventilation equivalent hours 0-24 h",
        y_label="Peak fluid accumulation 0-24 h",
        domain_pair="respiratory support ↔ fluid",
        rationale="Persistence of invasive respiratory support in relation to fluid burden.",
    ),

    # Bleeding / support / fluid
    dict(
        analysis_id="CD09",
        x="chest_drainage_sum_available_0_12_ml",
        y="nee_burden_equivalent_hours_0_24",
        x_label="Chest drainage 0-12 h",
        y_label="NEE burden 0-24 h",
        domain_pair="bleeding ↔ vasopressor support",
        rationale="Early bleeding burden in relation to vasopressor support requirement.",
    ),
    dict(
        analysis_id="CD10",
        x="chest_drainage_sum_available_0_12_ml",
        y="vis_burden_score_hours_0_24",
        x_label="Chest drainage 0-12 h",
        y_label="VIS burden 0-24 h",
        domain_pair="bleeding ↔ circulatory support",
        rationale="Early bleeding burden in relation to vasoactive/inotropic support.",
    ),
    dict(
        analysis_id="CD11",
        x="chest_drainage_sum_available_0_12_ml",
        y="fluid_accumulation_peak_pct_weight_0_24",
        x_label="Chest drainage 0-12 h",
        y_label="Peak fluid accumulation 0-24 h",
        domain_pair="bleeding ↔ fluid",
        rationale="Early bleeding burden in relation to postoperative fluid accumulation.",
    ),
]

SUPPORT_PROFILES = [
    dict(
        profile_id="SP01",
        support="nee_burden_equivalent_hours_0_24",
        support_label="NEE burden 0-24 h",
        physiology=["map_mean_0_24", "lactate_max_observed"],
    ),
    dict(
        profile_id="SP02",
        support="vis_burden_score_hours_0_24",
        support_label="VIS burden 0-24 h",
        physiology=["map_mean_0_24", "lactate_max_observed"],
    ),
    dict(
        profile_id="SP03",
        support="furosemide_iv_total_mg_0_24",
        support_label="IV furosemide dose 0-24 h",
        physiology=[
            "urine_output_h18_24",
            "fluid_accumulation_peak_pct_weight_0_24",
        ],
    ),
]


SUPPORT_STRATIFIED_OBSERVABILITY = [
    dict(
        analysis_id="OBS01",
        support="vis_burden_score_hours_0_24",
        support_label="VIS burden 0-24 h",
        observed_feature="lactate_max_observed",
        observed_label="Any observed lactate 0-24 h",
    ),
    dict(
        analysis_id="OBS02",
        support="nee_burden_equivalent_hours_0_24",
        support_label="NEE burden 0-24 h",
        observed_feature="lactate_max_observed",
        observed_label="Any observed lactate 0-24 h",
    ),
]

PHYSIOLOGY_LABELS = {
    "map_mean_0_24": "Mean MAP 0-24 h",
    "lactate_max_observed": "Maximum observed lactate 0-24 h",
    "urine_output_h18_24": "Urine output H18-24",
    "fluid_accumulation_peak_pct_weight_0_24": "Peak fluid accumulation 0-24 h",
}

OBSERVABILITY_VARIABLES = [
    "MAP",
    "Heart rate",
    "Lactate",
    "pH",
    "Base excess",
    "Glucose",
    "Urine output",
    "Fluid balance cumulative",
    "Fluid accumulation cumulative",
    "Invasive ventilation fraction",
    "P/F ratio",
    "Respiratory support",
    "Chest drainage",
]


# =============================================================================
# Helpers
# =============================================================================

def require_files() -> None:
    missing = [p for p in (TRAJ, TREAT, AVAIL) if not p.is_file()]
    if missing:
        raise FileNotFoundError(
            "Missing required canonical input(s):\n- "
            + "\n- ".join(str(p) for p in missing)
        )


def require_columns(df: pd.DataFrame, columns: set[str], label: str) -> None:
    missing = sorted(columns - set(df.columns))
    if missing:
        raise RuntimeError(f"{label} missing required columns: {missing}")


def validate_patient_tables(traj: pd.DataFrame, treat: pd.DataFrame) -> None:
    for df, label in ((traj, "trajectory"), (treat, "treatment")):
        if len(df) != EXPECTED_COHORT_N:
            raise RuntimeError(
                f"{label}: {len(df):,} rows; expected {EXPECTED_COHORT_N:,}."
            )
        if "stay_id" not in df.columns:
            raise RuntimeError(f"{label}: stay_id missing.")
        if df["stay_id"].duplicated().any():
            raise RuntimeError(f"{label}: duplicated stay_id detected.")
        if df["stay_id"].nunique() != EXPECTED_COHORT_N:
            raise RuntimeError(
                f"{label}: unexpected unique stays {df['stay_id'].nunique():,}."
            )

    if set(traj["stay_id"]) != set(treat["stay_id"]):
        raise RuntimeError("Trajectory/treatment stay_id sets do not match.")


def bootstrap_spearman(
    x: np.ndarray,
    y: np.ndarray,
    reps: int,
    rng: np.random.Generator,
) -> tuple[float, float]:
    n = len(x)
    vals = np.empty(reps, dtype=float)
    for i in range(reps):
        idx = rng.integers(0, n, n)
        vals[i] = spearmanr(x[idx], y[idx]).statistic
    vals = vals[np.isfinite(vals)]
    if len(vals) == 0:
        return np.nan, np.nan
    return (
        float(np.quantile(vals, 0.025)),
        float(np.quantile(vals, 0.975)),
    )


def summarize_series(s: pd.Series) -> dict[str, float | int]:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return dict(
            n=0, median=np.nan, p25=np.nan, p75=np.nan, p10=np.nan, p90=np.nan
        )
    return dict(
        n=int(len(s)),
        median=float(s.median()),
        p25=float(s.quantile(0.25)),
        p75=float(s.quantile(0.75)),
        p10=float(s.quantile(0.10)),
        p90=float(s.quantile(0.90)),
    )


def make_zero_plus_positive_tertiles(
    s: pd.Series,
) -> tuple[pd.Series, float, float, str]:
    """
    Categorize support burden using empirical positive-value quantiles while
    preserving ties.

    Standard case (q33 < q67):
      NONE
      POSITIVE_T1: 0 < value <= q33
      POSITIVE_T2: q33 < value <= q67
      POSITIVE_T3: value > q67

    Degenerate case (q33 == q67), which can occur for discrete treatment doses:
      NONE
      POSITIVE_LOW: 0 < value <= common quantile cut-point
      POSITIVE_HIGH: value > common quantile cut-point

    This avoids splitting identical values across artificial tertiles and avoids
    reporting a nonexistent middle tertile. Categories are descriptive and are
    not clinical thresholds.
    """
    numeric = pd.to_numeric(s, errors="coerce")
    out = pd.Series(pd.NA, index=s.index, dtype="object")

    valid = numeric.notna()
    out.loc[valid & (numeric <= 0)] = "NONE"

    pos_values = numeric.loc[valid & (numeric > 0)]
    if pos_values.empty:
        return out, np.nan, np.nan, "NO_POSITIVE_VALUES"

    q33 = float(pos_values.quantile(1 / 3, interpolation="linear"))
    q67 = float(pos_values.quantile(2 / 3, interpolation="linear"))

    pos = valid & (numeric > 0)

    if np.isclose(q33, q67, rtol=0.0, atol=1e-12):
        out.loc[pos & (numeric <= q33)] = "POSITIVE_LOW"
        out.loc[pos & (numeric > q33)] = "POSITIVE_HIGH"
        scheme = "COLLAPSED_EQUAL_QUANTILES"
    else:
        out.loc[pos & (numeric <= q33)] = "POSITIVE_T1"
        out.loc[pos & (numeric > q33) & (numeric <= q67)] = "POSITIVE_T2"
        out.loc[pos & (numeric > q67)] = "POSITIVE_T3"
        scheme = "THREE_POSITIVE_QUANTILE_GROUPS"

    return out, q33, q67, scheme


# =============================================================================
# Analysis builders
# =============================================================================

def build_manifest() -> pd.DataFrame:
    rows = []
    for p in CORRELATION_PAIRS:
        rows.append(
            {
                "analysis_family": "CROSS_DOMAIN_CORRELATION",
                "analysis_id": p["analysis_id"],
                "x": p["x"],
                "y": p["y"],
                "description": p["domain_pair"],
                "rationale": p["rationale"],
                "primary_statistic": "Spearman rho",
                "uncertainty": f"Percentile bootstrap 95% CI; {BOOTSTRAP_REPS} reps",
                "inference_note": "No p-value; descriptive structural analysis only",
            }
        )
    for p in SUPPORT_PROFILES:
        rows.append(
            {
                "analysis_family": "PHYSIOLOGY_SUPPORT_PROFILE",
                "analysis_id": p["profile_id"],
                "x": p["support"],
                "y": " | ".join(p["physiology"]),
                "description": p["support_label"],
                "rationale": (
                    "Describe achieved physiology across treatment/support burden."
                ),
                "primary_statistic": "Median [IQR], P10-P90",
                "uncertainty": "None in v2 profile table",
                "inference_note": (
                    "NONE + empirical positive-value quantile groups with ties preserved; equal q33/q67 collapsed; descriptive, not causal"
                ),
            }
        )
    return pd.DataFrame(rows)


def build_observability(avail: pd.DataFrame) -> pd.DataFrame:
    required = {
        "resolution", "variable", "window_index", "window_label",
        "eligible_stays_n", "available_stays_n", "availability_pct"
    }
    require_columns(avail, required, "02_02 availability summary")

    obs = avail.loc[
        (avail["resolution"] == "SIX_HOUR")
        & (avail["variable"].isin(OBSERVABILITY_VARIABLES))
    ].copy()

    # Chest drainage is canonically restricted to 0-12 h.
    # Windows beyond H12 are structurally outside the analytic horizon and
    # must not appear as 0/0 availability.
    chest = obs["variable"].eq("Chest drainage")
    beyond_h12 = chest & (pd.to_numeric(obs["window_index"], errors="coerce") >= 2)
    obs = obs.loc[~beyond_h12].copy()

    obs = obs.sort_values(["variable", "window_index"]).reset_index(drop=True)

    # Add first-to-last descriptive availability change where meaningful.
    first = (
        obs.sort_values("window_index")
        .groupby("variable", as_index=False)
        .first()[["variable", "availability_pct"]]
        .rename(columns={"availability_pct": "availability_pct_first_window"})
    )
    last = (
        obs.sort_values("window_index")
        .groupby("variable", as_index=False)
        .last()[["variable", "availability_pct"]]
        .rename(columns={"availability_pct": "availability_pct_last_window"})
    )
    summary = first.merge(last, on="variable", how="outer")
    summary["availability_change_pp_first_to_last"] = (
        summary["availability_pct_last_window"]
        - summary["availability_pct_first_window"]
    )

    obs = obs.merge(summary, on="variable", how="left")
    return obs


def build_support_stratified_observability(merged: pd.DataFrame) -> pd.DataFrame:
    """
    Descriptive observability of selected intermittently measured physiology
    across prespecified treatment/support burden categories.

    This is not an outcome analysis and not a statistical test. It quantifies
    informative measurement patterns already within the frozen Study-1 scope.
    """
    needed = set()
    for p in SUPPORT_STRATIFIED_OBSERVABILITY:
        needed.add(p["support"])
        needed.add(p["observed_feature"])
    require_columns(merged, needed, "merged Phase 02_04 table")

    category_order = {
        "NONE": 0,
        "POSITIVE_LOW": 1,
        "POSITIVE_T1": 1,
        "POSITIVE_T2": 2,
        "POSITIVE_HIGH": 3,
        "POSITIVE_T3": 3,
    }

    rows = []
    for p in SUPPORT_STRATIFIED_OBSERVABILITY:
        temp = merged[[p["support"], p["observed_feature"]]].copy()
        temp["support_category"], q33, q67, category_scheme = (
            make_zero_plus_positive_tertiles(temp[p["support"]])
        )

        categories_present = (
            temp["support_category"]
            .dropna()
            .drop_duplicates()
            .tolist()
        )
        categories_present = sorted(
            categories_present, key=lambda x: category_order.get(x, 99)
        )

        for category in categories_present:
            g = temp.loc[temp["support_category"] == category].copy()
            n = len(g)
            available_n = int(pd.to_numeric(
                g[p["observed_feature"]], errors="coerce"
            ).notna().sum())
            pct = 100.0 * available_n / n if n else np.nan

            rows.append(
                {
                    "analysis_id": p["analysis_id"],
                    "support_feature": p["support"],
                    "support_label": p["support_label"],
                    "support_category": category,
                    "support_category_order": category_order[category],
                    "stratum_n": n,
                    "positive_q33_cutpoint": q33,
                    "positive_q67_cutpoint": q67,
                    "category_scheme": category_scheme,
                    "observed_feature": p["observed_feature"],
                    "observed_label": p["observed_label"],
                    "available_n": available_n,
                    "availability_pct": pct,
                    "interpretation_note": (
                        "Descriptive measurement observability only; "
                        "NONE plus tertiles among positive support burden."
                    ),
                }
            )

    return pd.DataFrame(rows).sort_values(
        ["analysis_id", "support_category_order"]
    ).reset_index(drop=True)


def build_correlations(merged: pd.DataFrame) -> pd.DataFrame:
    needed = set()
    for p in CORRELATION_PAIRS:
        needed.update([p["x"], p["y"]])
    require_columns(merged, needed, "merged Phase 02_04 table")

    rng = np.random.default_rng(BOOTSTRAP_SEED)
    rows = []

    for p in CORRELATION_PAIRS:
        pair = merged[[p["x"], p["y"]]].copy()
        pair[p["x"]] = pd.to_numeric(pair[p["x"]], errors="coerce")
        pair[p["y"]] = pd.to_numeric(pair[p["y"]], errors="coerce")
        pair = pair.dropna()

        n = len(pair)
        if n < 3:
            rho = lo = hi = np.nan
        else:
            x = pair[p["x"]].to_numpy(dtype=float)
            y = pair[p["y"]].to_numpy(dtype=float)
            rho = float(spearmanr(x, y).statistic)
            lo, hi = bootstrap_spearman(x, y, BOOTSTRAP_REPS, rng)

        x_zero_pct = (
            100.0 * (pair[p["x"]] == 0).mean() if n else np.nan
        )
        y_zero_pct = (
            100.0 * (pair[p["y"]] == 0).mean() if n else np.nan
        )

        rows.append(
            {
                "analysis_id": p["analysis_id"],
                "domain_pair": p["domain_pair"],
                "x_feature": p["x"],
                "x_label": p["x_label"],
                "y_feature": p["y"],
                "y_label": p["y_label"],
                "pairwise_complete_n": n,
                "pairwise_complete_pct_cohort": 100.0 * n / EXPECTED_COHORT_N,
                "spearman_rho": rho,
                "bootstrap_ci95_low": lo,
                "bootstrap_ci95_high": hi,
                "x_zero_pct_among_complete": x_zero_pct,
                "y_zero_pct_among_complete": y_zero_pct,
                "rationale": p["rationale"],
                "interpretation_rule": (
                    "Magnitude/direction only; no significance threshold; "
                    "no causal or prognostic interpretation."
                ),
            }
        )

    return pd.DataFrame(rows)



def validate_corrected_renal_fluid_correlations(
    merged: pd.DataFrame,
    correlations: pd.DataFrame,
) -> None:
    """
    Recompute the five prespecified associations affected by corrected
    urine-output / fluid-accumulation features and require exact closure
    against the stored Phase 02_07 correlation table.
    """
    affected_ids = {"CD05", "CD06", "CD07", "CD08", "CD11"}
    specs = {
        p["analysis_id"]: p
        for p in CORRELATION_PAIRS
        if p["analysis_id"] in affected_ids
    }

    for analysis_id, p in specs.items():
        pair = merged[[p["x"], p["y"]]].copy()
        pair[p["x"]] = pd.to_numeric(pair[p["x"]], errors="coerce")
        pair[p["y"]] = pd.to_numeric(pair[p["y"]], errors="coerce")
        pair = pair.dropna()

        n_direct = len(pair)
        rho_direct = (
            float(spearmanr(
                pair[p["x"]].to_numpy(dtype=float),
                pair[p["y"]].to_numpy(dtype=float),
            ).statistic)
            if n_direct >= 3
            else np.nan
        )

        row = correlations.loc[
            correlations["analysis_id"] == analysis_id
        ]
        if len(row) != 1:
            raise RuntimeError(
                f"{analysis_id}: expected exactly one correlation row."
            )

        n_stored = int(row["pairwise_complete_n"].iloc[0])
        rho_stored = float(row["spearman_rho"].iloc[0])

        if n_stored != n_direct:
            raise RuntimeError(
                f"{analysis_id}: pairwise N mismatch "
                f"{n_stored} vs {n_direct}."
            )

        if not np.isclose(
            rho_stored, rho_direct, rtol=0.0, atol=1e-12,
            equal_nan=True,
        ):
            raise RuntimeError(
                f"{analysis_id}: Spearman rho mismatch "
                f"{rho_stored} vs {rho_direct}."
            )


def build_support_profiles(merged: pd.DataFrame) -> pd.DataFrame:
    needed = set()
    for p in SUPPORT_PROFILES:
        needed.add(p["support"])
        needed.update(p["physiology"])
    require_columns(merged, needed, "merged Phase 02_04 table")

    rows = []

    order = {
        "NONE": 0,
        "POSITIVE_LOW": 1,
        "POSITIVE_T1": 1,
        "POSITIVE_T2": 2,
        "POSITIVE_HIGH": 3,
        "POSITIVE_T3": 3,
    }

    for p in SUPPORT_PROFILES:
        temp = merged[[p["support"], *p["physiology"]]].copy()
        temp["support_category"], q33, q67, category_scheme = (
            make_zero_plus_positive_tertiles(temp[p["support"]])
        )

        for category, group in temp.dropna(subset=["support_category"]).groupby(
            "support_category", observed=False
        ):
            support_summary = summarize_series(group[p["support"]])

            for physiological_feature in p["physiology"]:
                phys_summary = summarize_series(group[physiological_feature])
                rows.append(
                    {
                        "profile_id": p["profile_id"],
                        "support_feature": p["support"],
                        "support_label": p["support_label"],
                        "support_category": category,
                        "support_category_order": order.get(category, 99),
                        "support_category_n": int(len(group)),
                        "positive_q33_cutpoint": q33,
                        "positive_q67_cutpoint": q67,
                        "category_scheme": category_scheme,
                        "support_median": support_summary["median"],
                        "support_p25": support_summary["p25"],
                        "support_p75": support_summary["p75"],
                        "physiology_feature": physiological_feature,
                        "physiology_label": PHYSIOLOGY_LABELS[physiological_feature],
                        "physiology_available_n": phys_summary["n"],
                        "physiology_median": phys_summary["median"],
                        "physiology_p25": phys_summary["p25"],
                        "physiology_p75": phys_summary["p75"],
                        "physiology_p10": phys_summary["p10"],
                        "physiology_p90": phys_summary["p90"],
                        "category_definition": (
                            "NONE = support value <=0; "
                            "T1: 0<value<=q33; T2: q33<value<=q67; T3: value>q67. "
                            "Empirical positive-value quantiles; ties preserved."
                        ),
                    }
                )

    out = pd.DataFrame(rows)
    return out.sort_values(
        ["profile_id", "support_category_order", "physiology_feature"]
    ).reset_index(drop=True)


def write_report(
    manifest: pd.DataFrame,
    observability: pd.DataFrame,
    correlations: pd.DataFrame,
    profiles: pd.DataFrame,
    stratified_observability: pd.DataFrame,
) -> None:
    lines = []
    lines.extend(
        [
            "=" * 112,
            "TRAJECTORIES — STUDY 1 PHASE 02_07 CANDIDATE",
            "Cross-domain structure, observability, and physiology-support context",
            "=" * 112,
            "",
            f"Cohort expected: {EXPECTED_COHORT_N:,}",
            f"Bootstrap repetitions: {BOOTSTRAP_REPS:,}",
            f"Bootstrap seed: {BOOTSTRAP_SEED}",
            "",
            "SCOPE LOCK",
            "-" * 112,
            "Descriptive Study-1 analysis only.",
            "No outcomes. No favorable/non-favorable discrimination. No prognosis.",
            "No p-values. No causal interpretation. No data-driven screening.",
            "",
            "OBSERVABILITY — SIX-HOUR WINDOWS",
            "-" * 112,
        ]
    )

    for variable in observability["variable"].drop_duplicates():
        sub = observability.loc[observability["variable"] == variable].sort_values(
            "window_index"
        )
        entries = [
            f"{r.window_label}: {r.available_stays_n}/{r.eligible_stays_n} "
            f"({r.availability_pct:.1f}%)"
            for r in sub.itertuples()
        ]
        lines.append(f"{variable}: " + " | ".join(entries))

    lines.extend(
        [
            "",
            "SUPPORT-STRATIFIED OBSERVABILITY",
            "-" * 112,
            (
                "Descriptive observability only. Categories are NONE plus tertiles "
                "among positive support burden; no hypothesis testing."
            ),
        ]
    )

    for aid in stratified_observability["analysis_id"].drop_duplicates():
        sub = stratified_observability.loc[
            stratified_observability["analysis_id"] == aid
        ].sort_values("support_category_order")
        lines.append("")
        lines.append(f"{aid} — {sub['support_label'].iloc[0]} vs {sub['observed_label'].iloc[0]}")
        lines.append(
            f"  Positive-value cut-points: "
            f"q33={sub['positive_q33_cutpoint'].iloc[0]:.6g}, "
            f"q67={sub['positive_q67_cutpoint'].iloc[0]:.6g}"
        )
        if "category_scheme" in sub.columns:
            lines.append(f"  Category scheme: {sub['category_scheme'].iloc[0]}")
        for r in sub.itertuples():
            lines.append(
                f"  {r.support_category}: "
                f"{r.available_n:,}/{r.stratum_n:,} ({r.availability_pct:.1f}%)"
            )

    lines.extend(
        [
            "",
            "PRESPECIFIED CROSS-DOMAIN SPEARMAN CORRELATIONS",
            "-" * 112,
        ]
    )

    for r in correlations.itertuples():
        lines.append(
            f"{r.analysis_id} | {r.x_label} vs {r.y_label} | "
            f"N={r.pairwise_complete_n:,} | "
            f"rho={r.spearman_rho:.3f} "
            f"[95% bootstrap CI {r.bootstrap_ci95_low:.3f}, {r.bootstrap_ci95_high:.3f}]"
        )

    lines.extend(
        [
            "",
            "PHYSIOLOGY-vs-SUPPORT PROFILES",
            "-" * 112,
            (
                "Support categories: NONE plus tertiles among positive values. "
                "These are distribution-based descriptive groups, not clinical cut-points."
            ),
        ]
    )

    for pid in profiles["profile_id"].drop_duplicates():
        sub = profiles.loc[profiles["profile_id"] == pid]
        lines.append("")
        lines.append(f"{pid} — {sub['support_label'].iloc[0]}")
        lines.append(
            f"  Positive-value cut-points: "
            f"q33={sub['positive_q33_cutpoint'].iloc[0]:.6g}, "
            f"q67={sub['positive_q67_cutpoint'].iloc[0]:.6g}"
        )
        if "category_scheme" in sub.columns:
            lines.append(f"  Category scheme: {sub['category_scheme'].iloc[0]}")
        profile_categories = (
            sub[["support_category", "support_category_order"]]
            .drop_duplicates()
            .sort_values("support_category_order")["support_category"]
            .tolist()
        )
        for cat in profile_categories:
            c = sub.loc[sub["support_category"] == cat]
            if c.empty:
                continue
            lines.append(f"  {cat}: n={int(c['support_category_n'].iloc[0]):,}")
            for r in c.itertuples():
                lines.append(
                    f"    {r.physiology_label}: "
                    f"n={r.physiology_available_n:,}, "
                    f"median={r.physiology_median:.3f} "
                    f"[{r.physiology_p25:.3f}-{r.physiology_p75:.3f}]"
                )

    lines.extend(
        [
            "",
            "INTERPRETATION GUARDRAILS",
            "-" * 112,
            "1. Correlation describes co-variation; it does not establish directionality or causality.",
            "2. Treatment burden is clinician-mediated and must not be interpreted as untreated disease severity.",
            "3. Lactate is selectively measured; lactate-based pairwise N and measurement selection must accompany interpretation.",
            "4. P/F is intentionally excluded from the primary cross-domain correlation set because later P/F observation is strongly conditioned on persistent invasive ventilation.",
            "5. Furosemide correlations combine treatment indication, timing, dose, fluid state, and renal response; they are descriptive only.",
            "6. Chest drainage is restricted to the frozen 0-12 h trajectory horizon.",
            "7. No correlation magnitude is used as a feature-selection threshold.",
            "8. Support-burden groups use empirical positive-value quantiles and preserve ties. If q33=q67, the positive distribution is represented as LOW versus HIGH around the common cut-point; no empty artificial tertile is reported.",
            "",
            "OUTPUTS",
            "-" * 112,
            str(OUT_MANIFEST),
            str(OUT_OBS),
            str(OUT_CORR),
            str(OUT_PROFILE),
            str(OUT_STRAT_OBS),
            str(OUT_REPORT),
            "",
            "=" * 112,
        ]
    )

    OUT_REPORT.write_text("\n".join(lines), encoding="utf-8")


# =============================================================================
# Main
# =============================================================================

def main() -> int:
    require_files()

    traj = pd.read_csv(TRAJ)
    treat = pd.read_csv(TREAT)
    avail = pd.read_csv(AVAIL)

    validate_patient_tables(traj, treat)

    # One-to-one merge preserving physiology/treatment conceptual separation
    # in the source files while permitting prespecified cross-domain analysis.
    merged = traj.merge(
        treat.drop(columns=[c for c in ["subject_id", "hadm_id"] if c in treat.columns]),
        on="stay_id",
        how="inner",
        validate="one_to_one",
        suffixes=("", "_treat"),
    )

    if len(merged) != EXPECTED_COHORT_N:
        raise RuntimeError(
            f"Merged patient table has {len(merged):,} rows; "
            f"expected {EXPECTED_COHORT_N:,}."
        )

    manifest = build_manifest()
    observability = build_observability(avail)
    correlations = build_correlations(merged)
    validate_corrected_renal_fluid_correlations(merged, correlations)
    profiles = build_support_profiles(merged)
    stratified_observability = build_support_stratified_observability(merged)

    for path in (
        OUT_MANIFEST, OUT_OBS, OUT_CORR, OUT_PROFILE, OUT_STRAT_OBS, OUT_REPORT
    ):
        if path.exists():
            path.unlink()

    manifest.to_csv(OUT_MANIFEST, index=False)
    observability.to_csv(OUT_OBS, index=False)
    correlations.to_csv(OUT_CORR, index=False)
    profiles.to_csv(OUT_PROFILE, index=False)
    stratified_observability.to_csv(OUT_STRAT_OBS, index=False)
    write_report(
        manifest,
        observability,
        correlations,
        profiles,
        stratified_observability,
    )

    print("=" * 112)
    print("TRAJECTORIES — Study 1 Phase 02_07 candidate complete")
    print("=" * 112)
    print(f"Cohort: {len(merged):,} stays")
    print("No outcomes. No prognosis. No p-values. No causal interpretation.")
    print()
    print("Renal/fluid-affected cross-domain correlations:")
    for r in correlations.loc[
        correlations["analysis_id"].isin(
            ["CD05", "CD06", "CD07", "CD08", "CD11"]
        )
    ].itertuples():
        print(
            f"{r.analysis_id}: rho={r.spearman_rho:.3f} "
            f"[{r.bootstrap_ci95_low:.3f}, {r.bootstrap_ci95_high:.3f}], "
            f"N={r.pairwise_complete_n:,}"
        )

    print()
    print("Cross-domain correlations:")
    for r in correlations.itertuples():
        print(
            f"{r.analysis_id}: rho={r.spearman_rho:.3f} "
            f"[{r.bootstrap_ci95_low:.3f}, {r.bootstrap_ci95_high:.3f}], "
            f"N={r.pairwise_complete_n:,}"
        )
    print()
    print("Support-stratified lactate observability:")
    for r in stratified_observability.itertuples():
        print(
            f"{r.analysis_id} {r.support_category}: "
            f"{r.available_n:,}/{r.stratum_n:,} ({r.availability_pct:.1f}%)"
        )
    print()
    print("Outputs:")
    for p in (
        OUT_MANIFEST, OUT_OBS, OUT_CORR, OUT_PROFILE, OUT_STRAT_OBS, OUT_REPORT
    ):
        print(" ", p)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("Interrupted by user.", file=sys.stderr)
        raise SystemExit(130)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise
