"""
01_10_extract_fluid_balance.py

Trajectories project
Postoperative ICU-recorded fluid balance - canonical boundary-corrected pipeline

Purpose
-------
Construct hourly and six-hour postoperative fluid-balance trajectories during
the first 24 hours after the frozen postoperative time origin.

The component represents the balance of fluids explicitly recorded in the ICU
after postoperative_time_zero. It is not a complete perioperative balance and
does not retrospectively include intraoperative crystalloid, cell-saver, blood
products, estimated blood loss, OR urine, PACU summaries, or pre-admission
totals.

Frozen methodological decisions
--------------------------------
Input
~~~~~
Primary ICU fluid-input source:
    INGREDIENTEVENTS Water (itemid discovered from d_items; locally 220490)

Water and INPUTEVENTS mL are alternative representations of the same
MetaVision orders and must not be added together.

Excluded input contexts:
    OR intake summaries
    Cath Lab intake
    PACU summaries
    pre-admission/non-ICU summaries

Interval allocation:
    explicit starttime-endtime segments are allocated proportionally to window
    overlap. No LOCF, interpolation beyond explicit order intervals, or
    extrapolation is used.

Input observability:
    stays with no valid ICU Water segment are structurally missing for net
    fluid balance. Missing Water is not interpreted as zero input.

Urine
~~~~~
Urine output is reused from the frozen 01_07 canonical outputs:
    data/01_07_urine_output_hourly_trajectory.parquet
    data/01_07_urine_output_six_hour_summary.parquet

Urine is not re-extracted in this component.

Chest and mediastinal drainage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Accepted itemids are allocated over right-closed previous-documentation
intervals. The first point after t0 starts at t0. A point exactly at t0 is
excluded as pre-horizon boundary volume. The first post-endpoint point within
six hours may serve as a right-boundary anchor; only the fraction overlapping
the analytical horizon is attributed.

Other non-urinary outputs
~~~~~~~~~~~~~~~~~~~~~~~~~
Gastric/ostomy/rectal collection outputs, other surgical drains, emesis, and
stool are assigned to their charttime in the primary representation. This
avoids uniformly spreading sparsely documented volumes across long intervals.

Renal replacement therapy
~~~~~~~~~~~~~~~~~~~~~~~~~
CRRT primary:
    224191 Hourly Patient Fluid Removal
    allocated over [charttime - 1 hour, charttime)

CRRT auxiliary:
    226457 Ultrafiltrate Output
    not merged with 224191 and not used as an automatic fallback

Intermittent hemodialysis:
    226499 Hemodialysis Output
    point event at charttime

Peritoneal dialysis:
    225806 Volume In paired with 225807 Volume Out
    net removal = Volume Out - Volume In
    assigned at the Volume Out charttime

A stay is marked RRT-incomplete when:
    - 226457 is present but 224191 is absent; or
    - a PD Volume Out in the horizon is unmatched; or
    - a PD Volume In in the horizon is unmatched.

Net balance is missing in RRT-incomplete stays.

Primary urine observability
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The primary net balance requires the frozen urine-output component to be
available and to cover at least 50% of the eligible portion of the current
window. This threshold follows the observability categories already frozen in
01_07 and excludes windows with only minimal urine observation.

A separate sensitivity retains the recorded-components balance whenever urine
has any positive-duration observed interval. Stricter thresholds (80%, 90%,
95%, 99%, and complete coverage) were evaluated in the pre-production audit
and produced materially similar median 24-hour balance estimates but
progressively smaller samples.

No uncovered urine time is imputed or extrapolated. The result remains an
ICU-recorded fluid balance with explicit urine-coverage metadata.

GU irrigation
~~~~~~~~~~~~~
The frozen 01_07 urine component already masks irrigation-affected windows.
Consequently, net balance remains missing where canonical urine output is
unavailable because of GU irrigation.

Canonical outputs
-----------------
data/01_10_fluid_balance_hourly_trajectory.parquet
data/01_10_fluid_balance_six_hour_summary.parquet
data/01_10_fluid_balance_extraction_audit.csv

Inputs
------
data/00_04_final_analytic_cohort.csv
Raw MIMIC-IV 3.1 files relative to MIMIC_ROOT:
  icu/d_items.csv.gz
  icu/inputevents.csv.gz
  icu/ingredientevents.csv.gz
  icu/outputevents.csv.gz
  icu/chartevents.csv.gz
  icu/icustays.csv.gz

The two canonical 01_07 urine outputs listed above are required inputs.
"""

from pathlib import Path
import duckdb


# =============================================================================
# Paths and constants
# =============================================================================

# Project directory containing the Scripts and data directories.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
# Set this path to the local MIMIC-IV 3.1 directory.
MIMIC_ROOT = Path("path/to/mimic-iv-3.1")

COHORT_FILE = PROJECT_ROOT / "data" / "00_04_final_analytic_cohort.csv"

URINE_HOURLY_FILE = (
    PROJECT_ROOT / "data" / "01_07_urine_output_hourly_trajectory.parquet"
)
URINE_SIX_FILE = (
    PROJECT_ROOT / "data" / "01_07_urine_output_six_hour_summary.parquet"
)

D_ITEMS_FILE = MIMIC_ROOT / "icu" / "d_items.csv.gz"
INPUTEVENTS_FILE = MIMIC_ROOT / "icu" / "inputevents.csv.gz"
INGREDIENTEVENTS_FILE = MIMIC_ROOT / "icu" / "ingredientevents.csv.gz"
OUTPUTEVENTS_FILE = MIMIC_ROOT / "icu" / "outputevents.csv.gz"
CHARTEVENTS_FILE = MIMIC_ROOT / "icu" / "chartevents.csv.gz"
ICUSTAYS_FILE = MIMIC_ROOT / "icu" / "icustays.csv.gz"

OUT_HOURLY = (
    PROJECT_ROOT / "data" / "01_10_fluid_balance_hourly_trajectory.parquet"
)
OUT_SIX = (
    PROJECT_ROOT / "data" / "01_10_fluid_balance_six_hour_summary.parquet"
)
OUT_AUDIT = (
    PROJECT_ROOT / "data" / "01_10_fluid_balance_extraction_audit.csv"
)

EXPECTED_COHORT_N = 9305
EXPECTED_INPUT_AVAILABLE_N = 8333

ANCHOR_SEARCH_HOURS = 6
PD_PAIR_MAX_HOURS = 12

# Chest / mediastinal / pleural drains: previous-interval semantics.
CHEST_DRAIN_ITEMIDS = (
    226588,  # Chest Tube #1
    226589,  # Chest Tube #2
    229413,  # Chest Tube #3
    229414,  # Chest Tube #4
    226592,  # Mediastinal
    226590,  # L Pleural #1
    226591,  # L Pleural #2
    226593,  # R Pleural #1
    226595,  # R Pleural #2
)

# Sparse collection outputs: primary point-time semantics.
GI_COLLECTION_ITEMIDS = (
    226576,  # Oral Gastric
    226575,  # Nasogastric
    226573,  # Gastric Tube
    226582,  # Ostomy output
    226583,  # Rectal Tube
    226580,  # Fecal Bag
)

OTHER_DRAIN_ITEMIDS = (
    226599,  # Jackson Pratt #1
    226600,  # Jackson Pratt #2
    226601,  # Jackson Pratt #3
    226597,  # JP Medial
    226598,  # JP Lateral
    226604,  # Hemovac #1
    226613,  # Wound Vac #1
    227701,  # Drainage Bag
    226610,  # Lumbar
    226603,  # T Tube
    226619,  # Pigtail #1
    226617,  # Sump #1
)

DISCRETE_OUTPUT_ITEMIDS = (
    226571,  # Emesis
    226579,  # Stool
)

POINT_NONURINARY_ITEMIDS = (
    *GI_COLLECTION_ITEMIDS,
    *OTHER_DRAIN_ITEMIDS,
    *DISCRETE_OUTPUT_ITEMIDS,
)

# RRT items.
CRRT_PATIENT_REMOVAL_ITEMID = 224191
CRRT_ULTRAFILTRATE_ITEMID = 226457
IHD_OUTPUT_ITEMID = 226499
PD_VOLUME_IN_ITEMID = 225806
PD_VOLUME_OUT_ITEMID = 225807
PD_VOLUME_NOT_REMOVED_ITEMID = 227438

RRT_ITEMIDS = (
    CRRT_PATIENT_REMOVAL_ITEMID,
    CRRT_ULTRAFILTRATE_ITEMID,
    IHD_OUTPUT_ITEMID,
    PD_VOLUME_IN_ITEMID,
    PD_VOLUME_OUT_ITEMID,
    PD_VOLUME_NOT_REMOVED_ITEMID,
)


def qpath(path: Path) -> str:
    return str(path).replace("'", "''")


def sql_ids(values) -> str:
    return ", ".join(str(int(x)) for x in values)


for required in [
    COHORT_FILE,
    URINE_HOURLY_FILE,
    URINE_SIX_FILE,
    D_ITEMS_FILE,
    INPUTEVENTS_FILE,
    INGREDIENTEVENTS_FILE,
    OUTPUTEVENTS_FILE,
    CHARTEVENTS_FILE,
    ICUSTAYS_FILE,
]:
    if not required.exists():
        raise FileNotFoundError(f"Required file not found: {required}")

OUT_HOURLY.parent.mkdir(parents=True, exist_ok=True)

CHEST_SQL = sql_ids(CHEST_DRAIN_ITEMIDS)
GI_SQL = sql_ids(GI_COLLECTION_ITEMIDS)
OTHER_SQL = sql_ids(OTHER_DRAIN_ITEMIDS)
DISCRETE_SQL = sql_ids(DISCRETE_OUTPUT_ITEMIDS)
POINT_OUTPUT_SQL = sql_ids(POINT_NONURINARY_ITEMIDS)
RRT_SQL = sql_ids(RRT_ITEMIDS)

con = duckdb.connect(database=":memory:")
con.execute("PRAGMA threads=4")


# =============================================================================
# Frozen cohort and urine grids
# =============================================================================

con.execute(
    f"""
    CREATE TABLE cohort AS
    SELECT
        c.subject_id::BIGINT AS subject_id,
        c.hadm_id::BIGINT AS hadm_id,
        c.stay_id::BIGINT AS stay_id,
        c.surgery_group,
        c.sex,
        c.age_at_icu_admission,
        c.postoperative_time_zero::TIMESTAMP AS postoperative_time_zero,
        c.observation_end_time::TIMESTAMP AS observation_end_time,
        c.observation_end_type,
        i.outtime::TIMESTAMP AS icu_outtime,

        least(
            c.postoperative_time_zero::TIMESTAMP + INTERVAL '24 hours',
            c.observation_end_time::TIMESTAMP
        ) AS analytical_end_time,

        -- Chest-drain right-boundary search:
        -- for early discharge/death, stop at the analytical endpoint;
        -- for 24-h-observable stays, permit the first post-24 h source point
        -- only while the patient remains in the same ICU stay, up to +6 h.
        CASE
            WHEN c.observation_end_time::TIMESTAMP
                 < c.postoperative_time_zero::TIMESTAMP + INTERVAL '24 hours'
                THEN c.observation_end_time::TIMESTAMP
            ELSE least(
                i.outtime::TIMESTAMP,
                c.postoperative_time_zero::TIMESTAMP
                    + INTERVAL '{24 + ANCHOR_SEARCH_HOURS} hours'
            )
        END AS anchor_search_end_time,

        -- CRRT item 224191 represents the preceding hour
        -- [charttime-1 h, charttime). A source timestamp up to one hour after
        -- the analytical endpoint may therefore overlap the 0-24 h horizon.
        CASE
            WHEN c.observation_end_time::TIMESTAMP
                 < c.postoperative_time_zero::TIMESTAMP + INTERVAL '24 hours'
                THEN c.observation_end_time::TIMESTAMP
            ELSE least(
                i.outtime::TIMESTAMP,
                c.postoperative_time_zero::TIMESTAMP + INTERVAL '25 hours'
            )
        END AS crrt_source_end_time

    FROM read_csv_auto(
        '{qpath(COHORT_FILE)}',
        header=true,
        all_varchar=false
    ) c
    LEFT JOIN read_csv_auto(
        '{qpath(ICUSTAYS_FILE)}',
        header=true,
        all_varchar=false
    ) i
        ON c.stay_id::BIGINT = i.stay_id::BIGINT
    """
)

con.execute(
    f"""
    CREATE TABLE urine_hourly AS
    SELECT *
    FROM read_parquet('{qpath(URINE_HOURLY_FILE)}')
    """
)

con.execute(
    f"""
    CREATE TABLE urine_six AS
    SELECT *
    FROM read_parquet('{qpath(URINE_SIX_FILE)}')
    """
)

cohort_n, cohort_unique_n = con.execute(
    "SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM cohort"
).fetchone()

hourly_n, hourly_unique_n = con.execute(
    "SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM urine_hourly"
).fetchone()

six_n, six_unique_n = con.execute(
    "SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM urine_six"
).fetchone()

if cohort_n != EXPECTED_COHORT_N or cohort_unique_n != EXPECTED_COHORT_N:
    raise RuntimeError(
        f"Unexpected cohort dimensions: {cohort_n} / {cohort_unique_n}"
    )

missing_true_icu_end_n = con.execute(
    """
    SELECT COUNT(*)
    FROM cohort
    WHERE observation_end_time
            >= postoperative_time_zero + INTERVAL '24 hours'
      AND icu_outtime IS NULL
    """
).fetchone()[0]

invalid_true_icu_end_n = con.execute(
    """
    SELECT COUNT(*)
    FROM cohort
    WHERE observation_end_time
            >= postoperative_time_zero + INTERVAL '24 hours'
      AND icu_outtime < analytical_end_time
    """
).fetchone()[0]

if missing_true_icu_end_n or invalid_true_icu_end_n:
    raise RuntimeError(
        "Invalid true ICU end for boundary reconstruction: "
        f"missing={missing_true_icu_end_n}, "
        f"before_analytical_end={invalid_true_icu_end_n}."
    )

if hourly_n != EXPECTED_COHORT_N * 24 or hourly_unique_n != EXPECTED_COHORT_N:
    raise RuntimeError(
        f"Unexpected urine hourly dimensions: {hourly_n} / {hourly_unique_n}"
    )

if six_n != EXPECTED_COHORT_N * 4 or six_unique_n != EXPECTED_COHORT_N:
    raise RuntimeError(
        f"Unexpected urine six-hour dimensions: {six_n} / {six_unique_n}"
    )

# Verify the urine files expose the frozen columns needed downstream.
required_urine_columns = {
    "stay_id",
    "window_index",
    "window_label",
    "window_start_time",
    "eligible_window_end_time",
    "eligible_minutes",
    "observability_status",
    "uo_weight_kg",
    "uo_weight_source",
    "uo_weight_charttime",
    "uo_weight_hours_from_t0",
    "uo_irrigation_any_24h_flag",
    "uo_irrigation_affected_window_flag",
    "uo_interval_covered_minutes",
    "uo_interval_coverage_fraction",
    "uo_interval_ml_observed",
    "uo_interval_ml_24h_observed",
    "uo_measurement_status_24h",
    "uo_primary_available_flag",
    "uo_measurement_status",
}

for table_name in ["urine_hourly", "urine_six"]:
    available = {
        row[0]
        for row in con.execute(
            f"DESCRIBE {table_name}"
        ).fetchall()
    }
    missing = sorted(required_urine_columns - available)
    if missing:
        raise RuntimeError(
            f"{table_name} is missing required frozen 01_07 columns: "
            + ", ".join(missing)
        )


# =============================================================================
# Item dictionary
# =============================================================================

con.execute(
    f"""
    CREATE TABLE d_items AS
    SELECT
        try_cast(itemid AS BIGINT) AS itemid,
        label,
        abbreviation,
        linksto,
        category,
        unitname,
        param_type
    FROM read_csv_auto(
        '{qpath(D_ITEMS_FILE)}',
        header=true,
        all_varchar=true
    )
    WHERE try_cast(itemid AS BIGINT) IS NOT NULL
    """
)

water_itemids = [
    row[0]
    for row in con.execute(
        """
        SELECT itemid
        FROM d_items
        WHERE lower(trim(coalesce(linksto, ''))) = 'ingredientevents'
          AND lower(coalesce(label, '')) LIKE '%water%'
        ORDER BY itemid
        """
    ).fetchall()
]

if not water_itemids:
    raise RuntimeError("No INGREDIENTEVENTS Water itemid discovered.")

WATER_SQL = sql_ids(water_itemids)


# =============================================================================
# ICU Water input source
# =============================================================================

# Use all non-rewritten INPUTEVENTS components to identify the clinical context
# of each exact order segment. The preferred label is the main order parameter.
con.execute(
    f"""
    CREATE TABLE input_context_raw AS
    SELECT
        c.stay_id,
        ie.starttime,
        ie.endtime,
        coalesce(ie.orderid::BIGINT, -1) AS orderid_key,
        ie.itemid::BIGINT AS itemid,
        di.label,
        di.category AS item_category,
        ie.ordercategoryname,
        ie.ordercomponenttypedescription,
        ie.statusdescription
    FROM read_csv_auto(
        '{qpath(INPUTEVENTS_FILE)}',
        header=true,
        all_varchar=false
    ) ie
    INNER JOIN cohort c
        ON ie.stay_id = c.stay_id
       AND ie.starttime < c.analytical_end_time
       AND ie.endtime > c.postoperative_time_zero
    LEFT JOIN d_items di
        ON ie.itemid = di.itemid
    WHERE lower(trim(coalesce(ie.statusdescription, ''))) <> 'rewritten'
    """
)

con.execute(
    """
    CREATE TABLE input_segment_context AS
    SELECT
        stay_id,
        orderid_key,
        starttime,
        endtime,

        first(
            label
            ORDER BY
                CASE
                    WHEN lower(
                        coalesce(ordercomponenttypedescription, '')
                    ) LIKE '%main order parameter%'
                        THEN 0
                    ELSE 1
                END,
                itemid
        ) AS main_label,

        first(
            item_category
            ORDER BY
                CASE
                    WHEN lower(
                        coalesce(ordercomponenttypedescription, '')
                    ) LIKE '%main order parameter%'
                        THEN 0
                    ELSE 1
                END,
                itemid
        ) AS main_item_category,

        first(
            ordercategoryname
            ORDER BY
                CASE
                    WHEN lower(
                        coalesce(ordercomponenttypedescription, '')
                    ) LIKE '%main order parameter%'
                        THEN 0
                    ELSE 1
                END,
                itemid
        ) AS main_order_category,

        string_agg(
            DISTINCT coalesce(label, '<NULL>'),
            ' | '
        ) AS all_labels,

        COUNT(*) AS input_component_rows_n

    FROM input_context_raw
    GROUP BY
        stay_id,
        orderid_key,
        starttime,
        endtime
    """
)

con.execute(
    f"""
    CREATE TABLE water_raw AS
    SELECT
        c.stay_id,
        c.postoperative_time_zero,
        c.analytical_end_time,
        ge.starttime,
        ge.endtime,
        coalesce(ge.orderid::BIGINT, -1) AS orderid_key,
        ge.itemid::BIGINT AS itemid,
        ge.amount::DOUBLE AS amount_ml,
        ge.amountuom,
        ge.statusdescription,
        date_diff(
            'second',
            ge.starttime,
            ge.endtime
        ) AS duration_seconds
    FROM read_csv_auto(
        '{qpath(INGREDIENTEVENTS_FILE)}',
        header=true,
        all_varchar=false
    ) ge
    INNER JOIN cohort c
        ON ge.stay_id = c.stay_id
       AND ge.starttime < c.analytical_end_time
       AND ge.endtime > c.postoperative_time_zero
    WHERE ge.itemid IN ({WATER_SQL})
      AND lower(trim(coalesce(ge.statusdescription, ''))) <> 'rewritten'
    """
)

con.execute(
    """
    CREATE TABLE water_valid AS
    SELECT *
    FROM water_raw
    WHERE duration_seconds > 0
      AND amount_ml IS NOT NULL
      AND amount_ml >= 0
      AND lower(replace(trim(coalesce(amountuom, '')), ' ', ''))
            IN ('ml', 'milliliter', 'milliliters')
    """
)

con.execute(
    """
    CREATE TABLE water_segments_classified AS
    SELECT
        w.*,
        s.main_label,
        s.main_item_category,
        s.main_order_category,
        s.all_labels,
        s.input_component_rows_n,

        CASE
            WHEN s.stay_id IS NULL THEN 'UNRESOLVED_CONTEXT'

            WHEN lower(coalesce(s.main_label, '')) LIKE 'or %'
              OR lower(coalesce(s.main_label, '')) LIKE '% or %intake%'
              OR lower(coalesce(s.main_label, '')) LIKE 'pacu %'
              OR lower(coalesce(s.main_label, '')) LIKE '%pacu%intake%'
              OR lower(coalesce(s.main_label, '')) LIKE '%cath lab%'
              OR lower(coalesce(s.main_label, '')) LIKE '%pre-admission%'
                THEN 'EXCLUDE_PERIOPERATIVE_SUMMARY'

            ELSE 'ICU_RECORDED'
        END AS input_context_class

    FROM water_valid w
    LEFT JOIN input_segment_context s
        ON w.stay_id = s.stay_id
       AND w.orderid_key = s.orderid_key
       AND w.starttime = s.starttime
       AND w.endtime = s.endtime
    """
)

unresolved_input_context_n = con.execute(
    """
    SELECT COUNT(*)
    FROM water_segments_classified
    WHERE input_context_class = 'UNRESOLVED_CONTEXT'
    """
).fetchone()[0]

if unresolved_input_context_n:
    raise RuntimeError(
        f"{unresolved_input_context_n} Water segments have unresolved "
        "INPUTEVENTS order context."
    )

con.execute(
    """
    CREATE TABLE icu_water_segments AS
    SELECT *
    FROM water_segments_classified
    WHERE input_context_class = 'ICU_RECORDED'
    """
)

con.execute(
    """
    CREATE TABLE input_patient_observability AS
    SELECT
        c.stay_id,
        CASE
            WHEN COUNT(w.stay_id) > 0 THEN 1
            ELSE 0
        END AS fb_input_available_flag,
        COUNT(w.stay_id) AS fb_input_segments_24h_n,
        SUM(
            w.amount_ml
            * greatest(
                0,
                date_diff(
                    'second',
                    greatest(
                        w.starttime,
                        c.postoperative_time_zero
                    ),
                    least(
                        w.endtime,
                        c.analytical_end_time
                    )
                )
            )
            / nullif(w.duration_seconds, 0)
        ) AS fb_icu_input_ml_24h
    FROM cohort c
    LEFT JOIN icu_water_segments w
        ON c.stay_id = w.stay_id
    GROUP BY c.stay_id
    """
)

input_available_n = con.execute(
    """
    SELECT COUNT(*)
    FROM input_patient_observability
    WHERE fb_input_available_flag = 1
    """
).fetchone()[0]

if input_available_n != EXPECTED_INPUT_AVAILABLE_N:
    raise RuntimeError(
        f"Unexpected ICU Water availability: {input_available_n}; "
        f"expected {EXPECTED_INPUT_AVAILABLE_N}."
    )


# =============================================================================
# Chest / mediastinal source points and right-boundary intervals
# =============================================================================

con.execute(
    f"""
    CREATE TABLE chest_raw AS
    SELECT
        c.stay_id,
        c.postoperative_time_zero,
        c.analytical_end_time,
        c.anchor_search_end_time,
        oe.charttime,
        oe.itemid::BIGINT AS itemid,
        di.label,
        oe.value::DOUBLE AS value_ml,
        oe.valueuom,
        CASE
            WHEN oe.charttime < c.analytical_end_time THEN 1
            ELSE 0
        END AS core_horizon_flag
    FROM read_csv_auto(
        '{qpath(OUTPUTEVENTS_FILE)}',
        header=true,
        all_varchar=false
    ) oe
    INNER JOIN cohort c
        ON oe.stay_id = c.stay_id
       AND oe.charttime >= c.postoperative_time_zero
       AND oe.charttime <= c.anchor_search_end_time
    LEFT JOIN d_items di
        ON oe.itemid = di.itemid
    WHERE oe.itemid IN ({CHEST_SQL})
      AND oe.value IS NOT NULL
      AND oe.value >= 0
      AND lower(replace(trim(coalesce(oe.valueuom, '')), ' ', ''))
            IN ('ml', 'milliliter', 'milliliters')
    """
)

con.execute(
    """
    CREATE TABLE chest_points AS
    SELECT
        stay_id,
        charttime,
        itemid,
        SUM(value_ml) AS chest_value_ml,
        COUNT(*) AS source_rows_n,
        any_value(label) AS label,
        max(core_horizon_flag) AS core_horizon_flag
    FROM chest_raw
    GROUP BY stay_id, charttime, itemid
    """
)

con.execute(
    """
    CREATE TABLE chest_core_ordered AS
    SELECT
        p.*,
        c.postoperative_time_zero,
        c.analytical_end_time,
        lag(p.charttime) OVER (
            PARTITION BY p.stay_id, p.itemid
            ORDER BY p.charttime
        ) AS previous_charttime
    FROM chest_points p
    INNER JOIN cohort c
        ON p.stay_id = c.stay_id
    WHERE p.core_horizon_flag = 1
    """
)

con.execute(
    """
    CREATE TABLE chest_core_intervals AS
    SELECT
        stay_id,
        itemid,
        label,

        CASE
            WHEN charttime = postoperative_time_zero THEN NULL
            WHEN previous_charttime IS NULL
                THEN postoperative_time_zero
            ELSE previous_charttime
        END AS interval_start,

        CASE
            WHEN charttime = postoperative_time_zero THEN NULL
            ELSE charttime
        END AS interval_end,

        CASE
            WHEN charttime = postoperative_time_zero THEN NULL
            ELSE chest_value_ml
        END AS interval_volume_ml,

        CASE
            WHEN charttime = postoperative_time_zero THEN chest_value_ml
            ELSE 0
        END AS exact_t0_boundary_ml,

        charttime AS event_time,
        0 AS anchor_flag

    FROM chest_core_ordered
    """
)

con.execute(
    """
    CREATE TABLE chest_last_core AS
    SELECT
        stay_id,
        itemid,
        max(charttime) AS last_core_charttime
    FROM chest_core_ordered
    GROUP BY stay_id, itemid
    """
)

con.execute(
    """
    CREATE TABLE chest_anchor_intervals AS
    SELECT
        l.stay_id,
        l.itemid,
        di.label,
        l.last_core_charttime AS interval_start,
        p.charttime AS interval_end,
        p.chest_value_ml AS interval_volume_ml,
        0.0 AS exact_t0_boundary_ml,
        p.charttime AS event_time,
        1 AS anchor_flag

    FROM chest_last_core l

    INNER JOIN LATERAL (
        SELECT
            x.charttime,
            x.chest_value_ml
        FROM chest_points x
        INNER JOIN cohort c
            ON x.stay_id = c.stay_id
        WHERE x.stay_id = l.stay_id
          AND x.itemid = l.itemid
          AND x.core_horizon_flag = 0
          AND x.charttime >= c.analytical_end_time
          AND x.charttime <= c.anchor_search_end_time
          AND x.charttime <= c.observation_end_time
        ORDER BY x.charttime
        LIMIT 1
    ) p ON true

    LEFT JOIN d_items di
        ON l.itemid = di.itemid

    INNER JOIN cohort c
        ON l.stay_id = c.stay_id

    WHERE l.last_core_charttime < c.analytical_end_time
    """
)

con.execute(
    """
    CREATE TABLE chest_intervals AS
    SELECT *
    FROM chest_core_intervals
    WHERE interval_start IS NOT NULL
      AND interval_end > interval_start
      AND interval_volume_ml IS NOT NULL

    UNION ALL

    SELECT *
    FROM chest_anchor_intervals
    WHERE interval_end > interval_start
      AND interval_volume_ml IS NOT NULL
    """
)


# =============================================================================
# Sparse non-chest outputs: point-time primary representation
# =============================================================================

con.execute(
    f"""
    CREATE TABLE point_output_raw AS
    SELECT
        c.stay_id,
        c.postoperative_time_zero,
        c.analytical_end_time,
        oe.charttime,
        oe.itemid::BIGINT AS itemid,
        di.label,
        oe.value::DOUBLE AS value_ml,

        CASE
            WHEN oe.itemid IN ({GI_SQL})
                THEN 'GI_COLLECTION'
            WHEN oe.itemid IN ({OTHER_SQL})
                THEN 'OTHER_DRAIN'
            WHEN oe.itemid IN ({DISCRETE_SQL})
                THEN 'DISCRETE_GI_LOSS'
            ELSE 'UNEXPECTED'
        END AS output_class

    FROM read_csv_auto(
        '{qpath(OUTPUTEVENTS_FILE)}',
        header=true,
        all_varchar=false
    ) oe
    INNER JOIN cohort c
        ON oe.stay_id = c.stay_id
       AND oe.charttime >= c.postoperative_time_zero
       AND oe.charttime < c.analytical_end_time
    LEFT JOIN d_items di
        ON oe.itemid = di.itemid
    WHERE oe.itemid IN ({POINT_OUTPUT_SQL})
      AND oe.value IS NOT NULL
      AND oe.value >= 0
      AND lower(replace(trim(coalesce(oe.valueuom, '')), ' ', ''))
            IN ('ml', 'milliliter', 'milliliters')
    """
)

# Collection devices exactly at t0 are boundary values and are excluded.
# Emesis and stool are discrete events and remain included at t0.
con.execute(
    """
    CREATE TABLE point_output_events AS
    SELECT
        stay_id,
        charttime,
        itemid,
        any_value(label) AS label,
        output_class,
        SUM(value_ml) AS value_ml,
        COUNT(*) AS source_rows_n
    FROM point_output_raw
    WHERE (
        output_class = 'DISCRETE_GI_LOSS'
        OR charttime > postoperative_time_zero
    )
    GROUP BY
        stay_id,
        charttime,
        itemid,
        output_class
    """
)

con.execute(
    """
    CREATE TABLE point_output_boundary AS
    SELECT
        stay_id,
        output_class,
        SUM(value_ml) AS exact_t0_boundary_ml,
        COUNT(*) AS exact_t0_points_n
    FROM point_output_raw
    WHERE output_class IN ('GI_COLLECTION', 'OTHER_DRAIN')
      AND charttime = postoperative_time_zero
    GROUP BY stay_id, output_class
    """
)


# =============================================================================
# Renal replacement therapy
# =============================================================================

# Include a 12-hour pre-t0 lookback so a PD output after t0 can be paired with
# its preceding instillation.
con.execute(
    f"""
    CREATE TABLE rrt_raw AS
    SELECT
        c.stay_id,
        c.postoperative_time_zero,
        c.analytical_end_time,
        c.crrt_source_end_time,
        ce.charttime,
        ce.itemid::BIGINT AS itemid,
        di.label,
        ce.valuenum::DOUBLE AS value_ml,
        ce.valueuom
    FROM read_csv_auto(
        '{qpath(CHARTEVENTS_FILE)}',
        header=true,
        all_varchar=false
    ) ce
    INNER JOIN cohort c
        ON ce.stay_id = c.stay_id
       AND ce.charttime
            >= c.postoperative_time_zero
                - INTERVAL '{PD_PAIR_MAX_HOURS} hours'
       AND (
            (
                ce.itemid = {CRRT_PATIENT_REMOVAL_ITEMID}
                AND ce.charttime <= c.crrt_source_end_time
            )
            OR
            (
                ce.itemid <> {CRRT_PATIENT_REMOVAL_ITEMID}
                AND ce.charttime < c.analytical_end_time
            )
       )
    LEFT JOIN d_items di
        ON ce.itemid = di.itemid
    WHERE ce.itemid IN ({RRT_SQL})
      AND ce.valuenum IS NOT NULL
    """
)

con.execute(
    """
    CREATE TABLE rrt_points AS
    SELECT
        stay_id,
        postoperative_time_zero,
        analytical_end_time,
        crrt_source_end_time,
        charttime,
        itemid,
        any_value(label) AS label,
        median(value_ml) AS value_ml,
        COUNT(*) AS source_rows_n,
        string_agg(
            DISTINCT coalesce(valueuom, '<NULL>'),
            ' | '
        ) AS units_seen
    FROM rrt_raw
    GROUP BY
        stay_id,
        postoperative_time_zero,
        analytical_end_time,
        crrt_source_end_time,
        charttime,
        itemid
    """
)

# CRRT primary source.
con.execute(
    f"""
    CREATE TABLE crrt_primary_points AS
    SELECT *
    FROM rrt_points
    WHERE itemid = {CRRT_PATIENT_REMOVAL_ITEMID}
      -- A point exactly at t0 represents the preceding preoperative hour and
      -- contributes no post-t0 exposure. A point after the endpoint is retained
      -- only if its preceding-hour interval still overlaps the analytical horizon.
      AND charttime > postoperative_time_zero
      AND charttime <= crrt_source_end_time
      AND charttime - INTERVAL '1 hour' < analytical_end_time
      AND value_ml >= 0
    """
)

con.execute(
    """
    CREATE TABLE crrt_intervals AS
    SELECT
        stay_id,
        charttime - INTERVAL '1 hour' AS interval_start,
        charttime AS interval_end,
        value_ml AS interval_volume_ml,
        charttime AS event_time
    FROM crrt_primary_points
    """
)

# Auxiliary-only CRRT patients are known to have RRT but lack the canonical
# patient-removal measure.
con.execute(
    f"""
    CREATE TABLE crrt_auxiliary_only_stays AS
    SELECT DISTINCT a.stay_id
    FROM rrt_points a
    WHERE a.itemid = {CRRT_ULTRAFILTRATE_ITEMID}
      AND a.charttime >= a.postoperative_time_zero
      AND a.charttime < a.analytical_end_time
      AND NOT EXISTS (
          SELECT 1
          FROM crrt_primary_points p
          WHERE p.stay_id = a.stay_id
      )
    """
)

# Intermittent hemodialysis point events.
con.execute(
    f"""
    CREATE TABLE ihd_events AS
    SELECT
        stay_id,
        charttime,
        value_ml
    FROM rrt_points
    WHERE itemid = {IHD_OUTPUT_ITEMID}
      AND charttime >= postoperative_time_zero
      AND charttime < analytical_end_time
      AND value_ml >= 0
    """
)

# Peritoneal dialysis pairing.
con.execute(
    f"""
    CREATE TABLE pd_in AS
    SELECT *
    FROM rrt_points
    WHERE itemid = {PD_VOLUME_IN_ITEMID}
      AND value_ml >= 0
    """
)

con.execute(
    f"""
    CREATE TABLE pd_out AS
    SELECT *
    FROM rrt_points
    WHERE itemid = {PD_VOLUME_OUT_ITEMID}
      AND charttime >= postoperative_time_zero
      AND charttime < analytical_end_time
      AND value_ml >= 0
    """
)

con.execute(
    f"""
    CREATE TABLE pd_pair_candidates AS
    SELECT
        i.stay_id,
        i.charttime AS volume_in_time,
        i.value_ml AS volume_in_ml,
        o.charttime AS volume_out_time,
        o.value_ml AS volume_out_ml,
        date_diff(
            'second',
            i.charttime,
            o.charttime
        ) / 3600.0 AS elapsed_hours,

        row_number() OVER (
            PARTITION BY i.stay_id, i.charttime
            ORDER BY o.charttime
        ) AS rank_for_in,

        row_number() OVER (
            PARTITION BY o.stay_id, o.charttime
            ORDER BY i.charttime DESC
        ) AS rank_for_out

    FROM pd_in i
    INNER JOIN pd_out o
        ON i.stay_id = o.stay_id
       AND o.charttime >= i.charttime
       AND o.charttime
            <= i.charttime + INTERVAL '{PD_PAIR_MAX_HOURS} hours'
    """
)

con.execute(
    """
    CREATE TABLE pd_pairs AS
    SELECT
        stay_id,
        volume_in_time,
        volume_in_ml,
        volume_out_time,
        volume_out_ml,
        elapsed_hours,
        volume_out_ml - volume_in_ml AS net_removed_ml
    FROM pd_pair_candidates
    WHERE rank_for_in = 1
      AND rank_for_out = 1
    """
)

con.execute(
    """
    CREATE TABLE pd_unmatched_out AS
    SELECT o.*
    FROM pd_out o
    WHERE NOT EXISTS (
        SELECT 1
        FROM pd_pairs p
        WHERE p.stay_id = o.stay_id
          AND p.volume_out_time = o.charttime
    )
    """
)

con.execute(
    """
    CREATE TABLE pd_unmatched_in_horizon AS
    SELECT i.*
    FROM pd_in i
    WHERE i.charttime >= i.postoperative_time_zero
      AND i.charttime < i.analytical_end_time
      AND NOT EXISTS (
          SELECT 1
          FROM pd_pairs p
          WHERE p.stay_id = i.stay_id
            AND p.volume_in_time = i.charttime
      )
    """
)

con.execute(
    """
    CREATE TABLE pd_events AS
    SELECT
        stay_id,
        volume_out_time AS charttime,
        net_removed_ml
    FROM pd_pairs
    """
)

con.execute(
    """
    CREATE TABLE rrt_incomplete_stays AS
    SELECT
        stay_id,
        string_agg(DISTINCT reason, ' | ') AS rrt_incomplete_reason
    FROM (
        SELECT
            stay_id,
            'CRRT_AUXILIARY_226457_WITHOUT_224191' AS reason
        FROM crrt_auxiliary_only_stays

        UNION ALL

        SELECT
            stay_id,
            'PD_UNMATCHED_VOLUME_OUT' AS reason
        FROM pd_unmatched_out

        UNION ALL

        SELECT
            stay_id,
            'PD_UNMATCHED_VOLUME_IN' AS reason
        FROM pd_unmatched_in_horizon
    )
    GROUP BY stay_id
    """
)


# =============================================================================
# Window component allocation
# =============================================================================

def create_window_components(prefix: str, grid_table: str) -> None:
    # ICU Water input.
    con.execute(
        f"""
        CREATE TABLE {prefix}_input AS
        SELECT
            g.stay_id,
            g.window_index,
            SUM(
                w.amount_ml
                * greatest(
                    0,
                    date_diff(
                        'second',
                        greatest(w.starttime, g.window_start_time),
                        least(w.endtime, g.eligible_window_end_time)
                    )
                )
                / nullif(w.duration_seconds, 0)
            ) AS input_ml,
            COUNT(w.starttime) AS contributing_segments_n
        FROM {grid_table} g
        LEFT JOIN icu_water_segments w
            ON w.stay_id = g.stay_id
           AND g.eligible_minutes > 0
           AND w.endtime > g.window_start_time
           AND w.starttime < g.eligible_window_end_time
        GROUP BY g.stay_id, g.window_index
        """
    )

    # Chest primary interval allocation, including right-boundary anchors.
    con.execute(
        f"""
        CREATE TABLE {prefix}_chest AS
        SELECT
            g.stay_id,
            g.window_index,

            SUM(
                i.interval_volume_ml
                * greatest(
                    0,
                    date_diff(
                        'second',
                        greatest(i.interval_start, g.window_start_time),
                        least(i.interval_end, g.eligible_window_end_time)
                    )
                )
                / nullif(
                    date_diff(
                        'second',
                        i.interval_start,
                        i.interval_end
                    ),
                    0
                )
            ) AS chest_interval_ml,

            SUM(
                i.interval_volume_ml
                * greatest(
                    0,
                    date_diff(
                        'second',
                        greatest(i.interval_start, g.window_start_time),
                        least(i.interval_end, g.eligible_window_end_time)
                    )
                )
                / nullif(
                    date_diff(
                        'second',
                        i.interval_start,
                        i.interval_end
                    ),
                    0
                )
            ) FILTER (
                WHERE i.anchor_flag = 0
            ) AS chest_no_anchor_ml,

            SUM(i.interval_volume_ml) FILTER (
                WHERE i.anchor_flag = 0
                  AND i.event_time >= g.window_start_time
                  AND i.event_time < g.eligible_window_end_time
            ) AS chest_event_time_ml,

            SUM(
                i.interval_volume_ml
                * greatest(
                    0,
                    date_diff(
                        'second',
                        greatest(i.interval_start, g.window_start_time),
                        least(i.interval_end, g.eligible_window_end_time)
                    )
                )
                / nullif(
                    date_diff(
                        'second',
                        i.interval_start,
                        i.interval_end
                    ),
                    0
                )
            ) FILTER (
                WHERE i.anchor_flag = 1
            ) AS chest_anchor_attributed_ml,

            COUNT(i.interval_start) FILTER (
                WHERE i.anchor_flag = 0
            ) AS chest_core_intervals_n,

            COUNT(i.interval_start) FILTER (
                WHERE i.anchor_flag = 1
            ) AS chest_anchor_intervals_n

        FROM {grid_table} g
        LEFT JOIN chest_intervals i
            ON i.stay_id = g.stay_id
           AND g.eligible_minutes > 0
           AND i.interval_end > g.window_start_time
           AND i.interval_start < g.eligible_window_end_time
        GROUP BY g.stay_id, g.window_index
        """
    )

    # Sparse non-chest outputs assigned to charttime.
    con.execute(
        f"""
        CREATE TABLE {prefix}_point_output AS
        SELECT
            g.stay_id,
            g.window_index,

            SUM(p.value_ml) FILTER (
                WHERE p.output_class IN (
                    'GI_COLLECTION',
                    'DISCRETE_GI_LOSS'
                )
            ) AS gi_output_ml,

            SUM(p.value_ml) FILTER (
                WHERE p.output_class = 'OTHER_DRAIN'
            ) AS other_drain_output_ml,

            COUNT(p.charttime) FILTER (
                WHERE p.output_class IN (
                    'GI_COLLECTION',
                    'DISCRETE_GI_LOSS'
                )
            ) AS gi_output_events_n,

            COUNT(p.charttime) FILTER (
                WHERE p.output_class = 'OTHER_DRAIN'
            ) AS other_drain_events_n

        FROM {grid_table} g
        LEFT JOIN point_output_events p
            ON p.stay_id = g.stay_id
           AND g.eligible_minutes > 0
           AND p.charttime >= g.window_start_time
           AND p.charttime < g.eligible_window_end_time
        GROUP BY g.stay_id, g.window_index
        """
    )

    # CRRT preceding-one-hour intervals.
    con.execute(
        f"""
        CREATE TABLE {prefix}_crrt AS
        SELECT
            g.stay_id,
            g.window_index,

            SUM(
                r.interval_volume_ml
                * greatest(
                    0,
                    date_diff(
                        'second',
                        greatest(r.interval_start, g.window_start_time),
                        least(r.interval_end, g.eligible_window_end_time)
                    )
                )
                / nullif(
                    date_diff(
                        'second',
                        r.interval_start,
                        r.interval_end
                    ),
                    0
                )
            ) AS crrt_removal_ml,

            COUNT(r.interval_start) AS crrt_intervals_n

        FROM {grid_table} g
        LEFT JOIN crrt_intervals r
            ON r.stay_id = g.stay_id
           AND g.eligible_minutes > 0
           AND r.interval_end > g.window_start_time
           AND r.interval_start < g.eligible_window_end_time
        GROUP BY g.stay_id, g.window_index
        """
    )

    # IHD and paired PD point events are aggregated separately to prevent
    # cross-multiplication when both modalities have more than one event in
    # the same stay/window.
    con.execute(
        f"""
        CREATE TABLE {prefix}_ihd AS
        SELECT
            g.stay_id,
            g.window_index,
            SUM(i.value_ml) AS ihd_removal_ml,
            COUNT(i.charttime) AS ihd_events_n
        FROM {grid_table} g
        LEFT JOIN ihd_events i
            ON i.stay_id = g.stay_id
           AND g.eligible_minutes > 0
           AND i.charttime >= g.window_start_time
           AND i.charttime < g.eligible_window_end_time
        GROUP BY g.stay_id, g.window_index
        """
    )

    con.execute(
        f"""
        CREATE TABLE {prefix}_pd AS
        SELECT
            g.stay_id,
            g.window_index,
            SUM(p.net_removed_ml) AS pd_net_removal_ml,
            COUNT(p.charttime) AS pd_paired_events_n
        FROM {grid_table} g
        LEFT JOIN pd_events p
            ON p.stay_id = g.stay_id
           AND g.eligible_minutes > 0
           AND p.charttime >= g.window_start_time
           AND p.charttime < g.eligible_window_end_time
        GROUP BY g.stay_id, g.window_index
        """
    )


create_window_components("hourly", "urine_hourly")
create_window_components("six", "urine_six")


# =============================================================================
# Primary window tables
# =============================================================================

def create_window_raw(output_table: str, grid_table: str, prefix: str) -> None:
    con.execute(
        f"""
        CREATE TABLE {output_table} AS
        SELECT
            g.*,

            ip.fb_input_available_flag,
            ip.fb_input_segments_24h_n,
            ip.fb_icu_input_ml_24h,

            CASE
                WHEN g.eligible_minutes <= 0 THEN NULL
                WHEN ip.fb_input_available_flag = 0 THEN NULL
                ELSE coalesce(inp.input_ml, 0.0)
            END AS fb_input_ml,

            coalesce(inp.contributing_segments_n, 0)
                AS fb_input_contributing_segments_n,

            coalesce(ch.chest_interval_ml, 0.0)
                AS fb_chest_mediastinal_output_ml,

            coalesce(ch.chest_no_anchor_ml, 0.0)
                AS fb_chest_output_no_anchor_sensitivity_ml,

            coalesce(ch.chest_event_time_ml, 0.0)
                AS fb_chest_output_event_time_sensitivity_ml,

            coalesce(ch.chest_anchor_attributed_ml, 0.0)
                AS fb_chest_anchor_attributed_ml,

            coalesce(ch.chest_core_intervals_n, 0)
                AS fb_chest_core_intervals_n,

            coalesce(ch.chest_anchor_intervals_n, 0)
                AS fb_chest_anchor_intervals_n,

            coalesce(po.gi_output_ml, 0.0)
                AS fb_gi_stool_ostomy_output_ml,

            coalesce(po.other_drain_output_ml, 0.0)
                AS fb_other_drain_output_ml,

            coalesce(po.gi_output_events_n, 0)
                AS fb_gi_stool_ostomy_events_n,

            coalesce(po.other_drain_events_n, 0)
                AS fb_other_drain_events_n,

            coalesce(cr.crrt_removal_ml, 0.0)
                AS fb_crrt_removal_ml,

            coalesce(cr.crrt_intervals_n, 0)
                AS fb_crrt_intervals_n,

            coalesce(ihd.ihd_removal_ml, 0.0)
                AS fb_ihd_removal_ml,

            coalesce(ihd.ihd_events_n, 0)
                AS fb_ihd_events_n,

            coalesce(pd.pd_net_removal_ml, 0.0)
                AS fb_pd_net_removal_ml,

            coalesce(pd.pd_paired_events_n, 0)
                AS fb_pd_paired_events_n,

            CASE
                WHEN ri.stay_id IS NOT NULL THEN 1
                ELSE 0
            END AS fb_rrt_incomplete_flag,

            ri.rrt_incomplete_reason AS fb_rrt_incomplete_reason,

            coalesce(cr.crrt_removal_ml, 0.0)
                + coalesce(ihd.ihd_removal_ml, 0.0)
                + coalesce(pd.pd_net_removal_ml, 0.0)
                AS fb_rrt_net_removal_ml,

            coalesce(ch.chest_interval_ml, 0.0)
                + coalesce(po.gi_output_ml, 0.0)
                + coalesce(po.other_drain_output_ml, 0.0)
                + coalesce(cr.crrt_removal_ml, 0.0)
                + coalesce(ihd.ihd_removal_ml, 0.0)
                + coalesce(pd.pd_net_removal_ml, 0.0)
                AS fb_nonurinary_output_ml,

            -- Sensitivity allowing any positive urine-interval coverage.
            -- This preserves the v2 "recorded components" representation but
            -- must not be interpreted as a complete-window fluid balance.
            CASE
                WHEN g.eligible_minutes <= 0 THEN NULL
                WHEN g.uo_interval_ml_observed IS NULL THEN NULL
                WHEN ri.stay_id IS NOT NULL THEN NULL
                ELSE g.uo_interval_ml_observed
                    + coalesce(ch.chest_interval_ml, 0.0)
                    + coalesce(po.gi_output_ml, 0.0)
                    + coalesce(po.other_drain_output_ml, 0.0)
                    + coalesce(cr.crrt_removal_ml, 0.0)
                    + coalesce(ihd.ihd_removal_ml, 0.0)
                    + coalesce(pd.pd_net_removal_ml, 0.0)
            END AS fb_total_output_ml_partial_urine_sensitivity,

            CASE
                WHEN g.eligible_minutes <= 0 THEN NULL
                WHEN ip.fb_input_available_flag = 0 THEN NULL
                WHEN g.uo_interval_ml_observed IS NULL THEN NULL
                WHEN ri.stay_id IS NOT NULL THEN NULL
                ELSE coalesce(inp.input_ml, 0.0)
                    - (
                        g.uo_interval_ml_observed
                        + coalesce(ch.chest_interval_ml, 0.0)
                        + coalesce(po.gi_output_ml, 0.0)
                        + coalesce(po.other_drain_output_ml, 0.0)
                        + coalesce(cr.crrt_removal_ml, 0.0)
                        + coalesce(ihd.ihd_removal_ml, 0.0)
                        + coalesce(pd.pd_net_removal_ml, 0.0)
                    )
            END AS fb_net_balance_ml_partial_urine_sensitivity,

            CASE
                WHEN g.eligible_minutes <= 0 THEN 0
                WHEN ip.fb_input_available_flag = 0 THEN 0
                WHEN g.uo_interval_ml_observed IS NULL THEN 0
                WHEN ri.stay_id IS NOT NULL THEN 0
                ELSE 1
            END AS fb_partial_urine_sensitivity_available_flag,

            -- Primary total output requires at least 50% urine-interval
            -- coverage for the eligible portion of the current window. This
            -- follows the frozen 01_07 observability categories and excludes
            -- minimally observed windows without imputing uncovered urine.
            CASE
                WHEN g.eligible_minutes <= 0 THEN NULL
                WHEN g.uo_primary_available_flag <> 1 THEN NULL
                WHEN coalesce(g.uo_interval_coverage_fraction, 0.0) < 0.5
                    THEN NULL
                WHEN ri.stay_id IS NOT NULL THEN NULL
                ELSE g.uo_interval_ml_observed
                    + coalesce(ch.chest_interval_ml, 0.0)
                    + coalesce(po.gi_output_ml, 0.0)
                    + coalesce(po.other_drain_output_ml, 0.0)
                    + coalesce(cr.crrt_removal_ml, 0.0)
                    + coalesce(ihd.ihd_removal_ml, 0.0)
                    + coalesce(pd.pd_net_removal_ml, 0.0)
            END AS fb_total_output_ml,

            CASE
                WHEN g.eligible_minutes <= 0 THEN NULL
                WHEN ip.fb_input_available_flag = 0 THEN NULL
                WHEN g.uo_primary_available_flag <> 1 THEN NULL
                WHEN coalesce(g.uo_interval_coverage_fraction, 0.0) < 0.5
                    THEN NULL
                WHEN ri.stay_id IS NOT NULL THEN NULL
                ELSE coalesce(inp.input_ml, 0.0)
                    - (
                        g.uo_interval_ml_observed
                        + coalesce(ch.chest_interval_ml, 0.0)
                        + coalesce(po.gi_output_ml, 0.0)
                        + coalesce(po.other_drain_output_ml, 0.0)
                        + coalesce(cr.crrt_removal_ml, 0.0)
                        + coalesce(ihd.ihd_removal_ml, 0.0)
                        + coalesce(pd.pd_net_removal_ml, 0.0)
                    )
            END AS fb_net_balance_ml,

            CASE
                WHEN g.eligible_minutes <= 0 THEN 0
                WHEN ip.fb_input_available_flag = 0 THEN 0
                WHEN g.uo_primary_available_flag <> 1 THEN 0
                WHEN coalesce(g.uo_interval_coverage_fraction, 0.0) < 0.5
                    THEN 0
                WHEN ri.stay_id IS NOT NULL THEN 0
                ELSE 1
            END AS fb_window_available_flag,

            CASE
                WHEN g.eligible_minutes <= 0 THEN 0
                WHEN g.uo_primary_available_flag <> 1 THEN 0
                WHEN coalesce(g.uo_interval_coverage_fraction, 0.0) < 0.5
                    THEN 0
                ELSE 1
            END AS fb_primary_urine_coverage_ge50_flag,

            CASE
                WHEN g.eligible_minutes <= 0
                    THEN 'NOT_OBSERVABLE'
                WHEN ip.fb_input_available_flag = 0
                    THEN 'INPUT_STRUCTURALLY_MISSING'
                WHEN ri.stay_id IS NOT NULL
                    THEN 'RRT_OUTPUT_INCOMPLETE'
                WHEN g.uo_irrigation_affected_window_flag = 1
                    THEN 'URINE_IRRIGATION_AFFECTED'
                WHEN g.uo_primary_available_flag <> 1
                    THEN 'URINE_OUTPUT_UNAVAILABLE'
                WHEN coalesce(g.uo_interval_coverage_fraction, 0.0) < 0.5
                    THEN 'URINE_INTERVAL_COVERAGE_LT50'
                WHEN g.uo_interval_coverage_fraction < 0.999
                    THEN 'AVAILABLE_URINE_COVERAGE_GE50_LT100'
                ELSE 'AVAILABLE_FULL_URINE_COVERAGE'
            END AS fb_measurement_status,

            CASE
                WHEN g.eligible_minutes <= 0 THEN NULL
                WHEN ip.fb_input_available_flag = 0 THEN NULL
                WHEN g.uo_primary_available_flag <> 1 THEN NULL
                WHEN coalesce(g.uo_interval_coverage_fraction, 0.0) < 0.5
                    THEN NULL
                WHEN ri.stay_id IS NOT NULL THEN NULL
                WHEN g.uo_weight_kg IS NULL THEN NULL
                ELSE (
                    coalesce(inp.input_ml, 0.0)
                    - (
                        g.uo_interval_ml_observed
                        + coalesce(ch.chest_interval_ml, 0.0)
                        + coalesce(po.gi_output_ml, 0.0)
                        + coalesce(po.other_drain_output_ml, 0.0)
                        + coalesce(cr.crrt_removal_ml, 0.0)
                        + coalesce(ihd.ihd_removal_ml, 0.0)
                        + coalesce(pd.pd_net_removal_ml, 0.0)
                    )
                ) / g.uo_weight_kg
            END AS fb_net_balance_ml_per_kg,

            CASE
                WHEN g.eligible_minutes <= 0 THEN NULL
                WHEN ip.fb_input_available_flag = 0 THEN NULL
                WHEN g.uo_interval_ml_observed IS NULL THEN NULL
                WHEN ri.stay_id IS NOT NULL THEN NULL
                WHEN g.uo_weight_kg IS NULL THEN NULL
                ELSE (
                    coalesce(inp.input_ml, 0.0)
                    - (
                        g.uo_interval_ml_observed
                        + coalesce(ch.chest_interval_ml, 0.0)
                        + coalesce(po.gi_output_ml, 0.0)
                        + coalesce(po.other_drain_output_ml, 0.0)
                        + coalesce(cr.crrt_removal_ml, 0.0)
                        + coalesce(ihd.ihd_removal_ml, 0.0)
                        + coalesce(pd.pd_net_removal_ml, 0.0)
                    )
                ) / g.uo_weight_kg
            END AS fb_net_balance_ml_per_kg_partial_urine_sensitivity

        FROM {grid_table} g

        INNER JOIN input_patient_observability ip
            ON g.stay_id = ip.stay_id

        INNER JOIN {prefix}_input inp
            ON g.stay_id = inp.stay_id
           AND g.window_index = inp.window_index

        INNER JOIN {prefix}_chest ch
            ON g.stay_id = ch.stay_id
           AND g.window_index = ch.window_index

        INNER JOIN {prefix}_point_output po
            ON g.stay_id = po.stay_id
           AND g.window_index = po.window_index

        INNER JOIN {prefix}_crrt cr
            ON g.stay_id = cr.stay_id
           AND g.window_index = cr.window_index

        INNER JOIN {prefix}_ihd ihd
            ON g.stay_id = ihd.stay_id
           AND g.window_index = ihd.window_index

        INNER JOIN {prefix}_pd pd
            ON g.stay_id = pd.stay_id
           AND g.window_index = pd.window_index

        LEFT JOIN rrt_incomplete_stays ri
            ON g.stay_id = ri.stay_id
        """
    )


create_window_raw("hourly_raw_output", "urine_hourly", "hourly")
create_window_raw("six_raw_output", "urine_six", "six")


# =============================================================================
# Cumulative trajectories
# =============================================================================

def create_cumulative(output_table: str, raw_table: str) -> None:
    con.execute(
        f"""
        CREATE TABLE {output_table} AS
        WITH c AS (
            SELECT
                r.*,

                min(fb_window_available_flag) OVER (
                    PARTITION BY stay_id
                    ORDER BY window_index
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS fb_cumulative_available_flag,

                SUM(fb_input_ml) OVER (
                    PARTITION BY stay_id
                    ORDER BY window_index
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_input_raw,

                SUM(uo_interval_ml_observed) OVER (
                    PARTITION BY stay_id
                    ORDER BY window_index
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_urine_raw,

                SUM(fb_chest_mediastinal_output_ml) OVER (
                    PARTITION BY stay_id
                    ORDER BY window_index
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_chest_raw,

                SUM(fb_gi_stool_ostomy_output_ml) OVER (
                    PARTITION BY stay_id
                    ORDER BY window_index
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_gi_raw,

                SUM(fb_other_drain_output_ml) OVER (
                    PARTITION BY stay_id
                    ORDER BY window_index
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_other_drain_raw,

                SUM(fb_rrt_net_removal_ml) OVER (
                    PARTITION BY stay_id
                    ORDER BY window_index
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_rrt_raw,

                SUM(fb_total_output_ml) OVER (
                    PARTITION BY stay_id
                    ORDER BY window_index
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_output_raw,

                SUM(fb_net_balance_ml) OVER (
                    PARTITION BY stay_id
                    ORDER BY window_index
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS cumulative_balance_raw

            FROM {raw_table} r
        )
        SELECT
            c.* EXCLUDE (
                cumulative_input_raw,
                cumulative_urine_raw,
                cumulative_chest_raw,
                cumulative_gi_raw,
                cumulative_other_drain_raw,
                cumulative_rrt_raw,
                cumulative_output_raw,
                cumulative_balance_raw
            ),

            CASE
                WHEN fb_cumulative_available_flag = 1
                    THEN cumulative_input_raw
                ELSE NULL
            END AS fb_cumulative_input_ml,

            CASE
                WHEN fb_cumulative_available_flag = 1
                    THEN cumulative_urine_raw
                ELSE NULL
            END AS fb_cumulative_urine_output_ml,

            CASE
                WHEN fb_cumulative_available_flag = 1
                    THEN cumulative_chest_raw
                ELSE NULL
            END AS fb_cumulative_chest_output_ml,

            CASE
                WHEN fb_cumulative_available_flag = 1
                    THEN cumulative_gi_raw
                ELSE NULL
            END AS fb_cumulative_gi_stool_ostomy_output_ml,

            CASE
                WHEN fb_cumulative_available_flag = 1
                    THEN cumulative_other_drain_raw
                ELSE NULL
            END AS fb_cumulative_other_drain_output_ml,

            CASE
                WHEN fb_cumulative_available_flag = 1
                    THEN cumulative_rrt_raw
                ELSE NULL
            END AS fb_cumulative_rrt_net_removal_ml,

            CASE
                WHEN fb_cumulative_available_flag = 1
                    THEN cumulative_output_raw
                ELSE NULL
            END AS fb_cumulative_total_output_ml,

            CASE
                WHEN fb_cumulative_available_flag = 1
                    THEN cumulative_balance_raw
                ELSE NULL
            END AS fb_cumulative_net_balance_ml,

            CASE
                WHEN fb_cumulative_available_flag = 1
                 AND uo_weight_kg IS NOT NULL
                    THEN cumulative_balance_raw / uo_weight_kg
                ELSE NULL
            END AS fb_cumulative_net_balance_ml_per_kg,

            CASE
                WHEN fb_cumulative_available_flag = 1
                 AND uo_weight_kg IS NOT NULL
                    THEN cumulative_balance_raw / (10.0 * uo_weight_kg)
                ELSE NULL
            END AS fb_cumulative_fluid_accumulation_pct_weight

        FROM c
        """
    )


create_cumulative("hourly_cumulative", "hourly_raw_output")
create_cumulative("six_cumulative", "six_raw_output")


# =============================================================================
# Patient-level 24-hour summary repeated in both canonical grids
# =============================================================================

con.execute(
    """
    CREATE TABLE patient_24h AS
    SELECT
        stay_id,

        min(fb_window_available_flag) AS fb_24h_available_flag,

        min(fb_partial_urine_sensitivity_available_flag)
            AS fb_24h_partial_urine_sensitivity_available_flag,

        CASE
            WHEN min(fb_partial_urine_sensitivity_available_flag) = 1
                THEN SUM(fb_total_output_ml_partial_urine_sensitivity)
            ELSE NULL
        END AS fb_total_output_ml_24h_partial_urine_sensitivity,

        CASE
            WHEN min(fb_partial_urine_sensitivity_available_flag) = 1
                THEN SUM(fb_net_balance_ml_partial_urine_sensitivity)
            ELSE NULL
        END AS fb_net_balance_ml_24h_partial_urine_sensitivity,

        CASE
            WHEN min(fb_partial_urine_sensitivity_available_flag) = 1
             AND max(uo_weight_kg) IS NOT NULL
                THEN SUM(fb_net_balance_ml_partial_urine_sensitivity)
                    / max(uo_weight_kg)
            ELSE NULL
        END AS fb_net_balance_ml_per_kg_24h_partial_urine_sensitivity,

        CASE
            WHEN min(fb_partial_urine_sensitivity_available_flag) = 1
             AND max(uo_weight_kg) IS NOT NULL
                THEN SUM(fb_net_balance_ml_partial_urine_sensitivity)
                    / (10.0 * max(uo_weight_kg))
            ELSE NULL
        END AS fb_fluid_accumulation_pct_weight_24h_partial_urine_sensitivity,

        CASE
            WHEN min(fb_window_available_flag) = 1
                THEN SUM(fb_input_ml)
            ELSE NULL
        END AS fb_input_ml_24h,

        CASE
            WHEN min(fb_window_available_flag) = 1
                THEN SUM(uo_interval_ml_observed)
            ELSE NULL
        END AS fb_urine_output_ml_24h,

        CASE
            WHEN min(fb_window_available_flag) = 1
                THEN SUM(fb_chest_mediastinal_output_ml)
            ELSE NULL
        END AS fb_chest_mediastinal_output_ml_24h,

        CASE
            WHEN min(fb_window_available_flag) = 1
                THEN SUM(fb_gi_stool_ostomy_output_ml)
            ELSE NULL
        END AS fb_gi_stool_ostomy_output_ml_24h,

        CASE
            WHEN min(fb_window_available_flag) = 1
                THEN SUM(fb_other_drain_output_ml)
            ELSE NULL
        END AS fb_other_drain_output_ml_24h,

        CASE
            WHEN min(fb_window_available_flag) = 1
                THEN SUM(fb_rrt_net_removal_ml)
            ELSE NULL
        END AS fb_rrt_net_removal_ml_24h,

        CASE
            WHEN min(fb_window_available_flag) = 1
                THEN SUM(fb_total_output_ml)
            ELSE NULL
        END AS fb_total_output_ml_24h,

        CASE
            WHEN min(fb_window_available_flag) = 1
                THEN SUM(fb_net_balance_ml)
            ELSE NULL
        END AS fb_net_balance_ml_24h,

        CASE
            WHEN min(fb_window_available_flag) = 1
             AND max(uo_weight_kg) IS NOT NULL
                THEN SUM(fb_net_balance_ml) / max(uo_weight_kg)
            ELSE NULL
        END AS fb_net_balance_ml_per_kg_24h,

        CASE
            WHEN min(fb_window_available_flag) = 1
             AND max(uo_weight_kg) IS NOT NULL
                THEN SUM(fb_net_balance_ml)
                    / (10.0 * max(uo_weight_kg))
            ELSE NULL
        END AS fb_fluid_accumulation_pct_weight_24h,

        max(fb_input_available_flag) AS fb_input_available_24h_flag,
        max(fb_rrt_incomplete_flag) AS fb_rrt_incomplete_24h_flag,
        max(uo_irrigation_any_24h_flag) AS fb_urine_irrigation_24h_flag,

        CASE
            WHEN min(fb_window_available_flag) = 1
                THEN 'AVAILABLE'
            WHEN max(fb_input_available_flag) = 0
                THEN 'INPUT_STRUCTURALLY_MISSING'
            WHEN max(fb_rrt_incomplete_flag) = 1
                THEN 'RRT_OUTPUT_INCOMPLETE'
            WHEN max(uo_irrigation_any_24h_flag) = 1
                THEN 'URINE_IRRIGATION_AFFECTED'
            WHEN max(
                CASE
                    WHEN uo_primary_available_flag = 1
                     AND coalesce(
                            uo_interval_coverage_fraction,
                            0.0
                         ) < 0.5
                        THEN 1
                    ELSE 0
                END
            ) = 1
                THEN 'URINE_INTERVAL_COVERAGE_LT50'
            ELSE 'URINE_OR_WINDOW_COVERAGE_INCOMPLETE'
        END AS fb_measurement_status_24h

    FROM six_cumulative
    GROUP BY stay_id
    """
)

con.execute(
    """
    CREATE TABLE hourly_output AS
    SELECT
        h.*,
        p.* EXCLUDE (stay_id)
    FROM hourly_cumulative h
    INNER JOIN patient_24h p
        ON h.stay_id = p.stay_id
    """
)

con.execute(
    """
    CREATE TABLE six_output AS
    SELECT
        s.*,
        p.* EXCLUDE (stay_id)
    FROM six_cumulative s
    INNER JOIN patient_24h p
        ON s.stay_id = p.stay_id
    """
)


# =============================================================================
# Validation invariants
# =============================================================================

hourly_output_n = con.execute(
    "SELECT COUNT(*) FROM hourly_output"
).fetchone()[0]

six_output_n = con.execute(
    "SELECT COUNT(*) FROM six_output"
).fetchone()[0]

patient_24h_n = con.execute(
    "SELECT COUNT(*) FROM patient_24h"
).fetchone()[0]

if hourly_output_n != EXPECTED_COHORT_N * 24:
    raise RuntimeError(
        f"Unexpected hourly output rows: {hourly_output_n}"
    )

if six_output_n != EXPECTED_COHORT_N * 4:
    raise RuntimeError(
        f"Unexpected six-hour output rows: {six_output_n}"
    )

if patient_24h_n != EXPECTED_COHORT_N:
    raise RuntimeError(
        f"Unexpected patient 24-hour rows: {patient_24h_n}"
    )

# Primary window availability requires at least 50% urine-interval coverage.
low_coverage_primary_available_n = con.execute(
    """
    SELECT COUNT(*)
    FROM six_output
    WHERE fb_window_available_flag = 1
      AND (
            uo_primary_available_flag <> 1
         OR coalesce(uo_interval_coverage_fraction, 0.0) < 0.5
      )
    """
).fetchone()[0]

if low_coverage_primary_available_n:
    raise RuntimeError(
        f"{low_coverage_primary_available_n} primary six-hour balance rows "
        "have urine coverage below 50% or unavailable primary urine output."
    )

# Window-level arithmetic identity.
formula_mismatch_n = con.execute(
    """
    SELECT COUNT(*)
    FROM six_output
    WHERE fb_window_available_flag = 1
      AND abs(
            fb_net_balance_ml
            - (
                fb_input_ml
                - fb_total_output_ml
            )
          ) > 0.001
    """
).fetchone()[0]

if formula_mismatch_n:
    raise RuntimeError(
        f"{formula_mismatch_n} six-hour rows violate the balance identity."
    )

# Frozen 01_07 patient-level 24-hour urine summaries must agree exactly
# between the imported hourly and six-hour files.
#
# The sums of the WINDOW-LEVEL primary variable uo_interval_ml_observed are
# intentionally not required to agree across grids. In 01_07, GU irrigation
# and absence of positive-duration intervals are masked at the current window
# resolution. One irrigation event may therefore mask an entire six-hour
# window while leaving unaffected hourly windows from the same six-hour period
# usable. This is valid resolution-dependent missingness, not loss of source
# volume.
urine_patient_summary_mismatch_n = con.execute(
    """
    WITH h AS (
        SELECT
            stay_id,
            any_value(uo_interval_ml_24h_observed)
                AS uo_interval_ml_24h_observed,
            any_value(uo_measurement_status_24h)
                AS uo_measurement_status_24h,
            any_value(uo_irrigation_any_24h_flag)
                AS uo_irrigation_any_24h_flag
        FROM urine_hourly
        GROUP BY stay_id
    ),
    s AS (
        SELECT
            stay_id,
            any_value(uo_interval_ml_24h_observed)
                AS uo_interval_ml_24h_observed,
            any_value(uo_measurement_status_24h)
                AS uo_measurement_status_24h,
            any_value(uo_irrigation_any_24h_flag)
                AS uo_irrigation_any_24h_flag
        FROM urine_six
        GROUP BY stay_id
    )
    SELECT COUNT(*)
    FROM h
    INNER JOIN s USING (stay_id)
    WHERE (
            h.uo_interval_ml_24h_observed IS NULL
        ) <> (
            s.uo_interval_ml_24h_observed IS NULL
        )
       OR (
            h.uo_interval_ml_24h_observed IS NOT NULL
        AND s.uo_interval_ml_24h_observed IS NOT NULL
        AND abs(
                h.uo_interval_ml_24h_observed
                - s.uo_interval_ml_24h_observed
            ) > 0.01
       )
       OR coalesce(h.uo_measurement_status_24h, '<NULL>')
            <> coalesce(s.uo_measurement_status_24h, '<NULL>')
       OR coalesce(h.uo_irrigation_any_24h_flag, false)
            <> coalesce(s.uo_irrigation_any_24h_flag, false)
    """
).fetchone()[0]

if urine_patient_summary_mismatch_n:
    raise RuntimeError(
        f"{urine_patient_summary_mismatch_n} stays have inconsistent frozen "
        "01_07 patient-level urine summaries between hourly and six-hour files."
    )

# Boundary-sensitive components must conserve the amount attributable to the
# analytical horizon itself. This catches missing right-edge source points even
# when hourly and six-hour outputs agree with each other.

chest_expected_in_horizon_ml = con.execute(
    """
    SELECT SUM(
        i.interval_volume_ml
        * greatest(
            0.0,
            date_diff(
                'second',
                greatest(i.interval_start, c.postoperative_time_zero),
                least(i.interval_end, c.analytical_end_time)
            )
        )
        / nullif(
            date_diff('second', i.interval_start, i.interval_end),
            0
        )
    )
    FROM chest_intervals i
    INNER JOIN cohort c
        ON i.stay_id = c.stay_id
    WHERE i.interval_end > c.postoperative_time_zero
      AND i.interval_start < c.analytical_end_time
    """
).fetchone()[0] or 0.0

chest_hourly_allocated_ml = con.execute(
    "SELECT SUM(fb_chest_mediastinal_output_ml) FROM hourly_output"
).fetchone()[0] or 0.0

chest_six_allocated_ml = con.execute(
    "SELECT SUM(fb_chest_mediastinal_output_ml) FROM six_output"
).fetchone()[0] or 0.0

if (
    abs(chest_expected_in_horizon_ml - chest_hourly_allocated_ml) > 0.05
    or abs(chest_expected_in_horizon_ml - chest_six_allocated_ml) > 0.05
):
    raise RuntimeError(
        "Chest boundary-volume conservation failed: "
        f"expected={chest_expected_in_horizon_ml}, "
        f"hourly={chest_hourly_allocated_ml}, "
        f"six={chest_six_allocated_ml}."
    )

crrt_expected_in_horizon_ml = con.execute(
    """
    SELECT SUM(
        r.interval_volume_ml
        * greatest(
            0.0,
            date_diff(
                'second',
                greatest(r.interval_start, c.postoperative_time_zero),
                least(r.interval_end, c.analytical_end_time)
            )
        )
        / nullif(
            date_diff('second', r.interval_start, r.interval_end),
            0
        )
    )
    FROM crrt_intervals r
    INNER JOIN cohort c
        ON r.stay_id = c.stay_id
    WHERE r.interval_end > c.postoperative_time_zero
      AND r.interval_start < c.analytical_end_time
    """
).fetchone()[0] or 0.0

crrt_hourly_allocated_ml = con.execute(
    "SELECT SUM(fb_crrt_removal_ml) FROM hourly_output"
).fetchone()[0] or 0.0

crrt_six_allocated_ml = con.execute(
    "SELECT SUM(fb_crrt_removal_ml) FROM six_output"
).fetchone()[0] or 0.0

if (
    abs(crrt_expected_in_horizon_ml - crrt_hourly_allocated_ml) > 0.05
    or abs(crrt_expected_in_horizon_ml - crrt_six_allocated_ml) > 0.05
):
    raise RuntimeError(
        "CRRT boundary-volume conservation failed: "
        f"expected={crrt_expected_in_horizon_ml}, "
        f"hourly={crrt_hourly_allocated_ml}, "
        f"six={crrt_six_allocated_ml}."
    )

# Components reconstructed directly by 01_10 must conserve volume exactly
# across hourly and six-hour grids.
component_checks = [
    ("input", "fb_input_ml"),
    ("chest", "fb_chest_mediastinal_output_ml"),
    ("gi", "fb_gi_stool_ostomy_output_ml"),
    ("other_drain", "fb_other_drain_output_ml"),
    ("rrt", "fb_rrt_net_removal_ml"),
]

for label, column in component_checks:
    hourly_total = con.execute(
        f"SELECT SUM({column}) FROM hourly_output"
    ).fetchone()[0] or 0.0

    six_total = con.execute(
        f"SELECT SUM({column}) FROM six_output"
    ).fetchone()[0] or 0.0

    if abs(hourly_total - six_total) > 0.05:
        raise RuntimeError(
            f"{label} volume differs between hourly and six-hour grids: "
            f"{hourly_total} vs {six_total}"
        )

# Record the expected resolution-dependent difference in the imported
# window-level primary urine variable for audit and console reporting.
urine_hourly_primary_total = con.execute(
    "SELECT SUM(uo_interval_ml_observed) FROM urine_hourly"
).fetchone()[0] or 0.0

urine_six_primary_total = con.execute(
    "SELECT SUM(uo_interval_ml_observed) FROM urine_six"
).fetchone()[0] or 0.0

urine_primary_grid_difference_ml = (
    urine_hourly_primary_total - urine_six_primary_total
)

# Final cumulative 24-hour balance must match patient-level 24-hour balance.
cumulative_mismatch_n = con.execute(
    """
    SELECT COUNT(*)
    FROM six_output
    WHERE window_index = 3
      AND fb_24h_available_flag = 1
      AND abs(
            fb_cumulative_net_balance_ml
            - fb_net_balance_ml_24h
          ) > 0.001
    """
).fetchone()[0]

if cumulative_mismatch_n:
    raise RuntimeError(
        f"{cumulative_mismatch_n} final cumulative balances do not "
        "match the patient-level 24-hour balance."
    )

# No 24-hour balance may be available with structurally missing input or
# incomplete RRT.
invalid_available_24h_n = con.execute(
    """
    SELECT COUNT(*)
    FROM patient_24h
    WHERE fb_24h_available_flag = 1
      AND (
            fb_input_available_24h_flag = 0
         OR fb_rrt_incomplete_24h_flag = 1
         OR fb_urine_irrigation_24h_flag = 1
      )
    """
).fetchone()[0]

if invalid_available_24h_n:
    raise RuntimeError(
        f"{invalid_available_24h_n} invalid 24-hour balances are marked "
        "available."
    )


# =============================================================================
# Audit table
# =============================================================================

con.execute(
    """
    CREATE TABLE audit (
        audit_level VARCHAR,
        subgroup VARCHAR,
        metric VARCHAR,
        value_num DOUBLE,
        value_text VARCHAR
    )
    """
)

raw_water_n = con.execute(
    "SELECT COUNT(*) FROM water_raw"
).fetchone()[0]

valid_water_n = con.execute(
    "SELECT COUNT(*) FROM water_valid"
).fetchone()[0]

icu_water_segments_n = con.execute(
    "SELECT COUNT(*) FROM icu_water_segments"
).fetchone()[0]

periop_water_segments_n = con.execute(
    """
    SELECT COUNT(*)
    FROM water_segments_classified
    WHERE input_context_class = 'EXCLUDE_PERIOPERATIVE_SUMMARY'
    """
).fetchone()[0]

icu_water_total = con.execute(
    """
    SELECT SUM(
        amount_ml
        * greatest(
            0,
            date_diff(
                'second',
                greatest(starttime, postoperative_time_zero),
                least(endtime, analytical_end_time)
            )
        )
        / nullif(duration_seconds, 0)
    )
    FROM icu_water_segments
    """
).fetchone()[0] or 0.0

periop_water_total = con.execute(
    """
    SELECT SUM(
        amount_ml
        * greatest(
            0,
            date_diff(
                'second',
                greatest(starttime, postoperative_time_zero),
                least(endtime, analytical_end_time)
            )
        )
        / nullif(duration_seconds, 0)
    )
    FROM water_segments_classified
    WHERE input_context_class = 'EXCLUDE_PERIOPERATIVE_SUMMARY'
    """
).fetchone()[0] or 0.0

chest_core_points_n = con.execute(
    """
    SELECT COUNT(*)
    FROM chest_points
    WHERE core_horizon_flag = 1
    """
).fetchone()[0]

chest_anchor_n = con.execute(
    "SELECT COUNT(*) FROM chest_anchor_intervals"
).fetchone()[0]

chest_anchor_patients_n = con.execute(
    "SELECT COUNT(DISTINCT stay_id) FROM chest_anchor_intervals"
).fetchone()[0]

chest_anchor_attributed_ml = con.execute(
    """
    SELECT SUM(fb_chest_anchor_attributed_ml)
    FROM six_output
    """
).fetchone()[0] or 0.0

chest_anchor_gap_context = con.execute(
    """
    SELECT
        median(date_diff('second', c.analytical_end_time, i.interval_end) / 60.0),
        quantile_cont(
            date_diff('second', c.analytical_end_time, i.interval_end) / 60.0,
            0.95
        ),
        max(date_diff('second', c.analytical_end_time, i.interval_end) / 60.0)
    FROM chest_anchor_intervals i
    INNER JOIN cohort c
        ON i.stay_id = c.stay_id
    """
).fetchone()

crrt_post_endpoint_points_n = con.execute(
    """
    SELECT COUNT(*)
    FROM crrt_primary_points
    WHERE charttime >= analytical_end_time
    """
).fetchone()[0]

crrt_post_endpoint_patients_n = con.execute(
    """
    SELECT COUNT(DISTINCT stay_id)
    FROM crrt_primary_points
    WHERE charttime >= analytical_end_time
    """
).fetchone()[0]

crrt_post_endpoint_attributed_ml = con.execute(
    """
    SELECT SUM(
        r.interval_volume_ml
        * greatest(
            0.0,
            date_diff(
                'second',
                greatest(r.interval_start, c.postoperative_time_zero),
                least(r.interval_end, c.analytical_end_time)
            )
        )
        / nullif(
            date_diff('second', r.interval_start, r.interval_end),
            0
        )
    )
    FROM crrt_intervals r
    INNER JOIN cohort c
        ON r.stay_id = c.stay_id
    WHERE r.event_time >= c.analytical_end_time
      AND r.interval_end > c.postoperative_time_zero
      AND r.interval_start < c.analytical_end_time
    """
).fetchone()[0] or 0.0

chest_t0_boundary_ml = con.execute(
    "SELECT SUM(exact_t0_boundary_ml) FROM chest_core_intervals"
).fetchone()[0] or 0.0

point_output_total_ml = con.execute(
    "SELECT SUM(value_ml) FROM point_output_events"
).fetchone()[0] or 0.0

rrt_incomplete_stays_n = con.execute(
    "SELECT COUNT(*) FROM rrt_incomplete_stays"
).fetchone()[0]

pd_pairs_n = con.execute(
    "SELECT COUNT(*) FROM pd_pairs"
).fetchone()[0]

pd_unmatched_in_n = con.execute(
    "SELECT COUNT(*) FROM pd_unmatched_in_horizon"
).fetchone()[0]

pd_unmatched_out_n = con.execute(
    "SELECT COUNT(*) FROM pd_unmatched_out"
).fetchone()[0]

balance_24h_available_n = con.execute(
    """
    SELECT COUNT(*)
    FROM patient_24h
    WHERE fb_24h_available_flag = 1
    """
).fetchone()[0]

balance_24h_partial_sensitivity_available_n = con.execute(
    """
    SELECT COUNT(*)
    FROM patient_24h
    WHERE fb_24h_partial_urine_sensitivity_available_flag = 1
    """
).fetchone()[0]

con.executemany(
    "INSERT INTO audit VALUES (?, ?, ?, ?, ?)",
    [
        ("OVERALL", None, "cohort_n", cohort_n, None),
        (
            "INPUT",
            "WATER",
            "raw_overlapping_rows_n",
            raw_water_n,
            None,
        ),
        (
            "INPUT",
            "WATER",
            "valid_overlapping_rows_n",
            valid_water_n,
            None,
        ),
        (
            "INPUT",
            "ICU_RECORDED",
            "segments_n",
            icu_water_segments_n,
            None,
        ),
        (
            "INPUT",
            "ICU_RECORDED",
            "patients_n",
            input_available_n,
            None,
        ),
        (
            "INPUT",
            "ICU_RECORDED",
            "attributed_24h_ml",
            icu_water_total,
            None,
        ),
        (
            "INPUT",
            "EXCLUDED_PERIOPERATIVE",
            "segments_n",
            periop_water_segments_n,
            None,
        ),
        (
            "INPUT",
            "EXCLUDED_PERIOPERATIVE",
            "attributed_24h_ml",
            periop_water_total,
            None,
        ),
        (
            "CHEST",
            None,
            "core_points_n",
            chest_core_points_n,
            None,
        ),
        (
            "CHEST",
            None,
            "right_boundary_anchor_intervals_n",
            chest_anchor_n,
            None,
        ),
        (
            "CHEST",
            None,
            "right_boundary_anchor_patients_n",
            chest_anchor_patients_n,
            None,
        ),
        (
            "CHEST",
            None,
            "right_boundary_anchor_attributed_ml",
            chest_anchor_attributed_ml,
            "portion attributed inside the analytical horizon only",
        ),
        (
            "CHEST",
            None,
            "expected_in_horizon_ml",
            chest_expected_in_horizon_ml,
            "must equal hourly and six-hour allocated chest volume",
        ),
        (
            "CHEST",
            None,
            "exact_t0_boundary_ml",
            chest_t0_boundary_ml,
            None,
        ),
        (
            "NONCHEST_POINT_OUTPUT",
            None,
            "source_total_ml",
            point_output_total_ml,
            None,
        ),
        (
            "RRT",
            None,
            "incomplete_stays_n",
            rrt_incomplete_stays_n,
            None,
        ),
        (
            "RRT",
            "CRRT_224191",
            "post_endpoint_source_points_n",
            crrt_post_endpoint_points_n,
            "points retained only because [charttime-1h, charttime) overlaps the analytical horizon",
        ),
        (
            "RRT",
            "CRRT_224191",
            "post_endpoint_source_patients_n",
            crrt_post_endpoint_patients_n,
            None,
        ),
        (
            "RRT",
            "CRRT_224191",
            "post_endpoint_attributed_in_horizon_ml",
            crrt_post_endpoint_attributed_ml,
            "post-endpoint source timestamps; only the pre-endpoint overlap is counted",
        ),
        (
            "RRT",
            "CRRT_224191",
            "expected_in_horizon_ml",
            crrt_expected_in_horizon_ml,
            "must equal hourly and six-hour allocated CRRT volume",
        ),
        (
            "RRT",
            "PD",
            "paired_cycles_n",
            pd_pairs_n,
            None,
        ),
        (
            "RRT",
            "PD",
            "unmatched_in_horizon_n",
            pd_unmatched_in_n,
            None,
        ),
        (
            "RRT",
            "PD",
            "unmatched_out_n",
            pd_unmatched_out_n,
            None,
        ),
        (
            "PATIENT_24H",
            "PRIMARY_URINE_COVERAGE_GE50",
            "balance_available_n",
            balance_24h_available_n,
            None,
        ),
        (
            "PATIENT_24H",
            "PARTIAL_URINE_SENSITIVITY",
            "balance_available_n",
            balance_24h_partial_sensitivity_available_n,
            None,
        ),
    ],
)

for metric, value in zip(
    [
        "anchor_gap_median_minutes",
        "anchor_gap_p95_minutes",
        "anchor_gap_max_minutes",
    ],
    chest_anchor_gap_context,
):
    con.execute(
        """
        INSERT INTO audit
        VALUES (
            'CHEST_BOUNDARY',
            NULL,
            ?,
            ?,
            'time from analytical endpoint to first accepted post-endpoint chest anchor'
        )
        """,
        [metric, None if value is None else float(value)],
    )

con.executemany(
    "INSERT INTO audit VALUES (?, ?, ?, ?, ?)",
    [
        (
            "URINE_IMPORT",
            "HOURLY_PRIMARY_WINDOWS",
            "sum_uo_interval_ml_observed",
            urine_hourly_primary_total,
            "Resolution-specific primary values from frozen 01_07",
        ),
        (
            "URINE_IMPORT",
            "SIX_HOUR_PRIMARY_WINDOWS",
            "sum_uo_interval_ml_observed",
            urine_six_primary_total,
            "Resolution-specific primary values from frozen 01_07",
        ),
        (
            "URINE_IMPORT",
            "GRID_DIFFERENCE",
            "hourly_minus_six_hour_primary_ml",
            urine_primary_grid_difference_ml,
            "Expected from window-level irrigation/coverage masking",
        ),
        (
            "URINE_IMPORT",
            "PATIENT_24H",
            "hourly_six_summary_mismatch_n",
            urine_patient_summary_mismatch_n,
            "Must equal zero",
        ),
    ],
)

# Input source composition by main label.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'INPUT_LABEL',
        coalesce(main_label, '<NULL>'),
        'attributed_24h_ml',
        SUM(
            amount_ml
            * greatest(
                0,
                date_diff(
                    'second',
                    greatest(starttime, postoperative_time_zero),
                    least(endtime, analytical_end_time)
                )
            )
            / nullif(duration_seconds, 0)
        )::DOUBLE,
        'segments=' || cast(COUNT(*) AS VARCHAR)
            || '; patients='
            || cast(COUNT(DISTINCT stay_id) AS VARCHAR)
    FROM icu_water_segments
    GROUP BY coalesce(main_label, '<NULL>')
    """
)

# Primary urine-coverage policy.
con.executemany(
    "INSERT INTO audit VALUES (?, ?, ?, ?, ?)",
    [
        (
            "URINE_COVERAGE_POLICY",
            "PRIMARY",
            "minimum_window_coverage_fraction",
            0.5,
            "Frozen primary threshold for net fluid balance",
        ),
        (
            "URINE_COVERAGE_POLICY",
            "ANY_POSITIVE_SENSITIVITY",
            "minimum_window_coverage_fraction",
            0.0,
            "Positive-duration observed urine interval; no percentage threshold",
        ),
    ],
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'URINE_COVERAGE_POLICY',
        window_label,
        'primary_ge50_available_n',
        COUNT(*) FILTER (
            WHERE fb_window_available_flag = 1
        )::DOUBLE,
        NULL
    FROM six_output
    GROUP BY window_index, window_label
    """
)

# 24-hour status.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'PATIENT_24H_STATUS',
        fb_measurement_status_24h,
        'patients_n',
        COUNT(*)::DOUBLE,
        NULL
    FROM patient_24h
    GROUP BY fb_measurement_status_24h
    """
)

# Available 24-hour distribution.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'PATIENT_24H',
        NULL,
        metric,
        value_num,
        NULL
    FROM (
        SELECT
            'median_input_ml' AS metric,
            median(fb_input_ml_24h)::DOUBLE AS value_num
        FROM patient_24h
        WHERE fb_24h_available_flag = 1

        UNION ALL

        SELECT
            'median_total_output_ml',
            median(fb_total_output_ml_24h)::DOUBLE
        FROM patient_24h
        WHERE fb_24h_available_flag = 1

        UNION ALL

        SELECT
            'median_net_balance_ml',
            median(fb_net_balance_ml_24h)::DOUBLE
        FROM patient_24h
        WHERE fb_24h_available_flag = 1

        UNION ALL

        SELECT
            'p10_net_balance_ml',
            quantile_cont(fb_net_balance_ml_24h, 0.10)::DOUBLE
        FROM patient_24h
        WHERE fb_24h_available_flag = 1

        UNION ALL

        SELECT
            'p90_net_balance_ml',
            quantile_cont(fb_net_balance_ml_24h, 0.90)::DOUBLE
        FROM patient_24h
        WHERE fb_24h_available_flag = 1

        UNION ALL

        SELECT
            'median_fluid_accumulation_pct_weight',
            median(fb_fluid_accumulation_pct_weight_24h)::DOUBLE
        FROM patient_24h
        WHERE fb_24h_available_flag = 1
          AND fb_fluid_accumulation_pct_weight_24h IS NOT NULL

        UNION ALL

        SELECT
            'partial_urine_sensitivity_median_net_balance_ml',
            median(
                fb_net_balance_ml_24h_partial_urine_sensitivity
            )::DOUBLE
        FROM patient_24h
        WHERE fb_24h_partial_urine_sensitivity_available_flag = 1

        UNION ALL

        SELECT
            'partial_urine_sensitivity_median_fluid_accumulation_pct_weight',
            median(
                fb_fluid_accumulation_pct_weight_24h_partial_urine_sensitivity
            )::DOUBLE
        FROM patient_24h
        WHERE fb_24h_partial_urine_sensitivity_available_flag = 1
          AND fb_fluid_accumulation_pct_weight_24h_partial_urine_sensitivity
                IS NOT NULL
    )
    """
)

# Window-level availability and distributions.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'SIX_HOUR',
        window_label,
        metric,
        value_num,
        NULL
    FROM (
        SELECT
            window_label,
            'available_n' AS metric,
            COUNT(*) FILTER (
                WHERE fb_window_available_flag = 1
            )::DOUBLE AS value_num
        FROM six_output
        GROUP BY window_label

        UNION ALL

        SELECT
            window_label,
            'median_input_ml',
            median(fb_input_ml)::DOUBLE
        FROM six_output
        WHERE fb_window_available_flag = 1
        GROUP BY window_label

        UNION ALL

        SELECT
            window_label,
            'median_total_output_ml',
            median(fb_total_output_ml)::DOUBLE
        FROM six_output
        WHERE fb_window_available_flag = 1
        GROUP BY window_label

        UNION ALL

        SELECT
            window_label,
            'median_net_balance_ml',
            median(fb_net_balance_ml)::DOUBLE
        FROM six_output
        WHERE fb_window_available_flag = 1
        GROUP BY window_label
    )
    """
)


# =============================================================================
# Export
# =============================================================================

con.execute(
    f"""
    COPY (
        SELECT *
        FROM hourly_output
        ORDER BY stay_id, window_index
    )
    TO '{qpath(OUT_HOURLY)}'
    (FORMAT PARQUET, COMPRESSION ZSTD)
    """
)

con.execute(
    f"""
    COPY (
        SELECT *
        FROM six_output
        ORDER BY stay_id, window_index
    )
    TO '{qpath(OUT_SIX)}'
    (FORMAT PARQUET, COMPRESSION ZSTD)
    """
)

con.execute(
    f"""
    COPY (
        SELECT *
        FROM audit
        ORDER BY
            CASE audit_level
                WHEN 'OVERALL' THEN 1
                WHEN 'INPUT' THEN 2
                WHEN 'INPUT_LABEL' THEN 3
                WHEN 'URINE_IMPORT' THEN 4
                WHEN 'URINE_COVERAGE_POLICY' THEN 5
                WHEN 'CHEST' THEN 6
                WHEN 'NONCHEST_POINT_OUTPUT' THEN 7
                WHEN 'RRT' THEN 8
                WHEN 'PATIENT_24H_STATUS' THEN 9
                WHEN 'PATIENT_24H' THEN 10
                WHEN 'SIX_HOUR' THEN 11
                ELSE 99
            END,
            subgroup NULLS FIRST,
            metric
    )
    TO '{qpath(OUT_AUDIT)}'
    (HEADER, DELIMITER ',')
    """
)


# =============================================================================
# Console report
# =============================================================================

print("=" * 118)
print("TRAJECTORIES - POSTOPERATIVE ICU-RECORDED FLUID BALANCE")
print("=" * 118)
print(f"Cohort rows / unique stays             : {cohort_n} / {cohort_unique_n}")
print(f"ICU Water input available              : {input_available_n}")
print(f"ICU Water input structurally missing   : {cohort_n - input_available_n}")
print(f"RRT-incomplete stays                   : {rrt_incomplete_stays_n}")
print(
    f"24-hour primary balance (urine GE50)   : "
    f"{balance_24h_available_n}"
)
print(
    f"24-hour partial-urine sensitivity      : "
    f"{balance_24h_partial_sensitivity_available_n}"
)
print()

print("Input source context")
print("--------------------")
print(
    con.execute(
        """
        SELECT
            input_context_class,
            COUNT(*) AS segments_n,
            COUNT(DISTINCT stay_id) AS patients_n,
            round(
                SUM(
                    amount_ml
                    * greatest(
                        0,
                        date_diff(
                            'second',
                            greatest(starttime, postoperative_time_zero),
                            least(endtime, analytical_end_time)
                        )
                    )
                    / nullif(duration_seconds, 0)
                ),
                1
            ) AS attributed_24h_ml
        FROM water_segments_classified
        GROUP BY input_context_class
        ORDER BY attributed_24h_ml DESC
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Imported frozen urine-output consistency")
print("------------------------------------------")
print(
    f"Hourly primary-window urine total       : "
    f"{urine_hourly_primary_total:.3f} mL"
)
print(
    f"Six-hour primary-window urine total     : "
    f"{urine_six_primary_total:.3f} mL"
)
print(
    f"Hourly minus six-hour primary total     : "
    f"{urine_primary_grid_difference_ml:.3f} mL"
)
print(
    "Patient-level 24-hour summary mismatch  : "
    f"{urine_patient_summary_mismatch_n}"
)
print(
    "Interpretation: expected grid-specific missingness; "
    "not a source-volume failure."
)
print()

print("Boundary reconstruction")
print("-----------------------")
print(f"Chest anchor intervals                 : {chest_anchor_n}")
print(f"Chest anchor patients                  : {chest_anchor_patients_n}")
print(f"Chest anchor attributed in-horizon mL  : {chest_anchor_attributed_ml:.1f}")
print(f"Chest expected/allocated mL            : {chest_expected_in_horizon_ml:.1f}")
print(f"CRRT post-endpoint source points       : {crrt_post_endpoint_points_n}")
print(f"CRRT post-endpoint patients            : {crrt_post_endpoint_patients_n}")
print(f"CRRT post-endpoint in-horizon mL       : {crrt_post_endpoint_attributed_ml:.1f}")
print(f"CRRT expected/allocated mL             : {crrt_expected_in_horizon_ml:.1f}")
print()

print("Non-urinary source totals")
print("-------------------------")
print(
    con.execute(
        """
        SELECT
            component,
            round(total_ml, 1) AS total_ml
        FROM (
            SELECT
                'CHEST_PRIMARY_INTERVAL_WITH_ANCHOR' AS component,
                SUM(fb_chest_mediastinal_output_ml) AS total_ml
            FROM six_output

            UNION ALL

            SELECT
                'GI_STOOL_OSTOMY_POINT_TIME',
                SUM(fb_gi_stool_ostomy_output_ml)
            FROM six_output

            UNION ALL

            SELECT
                'OTHER_DRAIN_POINT_TIME',
                SUM(fb_other_drain_output_ml)
            FROM six_output

            UNION ALL

            SELECT
                'CRRT_224191',
                SUM(fb_crrt_removal_ml)
            FROM six_output

            UNION ALL

            SELECT
                'IHD_226499',
                SUM(fb_ihd_removal_ml)
            FROM six_output

            UNION ALL

            SELECT
                'PD_PAIRED_NET',
                SUM(fb_pd_net_removal_ml)
            FROM six_output
        )
        ORDER BY component
        """
    ).fetchdf().to_string(index=False)
)
print()

print("RRT integration")
print("---------------")
print(
    con.execute(
        """
        SELECT
            metric,
            value_num
        FROM audit
        WHERE audit_level = 'RRT'
        ORDER BY subgroup NULLS FIRST, metric
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Six-hour fluid balance")
print("----------------------")
print(
    con.execute(
        """
        SELECT
            window_label,
            COUNT(*) FILTER (
                WHERE fb_window_available_flag = 1
            ) AS available_n,
            round(
                median(fb_input_ml) FILTER (
                    WHERE fb_window_available_flag = 1
                ),
                1
            ) AS median_input_ml,
            round(
                median(fb_total_output_ml) FILTER (
                    WHERE fb_window_available_flag = 1
                ),
                1
            ) AS median_output_ml,
            round(
                median(fb_net_balance_ml) FILTER (
                    WHERE fb_window_available_flag = 1
                ),
                1
            ) AS median_net_balance_ml,
            round(
                median(fb_cumulative_net_balance_ml) FILTER (
                    WHERE fb_cumulative_available_flag = 1
                ),
                1
            ) AS median_cumulative_balance_ml
        FROM six_output
        GROUP BY window_label, window_index
        ORDER BY window_index
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Patient-level 24-hour fluid balance")
print("-----------------------------------")
print(
    con.execute(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE fb_24h_available_flag = 1
            ) AS available_n,
            round(
                median(fb_input_ml_24h) FILTER (
                    WHERE fb_24h_available_flag = 1
                ),
                1
            ) AS median_input_ml,
            round(
                median(fb_total_output_ml_24h) FILTER (
                    WHERE fb_24h_available_flag = 1
                ),
                1
            ) AS median_output_ml,
            round(
                median(fb_net_balance_ml_24h) FILTER (
                    WHERE fb_24h_available_flag = 1
                ),
                1
            ) AS median_net_balance_ml,
            round(
                quantile_cont(
                    fb_net_balance_ml_24h,
                    0.10
                ) FILTER (
                    WHERE fb_24h_available_flag = 1
                ),
                1
            ) AS p10_net_balance_ml,
            round(
                quantile_cont(
                    fb_net_balance_ml_24h,
                    0.90
                ) FILTER (
                    WHERE fb_24h_available_flag = 1
                ),
                1
            ) AS p90_net_balance_ml,
            round(
                median(
                    fb_fluid_accumulation_pct_weight_24h
                ) FILTER (
                    WHERE fb_24h_available_flag = 1
                ),
                2
            ) AS median_pct_weight,
            COUNT(*) FILTER (
                WHERE fb_24h_partial_urine_sensitivity_available_flag = 1
            ) AS partial_sensitivity_n,
            round(
                median(
                    fb_net_balance_ml_24h_partial_urine_sensitivity
                ) FILTER (
                    WHERE fb_24h_partial_urine_sensitivity_available_flag = 1
                ),
                1
            ) AS partial_sensitivity_median_balance_ml
        FROM patient_24h
        """
    ).fetchdf().to_string(index=False)
)
print()

print("24-hour measurement status (primary GE50 policy)")
print("--------------------------")
print(
    con.execute(
        """
        SELECT
            fb_measurement_status_24h,
            COUNT(*) AS patients_n
        FROM patient_24h
        GROUP BY fb_measurement_status_24h
        ORDER BY patients_n DESC
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Canonical outputs")
print("-----------------")
print(OUT_HOURLY)
print(OUT_SIX)
print(OUT_AUDIT)
print()
print("Canonical postoperative ICU-recorded fluid-balance extraction complete.")
print("No intraoperative or perioperative summary volume was added.")
print("Missing ICU Water input was not interpreted as zero.")
