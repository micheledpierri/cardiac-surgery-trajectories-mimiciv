"""
01_08_extract_diuretic_exposure.py

Trajectories project
Postoperative diuretic-exposure component - canonical production pipeline

Frozen source architecture
--------------------------
INPUTEVENTS:
    authoritative source for ICU IV boluses and timed infusions.

eMAR + eMAR_DETAIL:
    supplementary source for discrete administrations not dose-concordantly
    duplicated in INPUTEVENTS.

PHARMACY / PRESCRIPTIONS:
    used only to resolve route metadata for an already documented eMAR
    administration; prescription/order rows are never treated as exposure.

Cross-source deduplication
--------------------------
An eMAR discrete administration is considered a duplicate of INPUTEVENTS when:
    - same stay and drug class;
    - mutual-nearest event;
    - time difference <= 60 minutes;
    - dose difference <= max(0.5 mg, 5% of the larger dose).

INPUTEVENTS is retained for a duplicate pair.

Temporal semantics
------------------
Bolus:
    point exposure at administration time;
    included at postoperative time zero;
    excluded at the analytical endpoint;
    no right-boundary anchor.

Infusion:
    proportional allocation over explicit starttime-endtime overlap;
    clipped to [postoperative time zero, analytical endpoint);
    no LOCF, pre-start carry, post-stop carry, or endpoint anchor.

Dose representation
-------------------
Furosemide is represented separately as:
    IV bolus dose;
    IV infusion dose;
    enteral bolus dose;
    unresolved-route eMAR bolus dose;
    INPUTEVENTS-only sensitivity dose;
    integrated best-available dose.

Other loop diuretics, thiazide-type diuretics, and acetazolamide remain
 drug-specific. No furosemide-equivalent conversion is applied.

Documented but non-quantifiable eMAR administrations and unmatched eMAR
infusion-start markers are retained as explicit flags/counts and are never
assigned an invented dose.

Canonical outputs
-----------------
data/01_08_diuretic_exposure_hourly_trajectory.parquet
data/01_08_diuretic_exposure_six_hour_summary.parquet
data/01_08_diuretic_exposure_extraction_audit.csv

Inputs
------
data/00_04_final_analytic_cohort.csv
Raw MIMIC-IV 3.1 files relative to MIMIC_ROOT:
  icu/d_items.csv.gz
  icu/inputevents.csv.gz
  hosp/emar.csv.gz
  hosp/emar_detail.csv.gz
  hosp/pharmacy.csv.gz
  hosp/prescriptions.csv.gz

Drug classes are selected from dictionary labels and medication names using
the implemented name-matching rules, rather than a fixed item-ID list.
The horizon ends at the earlier of t0 + 24 hours and observation_end_time;
outputs summarize hourly and six-hour exposure.
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
D_ITEMS_FILE = MIMIC_ROOT / "icu" / "d_items.csv.gz"
INPUTEVENTS_FILE = MIMIC_ROOT / "icu" / "inputevents.csv.gz"
EMAR_FILE = MIMIC_ROOT / "hosp" / "emar.csv.gz"
EMAR_DETAIL_FILE = MIMIC_ROOT / "hosp" / "emar_detail.csv.gz"
PHARMACY_FILE = MIMIC_ROOT / "hosp" / "pharmacy.csv.gz"
PRESCRIPTIONS_FILE = MIMIC_ROOT / "hosp" / "prescriptions.csv.gz"

OUT_HOURLY = PROJECT_ROOT / "data" / "01_08_diuretic_exposure_hourly_trajectory.parquet"
OUT_SIX = PROJECT_ROOT / "data" / "01_08_diuretic_exposure_six_hour_summary.parquet"
OUT_AUDIT = PROJECT_ROOT / "data" / "01_08_diuretic_exposure_extraction_audit.csv"

EXPECTED_COHORT_N = 9305
MATCH_WINDOW_MINUTES = 60


def qpath(path: Path) -> str:
    return str(path).replace("'", "''")


for required in [
    COHORT_FILE,
    D_ITEMS_FILE,
    INPUTEVENTS_FILE,
    EMAR_FILE,
    EMAR_DETAIL_FILE,
    PHARMACY_FILE,
    PRESCRIPTIONS_FILE,
]:
    if not required.exists():
        raise FileNotFoundError(f"Required file not found: {required}")

OUT_AUDIT.parent.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(database=":memory:")
con.execute("PRAGMA threads=4")


# =============================================================================
# Cohort
# =============================================================================

con.execute(
    f"""
    CREATE TABLE cohort AS
    SELECT
        subject_id::BIGINT AS subject_id,
        hadm_id::BIGINT AS hadm_id,
        stay_id::BIGINT AS stay_id,
        surgery_group,
        sex,
        age_at_icu_admission,
        postoperative_time_zero::TIMESTAMP AS postoperative_time_zero,
        observation_end_time::TIMESTAMP AS observation_end_time,
        observation_end_type,
        least(
            postoperative_time_zero::TIMESTAMP + INTERVAL '24 hours',
            observation_end_time::TIMESTAMP
        ) AS analytical_end_time,
        year(postoperative_time_zero::TIMESTAMP)::INTEGER AS postoperative_year
    FROM read_csv_auto(
        '{qpath(COHORT_FILE)}',
        header=true,
        all_varchar=false
    )
    """
)

(
    cohort_n,
    unique_stays_n,
    missing_t0_n,
    missing_end_n,
    invalid_horizon_n,
) = con.execute(
    """
    SELECT
        COUNT(*),
        COUNT(DISTINCT stay_id),
        COUNT(*) FILTER (WHERE postoperative_time_zero IS NULL),
        COUNT(*) FILTER (WHERE observation_end_time IS NULL),
        COUNT(*) FILTER (WHERE analytical_end_time <= postoperative_time_zero)
    FROM cohort
    """
).fetchone()

if cohort_n != EXPECTED_COHORT_N or unique_stays_n != EXPECTED_COHORT_N:
    raise RuntimeError(
        f"Unexpected cohort size: {cohort_n} rows / {unique_stays_n} stays; "
        f"expected {EXPECTED_COHORT_N}."
    )

if missing_t0_n or missing_end_n or invalid_horizon_n:
    raise RuntimeError(
        "Invalid cohort timing fields: "
        f"missing_t0={missing_t0_n}, missing_end={missing_end_n}, "
        f"invalid_horizon={invalid_horizon_n}."
    )


# =============================================================================
# Drug classification shared across sources
# =============================================================================

DRUG_CLASS_CASE = r"""
CASE
    WHEN lower(drug_text) LIKE '%furosemide%'
      OR lower(drug_text) LIKE '%lasix%'
        THEN 'LOOP_FUROSEMIDE'
    WHEN lower(drug_text) LIKE '%bumetanide%'
      OR lower(drug_text) LIKE '%bumex%'
        THEN 'LOOP_BUMETANIDE'
    WHEN lower(drug_text) LIKE '%torsemide%'
      OR lower(drug_text) LIKE '%torasemide%'
      OR lower(drug_text) LIKE '%demadex%'
        THEN 'LOOP_TORSEMIDE'
    WHEN lower(drug_text) LIKE '%ethacrynic%'
      OR lower(drug_text) LIKE '%edecrin%'
        THEN 'LOOP_ETHACRYNIC_ACID'
    WHEN lower(drug_text) LIKE '%chlorothiazide%'
      OR lower(drug_text) LIKE '%diuril%'
        THEN 'THIAZIDE_CHLOROTHIAZIDE'
    WHEN lower(drug_text) LIKE '%hydrochlorothiazide%'
      OR lower(drug_text) LIKE '%hydrodiuril%'
      OR regexp_matches(lower(drug_text), '(^|[^a-z])hctz([^a-z]|$)')
        THEN 'THIAZIDE_HYDROCHLOROTHIAZIDE'
    WHEN lower(drug_text) LIKE '%metolazone%'
      OR lower(drug_text) LIKE '%zaroxolyn%'
        THEN 'THIAZIDE_METOLAZONE'
    WHEN lower(drug_text) LIKE '%chlorthalidone%'
        THEN 'THIAZIDE_CHLORTHALIDONE'
    WHEN lower(drug_text) LIKE '%acetazolamide%'
      OR lower(drug_text) LIKE '%diamox%'
        THEN 'CARBONIC_ACETAZOLAMIDE'
    WHEN lower(drug_text) LIKE '%spironolactone%'
      OR lower(drug_text) LIKE '%aldactone%'
        THEN 'MRA_SPIRONOLACTONE'
    WHEN lower(drug_text) LIKE '%eplerenone%'
      OR lower(drug_text) LIKE '%inspra%'
        THEN 'MRA_EPLERENONE'
    WHEN lower(drug_text) LIKE '%amiloride%'
        THEN 'K_SPARING_AMILORIDE'
    WHEN lower(drug_text) LIKE '%triamterene%'
        THEN 'K_SPARING_TRIAMTERENE'
    WHEN lower(drug_text) LIKE '%mannitol%'
        THEN 'OSMOTIC_MANNITOL'
    ELSE NULL
END
"""


def class_case(expression: str) -> str:
    return DRUG_CLASS_CASE.replace("drug_text", expression)


# =============================================================================
# INPUTEVENTS: delivered bolus and infusion segments
# =============================================================================

con.execute(
    f"""
    CREATE TABLE d_items_diuretic AS
    WITH x AS (
        SELECT
            itemid,
            label,
            abbreviation,
            {class_case("coalesce(label, '') || ' ' || coalesce(abbreviation, '')")}
                AS drug_class
        FROM read_csv_auto(
            '{qpath(D_ITEMS_FILE)}',
            header=true,
            all_varchar=false
        )
        WHERE lower(linksto) = 'inputevents'
    )
    SELECT *
    FROM x
    WHERE drug_class IS NOT NULL
    """
)

con.execute(
    f"""
    CREATE TABLE input_raw AS
    SELECT
        c.subject_id,
        c.hadm_id,
        c.stay_id,
        c.postoperative_year,
        c.postoperative_time_zero,
        c.analytical_end_time,
        ie.starttime,
        ie.endtime,
        ie.storetime,
        ie.itemid,
        di.label,
        di.drug_class,
        ie.amount,
        ie.amountuom,
        ie.rate,
        ie.rateuom,
        ie.orderid,
        ie.linkorderid,
        ie.ordercategoryname,
        ie.ordercomponenttypedescription,
        ie.statusdescription,
        ie.originalamount,
        ie.originalrate,
        greatest(ie.starttime, c.postoperative_time_zero) AS overlap_start_time,
        least(ie.endtime, c.analytical_end_time) AS overlap_end_time,
        greatest(
            0.0,
            date_diff(
                'second',
                greatest(ie.starttime, c.postoperative_time_zero),
                least(ie.endtime, c.analytical_end_time)
            ) / 60.0
        ) AS overlap_minutes,
        date_diff('second', ie.starttime, ie.endtime) / 60.0
            AS recorded_duration_minutes
    FROM read_csv_auto(
        '{qpath(INPUTEVENTS_FILE)}',
        header=true,
        all_varchar=false
    ) ie
    INNER JOIN d_items_diuretic di
        ON ie.itemid = di.itemid
    INNER JOIN cohort c
        ON ie.stay_id = c.stay_id
       AND ie.starttime < c.analytical_end_time
       AND ie.endtime > c.postoperative_time_zero
    """
)

con.execute(
    """
    CREATE TABLE input_delivered AS
    SELECT
        row_number() OVER (
            ORDER BY stay_id, starttime, endtime, itemid, orderid
        ) AS input_point_id,
        *,
        CASE
            WHEN recorded_duration_minutes <= 1.01
                THEN 'BOLUS_OR_ONE_MINUTE_EVENT'
            ELSE 'TIMED_INFUSION_SEGMENT'
        END AS input_pattern,
        CASE
            WHEN lower(trim(coalesce(amountuom, ''))) IN ('mg', 'milligram', 'milligrams')
                THEN amount
            WHEN lower(trim(coalesce(amountuom, ''))) IN ('g', 'gram', 'grams')
                THEN amount * 1000.0
            ELSE NULL
        END AS input_amount_mg_full,
        CASE
            WHEN lower(trim(coalesce(rateuom, ''))) IN (
                'mg/hour', 'mg/hr', 'mg/h'
            ) THEN rate
            ELSE NULL
        END AS input_rate_mg_h,
        CASE
            WHEN recorded_duration_minutes <= 1.01
             AND starttime >= postoperative_time_zero
             AND starttime < analytical_end_time
             AND lower(trim(coalesce(amountuom, ''))) IN (
                 'mg', 'milligram', 'milligrams'
             )
                THEN amount
            WHEN recorded_duration_minutes > 1.01
             AND recorded_duration_minutes > 0
             AND lower(trim(coalesce(amountuom, ''))) IN (
                 'mg', 'milligram', 'milligrams'
             )
                THEN amount * overlap_minutes / recorded_duration_minutes
            WHEN recorded_duration_minutes > 1.01
             AND lower(trim(coalesce(rateuom, ''))) IN (
                 'mg/hour', 'mg/hr', 'mg/h'
             )
                THEN rate * overlap_minutes / 60.0
            ELSE NULL
        END AS input_amount_mg_horizon
    FROM input_raw
    WHERE coalesce(statusdescription, '') <> 'Rewritten'
      AND overlap_minutes > 0
      AND (amount IS NOT NULL OR rate IS NOT NULL)
    """
)


# =============================================================================
# eMAR and eMAR_DETAIL
# =============================================================================

con.execute(
    f"""
    CREATE TABLE emar_candidate AS
    WITH raw AS (
        SELECT
            c.subject_id,
            c.hadm_id,
            c.stay_id,
            c.postoperative_year,
            e.emar_id,
            e.emar_seq,
            e.poe_id,
            e.pharmacy_id,
            e.charttime,
            e.scheduletime,
            e.storetime,
            e.medication,
            e.event_txt,
            {class_case("coalesce(e.medication, '')")} AS drug_class
        FROM read_csv_auto(
            '{qpath(EMAR_FILE)}',
            header=true,
            all_varchar=false
        ) e
        INNER JOIN cohort c
            ON e.subject_id = c.subject_id
           AND (e.hadm_id IS NULL OR e.hadm_id = c.hadm_id)
           AND e.charttime >= c.postoperative_time_zero
           AND e.charttime < c.analytical_end_time
    )
    SELECT *
    FROM raw
    WHERE drug_class IS NOT NULL
    """
)

emar_assignment_dups_n = con.execute(
    """
    SELECT COUNT(*)
    FROM (
        SELECT emar_id
        FROM emar_candidate
        GROUP BY emar_id
        HAVING COUNT(*) > 1
    )
    """
).fetchone()[0]

if emar_assignment_dups_n:
    raise RuntimeError(
        f"{emar_assignment_dups_n} eMAR IDs mapped to multiple cohort stays."
    )

con.execute(
    f"""
    CREATE TABLE emar_detail_raw AS
    SELECT
        d.subject_id,
        d.emar_id,
        d.emar_seq,
        d.parent_field_ordinal,
        d.administration_type,
        d.complete_dose_not_given,
        d.dose_due,
        d.dose_due_unit,
        d.dose_given,
        d.dose_given_unit,
        d.product_amount_given,
        d.product_unit,
        d.product_description,
        d.product_description_other,
        d.prior_infusion_rate,
        d.infusion_rate,
        d.infusion_rate_adjustment,
        d.infusion_rate_adjustment_amount,
        d.infusion_rate_unit,
        d.route,
        d.infusion_complete,
        d.new_iv_bag_hung,
        d.continued_infusion_in_other_location,
        d.restart_interval
    FROM read_csv_auto(
        '{qpath(EMAR_DETAIL_FILE)}',
        header=true,
        all_varchar=true
    ) d
    INNER JOIN emar_candidate e
        ON d.emar_id = e.emar_id
    """
)

con.execute(
    """
    CREATE TABLE emar_detail_clean AS
    WITH base AS (
        SELECT
            *,
            nullif(trim(dose_given), '') AS dose_raw,
            lower(trim(replace(coalesce(dose_given_unit, ''), '.', '')))
                AS dose_unit_raw,
            upper(trim(coalesce(route, ''))) AS route_raw,
            CASE
                WHEN upper(trim(coalesce(route, ''))) LIKE '%IV%'
                  OR upper(trim(coalesce(route, ''))) LIKE '%INTRAVENOUS%'
                    THEN 'IV'
                WHEN upper(trim(coalesce(route, ''))) IN ('PO', 'ORAL')
                  OR upper(trim(coalesce(route, ''))) LIKE '%ORAL%'
                  OR upper(trim(coalesce(route, ''))) LIKE '%G TUBE%'
                  OR upper(trim(coalesce(route, ''))) LIKE '%G-TUBE%'
                  OR upper(trim(coalesce(route, ''))) LIKE '%NG%'
                  OR upper(trim(coalesce(route, ''))) LIKE '%OG%'
                  OR upper(trim(coalesce(route, ''))) LIKE '%ENTERAL%'
                    THEN 'ENTERAL'
                WHEN upper(trim(coalesce(route, ''))) LIKE '%IM%'
                  OR upper(trim(coalesce(route, ''))) LIKE '%INTRAMUSCULAR%'
                    THEN 'IM'
                WHEN trim(coalesce(route, '')) = '' THEN NULL
                ELSE 'OTHER'
            END AS normalized_route,
            CASE
                WHEN lower(trim(replace(coalesce(dose_given_unit, ''), '.', '')))
                     IN ('mg', 'milligram', 'milligrams')
                    THEN 'MG'
                WHEN lower(trim(replace(coalesce(dose_given_unit, ''), '.', '')))
                     IN ('g', 'gram', 'grams')
                    THEN 'G'
                WHEN lower(trim(replace(coalesce(dose_given_unit, ''), '.', '')))
                     IN ('mcg', 'microgram', 'micrograms')
                    THEN 'MCG'
                WHEN trim(coalesce(dose_given_unit, '')) = '' THEN NULL
                ELSE upper(trim(dose_given_unit))
            END AS normalized_dose_unit
        FROM emar_detail_raw
    ), parsed AS (
        SELECT
            *,
            CASE
                WHEN dose_raw IS NULL
                  OR dose_raw IN ('___', '__', '_', 'N/A', 'NA')
                    THEN NULL
                WHEN regexp_matches(
                    replace(dose_raw, ',', ''),
                    '^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)$'
                )
                    THEN try_cast(replace(dose_raw, ',', '') AS DOUBLE)
                ELSE NULL
            END AS parsed_dose_numeric
        FROM base
    )
    SELECT
        *,
        CASE
            WHEN parsed_dose_numeric IS NULL THEN NULL
            WHEN normalized_dose_unit = 'MG' THEN parsed_dose_numeric
            WHEN normalized_dose_unit = 'G' THEN parsed_dose_numeric * 1000.0
            WHEN normalized_dose_unit = 'MCG' THEN parsed_dose_numeric / 1000.0
            ELSE NULL
        END AS parsed_dose_mg
    FROM parsed
    """
)

con.execute(
    """
    CREATE TABLE emar_flat AS
    WITH agg AS (
        SELECT
            emar_id,
            COUNT(*) AS detail_rows_n,
            COUNT(*) FILTER (WHERE parent_field_ordinal IS NULL)
                AS parent_rows_n,
            COUNT(*) FILTER (WHERE parent_field_ordinal IS NOT NULL)
                AS product_rows_n,
            string_agg(
                DISTINCT nullif(trim(administration_type), ''),
                ' | ' ORDER BY nullif(trim(administration_type), '')
            ) AS administration_types,
            string_agg(
                DISTINCT nullif(trim(route), ''),
                ' | ' ORDER BY nullif(trim(route), '')
            ) AS routes_raw,
            string_agg(
                DISTINCT normalized_route,
                ' | ' ORDER BY normalized_route
            ) AS normalized_routes_seen,
            COUNT(DISTINCT normalized_route) FILTER (
                WHERE normalized_route IS NOT NULL
            ) AS normalized_route_n,
            max(normalized_route) FILTER (
                WHERE normalized_route IS NOT NULL
            ) AS single_route_candidate,
            string_agg(
                DISTINCT nullif(trim(dose_given), ''),
                ' | ' ORDER BY nullif(trim(dose_given), '')
            ) AS dose_values_raw,
            string_agg(
                DISTINCT nullif(trim(dose_given_unit), ''),
                ' | ' ORDER BY nullif(trim(dose_given_unit), '')
            ) AS dose_units_raw,
            COUNT(DISTINCT parsed_dose_mg) FILTER (
                WHERE parsed_dose_mg IS NOT NULL
            ) AS all_distinct_dose_mg_n,
            min(parsed_dose_mg) FILTER (
                WHERE parsed_dose_mg IS NOT NULL
            ) AS all_min_dose_mg,
            max(parsed_dose_mg) FILTER (
                WHERE parsed_dose_mg IS NOT NULL
            ) AS all_max_dose_mg,
            COUNT(DISTINCT parsed_dose_mg) FILTER (
                WHERE parent_field_ordinal IS NULL
                  AND parsed_dose_mg IS NOT NULL
            ) AS parent_distinct_dose_mg_n,
            max(parsed_dose_mg) FILTER (
                WHERE parent_field_ordinal IS NULL
                  AND parsed_dose_mg IS NOT NULL
            ) AS parent_dose_mg,
            string_agg(
                DISTINCT nullif(trim(infusion_rate), ''),
                ' | ' ORDER BY nullif(trim(infusion_rate), '')
            ) AS infusion_rate_values,
            string_agg(
                DISTINCT nullif(trim(infusion_rate_unit), ''),
                ' | ' ORDER BY nullif(trim(infusion_rate_unit), '')
            ) AS infusion_rate_units,
            max(
                CASE
                    WHEN trim(coalesce(infusion_rate, '')) <> '' THEN 1
                    ELSE 0
                END
            ) AS any_infusion_rate_flag,
            max(
                CASE
                    WHEN upper(coalesce(complete_dose_not_given, ''))
                         IN ('Y', 'YES', 'TRUE', '1')
                    THEN 1 ELSE 0
                END
            ) AS complete_dose_not_given_flag,
            max(
                CASE
                    WHEN upper(coalesce(new_iv_bag_hung, ''))
                         IN ('Y', 'YES', 'TRUE', '1')
                    THEN 1 ELSE 0
                END
            ) AS new_iv_bag_hung_flag
        FROM emar_detail_clean
        GROUP BY emar_id
    )
    SELECT
        e.*,
        coalesce(a.detail_rows_n, 0) AS detail_rows_n,
        coalesce(a.parent_rows_n, 0) AS parent_rows_n,
        coalesce(a.product_rows_n, 0) AS product_rows_n,
        a.administration_types,
        a.routes_raw,
        a.normalized_routes_seen,
        CASE
            WHEN a.normalized_route_n = 1 THEN a.single_route_candidate
            WHEN a.normalized_route_n > 1 THEN 'MIXED'
            ELSE 'UNKNOWN'
        END AS route_class,
        a.dose_values_raw,
        a.dose_units_raw,
        coalesce(a.all_distinct_dose_mg_n, 0) AS all_distinct_dose_mg_n,
        a.all_min_dose_mg,
        a.all_max_dose_mg,
        coalesce(a.parent_distinct_dose_mg_n, 0)
            AS parent_distinct_dose_mg_n,
        a.parent_dose_mg,
        CASE
            WHEN a.parent_distinct_dose_mg_n = 1 THEN a.parent_dose_mg
            WHEN a.parent_distinct_dose_mg_n = 0
             AND a.all_distinct_dose_mg_n = 1 THEN a.all_max_dose_mg
            ELSE NULL
        END AS emar_dose_mg,
        CASE
            WHEN a.parent_distinct_dose_mg_n = 1 THEN 'UNIQUE_PARENT_DOSE'
            WHEN a.parent_distinct_dose_mg_n = 0
             AND a.all_distinct_dose_mg_n = 1 THEN 'UNIQUE_ANY_DETAIL_DOSE'
            WHEN a.all_distinct_dose_mg_n = 0 THEN 'NO_PARSEABLE_MG_DOSE'
            ELSE 'MULTIPLE_DISTINCT_DOSES'
        END AS dose_selection_rule,
        a.infusion_rate_values,
        a.infusion_rate_units,
        coalesce(a.any_infusion_rate_flag, 0) AS any_infusion_rate_flag,
        coalesce(a.complete_dose_not_given_flag, 0)
            AS complete_dose_not_given_flag,
        coalesce(a.new_iv_bag_hung_flag, 0) AS new_iv_bag_hung_flag,
        CASE
            WHEN coalesce(a.complete_dose_not_given_flag, 0) = 1
                THEN 'NOT_ADMINISTERED'
            WHEN lower(trim(coalesce(e.event_txt, ''))) IN (
                'administered',
                'delayed administered',
                'administered in other location'
            ) THEN 'DISCRETE_ADMINISTERED'
            WHEN lower(trim(coalesce(e.event_txt, ''))) = 'started'
                THEN 'INFUSION_STARTED'
            WHEN lower(trim(coalesce(e.event_txt, ''))) LIKE 'stopped%'
                THEN 'INFUSION_STOPPED'
            WHEN lower(trim(coalesce(e.event_txt, ''))) IN (
                'not given', 'hold dose', 'not confirmed'
            ) THEN 'NOT_ADMINISTERED'
            ELSE 'AMBIGUOUS_STATUS'
        END AS status_class,
        CASE
            WHEN lower(trim(coalesce(e.event_txt, ''))) IN (
                'administered',
                'delayed administered',
                'administered in other location'
            )
             AND (
                 coalesce(a.any_infusion_rate_flag, 0) = 1
                 OR lower(coalesce(a.administration_types, '')) LIKE '%infus%'
                 OR coalesce(a.new_iv_bag_hung_flag, 0) = 1
             ) THEN 'INFUSION_RELATED_ADMINISTRATION'
            WHEN lower(trim(coalesce(e.event_txt, ''))) IN (
                'administered',
                'delayed administered',
                'administered in other location'
            ) THEN 'DISCRETE_DOSE_CANDIDATE'
            WHEN lower(trim(coalesce(e.event_txt, ''))) = 'started'
                THEN 'INFUSION_LIFECYCLE'
            ELSE 'NON_DOSE_OR_AMBIGUOUS'
        END AS emar_pattern
    FROM emar_candidate e
    LEFT JOIN agg a
        ON e.emar_id = a.emar_id
    """
)


# =============================================================================
# Dose-capable points and mutual-nearest matching
# =============================================================================

con.execute(
    """
    CREATE TABLE input_bolus_points AS
    SELECT
        input_point_id,
        stay_id,
        hadm_id,
        postoperative_year,
        drug_class,
        starttime AS administration_time,
        endtime,
        itemid,
        label,
        input_amount_mg_full AS dose_mg,
        amount,
        amountuom,
        statusdescription,
        orderid,
        linkorderid
    FROM input_delivered
    WHERE input_pattern = 'BOLUS_OR_ONE_MINUTE_EVENT'
      AND starttime >= postoperative_time_zero
      AND starttime < analytical_end_time
      AND input_amount_mg_full IS NOT NULL
      AND input_amount_mg_full > 0
    """
)

con.execute(
    """
    CREATE TABLE emar_discrete_points AS
    SELECT
        emar_id,
        stay_id,
        hadm_id,
        pharmacy_id,
        poe_id,
        postoperative_year,
        drug_class,
        charttime AS administration_time,
        medication,
        event_txt,
        route_class,
        emar_dose_mg AS dose_mg,
        dose_selection_rule,
        dose_values_raw,
        dose_units_raw,
        administration_types,
        emar_pattern
    FROM emar_flat
    WHERE status_class = 'DISCRETE_ADMINISTERED'
      AND emar_pattern = 'DISCRETE_DOSE_CANDIDATE'
    """
)

con.execute(
    f"""
    CREATE TABLE candidate_bolus_pairs AS
    SELECT
        i.input_point_id,
        e.emar_id,
        i.stay_id,
        i.postoperative_year,
        i.drug_class,
        i.administration_time AS input_time,
        e.administration_time AS emar_time,
        abs(date_diff('second', i.administration_time, e.administration_time))
            / 60.0 AS abs_time_difference_minutes,
        i.dose_mg AS input_dose_mg,
        e.dose_mg AS emar_dose_mg,
        e.route_class AS emar_route_class,
        abs(i.dose_mg - e.dose_mg) AS absolute_dose_difference_mg,
        abs(i.dose_mg - e.dose_mg)
            / nullif(greatest(abs(i.dose_mg), abs(e.dose_mg)), 0)
            AS relative_dose_difference,
        row_number() OVER (
            PARTITION BY i.input_point_id
            ORDER BY
                abs(date_diff('second', i.administration_time, e.administration_time)),
                abs(i.dose_mg - e.dose_mg),
                e.emar_id
        ) AS input_rank,
        row_number() OVER (
            PARTITION BY e.emar_id
            ORDER BY
                abs(date_diff('second', i.administration_time, e.administration_time)),
                abs(i.dose_mg - e.dose_mg),
                i.input_point_id
        ) AS emar_rank
    FROM input_bolus_points i
    INNER JOIN emar_discrete_points e
        ON i.stay_id = e.stay_id
       AND i.drug_class = e.drug_class
       AND e.dose_mg IS NOT NULL
       AND e.dose_mg > 0
       AND e.administration_time BETWEEN
           i.administration_time - INTERVAL '{MATCH_WINDOW_MINUTES} minutes'
           AND i.administration_time + INTERVAL '{MATCH_WINDOW_MINUTES} minutes'
    """
)

con.execute(
    """
    CREATE TABLE mutual_bolus_matches AS
    SELECT
        *,
        CASE
            WHEN absolute_dose_difference_mg <= greatest(
                0.5,
                0.05 * greatest(abs(input_dose_mg), abs(emar_dose_mg))
            ) THEN 1
            ELSE 0
        END AS dose_concordant_flag,
        CASE
            WHEN abs_time_difference_minutes <= 5 THEN 'T00_05M'
            WHEN abs_time_difference_minutes <= 15 THEN 'T05_15M'
            ELSE 'T15_60M'
        END AS time_match_bin
    FROM candidate_bolus_pairs
    WHERE input_rank = 1
      AND emar_rank = 1
    """
)

# Candidate integrated discrete administrations: all INPUTEVENTS boluses plus
# eMAR discrete administrations not dose-concordantly duplicated within 60 min.
con.execute(
    """
    CREATE TABLE emar_discrete_supplement AS
    SELECT e.*
    FROM emar_discrete_points e
    LEFT JOIN mutual_bolus_matches m
        ON e.emar_id = m.emar_id
       AND m.dose_concordant_flag = 1
    WHERE m.emar_id IS NULL
    """
)


# =============================================================================
# eMAR infusion life-cycle versus INPUTEVENTS infusion starts
# =============================================================================

con.execute(
    """
    CREATE TABLE input_infusion_points AS
    SELECT
        input_point_id,
        stay_id,
        postoperative_year,
        drug_class,
        starttime,
        endtime,
        input_amount_mg_horizon,
        input_rate_mg_h,
        amount,
        amountuom,
        rate,
        rateuom,
        statusdescription,
        linkorderid
    FROM input_delivered
    WHERE input_pattern = 'TIMED_INFUSION_SEGMENT'
    """
)

con.execute(
    """
    CREATE TABLE emar_started_points AS
    SELECT
        emar_id,
        stay_id,
        postoperative_year,
        drug_class,
        charttime,
        medication,
        route_class,
        emar_dose_mg,
        dose_selection_rule,
        infusion_rate_values,
        infusion_rate_units,
        administration_types
    FROM emar_flat
    WHERE status_class = 'INFUSION_STARTED'
    """
)

con.execute(
    """
    CREATE TABLE emar_started_nearest_input AS
    WITH pairs AS (
        SELECT
            e.emar_id,
            e.stay_id,
            e.postoperative_year,
            e.drug_class,
            e.charttime AS emar_started_time,
            i.input_point_id,
            i.starttime AS input_starttime,
            abs(date_diff('second', e.charttime, i.starttime)) / 60.0
                AS abs_time_difference_minutes,
            row_number() OVER (
                PARTITION BY e.emar_id
                ORDER BY
                    abs(date_diff('second', e.charttime, i.starttime)),
                    i.input_point_id
            ) AS rn
        FROM emar_started_points e
        INNER JOIN input_infusion_points i
            ON e.stay_id = i.stay_id
           AND e.drug_class = i.drug_class
           AND i.starttime BETWEEN
               e.charttime - INTERVAL '60 minutes'
               AND e.charttime + INTERVAL '60 minutes'
    )
    SELECT * EXCLUDE (rn)
    FROM pairs
    WHERE rn = 1
    """
)



# =============================================================================
# Source availability, route enhancement, and deduplication alternatives
# =============================================================================

# Actual eMAR availability is measured directly within each analytical horizon.
# Deidentified calendar years are not interpreted as chronological eras.
con.execute(
    f"""
    CREATE TABLE emar_horizon_availability AS
    SELECT DISTINCT
        c.stay_id
    FROM read_csv_auto(
        '{qpath(EMAR_FILE)}',
        header=true,
        all_varchar=false
    ) e
    INNER JOIN cohort c
        ON e.subject_id = c.subject_id
       AND (e.hadm_id IS NULL OR e.hadm_id = c.hadm_id)
       AND e.charttime >= c.postoperative_time_zero
       AND e.charttime < c.analytical_end_time
    """
)

# eMAR route is often absent in the dedicated route field. Administration type
# is used only as additional explicit route evidence; no route is inferred from
# drug name or dose.
con.execute(
    """
    CREATE TABLE emar_discrete_enhanced AS
    SELECT
        *,
        CASE
            WHEN route_class <> 'UNKNOWN' THEN route_class
            WHEN upper(coalesce(administration_types, '')) LIKE '%INTRAVENOUS%'
              OR upper(coalesce(administration_types, '')) LIKE '%IV PUSH%'
              OR upper(coalesce(administration_types, '')) LIKE '%IV BOLUS%'
              OR upper(coalesce(administration_types, '')) LIKE '%IVPB%'
              OR upper(coalesce(administration_types, '')) LIKE '%IV PIGGYBACK%'
              OR upper(coalesce(administration_types, '')) LIKE '%IV INFUS%'
                THEN 'IV'
            WHEN upper(coalesce(administration_types, '')) LIKE '%ORAL%'
              OR upper(coalesce(administration_types, '')) LIKE '%ENTERAL%'
              OR upper(coalesce(administration_types, '')) LIKE '%G TUBE%'
              OR upper(coalesce(administration_types, '')) LIKE '%G-TUBE%'
              OR regexp_matches(upper(coalesce(administration_types, '')), '(^|[^A-Z])NG([^A-Z]|$)')
              OR regexp_matches(upper(coalesce(administration_types, '')), '(^|[^A-Z])OG([^A-Z]|$)')
                THEN 'ENTERAL'
            WHEN upper(coalesce(administration_types, '')) LIKE '%INTRAMUSCULAR%'
              OR regexp_matches(upper(coalesce(administration_types, '')), '(^|[^A-Z])IM([^A-Z]|$)')
                THEN 'IM'
            ELSE 'UNKNOWN'
        END AS enhanced_route_class
    FROM emar_discrete_points
    """
)

# Resolve eMAR route using the explicit administration record first, followed
# by linked pharmacy/prescription order metadata. MIMIC links these tables on
# pharmacy_id. Order route is used only when the administration-level route is
# absent; the source and any disagreement are retained explicitly.
con.execute(
    f"""
    CREATE TABLE pharmacy_route_map AS
    WITH x AS (
        SELECT
            try_cast(pharmacy_id AS BIGINT) AS pharmacy_id,
            nullif(trim(route), '') AS route_raw,
            CASE
                WHEN upper(trim(coalesce(route, ''))) LIKE '%INTRAVENOUS%'
                  OR regexp_matches(
                      upper(trim(coalesce(route, ''))),
                      '(^|[^A-Z])IV([^A-Z]|$)'
                  )
                  OR upper(trim(coalesce(route, ''))) LIKE '%IV PUSH%'
                  OR upper(trim(coalesce(route, ''))) LIKE '%IVPB%'
                    THEN 'IV'
                WHEN upper(trim(coalesce(route, ''))) IN ('PO', 'ORAL')
                  OR upper(trim(coalesce(route, ''))) LIKE '%ORAL%'
                  OR upper(trim(coalesce(route, ''))) LIKE '%ENTERAL%'
                  OR upper(trim(coalesce(route, ''))) LIKE '%G TUBE%'
                  OR upper(trim(coalesce(route, ''))) LIKE '%G-TUBE%'
                  OR regexp_matches(
                      upper(trim(coalesce(route, ''))),
                      '(^|[^A-Z])NG([^A-Z]|$)'
                  )
                  OR regexp_matches(
                      upper(trim(coalesce(route, ''))),
                      '(^|[^A-Z])OG([^A-Z]|$)'
                  )
                    THEN 'ENTERAL'
                WHEN upper(trim(coalesce(route, ''))) LIKE '%INTRAMUSCULAR%'
                  OR regexp_matches(
                      upper(trim(coalesce(route, ''))),
                      '(^|[^A-Z])IM([^A-Z]|$)'
                  )
                    THEN 'IM'
                WHEN trim(coalesce(route, '')) = '' THEN NULL
                ELSE 'OTHER'
            END AS normalized_route
        FROM read_csv_auto(
            '{qpath(PHARMACY_FILE)}',
            header=true,
            all_varchar=true
        )
        WHERE try_cast(pharmacy_id AS BIGINT) IN (
            SELECT DISTINCT pharmacy_id
            FROM emar_discrete_enhanced
            WHERE pharmacy_id IS NOT NULL
        )
    )
    SELECT
        pharmacy_id,
        string_agg(
            DISTINCT route_raw,
            ' | ' ORDER BY route_raw
        ) AS pharmacy_routes_raw,
        COUNT(DISTINCT normalized_route) FILTER (
            WHERE normalized_route IS NOT NULL
        ) AS pharmacy_route_n,
        max(normalized_route) FILTER (
            WHERE normalized_route IS NOT NULL
        ) AS pharmacy_single_route,
        CASE
            WHEN COUNT(DISTINCT normalized_route) FILTER (
                WHERE normalized_route IS NOT NULL
            ) = 1
                THEN max(normalized_route) FILTER (
                    WHERE normalized_route IS NOT NULL
                )
            WHEN COUNT(DISTINCT normalized_route) FILTER (
                WHERE normalized_route IS NOT NULL
            ) > 1 THEN 'MIXED'
            ELSE 'UNKNOWN'
        END AS pharmacy_route_class
    FROM x
    GROUP BY pharmacy_id
    """
)

con.execute(
    f"""
    CREATE TABLE prescription_route_map AS
    WITH x AS (
        SELECT
            try_cast(pharmacy_id AS BIGINT) AS pharmacy_id,
            nullif(trim(route), '') AS route_raw,
            CASE
                WHEN upper(trim(coalesce(route, ''))) LIKE '%INTRAVENOUS%'
                  OR regexp_matches(
                      upper(trim(coalesce(route, ''))),
                      '(^|[^A-Z])IV([^A-Z]|$)'
                  )
                  OR upper(trim(coalesce(route, ''))) LIKE '%IV PUSH%'
                  OR upper(trim(coalesce(route, ''))) LIKE '%IVPB%'
                    THEN 'IV'
                WHEN upper(trim(coalesce(route, ''))) IN ('PO', 'ORAL')
                  OR upper(trim(coalesce(route, ''))) LIKE '%ORAL%'
                  OR upper(trim(coalesce(route, ''))) LIKE '%ENTERAL%'
                  OR upper(trim(coalesce(route, ''))) LIKE '%G TUBE%'
                  OR upper(trim(coalesce(route, ''))) LIKE '%G-TUBE%'
                  OR regexp_matches(
                      upper(trim(coalesce(route, ''))),
                      '(^|[^A-Z])NG([^A-Z]|$)'
                  )
                  OR regexp_matches(
                      upper(trim(coalesce(route, ''))),
                      '(^|[^A-Z])OG([^A-Z]|$)'
                  )
                    THEN 'ENTERAL'
                WHEN upper(trim(coalesce(route, ''))) LIKE '%INTRAMUSCULAR%'
                  OR regexp_matches(
                      upper(trim(coalesce(route, ''))),
                      '(^|[^A-Z])IM([^A-Z]|$)'
                  )
                    THEN 'IM'
                WHEN trim(coalesce(route, '')) = '' THEN NULL
                ELSE 'OTHER'
            END AS normalized_route
        FROM read_csv_auto(
            '{qpath(PRESCRIPTIONS_FILE)}',
            header=true,
            all_varchar=true
        )
        WHERE try_cast(pharmacy_id AS BIGINT) IN (
            SELECT DISTINCT pharmacy_id
            FROM emar_discrete_enhanced
            WHERE pharmacy_id IS NOT NULL
        )
    )
    SELECT
        pharmacy_id,
        string_agg(
            DISTINCT route_raw,
            ' | ' ORDER BY route_raw
        ) AS prescription_routes_raw,
        COUNT(DISTINCT normalized_route) FILTER (
            WHERE normalized_route IS NOT NULL
        ) AS prescription_route_n,
        max(normalized_route) FILTER (
            WHERE normalized_route IS NOT NULL
        ) AS prescription_single_route,
        CASE
            WHEN COUNT(DISTINCT normalized_route) FILTER (
                WHERE normalized_route IS NOT NULL
            ) = 1
                THEN max(normalized_route) FILTER (
                    WHERE normalized_route IS NOT NULL
                )
            WHEN COUNT(DISTINCT normalized_route) FILTER (
                WHERE normalized_route IS NOT NULL
            ) > 1 THEN 'MIXED'
            ELSE 'UNKNOWN'
        END AS prescription_route_class
    FROM x
    GROUP BY pharmacy_id
    """
)

con.execute(
    """
    CREATE TABLE emar_discrete_route_resolved AS
    SELECT
        e.*,
        p.pharmacy_routes_raw,
        p.pharmacy_route_class,
        r.prescription_routes_raw,
        r.prescription_route_class,
        CASE
            WHEN e.enhanced_route_class NOT IN ('UNKNOWN', 'MIXED')
                THEN e.enhanced_route_class
            WHEN p.pharmacy_route_class NOT IN ('UNKNOWN', 'MIXED')
                THEN p.pharmacy_route_class
            WHEN r.prescription_route_class NOT IN ('UNKNOWN', 'MIXED')
                THEN r.prescription_route_class
            ELSE 'UNKNOWN'
        END AS resolved_route_class,
        CASE
            WHEN e.route_class NOT IN ('UNKNOWN', 'MIXED')
                THEN 'EMAR_DETAIL_ROUTE'
            WHEN e.enhanced_route_class NOT IN ('UNKNOWN', 'MIXED')
                THEN 'EMAR_ADMINISTRATION_TYPE'
            WHEN p.pharmacy_route_class NOT IN ('UNKNOWN', 'MIXED')
                THEN 'PHARMACY_ORDER_ROUTE'
            WHEN r.prescription_route_class NOT IN ('UNKNOWN', 'MIXED')
                THEN 'PRESCRIPTION_ORDER_ROUTE'
            ELSE 'UNRESOLVED'
        END AS route_resolution_source,
        CASE
            WHEN e.enhanced_route_class NOT IN ('UNKNOWN', 'MIXED')
             AND p.pharmacy_route_class NOT IN ('UNKNOWN', 'MIXED')
             AND e.enhanced_route_class <> p.pharmacy_route_class
                THEN 1
            WHEN e.enhanced_route_class NOT IN ('UNKNOWN', 'MIXED')
             AND r.prescription_route_class NOT IN ('UNKNOWN', 'MIXED')
             AND e.enhanced_route_class <> r.prescription_route_class
                THEN 1
            WHEN p.pharmacy_route_class NOT IN ('UNKNOWN', 'MIXED')
             AND r.prescription_route_class NOT IN ('UNKNOWN', 'MIXED')
             AND p.pharmacy_route_class <> r.prescription_route_class
                THEN 1
            ELSE 0
        END AS route_conflict_flag
    FROM emar_discrete_enhanced e
    LEFT JOIN pharmacy_route_map p
        ON e.pharmacy_id = p.pharmacy_id
    LEFT JOIN prescription_route_map r
        ON e.pharmacy_id = r.pharmacy_id
    """
)

# Same-source nearby equal-dose administrations are used to identify situations
# where cross-source matching could be ambiguous.
con.execute(
    """
    CREATE TABLE input_same_source_context AS
    SELECT
        i.input_point_id,
        COUNT(j.input_point_id) AS competing_same_dose_60m_n
    FROM input_bolus_points i
    LEFT JOIN input_bolus_points j
        ON i.stay_id = j.stay_id
       AND i.drug_class = j.drug_class
       AND i.input_point_id <> j.input_point_id
       AND j.administration_time BETWEEN
           i.administration_time - INTERVAL '60 minutes'
           AND i.administration_time + INTERVAL '60 minutes'
       AND abs(i.dose_mg - j.dose_mg) <= greatest(
           0.5,
           0.05 * greatest(abs(i.dose_mg), abs(j.dose_mg))
       )
    GROUP BY i.input_point_id
    """
)

con.execute(
    """
    CREATE TABLE emar_same_source_context AS
    SELECT
        e.emar_id,
        COUNT(j.emar_id) AS competing_same_dose_60m_n
    FROM emar_discrete_route_resolved e
    LEFT JOIN emar_discrete_route_resolved j
        ON e.stay_id = j.stay_id
       AND e.drug_class = j.drug_class
       AND e.emar_id <> j.emar_id
       AND e.dose_mg IS NOT NULL
       AND e.dose_mg > 0
       AND j.dose_mg IS NOT NULL
       AND j.dose_mg > 0
       AND j.administration_time BETWEEN
           e.administration_time - INTERVAL '60 minutes'
           AND e.administration_time + INTERVAL '60 minutes'
       AND abs(e.dose_mg - j.dose_mg) <= greatest(
           0.5,
           0.05 * greatest(abs(e.dose_mg), abs(j.dose_mg))
       )
    GROUP BY e.emar_id
    """
)

con.execute(
    """
    CREATE TABLE mutual_bolus_matches_v5 AS
    SELECT
        m.*,
        coalesce(i.competing_same_dose_60m_n, 0)
            AS input_competing_same_dose_60m_n,
        coalesce(e.competing_same_dose_60m_n, 0)
            AS emar_competing_same_dose_60m_n,
        CASE
            WHEN m.dose_concordant_flag = 1
             AND m.abs_time_difference_minutes <= 15 THEN 1
            ELSE 0
        END AS duplicate_15m_flag,
        CASE
            WHEN m.dose_concordant_flag = 1
             AND m.abs_time_difference_minutes <= 60 THEN 1
            ELSE 0
        END AS duplicate_60m_flag
    FROM mutual_bolus_matches m
    LEFT JOIN input_same_source_context i
        ON m.input_point_id = i.input_point_id
    LEFT JOIN emar_same_source_context e
        ON m.emar_id = e.emar_id
    """
)

# In matched dose-concordant furosemide events, INPUTEVENTS provides a known IV
# reference. This is used only to audit the accuracy of order-based route
# resolution; it is not used to assign the route of unmatched eMAR events.
con.execute(
    """
    CREATE TABLE matched_route_validation AS
    SELECT
        m.emar_id,
        m.drug_class,
        m.abs_time_difference_minutes,
        m.dose_concordant_flag,
        e.enhanced_route_class,
        e.pharmacy_route_class,
        e.prescription_route_class,
        e.resolved_route_class,
        e.route_resolution_source,
        e.route_conflict_flag,
        CASE
            WHEN e.resolved_route_class = 'IV' THEN 1
            ELSE 0
        END AS resolved_matches_input_iv_flag
    FROM mutual_bolus_matches_v5 m
    INNER JOIN emar_discrete_route_resolved e
        ON m.emar_id = e.emar_id
    WHERE m.duplicate_60m_flag = 1
    """
)

con.execute(
    """
    CREATE TABLE emar_supplement_15m AS
    SELECT e.*
    FROM emar_discrete_route_resolved e
    LEFT JOIN mutual_bolus_matches_v5 m
        ON e.emar_id = m.emar_id
       AND m.duplicate_15m_flag = 1
    WHERE e.dose_mg IS NOT NULL
      AND e.dose_mg > 0
      AND m.emar_id IS NULL
    """
)

con.execute(
    """
    CREATE TABLE emar_supplement_60m AS
    SELECT e.*
    FROM emar_discrete_route_resolved e
    LEFT JOIN mutual_bolus_matches_v5 m
        ON e.emar_id = m.emar_id
       AND m.duplicate_60m_flag = 1
    WHERE e.dose_mg IS NOT NULL
      AND e.dose_mg > 0
      AND m.emar_id IS NULL
    """
)

# INPUTEVENTS is authoritative whenever a dose-concordant cross-source duplicate
# exists. eMAR supplements are retained as point events at administration_time.
def create_integrated_bolus(table_name: str, supplement_table: str, method: str) -> None:
    con.execute(
        f"""
        CREATE TABLE {table_name} AS
        SELECT
            '{method}' AS dedup_method,
            cast(input_point_id AS VARCHAR) AS source_event_id,
            stay_id,
            drug_class,
            administration_time,
            dose_mg,
            'INPUTEVENTS' AS source_name,
            'IV' AS route_class
        FROM input_bolus_points

        UNION ALL

        SELECT
            '{method}',
            emar_id,
            stay_id,
            drug_class,
            administration_time,
            dose_mg,
            'EMAR',
            resolved_route_class
        FROM {supplement_table}
        """
    )


create_integrated_bolus(
    'integrated_bolus_15m', 'emar_supplement_15m', 'DEDUP_15M'
)
create_integrated_bolus(
    'integrated_bolus_60m', 'emar_supplement_60m', 'DEDUP_60M'
)

con.execute(
    """
    CREATE TABLE integrated_bolus_all AS
    SELECT * FROM integrated_bolus_15m
    UNION ALL
    SELECT * FROM integrated_bolus_60m
    """
)

# Quantitative infusions come only from INPUTEVENTS. Explicit start/end times are
# clipped to [t0, analytical_end); no LOCF, endpoint anchor, or post-stop carry is
# permitted.
con.execute(
    """
    CREATE TABLE input_infusion_quantitative AS
    SELECT
        input_point_id,
        stay_id,
        drug_class,
        starttime,
        endtime,
        overlap_start_time,
        overlap_end_time,
        overlap_minutes,
        recorded_duration_minutes,
        input_amount_mg_full,
        input_rate_mg_h,
        input_amount_mg_horizon
    FROM input_delivered
    WHERE input_pattern = 'TIMED_INFUSION_SEGMENT'
      AND overlap_minutes > 0
      AND input_amount_mg_horizon IS NOT NULL
      AND input_amount_mg_horizon > 0
    """
)


# =============================================================================
# Temporal grids and allocation
# =============================================================================

con.execute(
    """
    CREATE TABLE hourly_windows AS
    SELECT
        i AS window_index,
        i AS start_hour,
        i + 1 AS end_hour,
        60 AS nominal_minutes,
        'H' || lpad(i::VARCHAR, 2, '0')
            || '_H' || lpad((i + 1)::VARCHAR, 2, '0') AS window_label
    FROM range(0, 24) t(i)
    """
)

con.execute(
    """
    CREATE TABLE six_hour_windows AS
    SELECT * FROM (
        VALUES
            (0, 0, 6, 360, 'H00_H06'),
            (1, 6, 12, 360, 'H06_H12'),
            (2, 12, 18, 360, 'H12_H18'),
            (3, 18, 24, 360, 'H18_H24')
    ) AS t(window_index, start_hour, end_hour, nominal_minutes, window_label)
    """
)


def create_grid(grid_name: str, windows_table: str, grid_type: str) -> None:
    con.execute(
        f"""
        CREATE TABLE {grid_name} AS
        SELECT
            '{grid_type}' AS grid_type,
            c.stay_id,
            c.postoperative_time_zero,
            c.analytical_end_time,
            w.window_index,
            w.window_label,
            w.start_hour,
            w.end_hour,
            w.nominal_minutes,
            c.postoperative_time_zero
                + w.start_hour * INTERVAL '1 hour' AS window_start_time,
            least(
                c.analytical_end_time,
                c.postoperative_time_zero
                    + w.end_hour * INTERVAL '1 hour'
            ) AS eligible_window_end_time,
            greatest(
                0.0,
                date_diff(
                    'second',
                    c.postoperative_time_zero
                        + w.start_hour * INTERVAL '1 hour',
                    least(
                        c.analytical_end_time,
                        c.postoperative_time_zero
                            + w.end_hour * INTERVAL '1 hour'
                    )
                ) / 60.0
            ) AS eligible_minutes
        FROM cohort c
        CROSS JOIN {windows_table} w
        """
    )


create_grid('hourly_grid', 'hourly_windows', 'HOURLY')
create_grid('six_grid', 'six_hour_windows', 'SIX_HOUR')

con.execute(
    """
    CREATE TABLE all_grids AS
    SELECT * FROM hourly_grid
    UNION ALL
    SELECT * FROM six_grid
    """
)

con.execute(
    """
    CREATE TABLE dedup_methods AS
    SELECT * FROM (VALUES ('DEDUP_15M'), ('DEDUP_60M')) AS t(dedup_method)
    """
)

# Bolus events are point exposures. Events at t0 are included; events exactly at
# analytical_end are excluded by source extraction and are not used as anchors.
con.execute(
    """
    CREATE TABLE bolus_window AS
    SELECT
        g.grid_type,
        m.dedup_method,
        g.stay_id,
        g.window_index,
        g.window_label,
        g.eligible_minutes,
        COUNT(b.source_event_id) AS all_bolus_events_n,
        COUNT(b.source_event_id) FILTER (
            WHERE b.drug_class = 'LOOP_FUROSEMIDE'
        ) AS furosemide_bolus_events_n,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_FUROSEMIDE'
        ), 0.0) AS furosemide_bolus_mg,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_FUROSEMIDE'
              AND b.source_name = 'INPUTEVENTS'
        ), 0.0) AS furosemide_input_bolus_mg,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_FUROSEMIDE'
              AND b.source_name = 'EMAR'
        ), 0.0) AS furosemide_emar_supplement_mg,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_FUROSEMIDE'
              AND b.source_name = 'EMAR'
              AND b.route_class = 'UNKNOWN'
        ), 0.0) AS furosemide_emar_unknown_route_mg,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_BUMETANIDE'
        ), 0.0) AS bumetanide_bolus_mg,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_TORSEMIDE'
        ), 0.0) AS torsemide_bolus_mg,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_ETHACRYNIC_ACID'
        ), 0.0) AS ethacrynic_acid_bolus_mg,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'THIAZIDE_CHLOROTHIAZIDE'
        ), 0.0) AS chlorothiazide_mg,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'THIAZIDE_METOLAZONE'
        ), 0.0) AS metolazone_mg,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'CARBONIC_ACETAZOLAMIDE'
        ), 0.0) AS acetazolamide_mg
    FROM all_grids g
    CROSS JOIN dedup_methods m
    LEFT JOIN integrated_bolus_all b
        ON b.dedup_method = m.dedup_method
       AND b.stay_id = g.stay_id
       AND g.eligible_minutes > 0
       AND b.administration_time >= g.window_start_time
       AND b.administration_time < g.eligible_window_end_time
    GROUP BY
        g.grid_type,
        m.dedup_method,
        g.stay_id,
        g.window_index,
        g.window_label,
        g.eligible_minutes
    """
)

# Infusion amounts are allocated by exact overlap with explicit start/end
# intervals. The CASE on NULL input IDs prevents false coverage/allocation from
# LEFT JOIN null semantics.
con.execute(
    """
    CREATE TABLE infusion_window AS
    WITH overlap AS (
        SELECT
            g.grid_type,
            g.stay_id,
            g.window_index,
            g.window_label,
            g.eligible_minutes,
            i.input_point_id,
            i.drug_class,
            CASE
                WHEN i.input_point_id IS NULL THEN 0.0
                ELSE greatest(
                    0.0,
                    date_diff(
                        'second',
                        greatest(i.overlap_start_time, g.window_start_time),
                        least(i.overlap_end_time, g.eligible_window_end_time)
                    ) / 60.0
                )
            END AS overlap_window_minutes,
            i.input_amount_mg_horizon,
            i.overlap_minutes
        FROM all_grids g
        LEFT JOIN input_infusion_quantitative i
            ON i.stay_id = g.stay_id
           AND g.eligible_minutes > 0
           AND i.overlap_start_time < g.eligible_window_end_time
           AND i.overlap_end_time > g.window_start_time
    )
    SELECT
        grid_type,
        stay_id,
        window_index,
        window_label,
        eligible_minutes,
        COUNT(input_point_id) FILTER (
            WHERE drug_class = 'LOOP_FUROSEMIDE'
              AND overlap_window_minutes > 0
        ) AS infusion_segments_n,
        coalesce(SUM(
            input_amount_mg_horizon
            * overlap_window_minutes / nullif(overlap_minutes, 0)
        ) FILTER (
            WHERE drug_class = 'LOOP_FUROSEMIDE'
              AND overlap_window_minutes > 0
        ), 0.0) AS furosemide_infusion_mg,
        coalesce(SUM(overlap_window_minutes) FILTER (
            WHERE drug_class = 'LOOP_FUROSEMIDE'
              AND overlap_window_minutes > 0
        ), 0.0) AS furosemide_infusion_minutes
    FROM overlap
    GROUP BY grid_type, stay_id, window_index, window_label, eligible_minutes
    """
)

con.execute(
    """
    CREATE TABLE window_method_output AS
    SELECT
        b.grid_type,
        b.dedup_method,
        b.stay_id,
        b.window_index,
        b.window_label,
        b.eligible_minutes,
        CASE WHEN a.stay_id IS NOT NULL THEN 1 ELSE 0 END
            AS emar_available_in_horizon_flag,
        b.all_bolus_events_n,
        b.furosemide_bolus_events_n,
        b.furosemide_bolus_mg,
        b.furosemide_input_bolus_mg,
        b.furosemide_emar_supplement_mg,
        b.furosemide_emar_unknown_route_mg,
        i.furosemide_infusion_mg,
        i.furosemide_infusion_minutes,
        b.furosemide_bolus_mg + i.furosemide_infusion_mg
            AS furosemide_total_mg,
        b.bumetanide_bolus_mg,
        b.torsemide_bolus_mg,
        b.ethacrynic_acid_bolus_mg,
        b.chlorothiazide_mg,
        b.metolazone_mg,
        b.acetazolamide_mg,
        CASE
            WHEN b.furosemide_bolus_mg + i.furosemide_infusion_mg > 0
                THEN 1 ELSE 0
        END AS furosemide_exposed_flag,
        CASE
            WHEN b.furosemide_bolus_mg + i.furosemide_infusion_mg > 0
              OR b.bumetanide_bolus_mg > 0
              OR b.torsemide_bolus_mg > 0
              OR b.ethacrynic_acid_bolus_mg > 0
                THEN 1 ELSE 0
        END AS any_loop_exposed_flag
    FROM bolus_window b
    INNER JOIN infusion_window i
        ON b.grid_type = i.grid_type
       AND b.stay_id = i.stay_id
       AND b.window_index = i.window_index
    LEFT JOIN emar_horizon_availability a
        ON b.stay_id = a.stay_id
    """
)

# Integrated 24-hour totals under both deduplication rules.
con.execute(
    """
    CREATE TABLE patient_24h_method AS
    SELECT
        c.stay_id,
        m.dedup_method,
        CASE WHEN a.stay_id IS NOT NULL THEN 1 ELSE 0 END
            AS emar_available_in_horizon_flag,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_FUROSEMIDE'
        ), 0.0) AS furosemide_bolus_mg,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_FUROSEMIDE'
              AND b.source_name = 'INPUTEVENTS'
        ), 0.0) AS furosemide_input_bolus_mg,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_FUROSEMIDE'
              AND b.source_name = 'EMAR'
        ), 0.0) AS furosemide_emar_supplement_mg,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_FUROSEMIDE'
              AND b.source_name = 'EMAR'
              AND b.route_class = 'UNKNOWN'
        ), 0.0) AS furosemide_emar_unknown_route_mg,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_BUMETANIDE'
        ), 0.0) AS bumetanide_mg,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_ETHACRYNIC_ACID'
        ), 0.0) AS ethacrynic_acid_mg,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_TORSEMIDE'
        ), 0.0) AS torsemide_mg
    FROM cohort c
    CROSS JOIN dedup_methods m
    LEFT JOIN integrated_bolus_all b
        ON b.stay_id = c.stay_id
       AND b.dedup_method = m.dedup_method
    LEFT JOIN emar_horizon_availability a
        ON c.stay_id = a.stay_id
    GROUP BY c.stay_id, m.dedup_method, a.stay_id
    """
)

con.execute(
    """
    CREATE TABLE patient_infusion_24h AS
    SELECT
        c.stay_id,
        coalesce(SUM(i.input_amount_mg_horizon) FILTER (
            WHERE i.drug_class = 'LOOP_FUROSEMIDE'
        ), 0.0) AS furosemide_infusion_mg,
        coalesce(SUM(i.overlap_minutes) FILTER (
            WHERE i.drug_class = 'LOOP_FUROSEMIDE'
        ), 0.0) AS furosemide_infusion_minutes
    FROM cohort c
    LEFT JOIN input_infusion_quantitative i
        ON c.stay_id = i.stay_id
    GROUP BY c.stay_id
    """
)

con.execute(
    """
    CREATE TABLE patient_dedup_comparison AS
    WITH wide AS (
        SELECT
            p.stay_id,
            max(p.emar_available_in_horizon_flag)
                AS emar_available_in_horizon_flag,
            max(p.furosemide_bolus_mg) FILTER (
                WHERE p.dedup_method = 'DEDUP_15M'
            ) AS furosemide_bolus_mg_15m,
            max(p.furosemide_bolus_mg) FILTER (
                WHERE p.dedup_method = 'DEDUP_60M'
            ) AS furosemide_bolus_mg_60m,
            max(p.furosemide_emar_supplement_mg) FILTER (
                WHERE p.dedup_method = 'DEDUP_15M'
            ) AS emar_supplement_mg_15m,
            max(p.furosemide_emar_supplement_mg) FILTER (
                WHERE p.dedup_method = 'DEDUP_60M'
            ) AS emar_supplement_mg_60m,
            max(p.furosemide_emar_unknown_route_mg) FILTER (
                WHERE p.dedup_method = 'DEDUP_60M'
            ) AS emar_unknown_route_mg_60m,
            max(p.bumetanide_mg) FILTER (
                WHERE p.dedup_method = 'DEDUP_60M'
            ) AS bumetanide_mg_60m,
            max(p.ethacrynic_acid_mg) FILTER (
                WHERE p.dedup_method = 'DEDUP_60M'
            ) AS ethacrynic_acid_mg_60m,
            max(p.torsemide_mg) FILTER (
                WHERE p.dedup_method = 'DEDUP_60M'
            ) AS torsemide_mg_60m
        FROM patient_24h_method p
        GROUP BY p.stay_id
    )
    SELECT
        w.*,
        i.furosemide_infusion_mg,
        i.furosemide_infusion_minutes,
        w.furosemide_bolus_mg_15m + i.furosemide_infusion_mg
            AS furosemide_total_mg_15m,
        w.furosemide_bolus_mg_60m + i.furosemide_infusion_mg
            AS furosemide_total_mg_60m,
        w.furosemide_bolus_mg_15m - w.furosemide_bolus_mg_60m
            AS furosemide_15m_minus_60m_mg,
        abs(w.furosemide_bolus_mg_15m - w.furosemide_bolus_mg_60m)
            AS abs_furosemide_method_difference_mg
    FROM wide w
    INNER JOIN patient_infusion_24h i
        ON w.stay_id = i.stay_id
    """
)



# =============================================================================
# Canonical 60-minute integrated representation
# =============================================================================

# Route-aware quantitative bolus allocation. Observable windows with no
# documented administration retain dose zero; non-observable windows are set to
# NULL only in the final output layer.
con.execute(
    """
    CREATE TABLE canonical_bolus_window AS
    SELECT
        g.grid_type,
        g.stay_id,
        g.window_index,
        g.window_label,
        g.eligible_minutes,

        COUNT(b.source_event_id) AS quantitative_bolus_events_n,
        COUNT(b.source_event_id) FILTER (
            WHERE b.drug_class = 'LOOP_FUROSEMIDE'
        ) AS furosemide_bolus_events_n,

        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_FUROSEMIDE'
              AND b.source_name = 'INPUTEVENTS'
        ), 0.0) AS furosemide_inputevents_iv_bolus_mg,

        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_FUROSEMIDE'
              AND b.source_name = 'EMAR'
              AND b.route_class = 'IV'
        ), 0.0) AS furosemide_emar_iv_bolus_mg,

        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_FUROSEMIDE'
              AND b.source_name = 'EMAR'
              AND b.route_class = 'ENTERAL'
        ), 0.0) AS furosemide_enteral_bolus_mg,

        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_FUROSEMIDE'
              AND b.source_name = 'EMAR'
              AND b.route_class = 'UNKNOWN'
        ), 0.0) AS furosemide_unknown_route_bolus_mg,

        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_FUROSEMIDE'
        ), 0.0) AS furosemide_all_route_bolus_mg,

        COUNT(b.source_event_id) FILTER (
            WHERE b.drug_class = 'LOOP_BUMETANIDE'
        ) AS bumetanide_bolus_events_n,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_BUMETANIDE'
              AND b.route_class = 'IV'
        ), 0.0) AS bumetanide_iv_mg,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_BUMETANIDE'
              AND b.route_class = 'ENTERAL'
        ), 0.0) AS bumetanide_enteral_mg,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_BUMETANIDE'
              AND b.route_class = 'UNKNOWN'
        ), 0.0) AS bumetanide_unknown_route_mg,

        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_TORSEMIDE'
        ), 0.0) AS torsemide_mg,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'LOOP_ETHACRYNIC_ACID'
        ), 0.0) AS ethacrynic_acid_mg,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'THIAZIDE_CHLOROTHIAZIDE'
        ), 0.0) AS chlorothiazide_mg,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'THIAZIDE_METOLAZONE'
        ), 0.0) AS metolazone_mg,
        coalesce(SUM(b.dose_mg) FILTER (
            WHERE b.drug_class = 'CARBONIC_ACETAZOLAMIDE'
        ), 0.0) AS acetazolamide_mg

    FROM all_grids g
    LEFT JOIN integrated_bolus_60m b
        ON b.stay_id = g.stay_id
       AND g.eligible_minutes > 0
       AND b.administration_time >= g.window_start_time
       AND b.administration_time < g.eligible_window_end_time
    GROUP BY
        g.grid_type,
        g.stay_id,
        g.window_index,
        g.window_label,
        g.eligible_minutes
    """
)

# Discrete eMAR administrations with no quantitative dose are retained only when
# no same-drug INPUTEVENTS bolus occurs within 60 minutes. This prevents a
# non-quantifiable eMAR display/documentation row from adding a second exposure
# flag beside an already quantified ICU bolus.
con.execute(
    """
    CREATE TABLE emar_unquantified_supplement AS
    WITH candidates AS (
        SELECT e.*
        FROM emar_discrete_route_resolved e
        WHERE e.dose_mg IS NULL OR e.dose_mg <= 0
    ), nearest_input AS (
        SELECT
            e.emar_id,
            min(abs(date_diff(
                'second', e.administration_time, i.administration_time
            )) / 60.0) AS nearest_input_minutes
        FROM candidates e
        LEFT JOIN input_bolus_points i
            ON e.stay_id = i.stay_id
           AND e.drug_class = i.drug_class
           AND i.administration_time BETWEEN
               e.administration_time - INTERVAL '60 minutes'
               AND e.administration_time + INTERVAL '60 minutes'
        GROUP BY e.emar_id
    )
    SELECT e.*
    FROM candidates e
    LEFT JOIN nearest_input n
        ON e.emar_id = n.emar_id
    WHERE n.nearest_input_minutes IS NULL
       OR n.nearest_input_minutes > 60
    """
)

# A Started marker is retained as unquantified exposure only when no matching
# INPUTEVENTS infusion start is available within 60 minutes.
con.execute(
    """
    CREATE TABLE emar_unmatched_started AS
    SELECT e.*
    FROM emar_started_points e
    LEFT JOIN emar_started_nearest_input m
        ON e.emar_id = m.emar_id
    WHERE m.emar_id IS NULL
    """
)

con.execute(
    """
    CREATE TABLE canonical_unquantified_window AS
    WITH point_events AS (
        SELECT
            stay_id,
            drug_class,
            administration_time AS event_time,
            'UNQUANTIFIED_DISCRETE' AS event_type
        FROM emar_unquantified_supplement

        UNION ALL

        SELECT
            stay_id,
            drug_class,
            charttime AS event_time,
            'UNMATCHED_INFUSION_STARTED' AS event_type
        FROM emar_unmatched_started
    )
    SELECT
        g.grid_type,
        g.stay_id,
        g.window_index,
        COUNT(p.event_time) FILTER (
            WHERE p.drug_class = 'LOOP_FUROSEMIDE'
              AND p.event_type = 'UNQUANTIFIED_DISCRETE'
        ) AS furosemide_unquantified_discrete_n,
        COUNT(p.event_time) FILTER (
            WHERE p.drug_class = 'LOOP_FUROSEMIDE'
              AND p.event_type = 'UNMATCHED_INFUSION_STARTED'
        ) AS furosemide_unmatched_started_n,
        COUNT(p.event_time) FILTER (
            WHERE p.drug_class IN (
                'LOOP_FUROSEMIDE', 'LOOP_BUMETANIDE',
                'LOOP_TORSEMIDE', 'LOOP_ETHACRYNIC_ACID'
            )
        ) AS any_loop_unquantified_events_n
    FROM all_grids g
    LEFT JOIN point_events p
        ON p.stay_id = g.stay_id
       AND g.eligible_minutes > 0
       AND p.event_time >= g.window_start_time
       AND p.event_time < g.eligible_window_end_time
    GROUP BY g.grid_type, g.stay_id, g.window_index
    """
)

# Canonical source-level window representation.
con.execute(
    """
    CREATE TABLE canonical_window_core AS
    SELECT
        g.grid_type,
        g.stay_id,
        g.window_index,
        g.window_label,
        g.start_hour,
        g.end_hour,
        g.nominal_minutes,
        g.window_start_time,
        g.eligible_window_end_time,
        g.eligible_minutes,

        CASE WHEN a.stay_id IS NOT NULL THEN 1 ELSE 0 END
            AS emar_available_in_horizon_flag,

        b.quantitative_bolus_events_n,
        b.furosemide_bolus_events_n,
        b.furosemide_inputevents_iv_bolus_mg,
        b.furosemide_emar_iv_bolus_mg,
        b.furosemide_enteral_bolus_mg,
        b.furosemide_unknown_route_bolus_mg,
        b.furosemide_all_route_bolus_mg,

        i.furosemide_infusion_mg,
        i.furosemide_infusion_minutes AS furosemide_infusion_segment_minutes,
        i.infusion_segments_n AS furosemide_infusion_segments_n,

        b.furosemide_inputevents_iv_bolus_mg
            + i.furosemide_infusion_mg
            AS furosemide_inputevents_only_mg,

        b.furosemide_inputevents_iv_bolus_mg
            + b.furosemide_emar_iv_bolus_mg
            + i.furosemide_infusion_mg
            AS furosemide_iv_total_mg,

        b.furosemide_all_route_bolus_mg
            + i.furosemide_infusion_mg
            AS furosemide_all_route_total_mg,

        b.furosemide_emar_iv_bolus_mg
            + b.furosemide_enteral_bolus_mg
            + b.furosemide_unknown_route_bolus_mg
            AS furosemide_emar_supplement_mg,

        b.bumetanide_bolus_events_n,
        b.bumetanide_iv_mg,
        b.bumetanide_enteral_mg,
        b.bumetanide_unknown_route_mg,
        b.torsemide_mg,
        b.ethacrynic_acid_mg,
        b.chlorothiazide_mg,
        b.metolazone_mg,
        b.acetazolamide_mg,

        u.furosemide_unquantified_discrete_n,
        u.furosemide_unmatched_started_n,
        u.any_loop_unquantified_events_n

    FROM all_grids g
    INNER JOIN canonical_bolus_window b
        ON g.grid_type = b.grid_type
       AND g.stay_id = b.stay_id
       AND g.window_index = b.window_index
    INNER JOIN infusion_window i
        ON g.grid_type = i.grid_type
       AND g.stay_id = i.stay_id
       AND g.window_index = i.window_index
    INNER JOIN canonical_unquantified_window u
        ON g.grid_type = u.grid_type
       AND g.stay_id = u.stay_id
       AND g.window_index = u.window_index
    LEFT JOIN emar_horizon_availability a
        ON g.stay_id = a.stay_id
    """
)

# Patient-level integrated summaries repeated in both canonical trajectory
# outputs for provenance and downstream sensitivity analyses.
con.execute(
    """
    CREATE TABLE canonical_patient_24h AS
    WITH q AS (
        SELECT
            stay_id,
            SUM(furosemide_inputevents_iv_bolus_mg)
                AS furosemide_inputevents_iv_bolus_24h_mg,
            SUM(furosemide_emar_iv_bolus_mg)
                AS furosemide_emar_iv_bolus_24h_mg,
            SUM(furosemide_enteral_bolus_mg)
                AS furosemide_enteral_bolus_24h_mg,
            SUM(furosemide_unknown_route_bolus_mg)
                AS furosemide_unknown_route_bolus_24h_mg,
            SUM(furosemide_all_route_bolus_mg)
                AS furosemide_all_route_bolus_24h_mg,
            SUM(bumetanide_iv_mg) AS bumetanide_iv_24h_mg,
            SUM(bumetanide_enteral_mg) AS bumetanide_enteral_24h_mg,
            SUM(bumetanide_unknown_route_mg)
                AS bumetanide_unknown_route_24h_mg,
            SUM(torsemide_mg) AS torsemide_24h_mg,
            SUM(ethacrynic_acid_mg) AS ethacrynic_acid_24h_mg,
            SUM(chlorothiazide_mg) AS chlorothiazide_24h_mg,
            SUM(metolazone_mg) AS metolazone_24h_mg,
            SUM(acetazolamide_mg) AS acetazolamide_24h_mg,
            SUM(furosemide_bolus_events_n)
                AS furosemide_bolus_events_24h_n
        FROM canonical_bolus_window
        WHERE grid_type = 'HOURLY'
        GROUP BY stay_id
    ), inf AS (
        SELECT
            c.stay_id,
            coalesce(SUM(i.input_amount_mg_horizon) FILTER (
                WHERE i.drug_class = 'LOOP_FUROSEMIDE'
            ), 0.0) AS furosemide_infusion_24h_mg,
            coalesce(SUM(i.overlap_minutes) FILTER (
                WHERE i.drug_class = 'LOOP_FUROSEMIDE'
            ), 0.0) AS furosemide_infusion_segment_24h_minutes,
            COUNT(i.input_point_id) FILTER (
                WHERE i.drug_class = 'LOOP_FUROSEMIDE'
            ) AS furosemide_infusion_segments_24h_n
        FROM cohort c
        LEFT JOIN input_infusion_quantitative i
            ON c.stay_id = i.stay_id
        GROUP BY c.stay_id
    ), uq AS (
        SELECT
            stay_id,
            SUM(furosemide_unquantified_discrete_n)
                AS furosemide_unquantified_discrete_24h_n,
            SUM(furosemide_unmatched_started_n)
                AS furosemide_unmatched_started_24h_n,
            SUM(any_loop_unquantified_events_n)
                AS any_loop_unquantified_events_24h_n
        FROM canonical_unquantified_window
        WHERE grid_type = 'HOURLY'
        GROUP BY stay_id
    )
    SELECT
        c.stay_id,
        CASE WHEN a.stay_id IS NOT NULL THEN 1 ELSE 0 END
            AS emar_available_in_horizon_flag,
        q.* EXCLUDE (stay_id),
        inf.* EXCLUDE (stay_id),
        uq.* EXCLUDE (stay_id),

        q.furosemide_inputevents_iv_bolus_24h_mg
            + inf.furosemide_infusion_24h_mg
            AS furosemide_inputevents_only_24h_mg,

        q.furosemide_inputevents_iv_bolus_24h_mg
            + q.furosemide_emar_iv_bolus_24h_mg
            + inf.furosemide_infusion_24h_mg
            AS furosemide_iv_total_24h_mg,

        q.furosemide_all_route_bolus_24h_mg
            + inf.furosemide_infusion_24h_mg
            AS furosemide_all_route_total_24h_mg,

        q.furosemide_emar_iv_bolus_24h_mg
            + q.furosemide_enteral_bolus_24h_mg
            + q.furosemide_unknown_route_bolus_24h_mg
            AS furosemide_emar_supplement_24h_mg,

        CASE
            WHEN q.furosemide_all_route_bolus_24h_mg
                   + inf.furosemide_infusion_24h_mg > 0
                THEN 1 ELSE 0
        END AS furosemide_quantitative_exposure_24h_flag,

        CASE
            WHEN q.furosemide_all_route_bolus_24h_mg
                   + inf.furosemide_infusion_24h_mg > 0
              OR uq.furosemide_unquantified_discrete_24h_n > 0
              OR uq.furosemide_unmatched_started_24h_n > 0
                THEN 1 ELSE 0
        END AS furosemide_any_documented_exposure_24h_flag,

        CASE
            WHEN q.furosemide_all_route_bolus_24h_mg
                   + inf.furosemide_infusion_24h_mg > 0
              OR q.bumetanide_iv_24h_mg
                   + q.bumetanide_enteral_24h_mg
                   + q.bumetanide_unknown_route_24h_mg > 0
              OR q.torsemide_24h_mg > 0
              OR q.ethacrynic_acid_24h_mg > 0
                THEN 1 ELSE 0
        END AS any_loop_quantitative_exposure_24h_flag,

        CASE
            WHEN (
                q.furosemide_all_route_bolus_24h_mg
                    + inf.furosemide_infusion_24h_mg > 0
                OR q.bumetanide_iv_24h_mg
                    + q.bumetanide_enteral_24h_mg
                    + q.bumetanide_unknown_route_24h_mg > 0
                OR q.torsemide_24h_mg > 0
                OR q.ethacrynic_acid_24h_mg > 0
            )
            AND (
                q.chlorothiazide_24h_mg > 0
                OR q.metolazone_24h_mg > 0
            ) THEN 1 ELSE 0
        END AS loop_plus_thiazide_24h_flag

    FROM cohort c
    INNER JOIN q ON c.stay_id = q.stay_id
    INNER JOIN inf ON c.stay_id = inf.stay_id
    INNER JOIN uq ON c.stay_id = uq.stay_id
    LEFT JOIN emar_horizon_availability a ON c.stay_id = a.stay_id
    """
)


def create_canonical_output(table_name: str, grid_type: str) -> None:
    con.execute(
        f"""
        CREATE TABLE {table_name} AS
        SELECT
            c.subject_id,
            c.hadm_id,
            c.stay_id,
            c.surgery_group,
            c.sex,
            c.age_at_icu_admission,
            c.postoperative_time_zero,
            c.observation_end_time,
            c.observation_end_type,
            c.analytical_end_time,

            w.window_index,
            w.window_label,
            w.start_hour,
            w.end_hour,
            w.nominal_minutes,
            w.window_start_time,
            c.postoperative_time_zero
                + w.end_hour * INTERVAL '1 hour'
                AS nominal_window_end_time,
            w.eligible_window_end_time,
            w.eligible_minutes,

            CASE
                WHEN w.eligible_minutes = 0 THEN 'NOT_OBSERVABLE'
                WHEN w.eligible_minutes < w.nominal_minutes
                    THEN 'PARTIALLY_OBSERVABLE'
                ELSE 'FULLY_OBSERVABLE'
            END AS observability_status,

            p.emar_available_in_horizon_flag,

            CASE WHEN w.eligible_minutes > 0
                THEN w.quantitative_bolus_events_n ELSE NULL END
                AS diuretic_quantitative_bolus_events_n,
            CASE WHEN w.eligible_minutes > 0
                THEN w.furosemide_bolus_events_n ELSE NULL END
                AS furosemide_bolus_events_n,

            CASE WHEN w.eligible_minutes > 0
                THEN w.furosemide_inputevents_iv_bolus_mg ELSE NULL END
                AS furosemide_inputevents_iv_bolus_mg,
            CASE WHEN w.eligible_minutes > 0
                THEN w.furosemide_emar_iv_bolus_mg ELSE NULL END
                AS furosemide_emar_iv_bolus_mg,
            CASE WHEN w.eligible_minutes > 0
                THEN w.furosemide_enteral_bolus_mg ELSE NULL END
                AS furosemide_enteral_bolus_mg,
            CASE WHEN w.eligible_minutes > 0
                THEN w.furosemide_unknown_route_bolus_mg ELSE NULL END
                AS furosemide_unknown_route_bolus_mg,
            CASE WHEN w.eligible_minutes > 0
                THEN w.furosemide_all_route_bolus_mg ELSE NULL END
                AS furosemide_all_route_bolus_mg,

            CASE WHEN w.eligible_minutes > 0
                THEN w.furosemide_infusion_mg ELSE NULL END
                AS furosemide_infusion_mg,
            CASE WHEN w.eligible_minutes > 0
                THEN w.furosemide_infusion_segment_minutes ELSE NULL END
                AS furosemide_infusion_segment_minutes,
            CASE WHEN w.eligible_minutes > 0
                THEN w.furosemide_infusion_segments_n ELSE NULL END
                AS furosemide_infusion_segments_n,

            CASE WHEN w.eligible_minutes > 0
                THEN w.furosemide_inputevents_only_mg ELSE NULL END
                AS furosemide_inputevents_only_mg,
            CASE WHEN w.eligible_minutes > 0
                THEN w.furosemide_emar_supplement_mg ELSE NULL END
                AS furosemide_emar_supplement_mg,
            CASE WHEN w.eligible_minutes > 0
                THEN w.furosemide_iv_total_mg ELSE NULL END
                AS furosemide_iv_total_mg,
            CASE WHEN w.eligible_minutes > 0
                THEN w.furosemide_all_route_total_mg ELSE NULL END
                AS furosemide_all_route_total_mg,

            CASE
                WHEN w.eligible_minutes = 0 THEN NULL
                WHEN w.furosemide_all_route_total_mg > 0 THEN 1
                ELSE 0
            END AS furosemide_quantitative_exposure_flag,

            CASE
                WHEN w.eligible_minutes = 0 THEN NULL
                WHEN w.furosemide_all_route_total_mg > 0
                  OR w.furosemide_unquantified_discrete_n > 0
                  OR w.furosemide_unmatched_started_n > 0
                    THEN 1
                ELSE 0
            END AS furosemide_any_documented_exposure_flag,

            CASE WHEN w.eligible_minutes > 0
                THEN w.furosemide_unquantified_discrete_n ELSE NULL END
                AS furosemide_unquantified_discrete_n,
            CASE WHEN w.eligible_minutes > 0
                THEN w.furosemide_unmatched_started_n ELSE NULL END
                AS furosemide_unmatched_started_n,

            CASE WHEN w.eligible_minutes > 0
                THEN w.bumetanide_iv_mg ELSE NULL END AS bumetanide_iv_mg,
            CASE WHEN w.eligible_minutes > 0
                THEN w.bumetanide_enteral_mg ELSE NULL END
                AS bumetanide_enteral_mg,
            CASE WHEN w.eligible_minutes > 0
                THEN w.bumetanide_unknown_route_mg ELSE NULL END
                AS bumetanide_unknown_route_mg,
            CASE WHEN w.eligible_minutes > 0
                THEN w.torsemide_mg ELSE NULL END AS torsemide_mg,
            CASE WHEN w.eligible_minutes > 0
                THEN w.ethacrynic_acid_mg ELSE NULL END
                AS ethacrynic_acid_mg,
            CASE WHEN w.eligible_minutes > 0
                THEN w.chlorothiazide_mg ELSE NULL END AS chlorothiazide_mg,
            CASE WHEN w.eligible_minutes > 0
                THEN w.metolazone_mg ELSE NULL END AS metolazone_mg,
            CASE WHEN w.eligible_minutes > 0
                THEN w.acetazolamide_mg ELSE NULL END AS acetazolamide_mg,

            CASE
                WHEN w.eligible_minutes = 0 THEN NULL
                WHEN w.furosemide_all_route_total_mg > 0
                  OR w.bumetanide_iv_mg + w.bumetanide_enteral_mg
                       + w.bumetanide_unknown_route_mg > 0
                  OR w.torsemide_mg > 0
                  OR w.ethacrynic_acid_mg > 0
                    THEN 1 ELSE 0
            END AS any_loop_quantitative_exposure_flag,

            CASE
                WHEN w.eligible_minutes = 0 THEN NULL
                WHEN w.chlorothiazide_mg > 0 OR w.metolazone_mg > 0
                    THEN 1 ELSE 0
            END AS thiazide_type_exposure_flag,

            p.furosemide_inputevents_iv_bolus_24h_mg,
            p.furosemide_emar_iv_bolus_24h_mg,
            p.furosemide_enteral_bolus_24h_mg,
            p.furosemide_unknown_route_bolus_24h_mg,
            p.furosemide_infusion_24h_mg,
            p.furosemide_inputevents_only_24h_mg,
            p.furosemide_emar_supplement_24h_mg,
            p.furosemide_iv_total_24h_mg,
            p.furosemide_all_route_total_24h_mg,
            p.furosemide_quantitative_exposure_24h_flag,
            p.furosemide_any_documented_exposure_24h_flag,
            p.any_loop_quantitative_exposure_24h_flag,
            p.loop_plus_thiazide_24h_flag

        FROM canonical_window_core w
        INNER JOIN cohort c ON w.stay_id = c.stay_id
        INNER JOIN canonical_patient_24h p ON w.stay_id = p.stay_id
        WHERE w.grid_type = '{grid_type}'
        """
    )


create_canonical_output('hourly_output', 'HOURLY')
create_canonical_output('six_output', 'SIX_HOUR')

# =============================================================================
# Validation invariants
# =============================================================================

# Every point bolus must be assigned exactly once to each temporal grid under
# each deduplication method.
for method, table_name in [
    ('DEDUP_15M', 'integrated_bolus_15m'),
    ('DEDUP_60M', 'integrated_bolus_60m'),
]:
    source_dose = con.execute(
        f"SELECT coalesce(SUM(dose_mg), 0) FROM {table_name}"
    ).fetchone()[0]
    hourly_dose = con.execute(
        f"""
        SELECT coalesce(SUM(
            furosemide_bolus_mg
            + bumetanide_bolus_mg
            + torsemide_bolus_mg
            + ethacrynic_acid_bolus_mg
            + chlorothiazide_mg
            + metolazone_mg
            + acetazolamide_mg
        ), 0)
        FROM bolus_window
        WHERE grid_type = 'HOURLY'
          AND dedup_method = '{method}'
        """
    ).fetchone()[0]
    six_dose = con.execute(
        f"""
        SELECT coalesce(SUM(
            furosemide_bolus_mg
            + bumetanide_bolus_mg
            + torsemide_bolus_mg
            + ethacrynic_acid_bolus_mg
            + chlorothiazide_mg
            + metolazone_mg
            + acetazolamide_mg
        ), 0)
        FROM bolus_window
        WHERE grid_type = 'SIX_HOUR'
          AND dedup_method = '{method}'
        """
    ).fetchone()[0]
    if abs(source_dose - hourly_dose) > 1e-6 or abs(source_dose - six_dose) > 1e-6:
        raise RuntimeError(
            f'Bolus conservation failed for {method}: '
            f'source={source_dose}, hourly={hourly_dose}, six={six_dose}.'
        )

infusion_source = con.execute(
    "SELECT coalesce(SUM(input_amount_mg_horizon), 0) FROM input_infusion_quantitative WHERE drug_class='LOOP_FUROSEMIDE'"
).fetchone()[0]
infusion_hourly = con.execute(
    "SELECT coalesce(SUM(furosemide_infusion_mg), 0) FROM infusion_window WHERE grid_type='HOURLY'"
).fetchone()[0]
infusion_six = con.execute(
    "SELECT coalesce(SUM(furosemide_infusion_mg), 0) FROM infusion_window WHERE grid_type='SIX_HOUR'"
).fetchone()[0]

if abs(infusion_source - infusion_hourly) > 1e-6 or abs(infusion_source - infusion_six) > 1e-6:
    raise RuntimeError(
        'Infusion conservation failed: '
        f'source={infusion_source}, hourly={infusion_hourly}, six={infusion_six}.'
    )



# Canonical output invariants.
hourly_n = con.execute('SELECT COUNT(*) FROM hourly_output').fetchone()[0]
six_n = con.execute('SELECT COUNT(*) FROM six_output').fetchone()[0]
if hourly_n != EXPECTED_COHORT_N * 24:
    raise RuntimeError(
        f'Unexpected canonical hourly rows: {hourly_n}; '
        f'expected {EXPECTED_COHORT_N * 24}.'
    )
if six_n != EXPECTED_COHORT_N * 4:
    raise RuntimeError(
        f'Unexpected canonical six-hour rows: {six_n}; '
        f'expected {EXPECTED_COHORT_N * 4}.'
    )

# Route and source decomposition must reconstruct the all-route furosemide dose.
for table_name in ['hourly_output', 'six_output']:
    bad_route_n = con.execute(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE eligible_minutes > 0
          AND abs(
              furosemide_all_route_total_mg
              - (
                  furosemide_inputevents_iv_bolus_mg
                  + furosemide_emar_iv_bolus_mg
                  + furosemide_enteral_bolus_mg
                  + furosemide_unknown_route_bolus_mg
                  + furosemide_infusion_mg
              )
          ) > 1e-6
        """
    ).fetchone()[0]
    if bad_route_n:
        raise RuntimeError(
            f'{table_name}: {bad_route_n} route-decomposition mismatches.'
        )

    false_nonobs_n = con.execute(
        f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE eligible_minutes = 0
          AND (
              furosemide_all_route_total_mg IS NOT NULL
              OR furosemide_quantitative_exposure_flag IS NOT NULL
          )
        """
    ).fetchone()[0]
    if false_nonobs_n:
        raise RuntimeError(
            f'{table_name}: {false_nonobs_n} non-observable rows contain '
            'exposure values.'
        )

# The canonical 60-minute integrated dose must be conserved in both grids.
canonical_bolus_source = con.execute(
    "SELECT coalesce(SUM(dose_mg), 0) FROM integrated_bolus_60m"
).fetchone()[0]
canonical_bolus_hourly = con.execute(
    """
    SELECT coalesce(SUM(
        furosemide_all_route_bolus_mg
        + bumetanide_iv_mg + bumetanide_enteral_mg
        + bumetanide_unknown_route_mg
        + torsemide_mg + ethacrynic_acid_mg
        + chlorothiazide_mg + metolazone_mg + acetazolamide_mg
    ), 0)
    FROM hourly_output
    WHERE eligible_minutes > 0
    """
).fetchone()[0]
canonical_bolus_six = con.execute(
    """
    SELECT coalesce(SUM(
        furosemide_all_route_bolus_mg
        + bumetanide_iv_mg + bumetanide_enteral_mg
        + bumetanide_unknown_route_mg
        + torsemide_mg + ethacrynic_acid_mg
        + chlorothiazide_mg + metolazone_mg + acetazolamide_mg
    ), 0)
    FROM six_output
    WHERE eligible_minutes > 0
    """
).fetchone()[0]
if abs(canonical_bolus_source - canonical_bolus_hourly) > 1e-6 \
        or abs(canonical_bolus_source - canonical_bolus_six) > 1e-6:
    raise RuntimeError(
        'Canonical bolus conservation failed: '
        f'source={canonical_bolus_source}, '
        f'hourly={canonical_bolus_hourly}, six={canonical_bolus_six}.'
    )

# =============================================================================
# Audit tables
# =============================================================================

con.execute(
    """
    CREATE TABLE audit (
        audit_section VARCHAR,
        subgroup VARCHAR,
        metric VARCHAR,
        value_num DOUBLE,
        value_text VARCHAR
    )
    """
)

# Overall counts and temporal rules.
overall_rows = [
    ('OVERALL', 'COHORT', 'cohort_n', float(cohort_n), None),
    ('OVERALL', 'INPUTEVENTS', 'bolus_points_n', float(con.execute(
        'SELECT COUNT(*) FROM input_bolus_points').fetchone()[0]), None),
    ('OVERALL', 'INPUTEVENTS', 'quantitative_infusion_segments_n', float(con.execute(
        'SELECT COUNT(*) FROM input_infusion_quantitative').fetchone()[0]), None),
    ('OVERALL', 'EMAR', 'discrete_candidates_n', float(con.execute(
        'SELECT COUNT(*) FROM emar_discrete_route_resolved').fetchone()[0]), None),
    ('OVERALL', 'EMAR', 'parseable_positive_discrete_n', float(con.execute(
        'SELECT COUNT(*) FROM emar_discrete_route_resolved WHERE dose_mg > 0').fetchone()[0]), None),
    ('OVERALL', 'EMAR', 'horizon_available_stays_n', float(con.execute(
        'SELECT COUNT(*) FROM emar_horizon_availability').fetchone()[0]),
        'any eMAR record in [t0, analytical_end)'),
    ('OVERALL', 'DEDUP', 'dose_concordant_duplicates_15m_n', float(con.execute(
        'SELECT COUNT(*) FROM mutual_bolus_matches_v5 WHERE duplicate_15m_flag=1').fetchone()[0]), None),
    ('OVERALL', 'DEDUP', 'dose_concordant_duplicates_60m_n', float(con.execute(
        'SELECT COUNT(*) FROM mutual_bolus_matches_v5 WHERE duplicate_60m_flag=1').fetchone()[0]), None),
    ('OVERALL', 'DEDUP', 'emar_supplement_15m_n', float(con.execute(
        'SELECT COUNT(*) FROM emar_supplement_15m').fetchone()[0]), None),
    ('OVERALL', 'DEDUP', 'emar_supplement_60m_n', float(con.execute(
        'SELECT COUNT(*) FROM emar_supplement_60m').fetchone()[0]), None),
    ('TEMPORAL_RULE', 'BOLUS', 'left_boundary', None, 'include event at t0'),
    ('TEMPORAL_RULE', 'BOLUS', 'right_boundary', None, 'exclude event at analytical_end; no anchor'),
    ('TEMPORAL_RULE', 'INFUSION', 'allocation', None, 'proportional explicit start/end overlap only'),
    ('TEMPORAL_RULE', 'INFUSION', 'reconstruction', None, 'no LOCF, no pre-start or post-stop carry'),
]
con.executemany('INSERT INTO audit VALUES (?, ?, ?, ?, ?)', overall_rows)

# Route distributions and provenance after linked order resolution.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'EMAR_ROUTE_RESOLVED',
        drug_class || ' | ALL_DISCRETE',
        resolved_route_class,
        COUNT(*)::DOUBLE,
        'patients=' || cast(COUNT(DISTINCT stay_id) AS VARCHAR)
    FROM emar_discrete_route_resolved
    GROUP BY drug_class, resolved_route_class
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'EMAR_ROUTE_RESOLVED',
        drug_class || ' | SUPPLEMENT_60M',
        resolved_route_class,
        COUNT(*)::DOUBLE,
        'patients=' || cast(COUNT(DISTINCT stay_id) AS VARCHAR)
    FROM emar_supplement_60m
    GROUP BY drug_class, resolved_route_class
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'EMAR_ROUTE_PROVENANCE',
        drug_class,
        route_resolution_source,
        COUNT(*)::DOUBLE,
        'patients=' || cast(COUNT(DISTINCT stay_id) AS VARCHAR)
    FROM emar_discrete_route_resolved
    GROUP BY drug_class, route_resolution_source
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'EMAR_ROUTE_VALIDATION',
        drug_class,
        resolved_route_class,
        COUNT(*)::DOUBLE,
        'matched_input_iv=' || cast(
            SUM(resolved_matches_input_iv_flag) AS VARCHAR
        ) || '; conflicts=' || cast(
            SUM(route_conflict_flag) AS VARCHAR
        )
    FROM matched_route_validation
    GROUP BY drug_class, resolved_route_class
    """
)

# Deduplication timing, dose concordance, and ambiguity context.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'DEDUP_MATCH',
        drug_class || ' | ' || time_match_bin,
        CASE
            WHEN dose_concordant_flag = 1 THEN 'DOSE_CONCORDANT'
            ELSE 'DOSE_DISCORDANT'
        END,
        COUNT(*)::DOUBLE,
        'same_source_competition=' || cast(
            COUNT(*) FILTER (
                WHERE input_competing_same_dose_60m_n > 0
                   OR emar_competing_same_dose_60m_n > 0
            ) AS VARCHAR
        )
    FROM mutual_bolus_matches_v5
    GROUP BY drug_class, time_match_bin, dose_concordant_flag
    """
)

# eMAR Started markers are never treated as quantitative doses.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'EMAR_INFUSION_LIFECYCLE',
        e.drug_class,
        metric,
        value_num,
        NULL
    FROM (
        SELECT drug_class, 'started_markers_n' AS metric,
               COUNT(*)::DOUBLE AS value_num
        FROM emar_started_points
        GROUP BY drug_class
        UNION ALL
        SELECT e.drug_class, 'matched_input_start_within_15m_n',
               COUNT(m.emar_id) FILTER (
                   WHERE m.abs_time_difference_minutes <= 15
               )::DOUBLE
        FROM emar_started_points e
        LEFT JOIN emar_started_nearest_input m ON e.emar_id=m.emar_id
        GROUP BY e.drug_class
        UNION ALL
        SELECT e.drug_class, 'matched_input_start_within_60m_n',
               COUNT(m.emar_id)::DOUBLE
        FROM emar_started_points e
        LEFT JOIN emar_started_nearest_input m ON e.emar_id=m.emar_id
        GROUP BY e.drug_class
    ) e
    """
)

# Boundary and infusion overlap context.
con.execute(
    """
    INSERT INTO audit
    SELECT 'TEMPORAL_BOUNDARY', 'BOLUS', metric, value_num, NULL
    FROM (
        SELECT 'input_bolus_exact_t0_n' AS metric,
               COUNT(*)::DOUBLE AS value_num
        FROM input_bolus_points i
        INNER JOIN cohort c ON i.stay_id=c.stay_id
        WHERE i.administration_time = c.postoperative_time_zero
        UNION ALL
        SELECT 'emar_bolus_exact_t0_n', COUNT(*)::DOUBLE
        FROM emar_discrete_route_resolved e
        INNER JOIN cohort c ON e.stay_id=c.stay_id
        WHERE e.administration_time = c.postoperative_time_zero
        UNION ALL
        SELECT 'infusions_crossing_t0_n', COUNT(*)::DOUBLE
        FROM input_infusion_quantitative i
        INNER JOIN cohort c ON i.stay_id=c.stay_id
        WHERE i.starttime < c.postoperative_time_zero
          AND i.endtime > c.postoperative_time_zero
        UNION ALL
        SELECT 'infusions_crossing_analytical_end_n', COUNT(*)::DOUBLE
        FROM input_infusion_quantitative i
        INNER JOIN cohort c ON i.stay_id=c.stay_id
        WHERE i.starttime < c.analytical_end_time
          AND i.endtime > c.analytical_end_time
    ) x
    """
)

# Source-dose conservation.
con.executemany(
    'INSERT INTO audit VALUES (?, ?, ?, ?, ?)',
    [
        ('CONSERVATION', 'INFUSION', 'source_horizon_mg', float(infusion_source), None),
        ('CONSERVATION', 'INFUSION', 'hourly_allocated_mg', float(infusion_hourly), None),
        ('CONSERVATION', 'INFUSION', 'six_hour_allocated_mg', float(infusion_six), None),
    ]
)

for method, table_name in [('DEDUP_15M','integrated_bolus_15m'),('DEDUP_60M','integrated_bolus_60m')]:
    source_total = con.execute(f'SELECT coalesce(SUM(dose_mg),0) FROM {table_name}').fetchone()[0]
    hourly_total = con.execute(f"""
        SELECT coalesce(SUM(
            furosemide_bolus_mg + bumetanide_bolus_mg + torsemide_bolus_mg
            + ethacrynic_acid_bolus_mg + chlorothiazide_mg + metolazone_mg
            + acetazolamide_mg
        ),0) FROM bolus_window
        WHERE grid_type='HOURLY' AND dedup_method='{method}'
    """).fetchone()[0]
    six_total = con.execute(f"""
        SELECT coalesce(SUM(
            furosemide_bolus_mg + bumetanide_bolus_mg + torsemide_bolus_mg
            + ethacrynic_acid_bolus_mg + chlorothiazide_mg + metolazone_mg
            + acetazolamide_mg
        ),0) FROM bolus_window
        WHERE grid_type='SIX_HOUR' AND dedup_method='{method}'
    """).fetchone()[0]
    con.executemany('INSERT INTO audit VALUES (?, ?, ?, ?, ?)', [
        ('CONSERVATION', method, 'source_bolus_mg', float(source_total), None),
        ('CONSERVATION', method, 'hourly_allocated_bolus_mg', float(hourly_total), None),
        ('CONSERVATION', method, 'six_hour_allocated_bolus_mg', float(six_total), None),
    ])

# Six-hour temporal exposure summaries under both deduplication rules.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'SIX_HOUR_EXPOSURE',
        dedup_method || ' | ' || window_label,
        metric,
        value_num,
        NULL
    FROM (
        SELECT dedup_method, window_index, window_label,
               'eligible_stays_n' AS metric,
               COUNT(*) FILTER (WHERE eligible_minutes > 0)::DOUBLE AS value_num
        FROM window_method_output
        WHERE grid_type='SIX_HOUR'
        GROUP BY dedup_method, window_index, window_label
        UNION ALL
        SELECT dedup_method, window_index, window_label,
               'furosemide_exposed_stays_n',
               COUNT(*) FILTER (
                   WHERE eligible_minutes > 0 AND furosemide_exposed_flag=1
               )::DOUBLE
        FROM window_method_output
        WHERE grid_type='SIX_HOUR'
        GROUP BY dedup_method, window_index, window_label
        UNION ALL
        SELECT dedup_method, window_index, window_label,
               'median_furosemide_total_mg_among_exposed',
               median(furosemide_total_mg)::DOUBLE
        FROM window_method_output
        WHERE grid_type='SIX_HOUR' AND furosemide_total_mg > 0
        GROUP BY dedup_method, window_index, window_label
        UNION ALL
        SELECT dedup_method, window_index, window_label,
               'furosemide_infusion_stays_n',
               COUNT(*) FILTER (
                   WHERE furosemide_infusion_mg > 0
               )::DOUBLE
        FROM window_method_output
        WHERE grid_type='SIX_HOUR'
        GROUP BY dedup_method, window_index, window_label
        UNION ALL
        SELECT dedup_method, window_index, window_label,
               'emar_supplement_stays_n',
               COUNT(*) FILTER (
                   WHERE furosemide_emar_supplement_mg > 0
               )::DOUBLE
        FROM window_method_output
        WHERE grid_type='SIX_HOUR'
        GROUP BY dedup_method, window_index, window_label
        UNION ALL
        SELECT dedup_method, window_index, window_label,
               'unknown_route_emar_supplement_stays_n',
               COUNT(*) FILTER (
                   WHERE furosemide_emar_unknown_route_mg > 0
               )::DOUBLE
        FROM window_method_output
        WHERE grid_type='SIX_HOUR'
        GROUP BY dedup_method, window_index, window_label
    ) x
    """
)

# Actual source availability and integrated exposure under the 60-minute rule.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'SOURCE_AVAILABILITY',
        CASE
            WHEN emar_available_in_horizon_flag=1 THEN 'EMAR_AVAILABLE'
            ELSE 'EMAR_NOT_OBSERVED'
        END,
        metric,
        value_num,
        NULL
    FROM (
        SELECT emar_available_in_horizon_flag,
               'cohort_stays_n' AS metric,
               COUNT(*)::DOUBLE AS value_num
        FROM patient_dedup_comparison
        GROUP BY emar_available_in_horizon_flag
        UNION ALL
        SELECT emar_available_in_horizon_flag,
               'furosemide_exposed_60m_n',
               COUNT(*) FILTER (WHERE furosemide_total_mg_60m > 0)::DOUBLE
        FROM patient_dedup_comparison
        GROUP BY emar_available_in_horizon_flag
        UNION ALL
        SELECT emar_available_in_horizon_flag,
               'emar_supplement_exposed_60m_n',
               COUNT(*) FILTER (WHERE emar_supplement_mg_60m > 0)::DOUBLE
        FROM patient_dedup_comparison
        GROUP BY emar_available_in_horizon_flag
    ) x
    """
)

# Integrated 24-hour comparison.
con.execute(
    """
    INSERT INTO audit
    SELECT 'METHOD_COMPARISON_24H', 'FUROSEMIDE', metric, value_num, NULL
    FROM (
        SELECT 'patients_changed_15m_vs_60m_n' AS metric,
               COUNT(*) FILTER (
                   WHERE abs_furosemide_method_difference_mg > 0
               )::DOUBLE AS value_num
        FROM patient_dedup_comparison
        UNION ALL
        SELECT 'median_abs_difference_mg_changed',
               median(abs_furosemide_method_difference_mg)::DOUBLE
        FROM patient_dedup_comparison
        WHERE abs_furosemide_method_difference_mg > 0
        UNION ALL
        SELECT 'p90_abs_difference_mg_changed',
               quantile_cont(abs_furosemide_method_difference_mg, 0.90)::DOUBLE
        FROM patient_dedup_comparison
        WHERE abs_furosemide_method_difference_mg > 0
        UNION ALL
        SELECT 'maximum_abs_difference_mg',
               max(abs_furosemide_method_difference_mg)::DOUBLE
        FROM patient_dedup_comparison
        UNION ALL
        SELECT 'exposed_patients_15m_n',
               COUNT(*) FILTER (WHERE furosemide_total_mg_15m > 0)::DOUBLE
        FROM patient_dedup_comparison
        UNION ALL
        SELECT 'exposed_patients_60m_n',
               COUNT(*) FILTER (WHERE furosemide_total_mg_60m > 0)::DOUBLE
        FROM patient_dedup_comparison
        UNION ALL
        SELECT 'patients_with_unknown_route_emar_supplement_60m_n',
               COUNT(*) FILTER (WHERE emar_unknown_route_mg_60m > 0)::DOUBLE
        FROM patient_dedup_comparison
    ) x
    """
)



# Canonical decisions and output summaries.
con.executemany(
    'INSERT INTO audit VALUES (?, ?, ?, ?, ?)',
    [
        ('CANONICAL_DECISION', 'DEDUP', 'matching_window_minutes', 60.0,
         'mutual-nearest and dose-concordant'),
        ('CANONICAL_DECISION', 'DOSE', 'furosemide_equivalent_conversion', None,
         'not applied; other drugs remain drug-specific'),
        ('CANONICAL_DECISION', 'ROUTE', 'unknown_route_handling', None,
         'retained explicitly; no forced IV/enteral assignment'),
        ('CANONICAL_DECISION', 'SOURCE', 'primary_representation', None,
         'best-available INPUTEVENTS plus deduplicated eMAR supplement'),
        ('CANONICAL_DECISION', 'SOURCE', 'sensitivity_representation', None,
         'INPUTEVENTS-only furosemide dose retained'),
        ('CONSERVATION', 'CANONICAL_60M', 'source_bolus_mg',
         float(canonical_bolus_source), None),
        ('CONSERVATION', 'CANONICAL_60M', 'hourly_bolus_mg',
         float(canonical_bolus_hourly), None),
        ('CONSERVATION', 'CANONICAL_60M', 'six_hour_bolus_mg',
         float(canonical_bolus_six), None),
    ]
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'CANONICAL_24H',
        'FUROSEMIDE',
        metric,
        value_num,
        NULL
    FROM (
        SELECT 'quantitative_exposed_patients_n' AS metric,
               COUNT(*) FILTER (
                   WHERE furosemide_quantitative_exposure_24h_flag = 1
               )::DOUBLE AS value_num
        FROM canonical_patient_24h
        UNION ALL
        SELECT 'any_documented_exposed_patients_n',
               COUNT(*) FILTER (
                   WHERE furosemide_any_documented_exposure_24h_flag = 1
               )::DOUBLE
        FROM canonical_patient_24h
        UNION ALL
        SELECT 'iv_exposed_patients_n',
               COUNT(*) FILTER (
                   WHERE furosemide_iv_total_24h_mg > 0
               )::DOUBLE
        FROM canonical_patient_24h
        UNION ALL
        SELECT 'enteral_exposed_patients_n',
               COUNT(*) FILTER (
                   WHERE furosemide_enteral_bolus_24h_mg > 0
               )::DOUBLE
        FROM canonical_patient_24h
        UNION ALL
        SELECT 'unknown_route_exposed_patients_n',
               COUNT(*) FILTER (
                   WHERE furosemide_unknown_route_bolus_24h_mg > 0
               )::DOUBLE
        FROM canonical_patient_24h
        UNION ALL
        SELECT 'infusion_exposed_patients_n',
               COUNT(*) FILTER (
                   WHERE furosemide_infusion_24h_mg > 0
               )::DOUBLE
        FROM canonical_patient_24h
        UNION ALL
        SELECT 'median_all_route_24h_mg_among_exposed',
               median(furosemide_all_route_total_24h_mg)::DOUBLE
        FROM canonical_patient_24h
        WHERE furosemide_all_route_total_24h_mg > 0
        UNION ALL
        SELECT 'p90_all_route_24h_mg_among_exposed',
               quantile_cont(
                   furosemide_all_route_total_24h_mg, 0.90
               )::DOUBLE
        FROM canonical_patient_24h
        WHERE furosemide_all_route_total_24h_mg > 0
    ) x
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'CANONICAL_SIX_HOUR',
        window_label,
        metric,
        value_num,
        NULL
    FROM (
        SELECT window_index, window_label,
               'eligible_stays_n' AS metric,
               COUNT(*) FILTER (
                   WHERE eligible_minutes > 0
               )::DOUBLE AS value_num
        FROM six_output
        GROUP BY window_index, window_label
        UNION ALL
        SELECT window_index, window_label,
               'furosemide_quantitative_exposed_n',
               COUNT(*) FILTER (
                   WHERE eligible_minutes > 0
                     AND furosemide_quantitative_exposure_flag = 1
               )::DOUBLE
        FROM six_output
        GROUP BY window_index, window_label
        UNION ALL
        SELECT window_index, window_label,
               'median_all_route_mg_among_exposed',
               median(furosemide_all_route_total_mg)::DOUBLE
        FROM six_output
        WHERE furosemide_all_route_total_mg > 0
        GROUP BY window_index, window_label
        UNION ALL
        SELECT window_index, window_label,
               'inputevents_only_exposed_n',
               COUNT(*) FILTER (
                   WHERE furosemide_inputevents_only_mg > 0
               )::DOUBLE
        FROM six_output
        GROUP BY window_index, window_label
        UNION ALL
        SELECT window_index, window_label,
               'emar_supplement_exposed_n',
               COUNT(*) FILTER (
                   WHERE furosemide_emar_supplement_mg > 0
               )::DOUBLE
        FROM six_output
        GROUP BY window_index, window_label
    ) x
    """
)

# =============================================================================
# Review rows
# =============================================================================

con.execute(
    """
    CREATE TABLE review_rows AS
    SELECT
        'DOSE_DISCORDANT_MUTUAL_MATCH' AS review_reason,
        m.stay_id,
        m.drug_class,
        m.input_time AS event_time,
        cast(m.input_point_id AS VARCHAR) AS input_id,
        m.emar_id,
        m.input_dose_mg,
        m.emar_dose_mg,
        m.abs_time_difference_minutes,
        e.resolved_route_class AS emar_route_class,
        e.medication AS source_label,
        e.dose_values_raw,
        e.dose_units_raw,
        coalesce(e.administration_types, '') || '; route_source=' || e.route_resolution_source AS context
    FROM mutual_bolus_matches_v5 m
    INNER JOIN emar_discrete_route_resolved e ON m.emar_id=e.emar_id
    WHERE m.dose_concordant_flag=0

    UNION ALL

    SELECT
        'CONCORDANT_15_TO_60M_WITH_SAME_SOURCE_COMPETITION',
        m.stay_id,
        m.drug_class,
        m.input_time,
        cast(m.input_point_id AS VARCHAR),
        m.emar_id,
        m.input_dose_mg,
        m.emar_dose_mg,
        m.abs_time_difference_minutes,
        e.resolved_route_class,
        e.medication,
        e.dose_values_raw,
        e.dose_units_raw,
        'input_competitors=' || cast(m.input_competing_same_dose_60m_n AS VARCHAR)
            || '; emar_competitors=' || cast(m.emar_competing_same_dose_60m_n AS VARCHAR)
            || '; route_source=' || e.route_resolution_source
    FROM mutual_bolus_matches_v5 m
    INNER JOIN emar_discrete_route_resolved e ON m.emar_id=e.emar_id
    WHERE m.dose_concordant_flag=1
      AND m.abs_time_difference_minutes > 15
      AND (
          m.input_competing_same_dose_60m_n > 0
          OR m.emar_competing_same_dose_60m_n > 0
      )

    UNION ALL

    SELECT
        'EMAR_SUPPLEMENT_UNKNOWN_ROUTE_60M',
        e.stay_id,
        e.drug_class,
        e.administration_time,
        NULL,
        e.emar_id,
        NULL,
        e.dose_mg,
        NULL,
        e.resolved_route_class,
        e.medication,
        e.dose_values_raw,
        e.dose_units_raw,
        e.administration_types
    FROM emar_supplement_60m e
    WHERE e.resolved_route_class='UNKNOWN'

    UNION ALL

    SELECT
        'EMAR_DISCRETE_UNPARSEABLE_DOSE',
        e.stay_id,
        e.drug_class,
        e.administration_time,
        NULL,
        e.emar_id,
        NULL,
        e.dose_mg,
        NULL,
        e.resolved_route_class,
        e.medication,
        e.dose_values_raw,
        e.dose_units_raw,
        e.dose_selection_rule
    FROM emar_discrete_route_resolved e
    WHERE e.dose_mg IS NULL OR e.dose_mg <= 0

    UNION ALL

    SELECT
        'EMAR_ROUTE_CONFLICT',
        e.stay_id,
        e.drug_class,
        e.administration_time,
        NULL,
        e.emar_id,
        NULL,
        e.dose_mg,
        NULL,
        e.resolved_route_class,
        e.medication,
        e.pharmacy_routes_raw,
        e.prescription_routes_raw,
        'enhanced=' || e.enhanced_route_class
            || '; source=' || e.route_resolution_source
    FROM emar_discrete_route_resolved e
    WHERE e.route_conflict_flag = 1

    UNION ALL

    SELECT
        'EMAR_STARTED_WITHOUT_INPUT_INFUSION_60M',
        e.stay_id,
        e.drug_class,
        e.charttime,
        NULL,
        e.emar_id,
        NULL,
        e.emar_dose_mg,
        NULL,
        e.route_class,
        e.medication,
        NULL,
        NULL,
        e.administration_types
    FROM emar_started_points e
    LEFT JOIN emar_started_nearest_input m ON e.emar_id=m.emar_id
    WHERE m.emar_id IS NULL
    """
)



# =============================================================================
# Export canonical outputs
# =============================================================================

con.execute(
    f"""
    COPY (
        SELECT *
        FROM hourly_output
        ORDER BY stay_id, window_index
    ) TO '{qpath(OUT_HOURLY)}'
    (FORMAT PARQUET, COMPRESSION ZSTD)
    """
)

con.execute(
    f"""
    COPY (
        SELECT *
        FROM six_output
        ORDER BY stay_id, window_index
    ) TO '{qpath(OUT_SIX)}'
    (FORMAT PARQUET, COMPRESSION ZSTD)
    """
)

con.execute(
    f"""
    COPY (
        SELECT *
        FROM audit
        ORDER BY audit_section, subgroup, metric
    ) TO '{qpath(OUT_AUDIT)}'
    (HEADER, DELIMITER ',')
    """
)


# =============================================================================
# Console report
# =============================================================================

print('=' * 104)
print('TRAJECTORIES - DIURETIC EXPOSURE CANONICAL INTEGRATED TRAJECTORY')
print('=' * 104)
print(f'Cohort rows / unique stays              : {cohort_n} / {unique_stays_n}')
print(f'INPUTEVENTS quantitative bolus points   : {con.execute("SELECT COUNT(*) FROM input_bolus_points").fetchone()[0]}')
print(f'INPUTEVENTS infusion segments           : {con.execute("SELECT COUNT(*) FROM input_infusion_quantitative").fetchone()[0]}')
print(f'eMAR quantitative supplements (60 min) : {con.execute("SELECT COUNT(*) FROM emar_supplement_60m").fetchone()[0]}')
print(f'eMAR horizon-available stays            : {con.execute("SELECT COUNT(*) FROM emar_horizon_availability").fetchone()[0]}')
print(f'Cross-source duplicates removed         : {con.execute("SELECT COUNT(*) FROM mutual_bolus_matches_v5 WHERE duplicate_60m_flag=1").fetchone()[0]}')
print(f'eMAR unquantified supplements           : {con.execute("SELECT COUNT(*) FROM emar_unquantified_supplement").fetchone()[0]}')
print(f'Unmatched eMAR infusion starts          : {con.execute("SELECT COUNT(*) FROM emar_unmatched_started").fetchone()[0]}')
print()

print('Route-resolved eMAR supplements after 60-minute deduplication')
print('----------------------------------------------------------------')
print(con.execute(
    """
    SELECT
        drug_class,
        resolved_route_class,
        COUNT(*) AS administrations_n,
        COUNT(DISTINCT stay_id) AS patients_n,
        round(SUM(dose_mg), 3) AS total_mg,
        round(median(dose_mg), 3) AS median_dose_mg
    FROM emar_supplement_60m
    GROUP BY drug_class, resolved_route_class
    ORDER BY drug_class, resolved_route_class
    """
).fetchdf().to_string(index=False))
print()

print('Canonical conservation invariants')
print('---------------------------------')
print(f'Bolus source dose                 : {canonical_bolus_source:.3f} mg')
print(f'Hourly allocated bolus dose       : {canonical_bolus_hourly:.3f} mg')
print(f'Six-hour allocated bolus dose     : {canonical_bolus_six:.3f} mg')
print(f'Infusion source horizon dose      : {infusion_source:.3f} mg')
print(f'Hourly allocated infusion dose    : {infusion_hourly:.3f} mg')
print(f'Six-hour allocated infusion dose  : {infusion_six:.3f} mg')
print()

print('Integrated 24-hour furosemide exposure')
print('---------------------------------------')
print(con.execute(
    """
    SELECT
        COUNT(*) FILTER (
            WHERE furosemide_quantitative_exposure_24h_flag = 1
        ) AS quantitative_exposed_n,
        COUNT(*) FILTER (
            WHERE furosemide_any_documented_exposure_24h_flag = 1
        ) AS any_documented_exposed_n,
        COUNT(*) FILTER (
            WHERE furosemide_iv_total_24h_mg > 0
        ) AS iv_exposed_n,
        COUNT(*) FILTER (
            WHERE furosemide_enteral_bolus_24h_mg > 0
        ) AS enteral_exposed_n,
        COUNT(*) FILTER (
            WHERE furosemide_unknown_route_bolus_24h_mg > 0
        ) AS unknown_route_exposed_n,
        COUNT(*) FILTER (
            WHERE furosemide_infusion_24h_mg > 0
        ) AS infusion_exposed_n,
        round(median(furosemide_all_route_total_24h_mg) FILTER (
            WHERE furosemide_all_route_total_24h_mg > 0
        ), 2) AS median_mg_among_exposed,
        round(quantile_cont(
            furosemide_all_route_total_24h_mg, 0.90
        ) FILTER (
            WHERE furosemide_all_route_total_24h_mg > 0
        ), 2) AS p90_mg_among_exposed
    FROM canonical_patient_24h
    """
).fetchdf().to_string(index=False))
print()

print('Six-hour canonical furosemide exposure')
print('--------------------------------------')
print(con.execute(
    """
    SELECT
        window_label,
        COUNT(*) FILTER (
            WHERE eligible_minutes > 0
        ) AS eligible_n,
        COUNT(*) FILTER (
            WHERE eligible_minutes > 0
              AND furosemide_quantitative_exposure_flag = 1
        ) AS quantitative_exposed_n,
        round(
            100.0 * COUNT(*) FILTER (
                WHERE eligible_minutes > 0
                  AND furosemide_quantitative_exposure_flag = 1
            ) / nullif(COUNT(*) FILTER (
                WHERE eligible_minutes > 0
            ), 0),
            2
        ) AS exposed_pct,
        round(median(furosemide_all_route_total_mg) FILTER (
            WHERE furosemide_all_route_total_mg > 0
        ), 2) AS median_mg_among_exposed,
        COUNT(*) FILTER (
            WHERE furosemide_infusion_mg > 0
        ) AS infusion_exposed_n,
        COUNT(*) FILTER (
            WHERE furosemide_emar_supplement_mg > 0
        ) AS emar_supplement_n,
        COUNT(*) FILTER (
            WHERE furosemide_unknown_route_bolus_mg > 0
        ) AS unknown_route_n
    FROM six_output
    GROUP BY window_index, window_label
    ORDER BY window_index
    """
).fetchdf().to_string(index=False))
print()

print('Canonical outputs')
print('-----------------')
print(OUT_HOURLY)
print(OUT_SIX)
print(OUT_AUDIT)
print()
print('Canonical diuretic-exposure extraction complete.')
print('Primary quantitative representation: route-aware furosemide dose.')
print('INPUTEVENTS-only dose is retained as a source-availability sensitivity.')
print('No furosemide-equivalent conversion was applied.')
