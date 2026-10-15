"""
01_07_extract_urine_output.py

Trajectories project
Renal / fluid-balance domain - canonical postoperative urine-output pipeline

Frozen methodological decisions
-------------------------------
Primary raw table:
    MIMIC-IV OUTPUTEVENTS

Accepted urinary-route itemids:
    226559  Foley
    226560  Void
    226561  Condom Cath
    226584  Ileoconduit
    226563  Suprapubic
    226564  Right Nephrostomy
    226565  Left Nephrostomy
    226567  Straight Cath
    226557  Right Ureteral Stent
    226558  Left Ureteral Stent

Excluded perioperative summary items:
    226627  OR Urine
    226631  PACU Urine

GU irrigation:
    227488  GU Irrigant Volume In
    227489  GU Irrigant/Urine Volume Out

    Windows containing either irrigation item are flagged as
    IRRIGATION_AFFECTED and are not assigned a primary urine-output value.
    This conservative rule avoids treating irrigant as urine or creating
    physiologically impossible negative urine output. Nonaffected windows from
    the same stay remain usable. Integrated 24-hour primary values are set to
    missing for any stay with GU irrigation during the analytical horizon.

Technical validity:
    - numeric OUTPUTEVENTS value required;
    - value must be >= 0 mL;
    - unit must be mL (case-insensitive);
    - no upper urine-volume cutoff is imposed.

Exact-timestamp integration:
    All accepted urinary-route rows at the same stay_id + charttime are summed.

Temporal representation:
    Each positive-duration charted volume is assigned uniformly backward over
    the complete interval from the previous urine-output charttime to the
    current charttime. For the first positive-duration point, postoperative
    time zero is used as the interval start.

    A first urine-output value charted exactly at postoperative time zero has
    zero post-time-zero duration. It is excluded from the primary interval
    trajectory, retained as uo_exact_t0_boundary_ml, and included naturally in
    the event-time sensitivity representation.

    At the right boundary, when the analytical horizon ends at t0+24 h while
    the ICU stay continues, the first valid urinary timestamp at/after t0+24 h
    is used only as an interval-closing anchor, and only when at least one valid
    urinary point already exists inside the analytical horizon. The resulting
    interval is clipped at t0+24 h, so no post-horizon time or volume is counted.
    No post-endpoint anchor is permitted when observation ends before 24 h.

    No gap cap is applied. Source audits showed that intervals >2 hours
    accounted for <1% of total urine volume and that a 2-hour cap had negligible
    influence on six-hour summaries.

Missingness and coverage:
    No absent period is imputed as zero. Each window retains the number of
    minutes covered by observed urine-output intervals. Rates are calculated
    over covered minutes, not nominal or merely observable minutes.

Body weight:
    Fixed baseline actual body weight is selected using:
        1. nearest Admission Weight (CHARTEVENTS 226512) within +/-24 h of t0;
        2. nearest Daily Weight (CHARTEVENTS 224639) within +/-24 h as fallback.

    The broad technical validity rule 0 < weight < 1500 kg follows the MIMIC
    derived concept. Values <30 kg or >250 kg are retained but flagged for
    sensitivity/review. No conventional 70-kg weight is imputed.

Primary trajectory feature:
    uo_rate_ml_per_kg_per_hour_observed

Companion continuous features:
    uo_interval_ml_observed
    uo_rate_ml_per_hour_observed
    uo_interval_covered_minutes
    uo_interval_coverage_fraction

Sensitivity representation:
    uo_event_time_ml_unadjusted

No KDIGO/oliguria thresholding is performed in this extraction component.
Threshold-based burden is outside this extraction component.

Canonical outputs
-----------------
data/01_07_urine_output_hourly_trajectory.parquet
data/01_07_urine_output_six_hour_summary.parquet
data/01_07_urine_output_extraction_audit.csv

Inputs
------
data/00_04_final_analytic_cohort.csv
Raw MIMIC-IV 3.1 files relative to MIMIC_ROOT:
  icu/outputevents.csv.gz
  icu/chartevents.csv.gz
  icu/icustays.csv.gz

ICUSTAYS supplies the ICU boundary used for right-boundary anchor handling.
The diuretic extractor is not an input dependency.
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
OUTPUTEVENTS_FILE = MIMIC_ROOT / "icu" / "outputevents.csv.gz"
CHARTEVENTS_FILE = MIMIC_ROOT / "icu" / "chartevents.csv.gz"
ICUSTAYS_FILE = MIMIC_ROOT / "icu" / "icustays.csv.gz"

OUT_HOURLY = (
    PROJECT_ROOT / "data" / "01_07_urine_output_hourly_trajectory.parquet"
)
OUT_SIX = (
    PROJECT_ROOT / "data" / "01_07_urine_output_six_hour_summary.parquet"
)
OUT_AUDIT = (
    PROJECT_ROOT / "data" / "01_07_urine_output_extraction_audit.csv"
)

EXPECTED_COHORT_N = 9305

URINARY_ROUTE_ITEMIDS = (
    226559,  # Foley
    226560,  # Void
    226561,  # Condom Cath
    226584,  # Ileoconduit
    226563,  # Suprapubic
    226564,  # Right Nephrostomy
    226565,  # Left Nephrostomy
    226567,  # Straight Cath
    226557,  # Right Ureteral Stent
    226558,  # Left Ureteral Stent
)

GU_IRRIGANT_IN = 227488
GU_IRRIGANT_OUT = 227489

ADMISSION_WEIGHT_KG = 226512
DAILY_WEIGHT_KG = 224639

WEIGHT_MIN_EXCLUSIVE = 0.0
WEIGHT_MAX_EXCLUSIVE = 1500.0
WEIGHT_REVIEW_LOW = 30.0
WEIGHT_REVIEW_HIGH = 250.0


# =============================================================================
# Helpers
# =============================================================================


def qpath(path: Path) -> str:
    return str(path).replace("'", "''")


def sql_ids(values: tuple[int, ...]) -> str:
    return ", ".join(str(x) for x in values)


for required in [COHORT_FILE, OUTPUTEVENTS_FILE, CHARTEVENTS_FILE, ICUSTAYS_FILE]:
    if not required.exists():
        raise FileNotFoundError(f"Required file not found: {required}")

OUT_HOURLY.parent.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(database=":memory:")
con.execute("PRAGMA threads=4")

URINARY_SQL = sql_ids(URINARY_ROUTE_ITEMIDS)
IRRIGATION_SQL = f"{GU_IRRIGANT_IN}, {GU_IRRIGANT_OUT}"


# =============================================================================
# Frozen cohort
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
        CASE
            WHEN c.observation_end_time::TIMESTAMP
                 >= c.postoperative_time_zero::TIMESTAMP + INTERVAL '24 hours'
                THEN 1
            ELSE 0
        END AS full_24h_observable_flag
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

(
    cohort_n,
    unique_stays_n,
    missing_t0_n,
    missing_end_n,
    invalid_order_n,
) = con.execute(
    """
    SELECT
        COUNT(*),
        COUNT(DISTINCT stay_id),
        COUNT(*) FILTER (WHERE postoperative_time_zero IS NULL),
        COUNT(*) FILTER (WHERE observation_end_time IS NULL),
        COUNT(*) FILTER (
            WHERE observation_end_time < postoperative_time_zero
        )
    FROM cohort
    """
).fetchone()

if cohort_n != EXPECTED_COHORT_N or unique_stays_n != EXPECTED_COHORT_N:
    raise RuntimeError(
        f"Unexpected cohort size: {cohort_n} rows / {unique_stays_n} stays; "
        f"expected {EXPECTED_COHORT_N}."
    )

if missing_t0_n or missing_end_n or invalid_order_n:
    raise RuntimeError(
        "Invalid cohort timing fields: "
        f"missing_t0={missing_t0_n}, missing_end={missing_end_n}, "
        f"invalid_order={invalid_order_n}."
    )


missing_true_outtime_n = con.execute(
    """
    SELECT COUNT(*)
    FROM cohort
    WHERE full_24h_observable_flag = 1
      AND icu_outtime IS NULL
    """
).fetchone()[0]

invalid_true_outtime_n = con.execute(
    """
    SELECT COUNT(*)
    FROM cohort
    WHERE full_24h_observable_flag = 1
      AND icu_outtime < analytical_end_time
    """
).fetchone()[0]

if missing_true_outtime_n or invalid_true_outtime_n:
    raise RuntimeError(
        "Invalid true ICU outtime for right-boundary reconstruction: "
        f"missing={missing_true_outtime_n}, "
        f"before_analytical_end={invalid_true_outtime_n}."
    )


# =============================================================================
# Raw urine-output and irrigation sources
# =============================================================================

# Half-open analytical horizon: [t0, min(t0 + 24 h, observation_end)).
con.execute(
    f"""
    CREATE TABLE selected_output AS
    SELECT
        c.subject_id,
        c.hadm_id,
        c.stay_id,
        c.postoperative_time_zero,
        c.observation_end_time,
        oe.charttime,
        oe.storetime,
        oe.itemid::BIGINT AS itemid,
        oe.value::DOUBLE AS value,
        oe.valueuom,
        date_diff(
            'second',
            c.postoperative_time_zero,
            oe.charttime
        ) / 3600.0 AS hours_from_t0,
        date_diff(
            'second',
            oe.charttime,
            oe.storetime
        ) / 60.0 AS store_lag_minutes
    FROM read_csv_auto(
        '{qpath(OUTPUTEVENTS_FILE)}',
        header=true,
        all_varchar=false
    ) oe
    INNER JOIN cohort c
        ON oe.stay_id = c.stay_id
       AND oe.itemid IN ({URINARY_SQL}, {IRRIGATION_SQL})
       AND oe.charttime >= c.postoperative_time_zero
       AND oe.charttime < least(
            c.postoperative_time_zero + INTERVAL '24 hours',
            c.observation_end_time
       )
    """
)

con.execute(
    f"""
    CREATE TABLE route_raw AS
    SELECT *
    FROM selected_output
    WHERE itemid IN ({URINARY_SQL})
    """
)

con.execute(
    f"""
    CREATE TABLE irrigation_raw AS
    SELECT *
    FROM selected_output
    WHERE itemid IN ({IRRIGATION_SQL})
    """
)


# Right-boundary support for backward-looking urine intervals.
#
# The canonical trajectory assigns each charted urinary volume to the interval
# since the previous urinary timestamp. Therefore, when the Study-1 analytical
# horizon ends administratively at t0+24 h while the ICU stay continues, the
# first valid urinary point at/after the endpoint is needed solely to close the
# final in-horizon interval. No post-endpoint time or volume is counted:
# allocation is later clipped to analytical_end_time.
#
# No arbitrary gap cap is introduced here, preserving the frozen interval
# semantics. For stays ending before 24 h (discharge/death), no post-endpoint
# anchor is permitted.
con.execute(
    f"""
    CREATE TABLE right_boundary_raw AS
    SELECT
        c.subject_id,
        c.hadm_id,
        c.stay_id,
        c.postoperative_time_zero,
        c.observation_end_time,
        c.analytical_end_time,
        c.icu_outtime,
        oe.charttime,
        oe.storetime,
        oe.itemid::BIGINT AS itemid,
        oe.value::DOUBLE AS value,
        oe.valueuom,
        date_diff(
            'second',
            c.postoperative_time_zero,
            oe.charttime
        ) / 3600.0 AS hours_from_t0,
        date_diff(
            'second',
            oe.charttime,
            oe.storetime
        ) / 60.0 AS store_lag_minutes
    FROM read_csv_auto(
        '{qpath(OUTPUTEVENTS_FILE)}',
        header=true,
        all_varchar=false
    ) oe
    INNER JOIN cohort c
        ON oe.stay_id = c.stay_id
       AND c.full_24h_observable_flag = 1
       AND oe.itemid IN ({URINARY_SQL})
       AND oe.charttime >= c.analytical_end_time
       AND oe.charttime <= c.icu_outtime
       AND EXISTS (
            SELECT 1
            FROM selected_output s
            WHERE s.stay_id = c.stay_id
              AND s.itemid IN ({URINARY_SQL})
              AND s.value IS NOT NULL
              AND s.value >= 0
              AND lower(trim(coalesce(s.valueuom, ''))) = 'ml'
       )
    WHERE oe.value IS NOT NULL
      AND oe.value >= 0
      AND lower(trim(coalesce(oe.valueuom, ''))) = 'ml'
    """
)

# Identify the first valid post-endpoint urinary timestamp for each eligible stay.
con.execute(
    """
    CREATE TABLE right_boundary_first_time AS
    SELECT
        stay_id,
        MIN(charttime) AS anchor_charttime
    FROM right_boundary_raw
    GROUP BY stay_id
    """
)

# As in the core trajectory, sum all accepted urinary routes documented at the
# same timestamp. These anchor points are used only for interval construction.
con.execute(
    """
    CREATE TABLE right_boundary_points AS
    SELECT
        r.stay_id,
        r.charttime,
        SUM(r.value) AS urine_output_ml,
        COUNT(*) AS raw_rows_n,
        COUNT(DISTINCT r.itemid) AS distinct_itemids_n,
        MIN(r.store_lag_minutes) AS min_store_lag_minutes,
        MAX(r.store_lag_minutes) AS max_store_lag_minutes,
        1 AS right_boundary_anchor_flag
    FROM right_boundary_raw r
    INNER JOIN right_boundary_first_time a
        ON r.stay_id = a.stay_id
       AND r.charttime = a.anchor_charttime
    GROUP BY r.stay_id, r.charttime
    """
)

# Zero is a valid documented output. No upper cutoff is imposed.
con.execute(
    """
    CREATE TABLE route_valid AS
    SELECT *
    FROM route_raw
    WHERE value IS NOT NULL
      AND value >= 0
      AND lower(trim(coalesce(valueuom, ''))) = 'ml'
    """
)

# Sum all accepted urinary routes documented at exactly the same time.
con.execute(
    """
    CREATE TABLE route_points AS
    SELECT
        stay_id,
        charttime,
        SUM(value) AS urine_output_ml,
        COUNT(*) AS raw_rows_n,
        COUNT(DISTINCT itemid) AS distinct_itemids_n,
        MIN(store_lag_minutes) AS min_store_lag_minutes,
        MAX(store_lag_minutes) AS max_store_lag_minutes,
        0 AS right_boundary_anchor_flag
    FROM route_valid
    GROUP BY stay_id, charttime
    """
)

con.execute(
    """
    CREATE TABLE irrigation_stays AS
    SELECT DISTINCT stay_id
    FROM irrigation_raw
    """
)


con.execute(
    """
    CREATE TABLE interval_route_points AS
    SELECT *
    FROM route_points

    UNION ALL

    SELECT *
    FROM right_boundary_points
    """
)


# =============================================================================
# Ordered points and positive-duration intervals
# =============================================================================

con.execute(
    """
    CREATE TABLE ordered_points AS
    SELECT
        p.stay_id,
        p.charttime,
        p.urine_output_ml,
        p.raw_rows_n,
        p.distinct_itemids_n,
        p.min_store_lag_minutes,
        p.max_store_lag_minutes,
        p.right_boundary_anchor_flag,
        c.postoperative_time_zero,
        c.observation_end_time,
        row_number() OVER (
            PARTITION BY p.stay_id
            ORDER BY p.charttime
        ) AS point_index,
        lag(p.charttime) OVER (
            PARTITION BY p.stay_id
            ORDER BY p.charttime
        ) AS previous_charttime
    FROM interval_route_points p
    INNER JOIN cohort c
        ON p.stay_id = c.stay_id
    """
)

con.execute(
    """
    CREATE TABLE intervals AS
    SELECT
        stay_id,
        point_index,
        charttime,
        urine_output_ml,
        raw_rows_n,
        distinct_itemids_n,
        right_boundary_anchor_flag,
        postoperative_time_zero,
        previous_charttime,
        coalesce(previous_charttime, postoperative_time_zero)
            AS interval_start,
        charttime AS interval_end,
        date_diff(
            'second',
            coalesce(previous_charttime, postoperative_time_zero),
            charttime
        ) AS interval_seconds,
        date_diff(
            'second',
            coalesce(previous_charttime, postoperative_time_zero),
            charttime
        ) / 3600.0 AS interval_hours,
        previous_charttime IS NULL AS first_point_flag,
        previous_charttime IS NULL
            AND charttime = postoperative_time_zero
            AS exact_t0_first_point_flag,
        CASE
            WHEN date_diff(
                    'second',
                    coalesce(previous_charttime, postoperative_time_zero),
                    charttime
                 ) > 0
            THEN urine_output_ml /
                 (
                    date_diff(
                        'second',
                        coalesce(previous_charttime, postoperative_time_zero),
                        charttime
                    ) / 3600.0
                 )
            ELSE NULL
        END AS implied_rate_ml_per_hour
    FROM ordered_points
    """
)

con.execute(
    """
    CREATE TABLE positive_intervals AS
    SELECT *
    FROM intervals
    WHERE interval_seconds > 0
    """
)

con.execute(
    """
    CREATE TABLE exact_t0_boundary AS
    SELECT
        stay_id,
        charttime,
        urine_output_ml AS exact_t0_boundary_ml
    FROM intervals
    WHERE exact_t0_first_point_flag
    """
)


# =============================================================================
# Fixed baseline body weight
# =============================================================================

# Broad +/-24-hour search reproduces the validated source review.
con.execute(
    f"""
    CREATE TABLE weight_raw AS
    SELECT
        c.subject_id,
        c.hadm_id,
        c.stay_id,
        c.postoperative_time_zero,
        ce.charttime,
        ce.storetime,
        ce.itemid::BIGINT AS itemid,
        ce.valuenum::DOUBLE AS weight_kg,
        ce.valueuom,
        date_diff(
            'second',
            c.postoperative_time_zero,
            ce.charttime
        ) / 3600.0 AS hours_from_t0
    FROM read_csv_auto(
        '{qpath(CHARTEVENTS_FILE)}',
        header=true,
        all_varchar=false
    ) ce
    INNER JOIN cohort c
        ON ce.stay_id = c.stay_id
       AND ce.itemid IN ({ADMISSION_WEIGHT_KG}, {DAILY_WEIGHT_KG})
       AND ce.charttime
            >= c.postoperative_time_zero - INTERVAL '24 hours'
       AND ce.charttime
            < c.postoperative_time_zero + INTERVAL '24 hours'
    """
)

con.execute(
    f"""
    CREATE TABLE weight_valid AS
    SELECT *
    FROM weight_raw
    WHERE weight_kg > {WEIGHT_MIN_EXCLUSIVE}
      AND weight_kg < {WEIGHT_MAX_EXCLUSIVE}
    """
)

con.execute(
    """
    CREATE TABLE weight_points AS
    SELECT
        stay_id,
        itemid,
        charttime,
        median(weight_kg) AS weight_kg,
        COUNT(*) AS raw_rows_n,
        MIN(weight_kg) AS min_raw_weight_kg,
        MAX(weight_kg) AS max_raw_weight_kg
    FROM weight_valid
    GROUP BY stay_id, itemid, charttime
    """
)

con.execute(
    f"""
    CREATE TABLE patient_weight AS
    WITH admission AS (
        SELECT
            w.stay_id,
            arg_min(
                w.weight_kg,
                abs(date_diff(
                    'second',
                    c.postoperative_time_zero,
                    w.charttime
                ))
            ) AS admission_weight_kg,
            arg_min(
                w.charttime,
                abs(date_diff(
                    'second',
                    c.postoperative_time_zero,
                    w.charttime
                ))
            ) AS admission_weight_charttime
        FROM weight_points w
        INNER JOIN cohort c
            ON w.stay_id = c.stay_id
        WHERE w.itemid = {ADMISSION_WEIGHT_KG}
        GROUP BY w.stay_id
    ),
    daily AS (
        SELECT
            w.stay_id,
            arg_min(
                w.weight_kg,
                abs(date_diff(
                    'second',
                    c.postoperative_time_zero,
                    w.charttime
                ))
            ) AS daily_weight_kg,
            arg_min(
                w.charttime,
                abs(date_diff(
                    'second',
                    c.postoperative_time_zero,
                    w.charttime
                ))
            ) AS daily_weight_charttime
        FROM weight_points w
        INNER JOIN cohort c
            ON w.stay_id = c.stay_id
        WHERE w.itemid = {DAILY_WEIGHT_KG}
        GROUP BY w.stay_id
    )
    SELECT
        c.stay_id,
        coalesce(a.admission_weight_kg, d.daily_weight_kg)
            AS uo_weight_kg,
        CASE
            WHEN a.admission_weight_kg IS NOT NULL
                THEN 'ADMISSION_WEIGHT'
            WHEN d.daily_weight_kg IS NOT NULL
                THEN 'DAILY_WEIGHT_FALLBACK'
            ELSE NULL
        END AS uo_weight_source,
        coalesce(
            a.admission_weight_charttime,
            d.daily_weight_charttime
        ) AS uo_weight_charttime,
        CASE
            WHEN coalesce(a.admission_weight_kg, d.daily_weight_kg) IS NULL
                THEN NULL
            WHEN coalesce(a.admission_weight_kg, d.daily_weight_kg)
                    < {WEIGHT_REVIEW_LOW}
              OR coalesce(a.admission_weight_kg, d.daily_weight_kg)
                    > {WEIGHT_REVIEW_HIGH}
                THEN 1
            ELSE 0
        END AS uo_weight_extreme_review_flag
    FROM cohort c
    LEFT JOIN admission a
        ON c.stay_id = a.stay_id
    LEFT JOIN daily d
        ON c.stay_id = d.stay_id
    """
)

con.execute(
    """
    ALTER TABLE patient_weight
    ADD COLUMN uo_weight_hours_from_t0 DOUBLE
    """
)

con.execute(
    """
    UPDATE patient_weight w
    SET uo_weight_hours_from_t0 = date_diff(
        'second',
        c.postoperative_time_zero,
        w.uo_weight_charttime
    ) / 3600.0
    FROM cohort c
    WHERE w.stay_id = c.stay_id
      AND w.uo_weight_charttime IS NOT NULL
    """
)


# =============================================================================
# Window definitions and complete cohort grids
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


def create_grid(grid_name: str, windows_table: str) -> None:
    con.execute(
        f"""
        CREATE TABLE {grid_name} AS
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

            w.window_index,
            w.window_label,
            w.start_hour,
            w.end_hour,
            w.nominal_minutes,

            c.postoperative_time_zero
                + w.start_hour * INTERVAL '1 hour'
                AS window_start_time,

            c.postoperative_time_zero
                + w.end_hour * INTERVAL '1 hour'
                AS nominal_window_end_time,

            least(
                c.observation_end_time,
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
                        c.observation_end_time,
                        c.postoperative_time_zero
                            + w.end_hour * INTERVAL '1 hour'
                    )
                ) / 60.0
            ) AS eligible_minutes,

            CASE
                WHEN c.observation_end_time
                     <= c.postoperative_time_zero
                        + w.start_hour * INTERVAL '1 hour'
                    THEN 'NOT_OBSERVABLE'
                WHEN c.observation_end_time
                     < c.postoperative_time_zero
                        + w.end_hour * INTERVAL '1 hour'
                    THEN 'PARTIALLY_OBSERVABLE'
                ELSE 'FULLY_OBSERVABLE'
            END AS observability_status

        FROM cohort c
        CROSS JOIN {windows_table} w
        """
    )


create_grid("hourly_grid", "hourly_windows")
create_grid("six_grid", "six_hour_windows")


# =============================================================================
# Interval allocation and sensitivity summaries
# =============================================================================


def create_allocations(prefix: str, grid_table: str) -> None:
    # Primary interval allocation before the conservative irrigation mask.
    con.execute(
        f"""
        CREATE TABLE {prefix}_interval_allocation AS
        SELECT
            g.stay_id,
            g.window_index,

            SUM(
                i.urine_output_ml
                * greatest(
                    0.0,
                    date_diff(
                        'second',
                        greatest(i.interval_start, g.window_start_time),
                        least(i.interval_end, g.eligible_window_end_time)
                    )
                  )
                / nullif(i.interval_seconds, 0)
            ) AS allocated_ml,

            SUM(
                greatest(
                    0.0,
                    date_diff(
                        'second',
                        greatest(i.interval_start, g.window_start_time),
                        least(i.interval_end, g.eligible_window_end_time)
                    )
                )
            ) / 60.0 AS covered_minutes,

            COUNT(i.charttime) AS contributing_intervals_n,
            MIN(i.interval_start) AS first_contributing_interval_start,
            MAX(i.interval_end) AS last_contributing_interval_end

        FROM {grid_table} g
        LEFT JOIN positive_intervals i
            ON i.stay_id = g.stay_id
           AND g.eligible_minutes > 0
           AND i.interval_end > g.window_start_time
           AND i.interval_start < g.eligible_window_end_time
        GROUP BY g.stay_id, g.window_index
        """
    )

    # Event-time sensitivity: volume is assigned entirely to charttime.
    con.execute(
        f"""
        CREATE TABLE {prefix}_event_time AS
        SELECT
            g.stay_id,
            g.window_index,
            SUM(p.urine_output_ml) AS event_time_ml,
            COUNT(p.charttime) AS event_time_points_n,
            COUNT(p.charttime) FILTER (
                WHERE p.urine_output_ml = 0
            ) AS event_time_zero_points_n
        FROM {grid_table} g
        LEFT JOIN route_points p
            ON p.stay_id = g.stay_id
           AND g.eligible_minutes > 0
           AND p.charttime >= g.window_start_time
           AND p.charttime < g.eligible_window_end_time
        GROUP BY g.stay_id, g.window_index
        """
    )

    # A first value charted exactly at t0 is retained as boundary metadata.
    con.execute(
        f"""
        CREATE TABLE {prefix}_boundary AS
        SELECT
            g.stay_id,
            g.window_index,
            SUM(b.exact_t0_boundary_ml) AS exact_t0_boundary_ml
        FROM {grid_table} g
        LEFT JOIN exact_t0_boundary b
            ON b.stay_id = g.stay_id
           AND g.eligible_minutes > 0
           AND b.charttime >= g.window_start_time
           AND b.charttime < g.eligible_window_end_time
        GROUP BY g.stay_id, g.window_index
        """
    )

    # Any GU irrigation event makes that window uninterpretable for primary UO.
    con.execute(
        f"""
        CREATE TABLE {prefix}_irrigation AS
        SELECT
            g.stay_id,
            g.window_index,
            COUNT(r.charttime) AS irrigation_rows_n,
            COUNT(r.charttime) FILTER (
                WHERE r.itemid = {GU_IRRIGANT_IN}
            ) AS irrigant_in_rows_n,
            COUNT(r.charttime) FILTER (
                WHERE r.itemid = {GU_IRRIGANT_OUT}
            ) AS irrigant_out_rows_n,
            SUM(r.value) FILTER (
                WHERE r.itemid = {GU_IRRIGANT_IN}
                  AND r.value IS NOT NULL
            ) AS irrigant_in_ml,
            SUM(r.value) FILTER (
                WHERE r.itemid = {GU_IRRIGANT_OUT}
                  AND r.value IS NOT NULL
            ) AS irrigant_out_ml
        FROM {grid_table} g
        LEFT JOIN irrigation_raw r
            ON r.stay_id = g.stay_id
           AND g.eligible_minutes > 0
           AND r.charttime >= g.window_start_time
           AND r.charttime < g.eligible_window_end_time
        GROUP BY g.stay_id, g.window_index
        """
    )


create_allocations("hourly", "hourly_grid")
create_allocations("six", "six_grid")


# =============================================================================
# Patient-level 24-hour summaries
# =============================================================================

# Summaries are calculated from the unmasked interval allocation. Primary
# 24-hour metrics are set to missing when any GU irrigation occurred.
con.execute(
    """
    CREATE TABLE patient_24h AS
    WITH h AS (
        SELECT
            g.stay_id,
            SUM(coalesce(a.allocated_ml, 0.0)) AS allocated_ml_24h_raw,
            SUM(coalesce(a.covered_minutes, 0.0)) AS covered_minutes_24h,
            SUM(coalesce(e.event_time_ml, 0.0)) AS event_time_ml_24h,
            SUM(coalesce(b.exact_t0_boundary_ml, 0.0))
                AS exact_t0_boundary_ml_24h,
            SUM(coalesce(i.irrigation_rows_n, 0)) AS irrigation_rows_24h_n,
            SUM(g.eligible_minutes) AS eligible_minutes_24h
        FROM hourly_grid g
        INNER JOIN hourly_interval_allocation a
            ON g.stay_id = a.stay_id
           AND g.window_index = a.window_index
        INNER JOIN hourly_event_time e
            ON g.stay_id = e.stay_id
           AND g.window_index = e.window_index
        INNER JOIN hourly_boundary b
            ON g.stay_id = b.stay_id
           AND g.window_index = b.window_index
        INNER JOIN hourly_irrigation i
            ON g.stay_id = i.stay_id
           AND g.window_index = i.window_index
        GROUP BY g.stay_id
    )
    SELECT
        c.stay_id,
        h.irrigation_rows_24h_n > 0 AS uo_irrigation_any_24h_flag,
        h.irrigation_rows_24h_n AS uo_irrigation_rows_24h_n,
        h.event_time_ml_24h AS uo_event_time_ml_24h_unadjusted,
        h.exact_t0_boundary_ml_24h AS uo_exact_t0_boundary_ml_24h,
        h.covered_minutes_24h AS uo_interval_covered_minutes_24h,
        h.eligible_minutes_24h AS uo_eligible_minutes_24h,
        CASE
            WHEN h.eligible_minutes_24h > 0
            THEN h.covered_minutes_24h / h.eligible_minutes_24h
            ELSE NULL
        END AS uo_interval_coverage_fraction_24h,
        CASE
            WHEN h.irrigation_rows_24h_n > 0 THEN NULL
            WHEN h.covered_minutes_24h <= 0 THEN NULL
            ELSE h.allocated_ml_24h_raw
        END AS uo_interval_ml_24h_observed,
        CASE
            WHEN h.irrigation_rows_24h_n > 0 THEN NULL
            WHEN h.covered_minutes_24h <= 0 THEN NULL
            ELSE h.allocated_ml_24h_raw
                 / (h.covered_minutes_24h / 60.0)
        END AS uo_rate_ml_per_hour_24h_observed,
        CASE
            WHEN h.irrigation_rows_24h_n > 0 THEN NULL
            WHEN h.covered_minutes_24h <= 0 THEN NULL
            WHEN w.uo_weight_kg IS NULL THEN NULL
            ELSE h.allocated_ml_24h_raw
                 / (h.covered_minutes_24h / 60.0)
                 / w.uo_weight_kg
        END AS uo_rate_ml_per_kg_per_hour_24h_observed,
        CASE
            WHEN h.eligible_minutes_24h <= 0 THEN 'NOT_OBSERVABLE'
            WHEN h.irrigation_rows_24h_n > 0 THEN 'IRRIGATION_AFFECTED'
            WHEN h.covered_minutes_24h <= 0
                THEN 'NO_POSITIVE_DURATION_INTERVAL'
            WHEN h.covered_minutes_24h >= h.eligible_minutes_24h - 0.01
                THEN 'FULL_INTERVAL_COVERAGE'
            WHEN h.covered_minutes_24h >= 0.5 * h.eligible_minutes_24h
                THEN 'PARTIAL_INTERVAL_COVERAGE_GE50'
            ELSE 'PARTIAL_INTERVAL_COVERAGE_LT50'
        END AS uo_measurement_status_24h
    FROM cohort c
    INNER JOIN h
        ON c.stay_id = h.stay_id
    INNER JOIN patient_weight w
        ON c.stay_id = w.stay_id
    """
)


# =============================================================================
# Canonical window outputs
# =============================================================================


def create_window_output(output_table: str, prefix: str, grid_table: str) -> None:
    con.execute(
        f"""
        CREATE TABLE {output_table} AS
        SELECT
            g.subject_id,
            g.hadm_id,
            g.stay_id,
            g.surgery_group,
            g.sex,
            g.age_at_icu_admission,
            g.postoperative_time_zero,
            g.observation_end_time,
            g.observation_end_type,

            g.window_index,
            g.window_label,
            g.start_hour,
            g.end_hour,
            g.nominal_minutes,
            g.window_start_time,
            g.nominal_window_end_time,
            g.eligible_window_end_time,
            g.eligible_minutes,
            g.observability_status,

            w.uo_weight_kg,
            w.uo_weight_source,
            w.uo_weight_charttime,
            w.uo_weight_hours_from_t0,
            w.uo_weight_extreme_review_flag,

            p24.uo_irrigation_any_24h_flag,
            p24.uo_irrigation_rows_24h_n,
            p24.uo_event_time_ml_24h_unadjusted,
            p24.uo_exact_t0_boundary_ml_24h,
            p24.uo_interval_covered_minutes_24h,
            p24.uo_eligible_minutes_24h,
            p24.uo_interval_coverage_fraction_24h,
            p24.uo_interval_ml_24h_observed,
            p24.uo_rate_ml_per_hour_24h_observed,
            p24.uo_rate_ml_per_kg_per_hour_24h_observed,
            p24.uo_measurement_status_24h,

            CASE
                WHEN coalesce(ir.irrigation_rows_n, 0) > 0 THEN 1
                WHEN g.eligible_minutes > 0 THEN 0
                ELSE NULL
            END AS uo_irrigation_affected_window_flag,

            coalesce(ir.irrigation_rows_n, 0) AS uo_irrigation_rows_n,
            coalesce(ir.irrigant_in_rows_n, 0) AS uo_irrigant_in_rows_n,
            coalesce(ir.irrigant_out_rows_n, 0) AS uo_irrigant_out_rows_n,
            ir.irrigant_in_ml AS uo_irrigant_in_ml,
            ir.irrigant_out_ml AS uo_irrigant_out_ml,

            coalesce(ev.event_time_points_n, 0)
                AS uo_event_time_points_n,
            coalesce(ev.event_time_zero_points_n, 0)
                AS uo_event_time_zero_points_n,
            coalesce(ev.event_time_ml, 0.0)
                AS uo_event_time_ml_unadjusted,

            coalesce(b.exact_t0_boundary_ml, 0.0)
                AS uo_exact_t0_boundary_ml,

            coalesce(a.contributing_intervals_n, 0)
                AS uo_contributing_intervals_n,
            coalesce(a.covered_minutes, 0.0)
                AS uo_interval_covered_minutes,
            CASE
                WHEN g.eligible_minutes > 0
                THEN coalesce(a.covered_minutes, 0.0) / g.eligible_minutes
                ELSE NULL
            END AS uo_interval_coverage_fraction,
            a.first_contributing_interval_start
                AS uo_first_contributing_interval_start,
            a.last_contributing_interval_end
                AS uo_last_contributing_interval_end,

            CASE
                WHEN g.eligible_minutes <= 0 THEN NULL
                WHEN coalesce(ir.irrigation_rows_n, 0) > 0 THEN NULL
                WHEN coalesce(a.covered_minutes, 0.0) <= 0 THEN NULL
                ELSE coalesce(a.allocated_ml, 0.0)
            END AS uo_interval_ml_observed,

            CASE
                WHEN g.eligible_minutes <= 0 THEN NULL
                WHEN coalesce(ir.irrigation_rows_n, 0) > 0 THEN NULL
                WHEN coalesce(a.covered_minutes, 0.0) <= 0 THEN NULL
                ELSE coalesce(a.allocated_ml, 0.0)
                     / (a.covered_minutes / 60.0)
            END AS uo_rate_ml_per_hour_observed,

            CASE
                WHEN g.eligible_minutes <= 0 THEN NULL
                WHEN coalesce(ir.irrigation_rows_n, 0) > 0 THEN NULL
                WHEN coalesce(a.covered_minutes, 0.0) <= 0 THEN NULL
                WHEN w.uo_weight_kg IS NULL THEN NULL
                ELSE coalesce(a.allocated_ml, 0.0)
                     / (a.covered_minutes / 60.0)
                     / w.uo_weight_kg
            END AS uo_rate_ml_per_kg_per_hour_observed,

            CASE
                WHEN g.eligible_minutes <= 0 THEN NULL
                WHEN coalesce(ir.irrigation_rows_n, 0) > 0 THEN NULL
                WHEN coalesce(a.covered_minutes, 0.0) <= 0 THEN NULL
                WHEN coalesce(a.allocated_ml, 0.0) = 0 THEN 1
                ELSE 0
            END AS uo_zero_output_observed_flag,

            CASE
                WHEN g.eligible_minutes <= 0 THEN 0
                WHEN coalesce(ir.irrigation_rows_n, 0) > 0 THEN 0
                WHEN coalesce(a.covered_minutes, 0.0) <= 0 THEN 0
                ELSE 1
            END AS uo_primary_available_flag,

            CASE
                WHEN g.eligible_minutes <= 0 THEN 'NOT_OBSERVABLE'
                WHEN coalesce(ir.irrigation_rows_n, 0) > 0
                    THEN 'IRRIGATION_AFFECTED'
                WHEN coalesce(a.covered_minutes, 0.0) <= 0
                    AND coalesce(b.exact_t0_boundary_ml, 0.0) > 0
                    THEN 'BOUNDARY_ONLY_NO_POSITIVE_INTERVAL'
                WHEN coalesce(a.covered_minutes, 0.0) <= 0
                    THEN 'OBSERVABLE_NO_POSITIVE_INTERVAL'
                WHEN a.covered_minutes >= g.eligible_minutes - 0.01
                    THEN 'FULL_INTERVAL_COVERAGE'
                WHEN a.covered_minutes >= 0.5 * g.eligible_minutes
                    THEN 'PARTIAL_INTERVAL_COVERAGE_GE50'
                ELSE 'PARTIAL_INTERVAL_COVERAGE_LT50'
            END AS uo_measurement_status

        FROM {grid_table} g
        INNER JOIN {prefix}_interval_allocation a
            ON g.stay_id = a.stay_id
           AND g.window_index = a.window_index
        INNER JOIN {prefix}_event_time ev
            ON g.stay_id = ev.stay_id
           AND g.window_index = ev.window_index
        INNER JOIN {prefix}_boundary b
            ON g.stay_id = b.stay_id
           AND g.window_index = b.window_index
        INNER JOIN {prefix}_irrigation ir
            ON g.stay_id = ir.stay_id
           AND g.window_index = ir.window_index
        INNER JOIN patient_weight w
            ON g.stay_id = w.stay_id
        INNER JOIN patient_24h p24
            ON g.stay_id = p24.stay_id
        """
    )


create_window_output("hourly_output", "hourly", "hourly_grid")
create_window_output("six_output", "six", "six_grid")


# =============================================================================
# Validation invariants
# =============================================================================

hourly_n = con.execute("SELECT COUNT(*) FROM hourly_output").fetchone()[0]
six_n = con.execute("SELECT COUNT(*) FROM six_output").fetchone()[0]

if hourly_n != EXPECTED_COHORT_N * 24:
    raise RuntimeError(
        f"Unexpected hourly rows: {hourly_n}; "
        f"expected {EXPECTED_COHORT_N * 24}."
    )

if six_n != EXPECTED_COHORT_N * 4:
    raise RuntimeError(
        f"Unexpected six-hour rows: {six_n}; "
        f"expected {EXPECTED_COHORT_N * 4}."
    )

# Some right-boundary intervals end after the analytical horizon. The invariant
# therefore targets only the fraction of each interval that actually overlaps
# [t0, analytical_end). This is the volume that must be conserved across both
# hourly and six-hour allocations.
positive_interval_volume_n = con.execute(
    """
    SELECT SUM(
        i.urine_output_ml
        * greatest(
            0.0,
            date_diff(
                'second',
                greatest(i.interval_start, c.postoperative_time_zero),
                least(i.interval_end, c.analytical_end_time)
            )
        )
        / nullif(i.interval_seconds, 0)
    )
    FROM positive_intervals i
    INNER JOIN cohort c
        ON i.stay_id = c.stay_id
    WHERE i.interval_end > c.postoperative_time_zero
      AND i.interval_start < c.analytical_end_time
    """
).fetchone()[0]

hourly_allocated_n = con.execute(
    "SELECT SUM(allocated_ml) FROM hourly_interval_allocation"
).fetchone()[0]

six_allocated_n = con.execute(
    "SELECT SUM(allocated_ml) FROM six_interval_allocation"
).fetchone()[0]

for label, allocated in [
    ("hourly", hourly_allocated_n),
    ("six-hour", six_allocated_n),
]:
    if abs((allocated or 0.0) - (positive_interval_volume_n or 0.0)) > 0.01:
        raise RuntimeError(
            f"{label} interval allocation does not conserve volume: "
            f"allocated={allocated}, expected={positive_interval_volume_n}."
        )

weight_rows_n = con.execute("SELECT COUNT(*) FROM patient_weight").fetchone()[0]
if weight_rows_n != EXPECTED_COHORT_N:
    raise RuntimeError(
        f"Unexpected patient_weight rows: {weight_rows_n}; "
        f"expected {EXPECTED_COHORT_N}."
    )


# =============================================================================
# Audit table
# =============================================================================

con.execute(
    """
    CREATE TABLE audit (
        audit_level VARCHAR,
        window_index INTEGER,
        window_label VARCHAR,
        metric VARCHAR,
        value_num DOUBLE,
        value_text VARCHAR
    )
    """
)

route_raw_n = con.execute("SELECT COUNT(*) FROM route_raw").fetchone()[0]
route_valid_n = con.execute("SELECT COUNT(*) FROM route_valid").fetchone()[0]
route_invalid_n = route_raw_n - route_valid_n
route_points_n = con.execute("SELECT COUNT(*) FROM route_points").fetchone()[0]
route_patients_n = con.execute(
    "SELECT COUNT(DISTINCT stay_id) FROM route_points"
).fetchone()[0]
positive_intervals_n = con.execute(
    "SELECT COUNT(*) FROM positive_intervals"
).fetchone()[0]
right_boundary_anchor_points_n = con.execute(
    "SELECT COUNT(*) FROM right_boundary_points"
).fetchone()[0]
right_boundary_anchor_patients_n = con.execute(
    "SELECT COUNT(DISTINCT stay_id) FROM right_boundary_points"
).fetchone()[0]
right_boundary_anchor_gap_context = con.execute(
    """
    SELECT
        median(date_diff('second', c.analytical_end_time, a.charttime) / 60.0),
        quantile_cont(
            date_diff('second', c.analytical_end_time, a.charttime) / 60.0,
            0.95
        ),
        max(date_diff('second', c.analytical_end_time, a.charttime) / 60.0)
    FROM right_boundary_points a
    INNER JOIN cohort c
        ON a.stay_id = c.stay_id
    """
).fetchone()
right_boundary_attributed_ml = con.execute(
    """
    SELECT SUM(
        i.urine_output_ml
        * greatest(
            0.0,
            date_diff(
                'second',
                greatest(i.interval_start, c.postoperative_time_zero),
                least(i.interval_end, c.analytical_end_time)
            )
        )
        / nullif(i.interval_seconds, 0)
    )
    FROM positive_intervals i
    INNER JOIN cohort c
        ON i.stay_id = c.stay_id
    WHERE i.right_boundary_anchor_flag = 1
      AND i.interval_end > c.postoperative_time_zero
      AND i.interval_start < c.analytical_end_time
    """
).fetchone()[0]
exact_t0_points_n = con.execute(
    "SELECT COUNT(*) FROM exact_t0_boundary"
).fetchone()[0]
exact_t0_volume_ml = con.execute(
    "SELECT SUM(exact_t0_boundary_ml) FROM exact_t0_boundary"
).fetchone()[0]
irrigation_rows_n = con.execute(
    "SELECT COUNT(*) FROM irrigation_raw"
).fetchone()[0]
irrigation_stays_n = con.execute(
    "SELECT COUNT(*) FROM irrigation_stays"
).fetchone()[0]
weight_available_n = con.execute(
    "SELECT COUNT(*) FROM patient_weight WHERE uo_weight_kg IS NOT NULL"
).fetchone()[0]
weight_admission_n = con.execute(
    """
    SELECT COUNT(*)
    FROM patient_weight
    WHERE uo_weight_source = 'ADMISSION_WEIGHT'
    """
).fetchone()[0]
weight_fallback_n = con.execute(
    """
    SELECT COUNT(*)
    FROM patient_weight
    WHERE uo_weight_source = 'DAILY_WEIGHT_FALLBACK'
    """
).fetchone()[0]
weight_extreme_n = con.execute(
    """
    SELECT COUNT(*)
    FROM patient_weight
    WHERE uo_weight_extreme_review_flag = 1
    """
).fetchone()[0]

value_context = con.execute(
    """
    SELECT
        MIN(urine_output_ml),
        median(urine_output_ml),
        quantile_cont(urine_output_ml, 0.99),
        MAX(urine_output_ml),
        COUNT(*) FILTER (WHERE urine_output_ml = 0),
        COUNT(*) FILTER (WHERE urine_output_ml > 1000),
        COUNT(*) FILTER (WHERE urine_output_ml > 2000)
    FROM route_points
    """
).fetchone()

interval_context = con.execute(
    """
    SELECT
        median(interval_hours),
        quantile_cont(interval_hours, 0.90),
        quantile_cont(interval_hours, 0.99),
        COUNT(*) FILTER (WHERE interval_hours > 2),
        COUNT(*) FILTER (WHERE implied_rate_ml_per_hour > 500),
        COUNT(*) FILTER (WHERE implied_rate_ml_per_hour > 1000),
        MAX(implied_rate_ml_per_hour)
    FROM positive_intervals
    """
).fetchone()

weight_context = con.execute(
    """
    SELECT
        median(uo_weight_kg),
        quantile_cont(uo_weight_kg, 0.01),
        quantile_cont(uo_weight_kg, 0.99),
        MIN(uo_weight_kg),
        MAX(uo_weight_kg)
    FROM patient_weight
    WHERE uo_weight_kg IS NOT NULL
    """
).fetchone()

overall_rows = [
    ("OVERALL", None, None, "cohort_n", cohort_n, None),
    ("OVERALL", None, None, "route_raw_rows_n", route_raw_n, None),
    ("OVERALL", None, None, "route_valid_rows_n", route_valid_n, None),
    (
        "OVERALL",
        None,
        None,
        "route_invalid_or_non_ml_rows_n",
        route_invalid_n,
        "excluded if null, negative, or unit not mL",
    ),
    ("OVERALL", None, None, "route_integrated_timestamps_n", route_points_n, None),
    ("OVERALL", None, None, "route_patients_n", route_patients_n, None),
    (
        "OVERALL",
        None,
        None,
        "patients_without_route_points_n",
        EXPECTED_COHORT_N - route_patients_n,
        None,
    ),
    ("OVERALL", None, None, "positive_duration_intervals_n", positive_intervals_n, None),
    (
        "OVERALL",
        None,
        None,
        "right_boundary_anchor_points_n",
        right_boundary_anchor_points_n,
        "first valid urinary timestamp at/after t0+24 h; interval construction only",
    ),
    (
        "OVERALL",
        None,
        None,
        "right_boundary_anchor_patients_n",
        right_boundary_anchor_patients_n,
        "restricted to stays observable through 24 h and still in ICU",
    ),
    (
        "OVERALL",
        None,
        None,
        "right_boundary_anchor_attributed_in_horizon_ml",
        right_boundary_attributed_ml,
        "only the interval fraction overlapping the Study-1 analytical horizon",
    ),
    ("OVERALL", None, None, "exact_t0_boundary_points_n", exact_t0_points_n, None),
    ("OVERALL", None, None, "exact_t0_boundary_total_ml", exact_t0_volume_ml, None),
    ("OVERALL", None, None, "irrigation_rows_n", irrigation_rows_n, None),
    ("OVERALL", None, None, "irrigation_stays_n", irrigation_stays_n, None),
    ("OVERALL", None, None, "weight_available_n", weight_available_n, None),
    (
        "OVERALL",
        None,
        None,
        "weight_missing_n",
        EXPECTED_COHORT_N - weight_available_n,
        None,
    ),
    ("OVERALL", None, None, "weight_admission_source_n", weight_admission_n, None),
    ("OVERALL", None, None, "weight_daily_fallback_n", weight_fallback_n, None),
    (
        "OVERALL",
        None,
        None,
        "weight_extreme_review_flag_n",
        weight_extreme_n,
        f"retained; flagged if <{WEIGHT_REVIEW_LOW} or >{WEIGHT_REVIEW_HIGH} kg",
    ),
    (
        "OVERALL",
        None,
        None,
        "positive_interval_volume_total_ml",
        positive_interval_volume_n,
        None,
    ),
    (
        "OVERALL",
        None,
        None,
        "hourly_allocated_volume_total_ml",
        hourly_allocated_n,
        "volume-conservation invariant",
    ),
    (
        "OVERALL",
        None,
        None,
        "six_hour_allocated_volume_total_ml",
        six_allocated_n,
        "volume-conservation invariant",
    ),
]

con.executemany(
    "INSERT INTO audit VALUES (?, ?, ?, ?, ?, ?)",
    overall_rows,
)

for metric, value in zip(
    [
        "right_boundary_anchor_gap_median_minutes",
        "right_boundary_anchor_gap_p95_minutes",
        "right_boundary_anchor_gap_max_minutes",
    ],
    right_boundary_anchor_gap_context,
):
    con.execute(
        """
        INSERT INTO audit
        VALUES (
            'RIGHT_BOUNDARY',
            NULL,
            NULL,
            ?,
            ?,
            'time from t0+24 analytical endpoint to first valid urinary anchor'
        )
        """,
        [metric, None if value is None else float(value)],
    )

value_metric_names = [
    "route_point_min_ml",
    "route_point_median_ml",
    "route_point_p99_ml",
    "route_point_max_ml",
    "route_zero_points_n",
    "route_points_gt1000ml_n",
    "route_points_gt2000ml_n",
]
for metric, value in zip(value_metric_names, value_context):
    con.execute(
        """
        INSERT INTO audit
        VALUES ('VALUE_CONTEXT', NULL, NULL, ?, ?,
                'descriptive; not a technical exclusion threshold')
        """,
        [metric, None if value is None else float(value)],
    )

interval_metric_names = [
    "interval_median_hours",
    "interval_p90_hours",
    "interval_p99_hours",
    "intervals_gt2h_n",
    "intervals_implied_rate_gt500ml_h_n",
    "intervals_implied_rate_gt1000ml_h_n",
    "interval_max_implied_rate_ml_h",
]
for metric, value in zip(interval_metric_names, interval_context):
    con.execute(
        """
        INSERT INTO audit
        VALUES ('INTERVAL_CONTEXT', NULL, NULL, ?, ?,
                'descriptive; rates are aggregated at window level')
        """,
        [metric, None if value is None else float(value)],
    )

weight_metric_names = [
    "weight_median_kg",
    "weight_p01_kg",
    "weight_p99_kg",
    "weight_min_kg",
    "weight_max_kg",
]
for metric, value in zip(weight_metric_names, weight_context):
    con.execute(
        """
        INSERT INTO audit
        VALUES ('WEIGHT_CONTEXT', NULL, NULL, ?, ?,
                'fixed baseline actual body weight')
        """,
        [metric, None if value is None else float(value)],
    )


# Source-route distribution.
con.execute(
    f"""
    INSERT INTO audit
    SELECT
        'ROUTE_SOURCE',
        NULL,
        NULL,
        'itemid_' || cast(itemid AS VARCHAR),
        COUNT(*)::DOUBLE,
        CASE itemid
            WHEN 226559 THEN 'Foley valid rows'
            WHEN 226560 THEN 'Void valid rows'
            WHEN 226561 THEN 'Condom Cath valid rows'
            WHEN 226584 THEN 'Ileoconduit valid rows'
            WHEN 226563 THEN 'Suprapubic valid rows'
            WHEN 226564 THEN 'Right Nephrostomy valid rows'
            WHEN 226565 THEN 'Left Nephrostomy valid rows'
            WHEN 226567 THEN 'Straight Cath valid rows'
            WHEN 226557 THEN 'Right Ureteral Stent valid rows'
            WHEN 226558 THEN 'Left Ureteral Stent valid rows'
            ELSE 'accepted urinary route'
        END
    FROM route_valid
    GROUP BY itemid
    """
)


def add_window_audit(table_name: str, level: str) -> None:
    con.execute(
        f"""
        INSERT INTO audit
        SELECT
            '{level}',
            window_index,
            window_label,
            metric,
            value_num,
            NULL
        FROM (
            SELECT
                window_index,
                window_label,
                'eligible_patients_n' AS metric,
                COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                )::DOUBLE AS value_num
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'primary_available_patients_n',
                COUNT(*) FILTER (
                    WHERE uo_primary_available_flag = 1
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'primary_available_pct',
                100.0 * COUNT(*) FILTER (
                    WHERE uo_primary_available_flag = 1
                ) / nullif(
                    COUNT(*) FILTER (WHERE eligible_minutes > 0),
                    0
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'irrigation_affected_windows_n',
                COUNT(*) FILTER (
                    WHERE uo_irrigation_affected_window_flag = 1
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'full_interval_coverage_pct',
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND uo_measurement_status = 'FULL_INTERVAL_COVERAGE'
                ) / nullif(
                    COUNT(*) FILTER (WHERE eligible_minutes > 0),
                    0
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'at_least_half_interval_coverage_pct',
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND uo_interval_covered_minutes
                            >= 0.5 * eligible_minutes
                ) / nullif(
                    COUNT(*) FILTER (WHERE eligible_minutes > 0),
                    0
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'median_interval_covered_minutes',
                median(uo_interval_covered_minutes)::DOUBLE
            FROM {table_name}
            WHERE eligible_minutes > 0
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'population_median_interval_ml_observed',
                median(uo_interval_ml_observed)::DOUBLE
            FROM {table_name}
            WHERE uo_primary_available_flag = 1
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'population_median_rate_ml_per_hour_observed',
                median(uo_rate_ml_per_hour_observed)::DOUBLE
            FROM {table_name}
            WHERE uo_primary_available_flag = 1
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'population_median_rate_ml_per_kg_per_hour_observed',
                median(uo_rate_ml_per_kg_per_hour_observed)::DOUBLE
            FROM {table_name}
            WHERE uo_primary_available_flag = 1
              AND uo_weight_kg IS NOT NULL
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'population_median_event_time_ml_unadjusted',
                median(uo_event_time_ml_unadjusted)::DOUBLE
            FROM {table_name}
            WHERE eligible_minutes > 0
            GROUP BY window_index, window_label

            UNION ALL

            SELECT
                window_index,
                window_label,
                'zero_output_observed_windows_n',
                COUNT(*) FILTER (
                    WHERE uo_zero_output_observed_flag = 1
                )::DOUBLE
            FROM {table_name}
            GROUP BY window_index, window_label
        )
        """
    )


add_window_audit("hourly_output", "HOURLY")
add_window_audit("six_output", "SIX_HOUR")

# 24-hour patient-level summary.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'PATIENT_24H',
        NULL,
        NULL,
        metric,
        value_num,
        NULL
    FROM (
        SELECT
            'primary_available_patients_n' AS metric,
            COUNT(*) FILTER (
                WHERE uo_interval_ml_24h_observed IS NOT NULL
            )::DOUBLE AS value_num
        FROM patient_24h

        UNION ALL

        SELECT
            'irrigation_affected_patients_n',
            COUNT(*) FILTER (
                WHERE uo_irrigation_any_24h_flag
            )::DOUBLE
        FROM patient_24h

        UNION ALL

        SELECT
            'median_interval_covered_hours',
            median(uo_interval_covered_minutes_24h / 60.0)::DOUBLE
        FROM patient_24h

        UNION ALL

        SELECT
            'median_interval_ml_observed',
            median(uo_interval_ml_24h_observed)::DOUBLE
        FROM patient_24h
        WHERE uo_interval_ml_24h_observed IS NOT NULL

        UNION ALL

        SELECT
            'median_rate_ml_per_hour_observed',
            median(uo_rate_ml_per_hour_24h_observed)::DOUBLE
        FROM patient_24h
        WHERE uo_rate_ml_per_hour_24h_observed IS NOT NULL

        UNION ALL

        SELECT
            'median_rate_ml_per_kg_per_hour_observed',
            median(uo_rate_ml_per_kg_per_hour_24h_observed)::DOUBLE
        FROM patient_24h
        WHERE uo_rate_ml_per_kg_per_hour_24h_observed IS NOT NULL
    )
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
                WHEN 'RIGHT_BOUNDARY' THEN 2
                WHEN 'VALUE_CONTEXT' THEN 3
                WHEN 'INTERVAL_CONTEXT' THEN 4
                WHEN 'WEIGHT_CONTEXT' THEN 5
                WHEN 'ROUTE_SOURCE' THEN 6
                WHEN 'PATIENT_24H' THEN 7
                WHEN 'HOURLY' THEN 8
                WHEN 'SIX_HOUR' THEN 9
                ELSE 99
            END,
            window_index NULLS FIRST,
            metric
    )
    TO '{qpath(OUT_AUDIT)}'
    (HEADER, DELIMITER ',')
    """
)


# =============================================================================
# Console report
# =============================================================================

print("=" * 104)
print("TRAJECTORIES - URINE OUTPUT CANONICAL INTERVAL TRAJECTORY")
print("=" * 104)
print(f"Cohort rows / unique stays         : {cohort_n} / {unique_stays_n}")
print(f"Raw accepted-route rows            : {route_raw_n}")
print(f"Valid accepted-route rows          : {route_valid_n}")
print(f"Invalid/non-mL route rows          : {route_invalid_n}")
print(f"Integrated route timestamps        : {route_points_n}")
print(f"Patients with route timestamps     : {route_patients_n}")
print(f"Positive-duration intervals        : {positive_intervals_n}")
print(f"Right-boundary anchor patients     : {right_boundary_anchor_patients_n}")
print(f"Right-boundary attributed mL       : {right_boundary_attributed_ml:.1f}")
print(f"Exact-t0 boundary points excluded  : {exact_t0_points_n}")
print(f"Exact-t0 boundary volume retained  : {exact_t0_volume_ml:.1f} mL")
print(f"GU irrigation stays                : {irrigation_stays_n}")
print(f"Baseline weight available          : {weight_available_n}")
print(f"Admission weight source            : {weight_admission_n}")
print(f"Daily-weight fallback              : {weight_fallback_n}")
print(f"Missing baseline weight            : {EXPECTED_COHORT_N - weight_available_n}")
print(f"Extreme-weight review flags        : {weight_extreme_n}")
print()

print("Volume-conservation invariants")
print("------------------------------")
print(f"Positive-interval source volume    : {positive_interval_volume_n:.3f} mL")
print(f"Hourly allocated volume            : {hourly_allocated_n:.3f} mL")
print(f"Six-hour allocated volume          : {six_allocated_n:.3f} mL")
print()

print("Hourly trajectory coverage")
print("--------------------------")
print(
    con.execute(
        """
        SELECT
            window_label,
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
            ) AS eligible_n,
            COUNT(*) FILTER (
                WHERE uo_primary_available_flag = 1
            ) AS primary_available_n,
            round(
                100.0 * COUNT(*) FILTER (
                    WHERE uo_primary_available_flag = 1
                ) / nullif(
                    COUNT(*) FILTER (WHERE eligible_minutes > 0),
                    0
                ),
                2
            ) AS primary_available_pct,
            round(median(uo_interval_covered_minutes), 1)
                AS median_covered_minutes,
            round(
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND uo_measurement_status = 'FULL_INTERVAL_COVERAGE'
                ) / nullif(
                    COUNT(*) FILTER (WHERE eligible_minutes > 0),
                    0
                ),
                2
            ) AS full_coverage_pct,
            round(
                median(uo_rate_ml_per_hour_observed) FILTER (
                    WHERE uo_primary_available_flag = 1
                ),
                2
            ) AS median_ml_h,
            round(
                median(uo_rate_ml_per_kg_per_hour_observed) FILTER (
                    WHERE uo_primary_available_flag = 1
                      AND uo_weight_kg IS NOT NULL
                ),
                4
            ) AS median_ml_kg_h,
            COUNT(*) FILTER (
                WHERE uo_irrigation_affected_window_flag = 1
            ) AS irrigation_windows_n
        FROM hourly_output
        GROUP BY window_index, window_label
        ORDER BY window_index
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Six-hour trajectory coverage")
print("----------------------------")
print(
    con.execute(
        """
        SELECT
            window_label,
            COUNT(*) FILTER (
                WHERE eligible_minutes > 0
            ) AS eligible_n,
            COUNT(*) FILTER (
                WHERE uo_primary_available_flag = 1
            ) AS primary_available_n,
            round(
                100.0 * COUNT(*) FILTER (
                    WHERE uo_primary_available_flag = 1
                ) / nullif(
                    COUNT(*) FILTER (WHERE eligible_minutes > 0),
                    0
                ),
                2
            ) AS primary_available_pct,
            round(median(uo_interval_covered_minutes), 1)
                AS median_covered_minutes,
            round(
                100.0 * COUNT(*) FILTER (
                    WHERE eligible_minutes > 0
                      AND uo_interval_covered_minutes >= 0.5 * eligible_minutes
                ) / nullif(
                    COUNT(*) FILTER (WHERE eligible_minutes > 0),
                    0
                ),
                2
            ) AS at_least_half_coverage_pct,
            round(
                median(uo_interval_ml_observed) FILTER (
                    WHERE uo_primary_available_flag = 1
                ),
                1
            ) AS median_interval_ml,
            round(
                median(uo_rate_ml_per_hour_observed) FILTER (
                    WHERE uo_primary_available_flag = 1
                ),
                2
            ) AS median_ml_h,
            round(
                median(uo_rate_ml_per_kg_per_hour_observed) FILTER (
                    WHERE uo_primary_available_flag = 1
                      AND uo_weight_kg IS NOT NULL
                ),
                4
            ) AS median_ml_kg_h,
            round(
                median(uo_event_time_ml_unadjusted) FILTER (
                    WHERE eligible_minutes > 0
                ),
                1
            ) AS median_event_time_ml
        FROM six_output
        GROUP BY window_index, window_label
        ORDER BY window_index
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Integrated 24-hour summary")
print("--------------------------")
print(
    con.execute(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE uo_interval_ml_24h_observed IS NOT NULL
            ) AS primary_available_n,
            COUNT(*) FILTER (
                WHERE uo_irrigation_any_24h_flag
            ) AS irrigation_affected_n,
            round(median(uo_interval_covered_minutes_24h / 60.0), 2)
                AS median_covered_hours,
            round(
                median(uo_interval_ml_24h_observed) FILTER (
                    WHERE uo_interval_ml_24h_observed IS NOT NULL
                ),
                1
            ) AS median_interval_ml,
            round(
                median(uo_rate_ml_per_hour_24h_observed) FILTER (
                    WHERE uo_rate_ml_per_hour_24h_observed IS NOT NULL
                ),
                2
            ) AS median_ml_h,
            round(
                median(uo_rate_ml_per_kg_per_hour_24h_observed) FILTER (
                    WHERE uo_rate_ml_per_kg_per_hour_24h_observed IS NOT NULL
                ),
                4
            ) AS median_ml_kg_h
        FROM patient_24h
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
print("Canonical urine-output extraction complete.")
print("Primary feature: uo_rate_ml_per_kg_per_hour_observed.")
print("Absolute rate and interval volume are retained as companion features.")
print("No absent time was imputed as zero.")
print("No KDIGO/oliguria thresholding was applied.")
