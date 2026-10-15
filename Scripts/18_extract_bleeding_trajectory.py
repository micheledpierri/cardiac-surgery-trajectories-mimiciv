"""
01_15_extract_bleeding_trajectory.py

Trajectories project
Bleeding domain - early postoperative chest/mediastinal drainage trajectory
FROZEN PRODUCTION COMPONENT

Purpose
-------
Construct the EARLY postoperative bleeding trajectory from chest, pleural, and
mediastinal drain output during the first 12 hours after the frozen
postoperative time origin.

Scientific role
---------------
This component represents the TRAJECTORY of postoperative bleeding.

Transfusions and surgical revision/re-exploration are NOT used to define this
trajectory; they are separate bleeding outcomes.

Primary analytical horizon
--------------------------
0-12 hours after postoperative_time_zero.

Primary resolution
------------------
12 hourly windows:
    H00_H01 ... H11_H12

Secondary resolution
--------------------
Two six-hour windows:
    H00_H06
    H06_H12

Derived early-bleeding features
-------------------------------
- cumulative drainage at 6 h and 12 h;
- absolute drainage in mL;
- weight-normalized drainage in mL/kg;
- hourly drainage rate in mL/h and mL/kg/h;
- maximum hourly drainage during the first 12 h;
- number of hours with recorded drainage >=200 mL.

The >=200 mL/h descriptor is retained only as a descriptive feature. It does
not define the trajectory.

Upstream methodological dependency
----------------------------------
The source definition and temporal semantics are inherited unchanged from the
validated chest/mediastinal drainage subcomponent of frozen component 01_10
(Postoperative ICU-recorded Fluid Balance):

- accepted chest/pleural/mediastinal OUTPUTEVENTS itemids are unchanged;
- same-time values are summed within stay_id/itemid/charttime;
- values exactly at postoperative_time_zero are treated as pre-horizon
  boundary amounts and excluded from postoperative drainage;
- the first accepted point after t0 represents the interval [t0, charttime];
- subsequent points represent [previous charttime, charttime];
- the first eligible point after the 12-h analytical endpoint and within 6 h
  may be used as a right-boundary anchor;
- only the fraction of an interval overlapping the 12-h analytical horizon is
  attributed;
- no LOCF or extrapolation beyond these explicit source rules is introduced.

Observability
-------------
The primary 12-h horizon is considered ICU-observable when observation_end_time
is at or beyond t0 + 12 h.

Source documentation is tracked separately:
- presence of any core chest-drain documentation within 0-12 h;
- interval contribution within each window.

Zero attributed drainage is never used to imply proven physiological absence
of bleeding when source documentation is absent.

Canonical outputs
-----------------
data/01_15_bleeding_hourly_trajectory.parquet
data/01_15_bleeding_six_hour_summary.parquet
data/01_15_bleeding_extraction_audit.csv

This is the frozen production extraction component.

Inputs
------
data/00_04_final_analytic_cohort.csv
Raw MIMIC-IV 3.1 files relative to MIMIC_ROOT:
  icu/d_items.csv.gz
  icu/outputevents.csv.gz

Additional input: data/01_07_urine_output_six_hour_summary.parquet supplies
the frozen baseline weight and its provenance. The relationship to 01_10 is
methodological: drainage is re-extracted here from raw OUTPUTEVENTS, and no
01_10 output dataset is read.
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
URINE_SIX_FILE = (
    PROJECT_ROOT / "data" / "01_07_urine_output_six_hour_summary.parquet"
)

D_ITEMS_FILE = MIMIC_ROOT / "icu" / "d_items.csv.gz"
OUTPUTEVENTS_FILE = MIMIC_ROOT / "icu" / "outputevents.csv.gz"

OUT_HOURLY = (
    PROJECT_ROOT / "data" / "01_15_bleeding_hourly_trajectory.parquet"
)
OUT_SIX = (
    PROJECT_ROOT / "data" / "01_15_bleeding_six_hour_summary.parquet"
)
OUT_AUDIT = (
    PROJECT_ROOT / "data" / "01_15_bleeding_extraction_audit.csv"
)

EXPECTED_COHORT_N = 9305

# Frozen validation targets for the validated drainage component.
EXPECTED_EXACT_T0_BOUNDARY_ML = 34889.0

ANALYTICAL_HOURS = 12
ANCHOR_SEARCH_HOURS = 6

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


def qpath(path: Path) -> str:
    return str(path).replace("'", "''")


def sql_ids(values) -> str:
    return ", ".join(str(int(x)) for x in values)


for required in [
    COHORT_FILE,
    URINE_SIX_FILE,
    D_ITEMS_FILE,
    OUTPUTEVENTS_FILE,
]:
    if not required.exists():
        raise FileNotFoundError(f"Required file not found: {required}")

OUT_HOURLY.parent.mkdir(parents=True, exist_ok=True)

CHEST_SQL = sql_ids(CHEST_DRAIN_ITEMIDS)

con = duckdb.connect(database=":memory:")
con.execute("PRAGMA threads=4")


# =============================================================================
# Frozen cohort
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
            postoperative_time_zero::TIMESTAMP
                + INTERVAL '{ANALYTICAL_HOURS} hours',
            observation_end_time::TIMESTAMP
        ) AS analytical_end_time,

        least(
            postoperative_time_zero::TIMESTAMP
                + INTERVAL '{ANALYTICAL_HOURS + ANCHOR_SEARCH_HOURS} hours',
            observation_end_time::TIMESTAMP
        ) AS anchor_search_end_time,

        CASE
            WHEN observation_end_time::TIMESTAMP
                    >= postoperative_time_zero::TIMESTAMP
                        + INTERVAL '{ANALYTICAL_HOURS} hours'
                THEN 1
            ELSE 0
        END AS bleeding_full_12h_observable_flag

    FROM read_csv_auto(
        '{qpath(COHORT_FILE)}',
        header=true,
        all_varchar=false
    )
    """
)

cohort_n, cohort_unique_n = con.execute(
    "SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM cohort"
).fetchone()

if cohort_n != EXPECTED_COHORT_N or cohort_unique_n != EXPECTED_COHORT_N:
    raise RuntimeError(
        f"Unexpected cohort dimensions: {cohort_n} / {cohort_unique_n}"
    )


# =============================================================================
# Frozen baseline weight reused from 01_07
# =============================================================================

con.execute(
    f"""
    CREATE TABLE urine_weight_source AS
    SELECT *
    FROM read_parquet('{qpath(URINE_SIX_FILE)}')
    """
)

weight_columns = {
    row[0]
    for row in con.execute("DESCRIBE urine_weight_source").fetchall()
}

required_weight_columns = {
    "stay_id",
    "uo_weight_kg",
    "uo_weight_source",
}

missing_weight_columns = sorted(required_weight_columns - weight_columns)
if missing_weight_columns:
    raise RuntimeError(
        "Frozen 01_07 file is missing required weight columns: "
        + ", ".join(missing_weight_columns)
    )

con.execute(
    """
    CREATE TABLE baseline_weight AS
    SELECT
        stay_id,
        max(uo_weight_kg) AS bleeding_weight_kg,
        any_value(uo_weight_source) AS bleeding_weight_source
    FROM urine_weight_source
    GROUP BY stay_id
    """
)

weight_rows, weight_unique = con.execute(
    "SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM baseline_weight"
).fetchone()

if weight_rows != EXPECTED_COHORT_N or weight_unique != EXPECTED_COHORT_N:
    raise RuntimeError(
        f"Unexpected baseline-weight dimensions: {weight_rows} / {weight_unique}"
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

itemid_dictionary_n = con.execute(
    f"""
    SELECT COUNT(DISTINCT itemid)
    FROM d_items
    WHERE itemid IN ({CHEST_SQL})
      AND lower(trim(coalesce(linksto, ''))) = 'outputevents'
    """
).fetchone()[0]

if itemid_dictionary_n != len(CHEST_DRAIN_ITEMIDS):
    raise RuntimeError(
        f"Only {itemid_dictionary_n}/{len(CHEST_DRAIN_ITEMIDS)} frozen "
        "chest-drain itemids were found in d_items as OUTPUTEVENTS items."
    )


# =============================================================================
# Source extraction
# =============================================================================

con.execute(
    f"""
    CREATE TABLE chest_raw AS
    SELECT
        c.subject_id,
        c.hadm_id,
        c.stay_id,
        c.postoperative_time_zero,
        c.analytical_end_time,
        c.anchor_search_end_time,
        c.observation_end_time,
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
       AND oe.charttime <= c.observation_end_time

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
        any_value(label) AS label,
        SUM(value_ml) AS chest_value_ml,
        COUNT(*) AS source_rows_n,
        max(core_horizon_flag) AS core_horizon_flag
    FROM chest_raw
    GROUP BY stay_id, charttime, itemid
    """
)


# =============================================================================
# Core intervals
# =============================================================================

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


# =============================================================================
# Right-boundary anchor
# =============================================================================

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
# Source-documentation flags
# =============================================================================

con.execute(
    """
    CREATE TABLE patient_source_flags AS
    SELECT
        c.stay_id,

        CASE
            WHEN COUNT(p.stay_id) FILTER (
                WHERE p.core_horizon_flag = 1
            ) > 0
                THEN 1
            ELSE 0
        END AS bleeding_any_core_source_documentation_flag,

        COUNT(DISTINCT p.itemid) FILTER (
            WHERE p.core_horizon_flag = 1
        ) AS bleeding_distinct_drain_itemids_12h_n,

        COUNT(p.stay_id) FILTER (
            WHERE p.core_horizon_flag = 1
        ) AS bleeding_core_charted_points_12h_n

    FROM cohort c
    LEFT JOIN chest_points p
        ON c.stay_id = p.stay_id
    GROUP BY c.stay_id
    """
)


# =============================================================================
# Time grids
# =============================================================================

con.execute(
    """
    CREATE TABLE hourly_grid AS
    SELECT
        c.*,
        gs AS window_index,
        'H'
            || lpad(cast(gs AS VARCHAR), 2, '0')
            || '_H'
            || lpad(cast(gs + 1 AS VARCHAR), 2, '0')
            AS window_label,

        c.postoperative_time_zero
            + gs * INTERVAL '1 hour'
            AS window_start_time,

        least(
            c.postoperative_time_zero
                + (gs + 1) * INTERVAL '1 hour',
            c.analytical_end_time
        ) AS eligible_window_end_time,

        greatest(
            0,
            date_diff(
                'second',
                c.postoperative_time_zero
                    + gs * INTERVAL '1 hour',
                least(
                    c.postoperative_time_zero
                        + (gs + 1) * INTERVAL '1 hour',
                    c.analytical_end_time
                )
            ) / 60.0
        ) AS eligible_minutes

    FROM cohort c
    CROSS JOIN generate_series(0, 11) AS t(gs)
    """
)

con.execute(
    """
    CREATE TABLE six_grid AS
    SELECT
        c.*,
        gs AS window_index,

        CASE gs
            WHEN 0 THEN 'H00_H06'
            WHEN 1 THEN 'H06_H12'
        END AS window_label,

        c.postoperative_time_zero
            + (gs * 6) * INTERVAL '1 hour'
            AS window_start_time,

        least(
            c.postoperative_time_zero
                + ((gs + 1) * 6) * INTERVAL '1 hour',
            c.analytical_end_time
        ) AS eligible_window_end_time,

        greatest(
            0,
            date_diff(
                'second',
                c.postoperative_time_zero
                    + (gs * 6) * INTERVAL '1 hour',
                least(
                    c.postoperative_time_zero
                        + ((gs + 1) * 6) * INTERVAL '1 hour',
                    c.analytical_end_time
                )
            ) / 60.0
        ) AS eligible_minutes

    FROM cohort c
    CROSS JOIN generate_series(0, 1) AS t(gs)
    """
)


# =============================================================================
# Window allocation
# =============================================================================

def build_window_trajectory(output_table: str, grid_table: str, full_minutes: int):
    con.execute(
        f"""
        CREATE TABLE {output_table}_base AS
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
            g.analytical_end_time,
            g.bleeding_full_12h_observable_flag,

            g.window_index,
            g.window_label,
            g.window_start_time,
            g.eligible_window_end_time,
            g.eligible_minutes,

            CASE
                WHEN abs(g.eligible_minutes - {float(full_minutes)}) < 0.001
                    THEN 1
                ELSE 0
            END AS bleeding_window_full_observable_flag,

            w.bleeding_weight_kg,
            w.bleeding_weight_source,

            ps.bleeding_any_core_source_documentation_flag,
            ps.bleeding_distinct_drain_itemids_12h_n,
            ps.bleeding_core_charted_points_12h_n,

            coalesce(
                SUM(
                    i.interval_volume_ml
                    * greatest(
                        0,
                        date_diff(
                            'second',
                            greatest(
                                i.interval_start,
                                g.window_start_time
                            ),
                            least(
                                i.interval_end,
                                g.eligible_window_end_time
                            )
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
                ),
                0.0
            ) AS bleeding_recorded_ml,

            coalesce(
                SUM(
                    i.interval_volume_ml
                    * greatest(
                        0,
                        date_diff(
                            'second',
                            greatest(
                                i.interval_start,
                                g.window_start_time
                            ),
                            least(
                                i.interval_end,
                                g.eligible_window_end_time
                            )
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
                ),
                0.0
            ) AS bleeding_recorded_ml_no_anchor_sensitivity,

            coalesce(
                SUM(
                    i.interval_volume_ml
                    * greatest(
                        0,
                        date_diff(
                            'second',
                            greatest(
                                i.interval_start,
                                g.window_start_time
                            ),
                            least(
                                i.interval_end,
                                g.eligible_window_end_time
                            )
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
                ),
                0.0
            ) AS bleeding_anchor_attributed_ml,

            COUNT(i.interval_start) FILTER (
                WHERE i.anchor_flag = 0
            ) AS bleeding_core_intervals_contributing_n,

            COUNT(i.interval_start) FILTER (
                WHERE i.anchor_flag = 1
            ) AS bleeding_anchor_intervals_contributing_n,

            COUNT(DISTINCT i.itemid) AS bleeding_contributing_drain_itemids_n,

            CASE
                WHEN COUNT(i.interval_start) > 0 THEN 1
                ELSE 0
            END AS bleeding_interval_contribution_flag

        FROM {grid_table} g

        INNER JOIN baseline_weight w
            ON g.stay_id = w.stay_id

        INNER JOIN patient_source_flags ps
            ON g.stay_id = ps.stay_id

        LEFT JOIN chest_intervals i
            ON i.stay_id = g.stay_id
           AND g.eligible_minutes > 0
           AND i.interval_end > g.window_start_time
           AND i.interval_start < g.eligible_window_end_time

        GROUP BY
            g.subject_id,
            g.hadm_id,
            g.stay_id,
            g.surgery_group,
            g.sex,
            g.age_at_icu_admission,
            g.postoperative_time_zero,
            g.observation_end_time,
            g.observation_end_type,
            g.analytical_end_time,
            g.bleeding_full_12h_observable_flag,
            g.window_index,
            g.window_label,
            g.window_start_time,
            g.eligible_window_end_time,
            g.eligible_minutes,
            w.bleeding_weight_kg,
            w.bleeding_weight_source,
            ps.bleeding_any_core_source_documentation_flag,
            ps.bleeding_distinct_drain_itemids_12h_n,
            ps.bleeding_core_charted_points_12h_n
        """
    )

    con.execute(
        f"""
        CREATE TABLE {output_table} AS
        SELECT
            b.*,

            CASE
                WHEN b.eligible_minutes <= 0 THEN NULL
                ELSE b.bleeding_recorded_ml
                    / (b.eligible_minutes / 60.0)
            END AS bleeding_recorded_rate_ml_per_h,

            CASE
                WHEN b.bleeding_weight_kg IS NULL
                  OR b.bleeding_weight_kg <= 0
                    THEN NULL
                ELSE b.bleeding_recorded_ml
                    / b.bleeding_weight_kg
            END AS bleeding_recorded_ml_per_kg,

            CASE
                WHEN b.eligible_minutes <= 0
                  OR b.bleeding_weight_kg IS NULL
                  OR b.bleeding_weight_kg <= 0
                    THEN NULL
                ELSE (
                    b.bleeding_recorded_ml
                    / (b.eligible_minutes / 60.0)
                ) / b.bleeding_weight_kg
            END AS bleeding_recorded_rate_ml_per_kg_h,

            SUM(b.bleeding_recorded_ml) OVER (
                PARTITION BY b.stay_id
                ORDER BY b.window_index
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS bleeding_cumulative_recorded_ml,

            CASE
                WHEN b.bleeding_weight_kg IS NULL
                  OR b.bleeding_weight_kg <= 0
                    THEN NULL
                ELSE SUM(b.bleeding_recorded_ml) OVER (
                    PARTITION BY b.stay_id
                    ORDER BY b.window_index
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) / b.bleeding_weight_kg
            END AS bleeding_cumulative_recorded_ml_per_kg,

            CASE
                WHEN b.eligible_minutes <= 0
                    THEN 'NOT_OBSERVABLE'
                WHEN b.bleeding_any_core_source_documentation_flag = 0
                    THEN 'NO_CHEST_DRAIN_SOURCE_DOCUMENTATION'
                WHEN b.bleeding_interval_contribution_flag = 0
                    THEN 'NO_ATTRIBUTED_DRAIN_INTERVAL'
                WHEN b.bleeding_window_full_observable_flag = 0
                    THEN 'PARTIAL_ICU_OBSERVATION'
                ELSE 'RECORDED'
            END AS bleeding_measurement_status

        FROM {output_table}_base b
        """
    )


build_window_trajectory("hourly_output", "hourly_grid", 60)
build_window_trajectory("six_output", "six_grid", 360)


# =============================================================================
# Patient-level early-bleeding features
# =============================================================================

con.execute(
    """
    CREATE TABLE patient_summary AS
    SELECT
        stay_id,

        max(bleeding_full_12h_observable_flag)
            AS bleeding_full_12h_observable_flag,

        max(bleeding_any_core_source_documentation_flag)
            AS bleeding_any_core_source_documentation_flag,

        max(bleeding_distinct_drain_itemids_12h_n)
            AS bleeding_distinct_drain_itemids_12h_n,

        max(bleeding_core_charted_points_12h_n)
            AS bleeding_core_charted_points_12h_n,

        max(bleeding_weight_kg)
            AS bleeding_weight_kg,

        any_value(bleeding_weight_source)
            AS bleeding_weight_source,

        SUM(bleeding_recorded_ml)
            FILTER (WHERE window_index = 0)
            AS bleeding_recorded_ml_0_6h,

        SUM(bleeding_recorded_ml)
            AS bleeding_recorded_ml_0_12h,

        SUM(bleeding_recorded_ml_no_anchor_sensitivity)
            AS bleeding_recorded_ml_0_12h_no_anchor_sensitivity,

        SUM(bleeding_anchor_attributed_ml)
            AS bleeding_anchor_attributed_ml_0_12h,

        CASE
            WHEN max(bleeding_weight_kg) IS NOT NULL
             AND max(bleeding_weight_kg) > 0
                THEN (
                    SUM(bleeding_recorded_ml)
                        FILTER (WHERE window_index = 0)
                ) / max(bleeding_weight_kg)
            ELSE NULL
        END AS bleeding_recorded_ml_per_kg_0_6h,

        CASE
            WHEN max(bleeding_weight_kg) IS NOT NULL
             AND max(bleeding_weight_kg) > 0
                THEN SUM(bleeding_recorded_ml)
                    / max(bleeding_weight_kg)
            ELSE NULL
        END AS bleeding_recorded_ml_per_kg_0_12h

    FROM six_output
    GROUP BY stay_id
    """
)

con.execute(
    """
    CREATE TABLE hourly_patient_features AS
    SELECT
        stay_id,

        max(bleeding_recorded_ml)
            FILTER (WHERE bleeding_window_full_observable_flag = 1)
            AS bleeding_max_hourly_recorded_ml_0_12h,

        max(bleeding_recorded_rate_ml_per_h)
            FILTER (WHERE bleeding_window_full_observable_flag = 1)
            AS bleeding_max_hourly_recorded_rate_ml_per_h_0_12h,

        max(bleeding_recorded_rate_ml_per_kg_h)
            FILTER (WHERE bleeding_window_full_observable_flag = 1)
            AS bleeding_max_hourly_recorded_rate_ml_per_kg_h_0_12h,

        COUNT(*) FILTER (
            WHERE bleeding_window_full_observable_flag = 1
              AND bleeding_recorded_ml >= 200
        ) AS bleeding_hours_ge_200ml_0_12h_n,

        max(
            CASE
                WHEN bleeding_window_full_observable_flag = 1
                 AND bleeding_recorded_ml >= 200
                    THEN 1
                ELSE 0
            END
        ) AS bleeding_any_hour_ge_200ml_0_12h_flag

    FROM hourly_output
    GROUP BY stay_id
    """
)

con.execute(
    """
    CREATE TABLE patient_features AS
    SELECT
        p.*,
        h.* EXCLUDE (stay_id),

        p.bleeding_recorded_ml_0_12h
            - p.bleeding_recorded_ml_0_12h_no_anchor_sensitivity
            AS bleeding_anchor_effect_ml_0_12h,

        CASE
            WHEN p.bleeding_recorded_ml_0_12h_no_anchor_sensitivity > 0
                THEN (
                    p.bleeding_recorded_ml_0_12h
                    - p.bleeding_recorded_ml_0_12h_no_anchor_sensitivity
                )
                / p.bleeding_recorded_ml_0_12h_no_anchor_sensitivity
            ELSE NULL
        END AS bleeding_anchor_effect_relative_0_12h,

        CASE
            WHEN p.bleeding_full_12h_observable_flag = 1
             AND p.bleeding_any_core_source_documentation_flag = 1
                THEN 1
            ELSE 0
        END AS bleeding_primary_12h_available_flag,

        CASE
            WHEN p.bleeding_full_12h_observable_flag = 0
                THEN 'INCOMPLETE_12H_ICU_OBSERVATION'
            WHEN p.bleeding_any_core_source_documentation_flag = 0
                THEN 'NO_CHEST_DRAIN_SOURCE_DOCUMENTATION'
            ELSE 'AVAILABLE'
        END AS bleeding_primary_12h_status

    FROM patient_summary p
    INNER JOIN hourly_patient_features h
        ON p.stay_id = h.stay_id
    """
)

con.execute(
    """
    CREATE TABLE hourly_final AS
    SELECT
        h.*,
        p.* EXCLUDE (
            stay_id,
            bleeding_full_12h_observable_flag,
            bleeding_any_core_source_documentation_flag,
            bleeding_distinct_drain_itemids_12h_n,
            bleeding_core_charted_points_12h_n,
            bleeding_weight_kg,
            bleeding_weight_source
        )
    FROM hourly_output h
    INNER JOIN patient_features p
        ON h.stay_id = p.stay_id
    """
)

con.execute(
    """
    CREATE TABLE six_final AS
    SELECT
        s.*,
        p.* EXCLUDE (
            stay_id,
            bleeding_full_12h_observable_flag,
            bleeding_any_core_source_documentation_flag,
            bleeding_distinct_drain_itemids_12h_n,
            bleeding_core_charted_points_12h_n,
            bleeding_weight_kg,
            bleeding_weight_source
        )
    FROM six_output s
    INNER JOIN patient_features p
        ON s.stay_id = p.stay_id
    """
)


# =============================================================================
# Validation invariants
# =============================================================================

hourly_n, hourly_unique = con.execute(
    "SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM hourly_final"
).fetchone()

six_n, six_unique = con.execute(
    "SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM six_final"
).fetchone()

if hourly_n != EXPECTED_COHORT_N * 12 or hourly_unique != EXPECTED_COHORT_N:
    raise RuntimeError(
        f"Unexpected hourly dimensions: {hourly_n} / {hourly_unique}"
    )

if six_n != EXPECTED_COHORT_N * 2 or six_unique != EXPECTED_COHORT_N:
    raise RuntimeError(
        f"Unexpected six-hour dimensions: {six_n} / {six_unique}"
    )

invalid_intervals_n = con.execute(
    """
    SELECT COUNT(*)
    FROM chest_intervals
    WHERE interval_end <= interval_start
       OR interval_volume_ml < 0
    """
).fetchone()[0]

if invalid_intervals_n:
    raise RuntimeError(
        f"{invalid_intervals_n} invalid chest-drain intervals detected."
    )

exact_t0_boundary_ml = con.execute(
    "SELECT SUM(exact_t0_boundary_ml) FROM chest_core_intervals"
).fetchone()[0] or 0.0

if abs(exact_t0_boundary_ml - EXPECTED_EXACT_T0_BOUNDARY_ML) > 0.05:
    raise RuntimeError(
        "Exact-t0 chest boundary volume differs from frozen 01_10 / v1: "
        f"{exact_t0_boundary_ml:.3f} vs "
        f"{EXPECTED_EXACT_T0_BOUNDARY_ML:.3f} mL"
    )

hourly_total_ml = con.execute(
    "SELECT SUM(bleeding_recorded_ml) FROM hourly_final"
).fetchone()[0] or 0.0

six_total_ml = con.execute(
    "SELECT SUM(bleeding_recorded_ml) FROM six_final"
).fetchone()[0] or 0.0

if abs(hourly_total_ml - six_total_ml) > 0.05:
    raise RuntimeError(
        "Bleeding volume differs between hourly and six-hour grids: "
        f"{hourly_total_ml:.3f} vs {six_total_ml:.3f} mL"
    )

positive_nonobservable_n = con.execute(
    """
    SELECT COUNT(*)
    FROM hourly_final
    WHERE eligible_minutes <= 0
      AND bleeding_recorded_ml > 0
    """
).fetchone()[0]

if positive_nonobservable_n:
    raise RuntimeError(
        f"{positive_nonobservable_n} non-observable hourly windows "
        "contain positive attributed bleeding."
    )

cumulative_grid_mismatch_n = con.execute(
    """
    WITH h AS (
        SELECT
            stay_id,
            max_by(
                bleeding_cumulative_recorded_ml,
                window_index
            ) AS final_hourly_cumulative_ml
        FROM hourly_final
        GROUP BY stay_id
    ),
    s AS (
        SELECT
            stay_id,
            max_by(
                bleeding_cumulative_recorded_ml,
                window_index
            ) AS final_six_cumulative_ml
        FROM six_final
        GROUP BY stay_id
    )
    SELECT COUNT(*)
    FROM h
    INNER JOIN s USING (stay_id)
    WHERE abs(
        final_hourly_cumulative_ml
        - final_six_cumulative_ml
    ) > 0.01
    """
).fetchone()[0]

if cumulative_grid_mismatch_n:
    raise RuntimeError(
        f"{cumulative_grid_mismatch_n} stays have different final cumulative "
        "bleeding between hourly and six-hour grids."
    )


# Primary-vs-no-anchor arithmetic identity.
anchor_identity_mismatch_n = con.execute(
    """
    SELECT COUNT(*)
    FROM patient_features
    WHERE abs(
        bleeding_recorded_ml_0_12h
        - bleeding_recorded_ml_0_12h_no_anchor_sensitivity
        - bleeding_anchor_attributed_ml_0_12h
    ) > 0.01
    """
).fetchone()[0]

if anchor_identity_mismatch_n:
    raise RuntimeError(
        f"{anchor_identity_mismatch_n} stays violate the anchor sensitivity "
        "arithmetic identity."
    )

# For fully observable patients with source documentation, all 12 hourly windows
# should usually be represented through interval allocation. We do not block on
# this because a true documentation gap can exist, but we audit it explicitly.
fully_observed_no_interval_windows_n = con.execute(
    """
    SELECT COUNT(*)
    FROM hourly_final
    WHERE bleeding_primary_12h_available_flag = 1
      AND bleeding_interval_contribution_flag = 0
    """
).fetchone()[0]


# =============================================================================
# Audit
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

core_points_n = con.execute(
    """
    SELECT COUNT(*)
    FROM chest_points
    WHERE core_horizon_flag = 1
    """
).fetchone()[0]

core_source_rows_n = con.execute(
    """
    SELECT SUM(source_rows_n)
    FROM chest_points
    WHERE core_horizon_flag = 1
    """
).fetchone()[0] or 0

core_patients_n = con.execute(
    """
    SELECT COUNT(*)
    FROM patient_source_flags
    WHERE bleeding_any_core_source_documentation_flag = 1
    """
).fetchone()[0]

no_core_patients_n = EXPECTED_COHORT_N - core_patients_n

anchor_intervals_n = con.execute(
    "SELECT COUNT(*) FROM chest_anchor_intervals"
).fetchone()[0]

anchor_patients_n = con.execute(
    "SELECT COUNT(DISTINCT stay_id) FROM chest_anchor_intervals"
).fetchone()[0]

anchor_attributed_total_ml = con.execute(
    "SELECT SUM(bleeding_anchor_attributed_ml) FROM six_final"
).fetchone()[0] or 0.0

full_12h_observable_n = con.execute(
    """
    SELECT COUNT(*)
    FROM cohort
    WHERE bleeding_full_12h_observable_flag = 1
    """
).fetchone()[0]

weight_available_n = con.execute(
    """
    SELECT COUNT(*)
    FROM baseline_weight
    WHERE bleeding_weight_kg IS NOT NULL
      AND bleeding_weight_kg > 0
    """
).fetchone()[0]

primary_available_n = con.execute(
    """
    SELECT COUNT(*)
    FROM patient_features
    WHERE bleeding_primary_12h_available_flag = 1
    """
).fetchone()[0]

con.executemany(
    "INSERT INTO audit VALUES (?, ?, ?, ?, ?)",
    [
        ("OVERALL", None, "cohort_n", cohort_n, None),
        (
            "DESIGN",
            "PRIMARY",
            "analytical_horizon_hours",
            ANALYTICAL_HOURS,
            "Early postoperative bleeding trajectory",
        ),
        (
            "DESIGN",
            "PRIMARY",
            "hourly_windows_n",
            12,
            None,
        ),
        (
            "DESIGN",
            "SECONDARY",
            "six_hour_windows_n",
            2,
            None,
        ),
        (
            "OBSERVABILITY",
            "ICU_HORIZON",
            "full_12h_observable_n",
            full_12h_observable_n,
            None,
        ),
        (
            "OBSERVABILITY",
            "PRIMARY_AVAILABLE",
            "patients_n",
            primary_available_n,
            "Full 12h ICU observation + chest-drain source documentation",
        ),
        (
            "OBSERVABILITY",
            "WEIGHT",
            "baseline_weight_available_n",
            weight_available_n,
            "Inherited from frozen 01_07",
        ),
        (
            "OBSERVABILITY",
            "HOURLY",
            "fully_observed_windows_without_interval_contribution_n",
            fully_observed_no_interval_windows_n,
            "Audited, not automatically imputed",
        ),
        (
            "SOURCE",
            "CHEST_DRAINS",
            "core_source_rows_n",
            core_source_rows_n,
            None,
        ),
        (
            "SOURCE",
            "CHEST_DRAINS",
            "core_consolidated_points_n",
            core_points_n,
            None,
        ),
        (
            "SOURCE",
            "CHEST_DRAINS",
            "patients_with_core_documentation_n",
            core_patients_n,
            None,
        ),
        (
            "SOURCE",
            "CHEST_DRAINS",
            "patients_without_core_documentation_n",
            no_core_patients_n,
            None,
        ),
        (
            "BOUNDARY",
            "LEFT",
            "exact_t0_boundary_ml",
            exact_t0_boundary_ml,
            "Excluded as pre-horizon boundary amount",
        ),
        (
            "BOUNDARY",
            "RIGHT_ANCHOR",
            "anchor_intervals_n",
            anchor_intervals_n,
            None,
        ),
        (
            "BOUNDARY",
            "RIGHT_ANCHOR",
            "patients_n",
            anchor_patients_n,
            None,
        ),
        (
            "BOUNDARY",
            "RIGHT_ANCHOR",
            "attributed_ml",
            anchor_attributed_total_ml,
            None,
        ),
        (
            "VOLUME",
            "0_12H",
            "hourly_total_ml",
            hourly_total_ml,
            None,
        ),
        (
            "VOLUME",
            "0_12H",
            "six_hour_total_ml",
            six_total_ml,
            "Must equal hourly total",
        ),
    ],
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'ITEMID',
        cast(p.itemid AS VARCHAR),
        'core_documentation',
        COUNT(*)::DOUBLE,
        'label=' || any_value(p.label)
            || '; patients='
            || cast(COUNT(DISTINCT p.stay_id) AS VARCHAR)
            || '; volume_ml='
            || cast(round(SUM(p.chest_value_ml), 1) AS VARCHAR)
    FROM chest_points p
    WHERE p.core_horizon_flag = 1
    GROUP BY p.itemid
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'HOURLY',
        window_label,
        metric,
        value_num,
        NULL
    FROM (
        SELECT
            window_label,
            'full_observable_n' AS metric,
            COUNT(*) FILTER (
                WHERE bleeding_window_full_observable_flag = 1
            )::DOUBLE AS value_num
        FROM hourly_final
        GROUP BY window_index, window_label

        UNION ALL

        SELECT
            window_label,
            'interval_contribution_n',
            COUNT(*) FILTER (
                WHERE bleeding_interval_contribution_flag = 1
            )::DOUBLE
        FROM hourly_final
        GROUP BY window_index, window_label

        UNION ALL

        SELECT
            window_label,
            'median_recorded_ml',
            median(bleeding_recorded_ml)::DOUBLE
        FROM hourly_final
        WHERE bleeding_window_full_observable_flag = 1
          AND bleeding_any_core_source_documentation_flag = 1
        GROUP BY window_index, window_label

        UNION ALL

        SELECT
            window_label,
            'p90_recorded_ml',
            quantile_cont(bleeding_recorded_ml, 0.90)::DOUBLE
        FROM hourly_final
        WHERE bleeding_window_full_observable_flag = 1
          AND bleeding_any_core_source_documentation_flag = 1
        GROUP BY window_index, window_label

        UNION ALL

        SELECT
            window_label,
            'p99_recorded_ml',
            quantile_cont(bleeding_recorded_ml, 0.99)::DOUBLE
        FROM hourly_final
        WHERE bleeding_window_full_observable_flag = 1
          AND bleeding_any_core_source_documentation_flag = 1
        GROUP BY window_index, window_label
    )
    """
)

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
            'full_observable_n' AS metric,
            COUNT(*) FILTER (
                WHERE bleeding_window_full_observable_flag = 1
            )::DOUBLE AS value_num
        FROM six_final
        GROUP BY window_index, window_label

        UNION ALL

        SELECT
            window_label,
            'median_recorded_ml',
            median(bleeding_recorded_ml)::DOUBLE
        FROM six_final
        WHERE bleeding_window_full_observable_flag = 1
          AND bleeding_any_core_source_documentation_flag = 1
        GROUP BY window_index, window_label

        UNION ALL

        SELECT
            window_label,
            'median_cumulative_recorded_ml',
            median(bleeding_cumulative_recorded_ml)::DOUBLE
        FROM six_final
        WHERE bleeding_window_full_observable_flag = 1
          AND bleeding_any_core_source_documentation_flag = 1
        GROUP BY window_index, window_label
    )
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'PATIENT',
        subgroup,
        metric,
        value_num,
        NULL
    FROM (
        SELECT
            'PRIMARY_12H_AVAILABLE' AS subgroup,
            'patients_n' AS metric,
            COUNT(*)::DOUBLE AS value_num
        FROM patient_features
        WHERE bleeding_primary_12h_available_flag = 1

        UNION ALL

        SELECT
            'PRIMARY_12H_AVAILABLE',
            'median_6h_ml',
            median(bleeding_recorded_ml_0_6h)::DOUBLE
        FROM patient_features
        WHERE bleeding_primary_12h_available_flag = 1

        UNION ALL

        SELECT
            'PRIMARY_12H_AVAILABLE',
            'p90_6h_ml',
            quantile_cont(bleeding_recorded_ml_0_6h, 0.90)::DOUBLE
        FROM patient_features
        WHERE bleeding_primary_12h_available_flag = 1

        UNION ALL

        SELECT
            'PRIMARY_12H_AVAILABLE',
            'median_12h_ml',
            median(bleeding_recorded_ml_0_12h)::DOUBLE
        FROM patient_features
        WHERE bleeding_primary_12h_available_flag = 1

        UNION ALL

        SELECT
            'PRIMARY_12H_AVAILABLE',
            'p90_12h_ml',
            quantile_cont(bleeding_recorded_ml_0_12h, 0.90)::DOUBLE
        FROM patient_features
        WHERE bleeding_primary_12h_available_flag = 1

        UNION ALL

        SELECT
            'PRIMARY_12H_AVAILABLE',
            'median_max_hourly_ml',
            median(bleeding_max_hourly_recorded_ml_0_12h)::DOUBLE
        FROM patient_features
        WHERE bleeding_primary_12h_available_flag = 1

        UNION ALL

        SELECT
            'PRIMARY_12H_AVAILABLE',
            'patients_with_any_hour_ge200ml_n',
            COUNT(*) FILTER (
                WHERE bleeding_any_hour_ge_200ml_0_12h_flag = 1
            )::DOUBLE
        FROM patient_features
        WHERE bleeding_primary_12h_available_flag = 1
    )
    """
)


# Right-boundary anchor sensitivity at 12 hours.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'ANCHOR_SENSITIVITY',
        'PRIMARY_12H_AVAILABLE',
        metric,
        value_num,
        NULL
    FROM (
        SELECT
            'median_with_anchor_ml' AS metric,
            median(bleeding_recorded_ml_0_12h)::DOUBLE AS value_num
        FROM patient_features
        WHERE bleeding_primary_12h_available_flag = 1

        UNION ALL

        SELECT
            'median_without_anchor_ml',
            median(bleeding_recorded_ml_0_12h_no_anchor_sensitivity)::DOUBLE
        FROM patient_features
        WHERE bleeding_primary_12h_available_flag = 1

        UNION ALL

        SELECT
            'median_anchor_effect_ml',
            median(bleeding_anchor_effect_ml_0_12h)::DOUBLE
        FROM patient_features
        WHERE bleeding_primary_12h_available_flag = 1

        UNION ALL

        SELECT
            'p90_with_anchor_ml',
            quantile_cont(bleeding_recorded_ml_0_12h, 0.90)::DOUBLE
        FROM patient_features
        WHERE bleeding_primary_12h_available_flag = 1

        UNION ALL

        SELECT
            'p90_without_anchor_ml',
            quantile_cont(
                bleeding_recorded_ml_0_12h_no_anchor_sensitivity,
                0.90
            )::DOUBLE
        FROM patient_features
        WHERE bleeding_primary_12h_available_flag = 1

        UNION ALL

        SELECT
            'p90_anchor_effect_ml',
            quantile_cont(bleeding_anchor_effect_ml_0_12h, 0.90)::DOUBLE
        FROM patient_features
        WHERE bleeding_primary_12h_available_flag = 1

        UNION ALL

        SELECT
            'patients_anchor_effect_gt0_n',
            COUNT(*) FILTER (
                WHERE bleeding_anchor_effect_ml_0_12h > 0.01
            )::DOUBLE
        FROM patient_features
        WHERE bleeding_primary_12h_available_flag = 1

        UNION ALL

        SELECT
            'patients_anchor_effect_ge50ml_n',
            COUNT(*) FILTER (
                WHERE bleeding_anchor_effect_ml_0_12h >= 50
            )::DOUBLE
        FROM patient_features
        WHERE bleeding_primary_12h_available_flag = 1

        UNION ALL

        SELECT
            'patients_anchor_effect_ge100ml_n',
            COUNT(*) FILTER (
                WHERE bleeding_anchor_effect_ml_0_12h >= 100
            )::DOUBLE
        FROM patient_features
        WHERE bleeding_primary_12h_available_flag = 1
    )
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'PATIENT_STATUS',
        bleeding_primary_12h_status,
        'patients_n',
        COUNT(*)::DOUBLE,
        NULL
    FROM patient_features
    GROUP BY bleeding_primary_12h_status
    """
)


# =============================================================================
# Export
# =============================================================================

con.execute(
    f"""
    COPY (
        SELECT *
        FROM hourly_final
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
        FROM six_final
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
                WHEN 'DESIGN' THEN 2
                WHEN 'OBSERVABILITY' THEN 3
                WHEN 'SOURCE' THEN 4
                WHEN 'ITEMID' THEN 5
                WHEN 'BOUNDARY' THEN 6
                WHEN 'VOLUME' THEN 7
                WHEN 'HOURLY' THEN 8
                WHEN 'SIX_HOUR' THEN 9
                WHEN 'PATIENT' THEN 10
                WHEN 'ANCHOR_SENSITIVITY' THEN 11
                WHEN 'PATIENT_STATUS' THEN 12
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
print("TRAJECTORIES - EARLY BLEEDING TRAJECTORY v2")
print("CHEST / PLEURAL / MEDIASTINAL DRAINAGE - PRIMARY HORIZON 0-12 HOURS")
print("=" * 118)
print(f"Cohort rows / unique stays              : {cohort_n} / {cohort_unique_n}")
print(f"Full 12-h ICU observation               : {full_12h_observable_n}")
print(f"Primary 12-h bleeding available         : {primary_available_n}")
print(f"Baseline weight available               : {weight_available_n}")
print(f"Patients with core chest documentation  : {core_patients_n}")
print(f"Patients without core documentation     : {no_core_patients_n}")
print()
print("Boundary validation")
print("-------------------")
print(f"Exact-t0 boundary excluded               : {exact_t0_boundary_ml:.1f} mL")
print(f"Right-boundary anchor intervals          : {anchor_intervals_n}")
print(f"Patients with right-boundary anchor      : {anchor_patients_n}")
print(f"Anchor-attributed volume                 : {anchor_attributed_total_ml:.1f} mL")
print()
print("Volume conservation 0-12 h")
print("--------------------------")
print(f"Hourly total                             : {hourly_total_ml:.1f} mL")
print(f"Six-hour total                           : {six_total_ml:.1f} mL")
print()
print("Hourly trajectory 0-12 h")
print("------------------------")
print(
    con.execute(
        """
        SELECT
            window_label,
            COUNT(*) FILTER (
                WHERE bleeding_window_full_observable_flag = 1
            ) AS full_observable_n,
            COUNT(*) FILTER (
                WHERE bleeding_interval_contribution_flag = 1
            ) AS interval_contribution_n,
            round(
                median(bleeding_recorded_ml) FILTER (
                    WHERE bleeding_window_full_observable_flag = 1
                      AND bleeding_any_core_source_documentation_flag = 1
                ),
                1
            ) AS median_ml,
            round(
                quantile_cont(bleeding_recorded_ml, 0.90) FILTER (
                    WHERE bleeding_window_full_observable_flag = 1
                      AND bleeding_any_core_source_documentation_flag = 1
                ),
                1
            ) AS p90_ml,
            round(
                quantile_cont(bleeding_recorded_ml, 0.99) FILTER (
                    WHERE bleeding_window_full_observable_flag = 1
                      AND bleeding_any_core_source_documentation_flag = 1
                ),
                1
            ) AS p99_ml
        FROM hourly_final
        GROUP BY window_index, window_label
        ORDER BY window_index
        """
    ).fetchdf().to_string(index=False)
)
print()
print("Six-hour trajectory")
print("-------------------")
print(
    con.execute(
        """
        SELECT
            window_label,
            COUNT(*) FILTER (
                WHERE bleeding_window_full_observable_flag = 1
            ) AS full_observable_n,
            round(
                median(bleeding_recorded_ml) FILTER (
                    WHERE bleeding_window_full_observable_flag = 1
                      AND bleeding_any_core_source_documentation_flag = 1
                ),
                1
            ) AS median_ml,
            round(
                median(bleeding_cumulative_recorded_ml) FILTER (
                    WHERE bleeding_window_full_observable_flag = 1
                      AND bleeding_any_core_source_documentation_flag = 1
                ),
                1
            ) AS median_cumulative_ml
        FROM six_final
        GROUP BY window_index, window_label
        ORDER BY window_index
        """
    ).fetchdf().to_string(index=False)
)
print()
print("Patient-level primary 12-h summaries")
print("------------------------------------")
print(
    con.execute(
        """
        SELECT
            COUNT(*) AS patients_n,
            round(median(bleeding_recorded_ml_0_6h), 1) AS median_6h_ml,
            round(
                quantile_cont(bleeding_recorded_ml_0_6h, 0.90),
                1
            ) AS p90_6h_ml,
            round(median(bleeding_recorded_ml_0_12h), 1) AS median_12h_ml,
            round(
                quantile_cont(bleeding_recorded_ml_0_12h, 0.90),
                1
            ) AS p90_12h_ml,
            round(
                median(bleeding_max_hourly_recorded_ml_0_12h),
                1
            ) AS median_patient_max_hourly_ml,
            COUNT(*) FILTER (
                WHERE bleeding_any_hour_ge_200ml_0_12h_flag = 1
            ) AS patients_any_hour_ge200ml_n
        FROM patient_features
        WHERE bleeding_primary_12h_available_flag = 1
        """
    ).fetchdf().to_string(index=False)
)
print()

print("12-h right-boundary anchor sensitivity")
print("--------------------------------------")
print(
    con.execute(
        """
        SELECT
            COUNT(*) AS patients_n,
            round(
                median(bleeding_recorded_ml_0_12h),
                1
            ) AS median_with_anchor_ml,
            round(
                median(bleeding_recorded_ml_0_12h_no_anchor_sensitivity),
                1
            ) AS median_without_anchor_ml,
            round(
                median(bleeding_anchor_effect_ml_0_12h),
                1
            ) AS median_anchor_effect_ml,
            round(
                quantile_cont(bleeding_recorded_ml_0_12h, 0.90),
                1
            ) AS p90_with_anchor_ml,
            round(
                quantile_cont(
                    bleeding_recorded_ml_0_12h_no_anchor_sensitivity,
                    0.90
                ),
                1
            ) AS p90_without_anchor_ml,
            COUNT(*) FILTER (
                WHERE bleeding_anchor_effect_ml_0_12h >= 50
            ) AS anchor_effect_ge50ml_n,
            COUNT(*) FILTER (
                WHERE bleeding_anchor_effect_ml_0_12h >= 100
            ) AS anchor_effect_ge100ml_n
        FROM patient_features
        WHERE bleeding_primary_12h_available_flag = 1
        """
    ).fetchdf().to_string(index=False)
)
print()
print("Primary 12-h measurement status")
print("-------------------------------")
print(
    con.execute(
        """
        SELECT
            bleeding_primary_12h_status,
            COUNT(*) AS patients_n
        FROM patient_features
        GROUP BY bleeding_primary_12h_status
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
print("Early bleeding trajectory v3 production extraction complete.")
print("No 12-24 h drainage is included in the canonical outputs.")
print("Transfusions and surgical revision remain separate outcomes.")
