"""
01_13_extract_pf.py

Trajectories project
Canonical PaO2/FiO2 trajectory component
Status: frozen production component

PURPOSE
-------
Construct reproducible postoperative PaO2/FiO2 trajectory features during
invasive mechanical ventilation using the frozen upstream components:

    data/01_12_fio2_pao2_matches.parquet
    data/01_11_respiratory_support_hourly.parquet
    data/01_11_respiratory_support_six_hour.parquet

The component does not re-extract PaO2, FiO2, or respiratory-support events.

PRIMARY REPRESENTATION
----------------------
Four 6-hour windows across the first 24 postoperative ICU hours:

    H00_H06
    H06_H12
    H12_H18
    H18_H24

Primary trajectory value:
    median invasive P/F within each window

Secondary severity value:
    minimum invasive P/F within each window

Additional descriptive values:
    first P/F
    last P/F
    maximum P/F
    event count
    first-to-last change
    FiO2 source counts
    median previous-FiO2 temporal gap
    invasive minutes
    invasive fraction
    invasive eligibility class
    explicit observability and missingness flags

SECONDARY REPRESENTATION
------------------------
Twenty-four 1-hour windows are retained as a granular secondary output.

TEMPORAL IMPUTATION
-------------------
No LOCF.
No interpolation.
No backward extrapolation.
No forward extrapolation.

WINDOW ELIGIBILITY
------------------
Each window is classified as one of:

    FULL_INVASIVE
    PARTIAL_INVASIVE
    NOT_INVASIVE
    UNKNOWN_RESPIRATORY
    NOT_OBSERVABLE

The primary invasive P/F trajectory is eligible only when invasive ventilation
minutes are greater than zero.

Missingness is separated into:

    invasive_pf_observed_flag
    invasive_pf_measurement_missing_flag
    structurally_ineligible_flag
    invasive_eligibility_unknown_flag

CANONICAL OUTPUTS
-----------------
data/01_13_pf_hourly.parquet
data/01_13_pf_six_hour.parquet
data/01_13_pf_audit.csv

The input list above also requires data/00_04_final_analytic_cohort.csv.
"""

from pathlib import Path
import duckdb

# =============================================================================
# Paths and constants
# =============================================================================

# Project directory containing the Scripts and data directories.
PROJECT_ROOT = Path(__file__).resolve().parent.parent

COHORT_FILE = PROJECT_ROOT / "data" / "00_04_final_analytic_cohort.csv"
PF_MATCH_FILE = PROJECT_ROOT / "data" / "01_12_fio2_pao2_matches.parquet"

RESP_HOURLY_FILE = (
    PROJECT_ROOT / "data" / "01_11_respiratory_support_hourly.parquet"
)
RESP_SIX_HOUR_FILE = (
    PROJECT_ROOT / "data" / "01_11_respiratory_support_six_hour.parquet"
)

OUT_HOURLY = PROJECT_ROOT / "data" / "01_13_pf_hourly.parquet"
OUT_SIX_HOUR = PROJECT_ROOT / "data" / "01_13_pf_six_hour.parquet"
OUT_AUDIT = PROJECT_ROOT / "data" / "01_13_pf_audit.csv"

EXPECTED_COHORT_N = 9305
PRIMARY_RESPIRATORY_STATE = "INVASIVE_VENT"
FULL_INVASIVE_TOLERANCE = 0.999


# =============================================================================
# Helpers
# =============================================================================

def qpath(path: Path) -> str:
    """Escape a local path for insertion into a DuckDB SQL string."""
    return str(path).replace("'", "''")


def get_columns(connection, table_name: str) -> set[str]:
    """Return lower-case column names for a DuckDB table."""
    return {
        row[0].lower()
        for row in connection.execute(f"DESCRIBE {table_name}").fetchall()
    }


def choose_column(
    columns: set[str],
    candidates: tuple[str, ...],
    label: str,
) -> str:
    """Select the first available column from an ordered candidate list."""
    for candidate in candidates:
        candidate_lower = candidate.lower()
        if candidate_lower in columns:
            return candidate_lower

    raise RuntimeError(
        f"Unable to identify {label}. "
        f"Expected one of: {', '.join(candidates)}. "
        f"Available columns: {', '.join(sorted(columns))}"
    )


for required_path in (
    COHORT_FILE,
    PF_MATCH_FILE,
    RESP_HOURLY_FILE,
    RESP_SIX_HOUR_FILE,
):
    if not required_path.exists():
        raise FileNotFoundError(required_path)

OUT_HOURLY.parent.mkdir(parents=True, exist_ok=True)

con = duckdb.connect(":memory:")
con.execute("PRAGMA threads=4")
con.execute("PRAGMA preserve_insertion_order=false")


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
        postoperative_time_zero::TIMESTAMP AS t0,
        observation_end_time::TIMESTAMP AS observation_end_time,
        least(
            postoperative_time_zero::TIMESTAMP + INTERVAL '24 hours',
            observation_end_time::TIMESTAMP
        ) AS analytical_end_time
    FROM read_csv_auto('{qpath(COHORT_FILE)}', header=true)
    """
)

cohort_check = con.execute(
    """
    SELECT
        COUNT(*) AS rows_n,
        COUNT(DISTINCT stay_id) AS stays_n,
        COUNT(*) FILTER (WHERE t0 IS NULL) AS missing_t0_n,
        COUNT(*) FILTER (
            WHERE analytical_end_time IS NULL
               OR analytical_end_time < t0
        ) AS invalid_end_n
    FROM cohort
    """
).fetchone()

if cohort_check[:2] != (EXPECTED_COHORT_N, EXPECTED_COHORT_N):
    raise RuntimeError(f"Unexpected cohort dimensions: {cohort_check[:2]}.")

if cohort_check[2] or cohort_check[3]:
    raise RuntimeError(f"Invalid cohort times: {cohort_check[2:]}.")


# =============================================================================
# Canonical upstream inputs
# =============================================================================

con.execute(
    f"""
    CREATE TABLE pf_input AS
    SELECT *
    FROM read_parquet('{qpath(PF_MATCH_FILE)}')
    """
)

con.execute(
    f"""
    CREATE TABLE respiratory_hourly_raw AS
    SELECT *
    FROM read_parquet('{qpath(RESP_HOURLY_FILE)}')
    """
)

con.execute(
    f"""
    CREATE TABLE respiratory_six_hour_raw AS
    SELECT *
    FROM read_parquet('{qpath(RESP_SIX_HOUR_FILE)}')
    """
)


# =============================================================================
# Validate P/F input schema
# =============================================================================

required_pf_columns = {
    "stay_id",
    "subject_id",
    "hadm_id",
    "pao2_charttime",
    "pao2_mmhg",
    "respiratory_state",
    "selected_fio2_fraction",
    "selected_fio2_source",
    "selected_fio2_minutes_before_pao2",
    "pf_ratio",
}

pf_columns = get_columns(con, "pf_input")
missing_pf_columns = required_pf_columns - pf_columns

if missing_pf_columns:
    raise RuntimeError(
        "Missing required 01_12 columns: "
        + ", ".join(sorted(missing_pf_columns))
    )


# =============================================================================
# Discover and standardize respiratory-output schemas
# =============================================================================

hourly_columns = get_columns(con, "respiratory_hourly_raw")
six_hour_columns = get_columns(con, "respiratory_six_hour_raw")

hourly_stay_col = choose_column(
    hourly_columns,
    ("stay_id",),
    "hourly stay identifier",
)
hourly_window_col = choose_column(
    hourly_columns,
    ("window_index", "hour_index", "hour"),
    "hourly window index",
)
hourly_fraction_col = choose_column(
    hourly_columns,
    (
        "invasive_ventilation_fraction",
        "invasive_fraction",
        "invasive_vent_fraction",
        "invasive_time_fraction",
    ),
    "hourly invasive ventilation fraction",
)

six_stay_col = choose_column(
    six_hour_columns,
    ("stay_id",),
    "six-hour stay identifier",
)
six_window_col = choose_column(
    six_hour_columns,
    ("window_index", "six_hour_window_index", "window"),
    "six-hour window index",
)
six_fraction_col = choose_column(
    six_hour_columns,
    (
        "invasive_ventilation_fraction",
        "invasive_fraction",
        "invasive_vent_fraction",
        "invasive_time_fraction",
    ),
    "six-hour invasive ventilation fraction",
)

hourly_eligible_col = next(
    (
        candidate
        for candidate in (
            "eligible_minutes",
            "window_eligible_minutes",
            "observed_minutes",
        )
        if candidate in hourly_columns
    ),
    None,
)

six_eligible_col = next(
    (
        candidate
        for candidate in (
            "eligible_minutes",
            "window_eligible_minutes",
            "observed_minutes",
        )
        if candidate in six_hour_columns
    ),
    None,
)


# =============================================================================
# Standardized respiratory windows
# =============================================================================

if hourly_eligible_col is not None:
    hourly_eligible_expr = f"r.{hourly_eligible_col}"
else:
    hourly_eligible_expr = f"""
        greatest(
            0.0,
            date_diff(
                'second',
                c.t0 + r.{hourly_window_col} * INTERVAL '1 hour',
                least(
                    c.analytical_end_time,
                    c.t0
                        + (r.{hourly_window_col} + 1)
                        * INTERVAL '1 hour'
                )
            ) / 60.0
        )
    """

con.execute(
    f"""
    CREATE TABLE respiratory_hourly AS
    SELECT
        r.{hourly_stay_col}::BIGINT AS stay_id,
        r.{hourly_window_col}::INTEGER AS window_index,
        {hourly_eligible_expr}::DOUBLE AS eligible_minutes,
        r.{hourly_fraction_col}::DOUBLE
            AS invasive_ventilation_fraction
    FROM respiratory_hourly_raw r
    INNER JOIN cohort c
        ON r.{hourly_stay_col} = c.stay_id
    """
)

if six_eligible_col is not None:
    six_eligible_expr = f"r.{six_eligible_col}"
else:
    six_eligible_expr = f"""
        greatest(
            0.0,
            date_diff(
                'second',
                c.t0 + r.{six_window_col} * INTERVAL '6 hours',
                least(
                    c.analytical_end_time,
                    c.t0
                        + (r.{six_window_col} + 1)
                        * INTERVAL '6 hours'
                )
            ) / 60.0
        )
    """

con.execute(
    f"""
    CREATE TABLE respiratory_six_hour AS
    SELECT
        r.{six_stay_col}::BIGINT AS stay_id,
        r.{six_window_col}::INTEGER AS window_index,
        {six_eligible_expr}::DOUBLE AS eligible_minutes,
        r.{six_fraction_col}::DOUBLE
            AS invasive_ventilation_fraction
    FROM respiratory_six_hour_raw r
    INNER JOIN cohort c
        ON r.{six_stay_col} = c.stay_id
    """
)

for source_table, final_table in (
    ("respiratory_hourly", "respiratory_hourly_final"),
    ("respiratory_six_hour", "respiratory_six_hour_final"),
):
    con.execute(
        f"""
        CREATE TABLE {final_table} AS
        SELECT
            stay_id,
            window_index,
            eligible_minutes,
            invasive_ventilation_fraction,
            CASE
                WHEN invasive_ventilation_fraction IS NOT NULL THEN
                    eligible_minutes * invasive_ventilation_fraction
                ELSE NULL
            END AS invasive_minutes,
            CASE
                WHEN eligible_minutes <= 0
                    THEN 'NOT_OBSERVABLE'
                WHEN invasive_ventilation_fraction IS NULL
                    THEN 'UNKNOWN_RESPIRATORY'
                WHEN invasive_ventilation_fraction <= 0
                    THEN 'NOT_INVASIVE'
                WHEN invasive_ventilation_fraction
                     >= {FULL_INVASIVE_TOLERANCE}
                    THEN 'FULL_INVASIVE'
                ELSE 'PARTIAL_INVASIVE'
            END AS invasive_eligibility_class
        FROM {source_table}
        """
    )


# =============================================================================
# Validate and enrich event-level P/F input
# =============================================================================

con.execute(
    """
    CREATE TABLE pf_events AS
    SELECT
        p.*,
        c.t0,
        c.analytical_end_time,
        date_diff(
            'second',
            c.t0,
            p.pao2_charttime
        ) / 3600.0 AS hours_from_t0,
        CASE
            WHEN p.pf_ratio IS NOT NULL
             AND p.selected_fio2_fraction IS NOT NULL
                THEN 1
            ELSE 0
        END AS matched_pf_flag
    FROM pf_input p
    INNER JOIN cohort c
        ON p.stay_id = c.stay_id
    """
)

input_check = con.execute(
    """
    SELECT
        COUNT(*) AS rows_n,
        COUNT(DISTINCT stay_id) AS stays_n,
        COUNT(*) FILTER (
            WHERE pao2_charttime < t0
               OR pao2_charttime >= analytical_end_time
        ) AS outside_horizon_n,
        COUNT(*) FILTER (
            WHERE pf_ratio IS NOT NULL
              AND (
                    pf_ratio <= 0
                    OR selected_fio2_fraction < 0.21
                    OR selected_fio2_fraction > 1.0
              )
        ) AS invalid_pf_n
    FROM pf_events
    """
).fetchone()

if input_check[2]:
    raise RuntimeError(
        f"P/F events outside analytical horizon: {input_check[2]}."
    )

if input_check[3]:
    raise RuntimeError(f"Invalid P/F events: {input_check[3]}.")

duplicate_events_n = con.execute(
    """
    SELECT COUNT(*)
    FROM (
        SELECT
            stay_id,
            pao2_charttime,
            COUNT(*) AS rows_n
        FROM pf_events
        GROUP BY stay_id, pao2_charttime
        HAVING COUNT(*) > 1
    )
    """
).fetchone()[0]

if duplicate_events_n:
    raise RuntimeError(f"Duplicate PaO2 events: {duplicate_events_n}.")


# =============================================================================
# Window grids
# =============================================================================

con.execute(
    """
    CREATE TABLE hourly_grid AS
    SELECT
        c.stay_id,
        h AS window_index,
        'H'
            || lpad(h::VARCHAR, 2, '0')
            || '_H'
            || lpad((h + 1)::VARCHAR, 2, '0')
            AS window_label,
        c.t0 + h * INTERVAL '1 hour' AS window_start_time,
        least(
            c.analytical_end_time,
            c.t0 + (h + 1) * INTERVAL '1 hour'
        ) AS window_end_time
    FROM cohort c
    CROSS JOIN range(0, 24) r(h)
    """
)

con.execute(
    """
    CREATE TABLE six_hour_grid AS
    SELECT
        c.stay_id,
        w.window_index,
        w.window_label,
        c.t0 + w.start_hour * INTERVAL '1 hour'
            AS window_start_time,
        least(
            c.analytical_end_time,
            c.t0 + w.end_hour * INTERVAL '1 hour'
        ) AS window_end_time
    FROM cohort c
    CROSS JOIN (
        VALUES
            (0, 'H00_H06', 0, 6),
            (1, 'H06_H12', 6, 12),
            (2, 'H12_H18', 12, 18),
            (3, 'H18_H24', 18, 24)
    ) w(window_index, window_label, start_hour, end_hour)
    """
)


# =============================================================================
# Canonical window aggregation
# =============================================================================

def create_window_table(
    output_table: str,
    grid_table: str,
    respiratory_table: str,
) -> None:
    """Create one canonical P/F trajectory row per stay and time window."""
    con.execute(
        f"""
        CREATE TABLE {output_table} AS
        SELECT
            g.stay_id,
            g.window_index,
            g.window_label,
            g.window_start_time,
            g.window_end_time,

            r.eligible_minutes,
            r.invasive_minutes,
            r.invasive_ventilation_fraction,
            r.invasive_eligibility_class,

            COUNT(e.pao2_charttime) AS arterial_pao2_events_n,

            COUNT(e.pao2_charttime) FILTER (
                WHERE e.matched_pf_flag = 1
            ) AS matched_pf_events_all_states_n,

            COUNT(e.pao2_charttime) FILTER (
                WHERE e.matched_pf_flag = 1
                  AND e.respiratory_state = '{PRIMARY_RESPIRATORY_STATE}'
            ) AS invasive_pf_events_n,

            arg_min(e.pf_ratio, e.pao2_charttime) FILTER (
                WHERE e.matched_pf_flag = 1
                  AND e.respiratory_state = '{PRIMARY_RESPIRATORY_STATE}'
            ) AS invasive_pf_first,

            min(e.pao2_charttime) FILTER (
                WHERE e.matched_pf_flag = 1
                  AND e.respiratory_state = '{PRIMARY_RESPIRATORY_STATE}'
            ) AS invasive_pf_first_time,

            arg_max(e.pf_ratio, e.pao2_charttime) FILTER (
                WHERE e.matched_pf_flag = 1
                  AND e.respiratory_state = '{PRIMARY_RESPIRATORY_STATE}'
            ) AS invasive_pf_last,

            max(e.pao2_charttime) FILTER (
                WHERE e.matched_pf_flag = 1
                  AND e.respiratory_state = '{PRIMARY_RESPIRATORY_STATE}'
            ) AS invasive_pf_last_time,

            min(e.pf_ratio) FILTER (
                WHERE e.matched_pf_flag = 1
                  AND e.respiratory_state = '{PRIMARY_RESPIRATORY_STATE}'
            ) AS invasive_pf_min,

            median(e.pf_ratio) FILTER (
                WHERE e.matched_pf_flag = 1
                  AND e.respiratory_state = '{PRIMARY_RESPIRATORY_STATE}'
            ) AS invasive_pf_median,

            max(e.pf_ratio) FILTER (
                WHERE e.matched_pf_flag = 1
                  AND e.respiratory_state = '{PRIMARY_RESPIRATORY_STATE}'
            ) AS invasive_pf_max,

            arg_max(e.pf_ratio, e.pao2_charttime) FILTER (
                WHERE e.matched_pf_flag = 1
                  AND e.respiratory_state = '{PRIMARY_RESPIRATORY_STATE}'
            )
            -
            arg_min(e.pf_ratio, e.pao2_charttime) FILTER (
                WHERE e.matched_pf_flag = 1
                  AND e.respiratory_state = '{PRIMARY_RESPIRATORY_STATE}'
            ) AS invasive_pf_first_to_last_change,

            median(
                e.selected_fio2_minutes_before_pao2
            ) FILTER (
                WHERE e.matched_pf_flag = 1
                  AND e.respiratory_state = '{PRIMARY_RESPIRATORY_STATE}'
                  AND e.selected_fio2_source = 'PREVIOUS_CHART'
            ) AS previous_fio2_gap_median_minutes,

            max(
                e.selected_fio2_minutes_before_pao2
            ) FILTER (
                WHERE e.matched_pf_flag = 1
                  AND e.respiratory_state = '{PRIMARY_RESPIRATORY_STATE}'
                  AND e.selected_fio2_source = 'PREVIOUS_CHART'
            ) AS previous_fio2_gap_max_minutes,

            COUNT(e.pao2_charttime) FILTER (
                WHERE e.matched_pf_flag = 1
                  AND e.respiratory_state = '{PRIMARY_RESPIRATORY_STATE}'
                  AND e.selected_fio2_source = 'SAME_PANEL_LAB'
            ) AS source_same_panel_lab_n,

            COUNT(e.pao2_charttime) FILTER (
                WHERE e.matched_pf_flag = 1
                  AND e.respiratory_state = '{PRIMARY_RESPIRATORY_STATE}'
                  AND e.selected_fio2_source = 'SAME_TIMESTAMP_CHART'
            ) AS source_same_timestamp_chart_n,

            COUNT(e.pao2_charttime) FILTER (
                WHERE e.matched_pf_flag = 1
                  AND e.respiratory_state = '{PRIMARY_RESPIRATORY_STATE}'
                  AND e.selected_fio2_source = 'PREVIOUS_CHART'
            ) AS source_previous_chart_n,

            CASE
                WHEN r.invasive_eligibility_class IN (
                    'FULL_INVASIVE',
                    'PARTIAL_INVASIVE'
                ) THEN 1
                ELSE 0
            END AS invasive_window_eligible_flag,

            CASE
                WHEN r.invasive_eligibility_class IN (
                        'FULL_INVASIVE',
                        'PARTIAL_INVASIVE'
                     )
                 AND COUNT(e.pao2_charttime) FILTER (
                        WHERE e.matched_pf_flag = 1
                          AND e.respiratory_state
                              = '{PRIMARY_RESPIRATORY_STATE}'
                     ) > 0
                    THEN 1
                ELSE 0
            END AS invasive_pf_observed_flag,

            CASE
                WHEN r.invasive_eligibility_class IN (
                        'FULL_INVASIVE',
                        'PARTIAL_INVASIVE'
                     )
                 AND COUNT(e.pao2_charttime) FILTER (
                        WHERE e.matched_pf_flag = 1
                          AND e.respiratory_state
                              = '{PRIMARY_RESPIRATORY_STATE}'
                     ) = 0
                    THEN 1
                ELSE 0
            END AS invasive_pf_measurement_missing_flag,

            CASE
                WHEN r.invasive_eligibility_class = 'NOT_INVASIVE'
                    THEN 1
                ELSE 0
            END AS structurally_ineligible_flag,

            CASE
                WHEN r.invasive_eligibility_class IN (
                    'UNKNOWN_RESPIRATORY',
                    'NOT_OBSERVABLE'
                ) THEN 1
                ELSE 0
            END AS invasive_eligibility_unknown_flag,

            CASE
                WHEN COUNT(e.pao2_charttime) FILTER (
                    WHERE e.matched_pf_flag = 1
                      AND e.respiratory_state
                          = '{PRIMARY_RESPIRATORY_STATE}'
                ) >= 2
                    THEN 1
                ELSE 0
            END AS multiple_invasive_pf_flag

        FROM {grid_table} g
        INNER JOIN {respiratory_table} r
            ON g.stay_id = r.stay_id
           AND g.window_index = r.window_index
        LEFT JOIN pf_events e
            ON g.stay_id = e.stay_id
           AND e.pao2_charttime >= g.window_start_time
           AND e.pao2_charttime < g.window_end_time
        GROUP BY
            g.stay_id,
            g.window_index,
            g.window_label,
            g.window_start_time,
            g.window_end_time,
            r.eligible_minutes,
            r.invasive_minutes,
            r.invasive_ventilation_fraction,
            r.invasive_eligibility_class
        """
    )


create_window_table(
    "pf_hourly",
    "hourly_grid",
    "respiratory_hourly_final",
)

create_window_table(
    "pf_six_hour",
    "six_hour_grid",
    "respiratory_six_hour_final",
)


# =============================================================================
# Canonical output files
# =============================================================================

con.execute(
    f"""
    COPY (
        SELECT *
        FROM pf_hourly
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
        FROM pf_six_hour
        ORDER BY stay_id, window_index
    )
    TO '{qpath(OUT_SIX_HOUR)}'
    (FORMAT PARQUET, COMPRESSION ZSTD)
    """
)


# =============================================================================
# Canonical long-format audit
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

con.executemany(
    "INSERT INTO audit VALUES (?, ?, ?, ?, ?)",
    [
        ("DESIGN", None, "internal_version", 5, "v5"),
        (
            "DESIGN",
            None,
            "primary_temporal_representation",
            None,
            "Four 6-hour windows",
        ),
        (
            "DESIGN",
            None,
            "secondary_temporal_representation",
            None,
            "Twenty-four 1-hour windows",
        ),
        (
            "DESIGN",
            None,
            "primary_trajectory_value",
            None,
            "Median invasive P/F",
        ),
        (
            "DESIGN",
            None,
            "secondary_severity_value",
            None,
            "Minimum invasive P/F",
        ),
        (
            "DESIGN",
            None,
            "primary_respiratory_state",
            None,
            PRIMARY_RESPIRATORY_STATE,
        ),
        (
            "DESIGN",
            None,
            "window_eligibility",
            None,
            "Invasive ventilation minutes > 0",
        ),
        ("DESIGN", None, "locf_flag", 0, "No LOCF"),
        (
            "DESIGN",
            None,
            "interpolation_flag",
            0,
            "No interpolation or extrapolation",
        ),
        ("COHORT", None, "cohort_n", EXPECTED_COHORT_N, None),
    ],
)

# Input provenance.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'INPUT',
        NULL,
        'arterial_pao2_events_n',
        COUNT(*)::DOUBLE,
        NULL
    FROM pf_events
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'INPUT',
        NULL,
        'matched_pf_events_all_states_n',
        COUNT(*) FILTER (
            WHERE matched_pf_flag = 1
        )::DOUBLE,
        NULL
    FROM pf_events
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'INPUT',
        NULL,
        'matched_invasive_pf_events_n',
        COUNT(*) FILTER (
            WHERE matched_pf_flag = 1
              AND respiratory_state = 'INVASIVE_VENT'
        )::DOUBLE,
        NULL
    FROM pf_events
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'INPUT',
        NULL,
        'stays_with_invasive_pf_n',
        COUNT(DISTINCT stay_id) FILTER (
            WHERE matched_pf_flag = 1
              AND respiratory_state = 'INVASIVE_VENT'
        )::DOUBLE,
        NULL
    FROM pf_events
    """
)

# Representation-level audit.
for table_name, representation in (
    ("pf_hourly", "HOURLY"),
    ("pf_six_hour", "SIX_HOUR"),
):
    con.execute(
        f"""
        INSERT INTO audit
        SELECT
            'REPRESENTATION',
            '{representation}',
            'total_windows_n',
            COUNT(*)::DOUBLE,
            NULL
        FROM {table_name}
        """
    )

    con.execute(
        f"""
        INSERT INTO audit
        SELECT
            'REPRESENTATION',
            '{representation}',
            'invasive_eligible_windows_n',
            COUNT(*) FILTER (
                WHERE invasive_window_eligible_flag = 1
            )::DOUBLE,
            NULL
        FROM {table_name}
        """
    )

    con.execute(
        f"""
        INSERT INTO audit
        SELECT
            'REPRESENTATION',
            '{representation}',
            'observed_windows_n',
            COUNT(*) FILTER (
                WHERE invasive_pf_observed_flag = 1
            )::DOUBLE,
            NULL
        FROM {table_name}
        """
    )

    con.execute(
        f"""
        INSERT INTO audit
        SELECT
            'REPRESENTATION',
            '{representation}',
            'measurement_missing_windows_n',
            COUNT(*) FILTER (
                WHERE invasive_pf_measurement_missing_flag = 1
            )::DOUBLE,
            NULL
        FROM {table_name}
        """
    )

    con.execute(
        f"""
        INSERT INTO audit
        SELECT
            'REPRESENTATION',
            '{representation}',
            'structurally_ineligible_windows_n',
            COUNT(*) FILTER (
                WHERE structurally_ineligible_flag = 1
            )::DOUBLE,
            NULL
        FROM {table_name}
        """
    )

    con.execute(
        f"""
        INSERT INTO audit
        SELECT
            'REPRESENTATION',
            '{representation}',
            'eligibility_unknown_windows_n',
            COUNT(*) FILTER (
                WHERE invasive_eligibility_unknown_flag = 1
            )::DOUBLE,
            NULL
        FROM {table_name}
        """
    )

    con.execute(
        f"""
        INSERT INTO audit
        SELECT
            'REPRESENTATION',
            '{representation}',
            'observed_among_invasive_eligible_pct',
            100.0 * COUNT(*) FILTER (
                WHERE invasive_pf_observed_flag = 1
            )
            / nullif(
                COUNT(*) FILTER (
                    WHERE invasive_window_eligible_flag = 1
                ),
                0
            ),
            NULL
        FROM {table_name}
        """
    )

    con.execute(
        f"""
        INSERT INTO audit
        SELECT
            'REPRESENTATION',
            '{representation}',
            'multi_observation_among_observed_pct',
            100.0 * COUNT(*) FILTER (
                WHERE multiple_invasive_pf_flag = 1
            )
            / nullif(
                COUNT(*) FILTER (
                    WHERE invasive_pf_observed_flag = 1
                ),
                0
            ),
            NULL
        FROM {table_name}
        """
    )

    con.execute(
        f"""
        INSERT INTO audit
        SELECT
            'REPRESENTATION',
            '{representation}',
            'median_events_per_observed_window',
            median(invasive_pf_events_n) FILTER (
                WHERE invasive_pf_observed_flag = 1
            ),
            NULL
        FROM {table_name}
        """
    )

    con.execute(
        f"""
        INSERT INTO audit
        SELECT
            'REPRESENTATION',
            '{representation}',
            'median_abs_median_minus_min',
            median(
                abs(invasive_pf_median - invasive_pf_min)
            ) FILTER (
                WHERE multiple_invasive_pf_flag = 1
            ),
            NULL
        FROM {table_name}
        """
    )

# Six-hour trajectory-window audit.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'SIX_HOUR_WINDOW',
        window_label,
        'invasive_eligible_stays_n',
        COUNT(*) FILTER (
            WHERE invasive_window_eligible_flag = 1
        )::DOUBLE,
        NULL
    FROM pf_six_hour
    GROUP BY window_index, window_label
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'SIX_HOUR_WINDOW',
        window_label,
        'full_invasive_stays_n',
        COUNT(*) FILTER (
            WHERE invasive_eligibility_class = 'FULL_INVASIVE'
        )::DOUBLE,
        NULL
    FROM pf_six_hour
    GROUP BY window_index, window_label
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'SIX_HOUR_WINDOW',
        window_label,
        'partial_invasive_stays_n',
        COUNT(*) FILTER (
            WHERE invasive_eligibility_class = 'PARTIAL_INVASIVE'
        )::DOUBLE,
        NULL
    FROM pf_six_hour
    GROUP BY window_index, window_label
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'SIX_HOUR_WINDOW',
        window_label,
        'observed_stays_n',
        COUNT(*) FILTER (
            WHERE invasive_pf_observed_flag = 1
        )::DOUBLE,
        NULL
    FROM pf_six_hour
    GROUP BY window_index, window_label
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'SIX_HOUR_WINDOW',
        window_label,
        'observed_among_invasive_eligible_pct',
        100.0 * COUNT(*) FILTER (
            WHERE invasive_pf_observed_flag = 1
        )
        / nullif(
            COUNT(*) FILTER (
                WHERE invasive_window_eligible_flag = 1
            ),
            0
        ),
        NULL
    FROM pf_six_hour
    GROUP BY window_index, window_label
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'SIX_HOUR_WINDOW',
        window_label,
        'median_of_window_medians',
        median(invasive_pf_median),
        NULL
    FROM pf_six_hour
    GROUP BY window_index, window_label
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'SIX_HOUR_WINDOW',
        window_label,
        'median_of_window_minima',
        median(invasive_pf_min),
        NULL
    FROM pf_six_hour
    GROUP BY window_index, window_label
    """
)

# Eligibility-class audit within six-hour windows.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'SIX_HOUR_ELIGIBILITY_CLASS',
        window_label || '_' || invasive_eligibility_class,
        'windows_n',
        COUNT(*)::DOUBLE,
        NULL
    FROM pf_six_hour
    GROUP BY
        window_index,
        window_label,
        invasive_eligibility_class
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'SIX_HOUR_ELIGIBILITY_CLASS',
        window_label || '_' || invasive_eligibility_class,
        'observed_among_eligible_pct',
        100.0 * COUNT(*) FILTER (
            WHERE invasive_pf_observed_flag = 1
        )
        / nullif(
            COUNT(*) FILTER (
                WHERE invasive_window_eligible_flag = 1
            ),
            0
        ),
        NULL
    FROM pf_six_hour
    GROUP BY
        window_index,
        window_label,
        invasive_eligibility_class
    """
)

# FiO2 source provenance in the primary six-hour output.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'FIO2_SOURCE',
        'SAME_PANEL_LAB',
        'events_n',
        SUM(source_same_panel_lab_n)::DOUBLE,
        NULL
    FROM pf_six_hour
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'FIO2_SOURCE',
        'SAME_TIMESTAMP_CHART',
        'events_n',
        SUM(source_same_timestamp_chart_n)::DOUBLE,
        NULL
    FROM pf_six_hour
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'FIO2_SOURCE',
        'PREVIOUS_CHART',
        'events_n',
        SUM(source_previous_chart_n)::DOUBLE,
        NULL
    FROM pf_six_hour
    """
)

con.execute(
    f"""
    COPY (
        SELECT *
        FROM audit
        ORDER BY audit_level, subgroup, metric
    )
    TO '{qpath(OUT_AUDIT)}'
    (HEADER, DELIMITER ',')
    """
)


# =============================================================================
# Blocking validation
# =============================================================================

expected_hourly_rows = EXPECTED_COHORT_N * 24
expected_six_hour_rows = EXPECTED_COHORT_N * 4

hourly_rows_n = con.execute(
    "SELECT COUNT(*) FROM pf_hourly"
).fetchone()[0]

six_hour_rows_n = con.execute(
    "SELECT COUNT(*) FROM pf_six_hour"
).fetchone()[0]

if hourly_rows_n != expected_hourly_rows:
    raise RuntimeError(
        f"Unexpected hourly rows: {hourly_rows_n} "
        f"vs {expected_hourly_rows}."
    )

if six_hour_rows_n != expected_six_hour_rows:
    raise RuntimeError(
        f"Unexpected six-hour rows: {six_hour_rows_n} "
        f"vs {expected_six_hour_rows}."
    )

invalid_resp_fraction_n = con.execute(
    """
    SELECT COUNT(*)
    FROM (
        SELECT invasive_ventilation_fraction
        FROM respiratory_hourly_final
        UNION ALL
        SELECT invasive_ventilation_fraction
        FROM respiratory_six_hour_final
    )
    WHERE invasive_ventilation_fraction < -0.000001
       OR invasive_ventilation_fraction > 1.000001
    """
).fetchone()[0]

if invalid_resp_fraction_n:
    raise RuntimeError(
        f"Invalid invasive ventilation fractions: "
        f"{invalid_resp_fraction_n}."
    )

invalid_window_logic_n = con.execute(
    """
    SELECT COUNT(*)
    FROM (
        SELECT
            invasive_pf_observed_flag,
            invasive_pf_measurement_missing_flag,
            structurally_ineligible_flag,
            invasive_eligibility_unknown_flag,
            invasive_pf_events_n
        FROM pf_hourly

        UNION ALL

        SELECT
            invasive_pf_observed_flag,
            invasive_pf_measurement_missing_flag,
            structurally_ineligible_flag,
            invasive_eligibility_unknown_flag,
            invasive_pf_events_n
        FROM pf_six_hour
    )
    WHERE invasive_pf_observed_flag
          + invasive_pf_measurement_missing_flag
          + structurally_ineligible_flag
          + invasive_eligibility_unknown_flag <> 1
       OR (
            invasive_pf_observed_flag = 1
            AND invasive_pf_events_n = 0
       )
       OR (
            invasive_pf_measurement_missing_flag = 1
            AND invasive_pf_events_n > 0
       )
    """
).fetchone()[0]

if invalid_window_logic_n:
    raise RuntimeError(
        "Invalid structural/measurement missingness logic: "
        f"{invalid_window_logic_n}."
    )

input_event_n = con.execute(
    "SELECT COUNT(*) FROM pf_events"
).fetchone()[0]

hourly_event_sum = con.execute(
    "SELECT SUM(arterial_pao2_events_n) FROM pf_hourly"
).fetchone()[0]

six_hour_event_sum = con.execute(
    "SELECT SUM(arterial_pao2_events_n) FROM pf_six_hour"
).fetchone()[0]

if hourly_event_sum != input_event_n:
    raise RuntimeError(
        f"Hourly event conservation failed: "
        f"{hourly_event_sum} vs {input_event_n}."
    )

if six_hour_event_sum != input_event_n:
    raise RuntimeError(
        f"Six-hour event conservation failed: "
        f"{six_hour_event_sum} vs {input_event_n}."
    )

invasive_event_n = con.execute(
    """
    SELECT COUNT(*)
    FROM pf_events
    WHERE matched_pf_flag = 1
      AND respiratory_state = 'INVASIVE_VENT'
    """
).fetchone()[0]

hourly_invasive_event_sum = con.execute(
    "SELECT SUM(invasive_pf_events_n) FROM pf_hourly"
).fetchone()[0]

six_hour_invasive_event_sum = con.execute(
    "SELECT SUM(invasive_pf_events_n) FROM pf_six_hour"
).fetchone()[0]

if hourly_invasive_event_sum != invasive_event_n:
    raise RuntimeError(
        "Hourly invasive P/F event conservation failed: "
        f"{hourly_invasive_event_sum} vs {invasive_event_n}."
    )

if six_hour_invasive_event_sum != invasive_event_n:
    raise RuntimeError(
        "Six-hour invasive P/F event conservation failed: "
        f"{six_hour_invasive_event_sum} vs {invasive_event_n}."
    )


# =============================================================================
# Console report
# =============================================================================

print("=" * 108)
print("TRAJECTORIES - CANONICAL PAO2/FIO2 TRAJECTORY V5")
print("=" * 108)

print("Detected upstream respiratory schema")
print("------------------------------------")
print(f"Hourly invasive fraction column: {hourly_fraction_col}")
print(f"Hourly eligible-minute column: {hourly_eligible_col}")
print(f"Six-hour invasive fraction column: {six_fraction_col}")
print(f"Six-hour eligible-minute column: {six_eligible_col}")
print()

print("Canonical six-hour trajectory")
print("-----------------------------")
print(
    con.execute(
        """
        SELECT
            window_label,
            COUNT(*) FILTER (
                WHERE invasive_window_eligible_flag = 1
            ) AS invasive_eligible_stays_n,
            COUNT(*) FILTER (
                WHERE invasive_eligibility_class = 'FULL_INVASIVE'
            ) AS full_invasive_stays_n,
            COUNT(*) FILTER (
                WHERE invasive_eligibility_class = 'PARTIAL_INVASIVE'
            ) AS partial_invasive_stays_n,
            COUNT(*) FILTER (
                WHERE invasive_pf_observed_flag = 1
            ) AS observed_stays_n,
            round(
                100.0 * COUNT(*) FILTER (
                    WHERE invasive_pf_observed_flag = 1
                )
                / nullif(
                    COUNT(*) FILTER (
                        WHERE invasive_window_eligible_flag = 1
                    ),
                    0
                ),
                2
            ) AS observed_among_eligible_pct,
            median(invasive_pf_events_n) FILTER (
                WHERE invasive_pf_observed_flag = 1
            ) AS median_events_per_observed_window,
            round(median(invasive_pf_median), 1)
                AS median_of_window_medians,
            round(median(invasive_pf_min), 1)
                AS median_of_window_minima
        FROM pf_six_hour
        GROUP BY window_index, window_label
        ORDER BY window_index
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Representation comparison")
print("-------------------------")
print(
    con.execute(
        """
        SELECT
            subgroup AS representation,
            max(value_num) FILTER (
                WHERE metric = 'invasive_eligible_windows_n'
            ) AS invasive_eligible_windows_n,
            max(value_num) FILTER (
                WHERE metric = 'observed_windows_n'
            ) AS observed_windows_n,
            round(
                max(value_num) FILTER (
                    WHERE metric
                        = 'observed_among_invasive_eligible_pct'
                ),
                2
            ) AS observed_among_invasive_eligible_pct,
            round(
                max(value_num) FILTER (
                    WHERE metric
                        = 'multi_observation_among_observed_pct'
                ),
                2
            ) AS multi_observation_among_observed_pct,
            max(value_num) FILTER (
                WHERE metric = 'median_events_per_observed_window'
            ) AS median_events_per_observed_window,
            round(
                max(value_num) FILTER (
                    WHERE metric = 'median_abs_median_minus_min'
                ),
                1
            ) AS median_abs_median_minus_min
        FROM audit
        WHERE audit_level = 'REPRESENTATION'
        GROUP BY subgroup
        ORDER BY subgroup
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Canonical outputs")
print("-----------------")
for path in (OUT_HOURLY, OUT_SIX_HOUR, OUT_AUDIT):
    print(path)

print()
print("Canonical PaO2/FiO2 trajectory component completed successfully.")
print("Four 6-hour windows are the primary representation.")
print("Twenty-four hourly windows are retained as a secondary output.")
