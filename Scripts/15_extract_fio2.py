"""
01_12_extract_fio2.py

Trajectories project
Canonical FiO2 normalization and PaO2 matching component
Status: frozen production component

PURPOSE
-------
Normalize FiO2 observations and assign a clinically plausible FiO2 value to
each arterial PaO2 observation in the first 24 postoperative ICU hours.

FiO2 is treated primarily as a technical input for subsequent PaO2/FiO2
trajectory construction, not as an autonomous continuous trajectory.

PRIMARY MATCHING POLICY
-----------------------
For every clearly arterial PaO2 observation:

1. SAME_PANEL_LAB
   Use normalized LABEVENTS FiO2 itemid 50816 recorded in the same blood-gas
   panel/time.

2. SAME_TIMESTAMP_CHART
   If same-panel FiO2 is unavailable, use normalized CHARTEVENTS FiO2 itemid
   223835 recorded at the exact PaO2 timestamp.

3. PREVIOUS_CHART
   If neither source is available, use the most recent preceding valid chart
   FiO2 when:
       - it occurs in the same reconstructed respiratory-support interval;
       - it precedes the PaO2 observation;
       - it is no more than 120 minutes earlier.

4. NO_MATCH
   Otherwise leave FiO2 and P/F missing.

No future FiO2 is assigned backward.
No previous chart FiO2 may cross a respiratory-state transition.

SENSITIVITY POLICIES
--------------------
The same matching algorithm is evaluated using maximum previous-chart gaps of:
    60 minutes
    120 minutes  [PRIMARY]
    240 minutes

FIO2 NORMALIZATION
------------------
For both CHARTEVENTS 223835 and LABEVENTS 50816:

    0.21 <= value <= 1.00
        retain as FiO2 fraction

    21 <= value <= 100
        divide by 100

    all other values
        exclude without repair

Values between 1 and 21 are excluded because prior audits showed that they
predominantly represent oxygen flow rather than FiO2.

ARTERIAL PAO2
-------------
PaO2 uses LABEVENTS itemid 50821 and is retained only when the same panel/time
contains specimen itemid 52033 clearly classified as arterial:
    ART.
    ART
    ARTERIAL
    ARTERIAL BLOOD

Mixed or ambiguous specimen combinations are excluded from the primary set.

RESPIRATORY SUPPORT
-------------------
Exact respiratory intervals are reconstructed internally using the frozen
01_11 rules from:
    226732 O2 Delivery Device(s)
    223849 Ventilator Mode
    229314 Hamilton Ventilator Mode

State priority:
    TRACHEOSTOMY
    > INVASIVE_VENT
    > NON_INVASIVE_VENT
    > HFNC
    > SUPPLEMENTAL_OXYGEN
    > NONE

Respiratory-state persistence is capped at 6 hours.
The first classified state may be assigned backward to t0 only when recorded
within 60 minutes.

CANONICAL OUTPUTS
-----------------
1. data/01_12_fio2_pao2_matches.parquet
   One row per clearly arterial PaO2 event using the primary 120-minute policy.

2. data/01_12_fio2_matching_sensitivity.csv
   Aggregate comparison of 60-, 120-, and 240-minute policies.

3. data/01_12_fio2_matching_audit.csv
   Long-format provenance, normalization, specimen, source-agreement, context,
   completeness, and validation audit.

This component does not create hourly or six-hour P/F trajectories.
Those belong to the subsequent PaO2/FiO2 trajectory component.

Inputs
------
data/00_04_final_analytic_cohort.csv
Raw MIMIC-IV 3.1 files relative to MIMIC_ROOT:
  icu/chartevents.csv.gz
  hosp/labevents.csv.gz

The 01_11 respiratory-state rules are implemented internally from raw events;
this script does not read an output dataset from 01_11.
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
CHARTEVENTS_FILE = MIMIC_ROOT / "icu" / "chartevents.csv.gz"
LABEVENTS_FILE = MIMIC_ROOT / "hosp" / "labevents.csv.gz"

OUT_MATCHES = PROJECT_ROOT / "data" / "01_12_fio2_pao2_matches.parquet"
OUT_SENSITIVITY = (
    PROJECT_ROOT / "data" / "01_12_fio2_matching_sensitivity.csv"
)
OUT_AUDIT = PROJECT_ROOT / "data" / "01_12_fio2_matching_audit.csv"

EXPECTED_COHORT_N = 9305
PRIMARY_MATCH_WINDOW_MINUTES = 120
SENSITIVITY_WINDOWS_MINUTES = (60, 120, 240)

FIO2_CHART_ITEMID = 223835
PAO2_LAB_ITEMID = 50821
FIO2_LAB_ITEMID = 50816
SPECIMEN_LAB_ITEMID = 52033

DEVICE_ITEMID = 226732
VENT_MODE_ITEMID = 223849
HAMILTON_MODE_ITEMID = 229314

RESPIRATORY_PERSISTENCE_HOURS = 6
RESPIRATORY_BACKWARD_LIMIT_MINUTES = 60


# =============================================================================
# Frozen respiratory mappings
# =============================================================================

TRACHEOSTOMY_DEVICES = (
    "Tracheostomy tube",
    "Trach mask ",
)

INVASIVE_DEVICES = (
    "Endotracheal tube",
)

NONINVASIVE_DEVICES = (
    "Bipap mask ",
    "CPAP mask ",
)

HFNC_DEVICES = (
    "High flow nasal cannula",
)

SUPPLEMENTAL_DEVICES = (
    "Non-rebreather",
    "Face tent",
    "Aerosol-cool",
    "Venti mask ",
    "Medium conc mask ",
    "Ultrasonic neb",
    "Vapomist",
    "Oxymizer",
    "High flow neb",
    "Nasal cannula",
)

NONE_DEVICES = ("None",)

STANDARD_INVASIVE_MODES = (
    "(S) CMV",
    "APRV",
    "APRV/Biphasic+ApnPress",
    "APRV/Biphasic+ApnVol",
    "APV (cmv)",
    "Ambient",
    "Apnea Ventilation",
    "CMV",
    "CMV/ASSIST",
    "CMV/ASSIST/AutoFlow",
    "CMV/AutoFlow",
    "CPAP/PPS",
    "CPAP/PSV",
    "CPAP/PSV+Apn TCPL",
    "CPAP/PSV+ApnPres",
    "CPAP/PSV+ApnVol",
    "MMV",
    "MMV/AutoFlow",
    "MMV/PSV",
    "MMV/PSV/AutoFlow",
    "P-CMV",
    "PCV+",
    "PCV+/PSV",
    "PCV+Assist",
    "PRES/AC",
    "PRVC/AC",
    "PRVC/SIMV",
    "PSV/SBT",
    "SIMV",
    "SIMV/AutoFlow",
    "SIMV/PRES",
    "SIMV/PSV",
    "SIMV/PSV/AutoFlow",
    "SIMV/VOL",
    "SPONT",
    "SYNCHRON MASTER",
    "SYNCHRON SLAVE",
    "VOL/AC",
)

HAMILTON_INVASIVE_MODES = (
    "APRV",
    "APV (cmv)",
    "Ambient",
    "(S) CMV",
    "P-CMV",
    "SIMV",
    "APV (simv)",
    "P-SIMV",
    "VS",
    "ASV",
    "SPONT",
)

HAMILTON_NONINVASIVE_MODES = (
    "DuoPaP",
    "NIV",
    "NIV-ST",
)


# =============================================================================
# Helpers
# =============================================================================

def qpath(path: Path) -> str:
    """Escape a local path for insertion into a DuckDB SQL string."""
    return str(path).replace("'", "''")


def sql_list(values) -> str:
    """Convert Python strings into a safely quoted SQL IN-list."""
    return ", ".join(
        "'" + str(value).replace("'", "''") + "'"
        for value in values
    )


for required_path in (COHORT_FILE, CHARTEVENTS_FILE, LABEVENTS_FILE):
    if not required_path.exists():
        raise FileNotFoundError(required_path)

OUT_MATCHES.parent.mkdir(parents=True, exist_ok=True)

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
# Focused CHARTEVENTS extraction
# =============================================================================

con.execute(
    f"""
    CREATE TABLE chart_selected AS
    SELECT
        ce.subject_id::BIGINT AS subject_id,
        ce.hadm_id::BIGINT AS hadm_id,
        ce.stay_id::BIGINT AS stay_id,
        ce.charttime::TIMESTAMP AS charttime,
        ce.storetime::TIMESTAMP AS storetime,
        ce.itemid::BIGINT AS itemid,
        ce.value,
        ce.valuenum,
        ce.valueuom,
        ce.warning,
        c.t0,
        c.analytical_end_time
    FROM read_csv_auto('{qpath(CHARTEVENTS_FILE)}', header=true) ce
    INNER JOIN cohort c
        ON ce.stay_id = c.stay_id
       AND ce.charttime >= c.t0
       AND ce.charttime < c.analytical_end_time
    WHERE ce.itemid IN (
        {FIO2_CHART_ITEMID},
        {DEVICE_ITEMID},
        {VENT_MODE_ITEMID},
        {HAMILTON_MODE_ITEMID}
    )
    """
)


# =============================================================================
# Reconstruct exact respiratory-support intervals
# =============================================================================

con.execute(
    f"""
    CREATE TABLE respiratory_rows AS
    SELECT
        *,
        CASE
            WHEN itemid = {DEVICE_ITEMID}
             AND value IN ({sql_list(TRACHEOSTOMY_DEVICES)})
                THEN 'TRACHEOSTOMY'

            WHEN itemid = {DEVICE_ITEMID}
             AND value IN ({sql_list(INVASIVE_DEVICES)})
                THEN 'INVASIVE_VENT'

            WHEN itemid = {DEVICE_ITEMID}
             AND value IN ({sql_list(NONINVASIVE_DEVICES)})
                THEN 'NON_INVASIVE_VENT'

            WHEN itemid = {DEVICE_ITEMID}
             AND value IN ({sql_list(HFNC_DEVICES)})
                THEN 'HFNC'

            WHEN itemid = {DEVICE_ITEMID}
             AND value IN ({sql_list(SUPPLEMENTAL_DEVICES)})
                THEN 'SUPPLEMENTAL_OXYGEN'

            WHEN itemid = {DEVICE_ITEMID}
             AND value IN ({sql_list(NONE_DEVICES)})
                THEN 'NONE'

            WHEN itemid = {VENT_MODE_ITEMID}
             AND value IN ({sql_list(STANDARD_INVASIVE_MODES)})
                THEN 'INVASIVE_VENT'

            WHEN itemid = {HAMILTON_MODE_ITEMID}
             AND value IN ({sql_list(HAMILTON_INVASIVE_MODES)})
                THEN 'INVASIVE_VENT'

            WHEN itemid = {HAMILTON_MODE_ITEMID}
             AND value IN ({sql_list(HAMILTON_NONINVASIVE_MODES)})
                THEN 'NON_INVASIVE_VENT'

            ELSE 'UNCLASSIFIED'
        END AS candidate_state
    FROM chart_selected
    WHERE itemid IN (
        {DEVICE_ITEMID},
        {VENT_MODE_ITEMID},
        {HAMILTON_MODE_ITEMID}
    )
    """
)

con.execute(
    """
    CREATE TABLE respiratory_timestamps_base AS
    SELECT
        stay_id,
        charttime,
        any_value(t0) AS t0,
        any_value(analytical_end_time) AS analytical_end_time,
        max(
            CASE candidate_state
                WHEN 'TRACHEOSTOMY' THEN 6
                WHEN 'INVASIVE_VENT' THEN 5
                WHEN 'NON_INVASIVE_VENT' THEN 4
                WHEN 'HFNC' THEN 3
                WHEN 'SUPPLEMENTAL_OXYGEN' THEN 2
                WHEN 'NONE' THEN 1
                ELSE 0
            END
        ) AS state_priority,
        COUNT(*) AS source_rows_n
    FROM respiratory_rows
    GROUP BY stay_id, charttime
    """
)

con.execute(
    """
    CREATE TABLE respiratory_timestamps AS
    SELECT
        *,
        CASE
            WHEN state_priority = 6 THEN 'TRACHEOSTOMY'
            WHEN state_priority = 5 THEN 'INVASIVE_VENT'
            WHEN state_priority = 4 THEN 'NON_INVASIVE_VENT'
            WHEN state_priority = 3 THEN 'HFNC'
            WHEN state_priority = 2 THEN 'SUPPLEMENTAL_OXYGEN'
            WHEN state_priority = 1 THEN 'NONE'
            ELSE 'UNCLASSIFIED'
        END AS respiratory_state
    FROM respiratory_timestamps_base
    """
)

con.execute(
    """
    CREATE TABLE respiratory_ordered AS
    SELECT
        *,
        row_number() OVER (
            PARTITION BY stay_id
            ORDER BY charttime
        ) AS sequence_number,
        lead(charttime) OVER (
            PARTITION BY stay_id
            ORDER BY charttime
        ) AS next_charttime
    FROM respiratory_timestamps
    WHERE respiratory_state <> 'UNCLASSIFIED'
    """
)

con.execute(
    f"""
    CREATE TABLE respiratory_intervals AS
    SELECT
        stay_id,
        respiratory_state,
        charttime AS respiratory_source_time,
        CASE
            WHEN sequence_number = 1
             AND date_diff('second', t0, charttime) / 60.0
                 <= {RESPIRATORY_BACKWARD_LIMIT_MINUTES}
                THEN t0
            ELSE charttime
        END AS interval_start,
        least(
            coalesce(
                next_charttime,
                charttime + INTERVAL '{RESPIRATORY_PERSISTENCE_HOURS} hours'
            ),
            charttime + INTERVAL '{RESPIRATORY_PERSISTENCE_HOURS} hours',
            analytical_end_time
        ) AS interval_end
    FROM respiratory_ordered
    """
)

con.execute(
    """
    CREATE TABLE valid_respiratory_intervals AS
    SELECT *
    FROM respiratory_intervals
    WHERE interval_end > interval_start
    """
)

respiratory_overlap_n = con.execute(
    """
    SELECT COUNT(*)
    FROM (
        SELECT
            stay_id,
            interval_start,
            lag(interval_end) OVER (
                PARTITION BY stay_id
                ORDER BY interval_start, interval_end
            ) AS previous_interval_end
        FROM valid_respiratory_intervals
    )
    WHERE previous_interval_end > interval_start
    """
).fetchone()[0]

if respiratory_overlap_n:
    raise RuntimeError(
        f"Overlapping respiratory intervals: {respiratory_overlap_n}."
    )


# =============================================================================
# Normalize chart FiO2
# =============================================================================

con.execute(
    f"""
    CREATE TABLE chart_fio2_raw AS
    SELECT
        *,
        CASE
            WHEN valuenum IS NULL THEN 'NONNUMERIC_OR_NULL'
            WHEN valuenum <= 0 THEN 'ZERO_OR_NEGATIVE'
            WHEN valuenum > 0 AND valuenum < 0.21
                THEN 'FRACTION_LOW'
            WHEN valuenum >= 0.21 AND valuenum <= 1
                THEN 'FRACTION_VALID'
            WHEN valuenum > 1 AND valuenum < 21
                THEN 'LOW_INTEGER_SUSPECT'
            WHEN valuenum >= 21 AND valuenum <= 100
                THEN 'PERCENT_VALID'
            WHEN valuenum > 100 THEN 'ABOVE_100'
            ELSE 'UNRESOLVED'
        END AS value_class,
        CASE
            WHEN valuenum >= 0.21 AND valuenum <= 1
                THEN valuenum
            WHEN valuenum >= 21 AND valuenum <= 100
                THEN valuenum / 100.0
            ELSE NULL
        END AS fio2_fraction
    FROM chart_selected
    WHERE itemid = {FIO2_CHART_ITEMID}
    """
)

con.execute(
    """
    CREATE TABLE chart_fio2_valid AS
    SELECT
        stay_id,
        charttime,
        median(fio2_fraction) AS fio2_fraction,
        COUNT(*) AS raw_rows_n,
        COUNT(DISTINCT fio2_fraction) AS distinct_values_n,
        CASE
            WHEN COUNT(DISTINCT fio2_fraction) = 1
                THEN 'SINGLE_OR_IDENTICAL'
            ELSE 'DISCORDANT_SAME_TIMESTAMP'
        END AS consolidation_status
    FROM chart_fio2_raw
    WHERE fio2_fraction IS NOT NULL
    GROUP BY stay_id, charttime
    """
)


# =============================================================================
# Focused LABEVENTS extraction and blood-gas panel construction
# =============================================================================

con.execute(
    f"""
    CREATE TABLE lab_selected AS
    SELECT
        le.subject_id::BIGINT AS subject_id,
        le.hadm_id::BIGINT AS hadm_id,
        le.charttime::TIMESTAMP AS charttime,
        le.itemid::BIGINT AS itemid,
        le.value,
        le.valuenum,
        le.valueuom
    FROM read_csv_auto('{qpath(LABEVENTS_FILE)}', header=true) le
    INNER JOIN (
        SELECT DISTINCT
            subject_id,
            hadm_id,
            t0,
            analytical_end_time
        FROM cohort
    ) c
        ON le.subject_id = c.subject_id
       AND le.hadm_id = c.hadm_id
       AND le.charttime >= c.t0
       AND le.charttime < c.analytical_end_time
    WHERE le.itemid IN (
        {PAO2_LAB_ITEMID},
        {FIO2_LAB_ITEMID},
        {SPECIMEN_LAB_ITEMID}
    )
    """
)

con.execute(
    f"""
    CREATE TABLE lab_panels AS
    SELECT
        subject_id,
        hadm_id,
        charttime,
        median(valuenum) FILTER (
            WHERE itemid = {PAO2_LAB_ITEMID}
              AND valuenum IS NOT NULL
        ) AS pao2_mmHg,
        median(
            CASE
                WHEN itemid = {FIO2_LAB_ITEMID}
                 AND valuenum >= 0.21
                 AND valuenum <= 1
                    THEN valuenum
                WHEN itemid = {FIO2_LAB_ITEMID}
                 AND valuenum >= 21
                 AND valuenum <= 100
                    THEN valuenum / 100.0
                ELSE NULL
            END
        ) AS lab_fio2_fraction,
        string_agg(
            DISTINCT value,
            ' | '
            ORDER BY value
        ) FILTER (
            WHERE itemid = {SPECIMEN_LAB_ITEMID}
        ) AS specimen_values
    FROM lab_selected
    GROUP BY subject_id, hadm_id, charttime
    """
)

con.execute(
    """
    CREATE TABLE lab_panels_classified AS
    SELECT
        *,
        CASE
            WHEN specimen_values IS NULL THEN 'MISSING'
            WHEN upper(trim(specimen_values)) IN (
                'ART.',
                'ART',
                'ARTERIAL',
                'ARTERIAL BLOOD'
            ) THEN 'ARTERIAL'
            WHEN upper(trim(specimen_values)) IN (
                'VEN.',
                'VEN',
                'VENOUS',
                'VENOUS BLOOD'
            ) THEN 'VENOUS'
            WHEN upper(trim(specimen_values)) IN (
                'MIX.',
                'MIX',
                'MIXED',
                'MIXED VENOUS',
                'MIXED VENOUS BLOOD'
            ) THEN 'MIXED_VENOUS'
            WHEN upper(trim(specimen_values)) LIKE '%ARTERIAL%'
                THEN 'ARTERIAL'
            WHEN upper(trim(specimen_values)) LIKE '%VENOUS%'
                THEN 'VENOUS_OTHER'
            WHEN upper(trim(specimen_values)) LIKE '%CAPILLARY%'
                THEN 'CAPILLARY'
            ELSE 'OTHER'
        END AS specimen_class
    FROM lab_panels
    """
)

con.execute(
    """
    CREATE TABLE arterial_panels AS
    SELECT
        c.stay_id,
        p.subject_id,
        p.hadm_id,
        p.charttime AS pao2_charttime,
        p.pao2_mmHg,
        p.lab_fio2_fraction,
        p.specimen_values,
        p.specimen_class
    FROM lab_panels_classified p
    INNER JOIN cohort c
        ON p.subject_id = c.subject_id
       AND p.hadm_id = c.hadm_id
       AND p.charttime >= c.t0
       AND p.charttime < c.analytical_end_time
    WHERE p.pao2_mmHg IS NOT NULL
      AND p.pao2_mmHg > 0
      AND p.specimen_class = 'ARTERIAL'
    """
)


# =============================================================================
# Attach exact respiratory context to arterial PaO2
# =============================================================================

con.execute(
    """
    CREATE TABLE arterial_context_link AS
    SELECT
        p.*,
        r.respiratory_state,
        r.interval_start AS respiratory_interval_start,
        r.interval_end AS respiratory_interval_end,
        r.respiratory_source_time,
        row_number() OVER (
            PARTITION BY p.stay_id, p.pao2_charttime
            ORDER BY r.interval_start DESC
        ) AS respiratory_link_rank
    FROM arterial_panels p
    LEFT JOIN valid_respiratory_intervals r
        ON p.stay_id = r.stay_id
       AND p.pao2_charttime >= r.interval_start
       AND p.pao2_charttime < r.interval_end
    """
)

con.execute(
    """
    CREATE TABLE arterial_context AS
    SELECT
        stay_id,
        subject_id,
        hadm_id,
        pao2_charttime,
        pao2_mmHg,
        lab_fio2_fraction,
        specimen_values,
        specimen_class,
        respiratory_state,
        respiratory_interval_start,
        respiratory_interval_end,
        respiratory_source_time
    FROM arterial_context_link
    WHERE respiratory_link_rank = 1
       OR respiratory_link_rank IS NULL
    """
)


# =============================================================================
# Chart FiO2 candidates at or before PaO2
# =============================================================================

con.execute(
    """
    CREATE TABLE arterial_chart_candidates AS
    SELECT
        p.stay_id,
        p.pao2_charttime,
        exact.fio2_fraction AS exact_chart_fio2,
        previous.charttime AS previous_chart_fio2_time,
        previous.fio2_fraction AS previous_chart_fio2,
        CASE
            WHEN previous.charttime IS NOT NULL THEN
                date_diff(
                    'second',
                    previous.charttime,
                    p.pao2_charttime
                ) / 60.0
            ELSE NULL
        END AS previous_chart_minutes_before_pao2
    FROM arterial_context p

    LEFT JOIN chart_fio2_valid exact
        ON p.stay_id = exact.stay_id
       AND p.pao2_charttime = exact.charttime

    LEFT JOIN LATERAL (
        SELECT
            f.charttime,
            f.fio2_fraction
        FROM chart_fio2_valid f
        WHERE f.stay_id = p.stay_id
          AND f.charttime <= p.pao2_charttime
          AND p.respiratory_interval_start IS NOT NULL
          AND p.respiratory_interval_end IS NOT NULL
          AND f.charttime >= p.respiratory_interval_start
          AND f.charttime < p.respiratory_interval_end
        ORDER BY f.charttime DESC
        LIMIT 1
    ) previous ON TRUE
    """
)


# =============================================================================
# Apply primary and sensitivity matching policies
# =============================================================================

con.execute(
    """
    CREATE TABLE match_windows AS
    SELECT * FROM (
        VALUES (60), (120), (240)
    ) AS t(match_window_minutes)
    """
)

con.execute(
    """
    CREATE TABLE all_match_policies AS
    SELECT
        p.*,
        c.exact_chart_fio2,
        c.previous_chart_fio2_time,
        c.previous_chart_fio2,
        c.previous_chart_minutes_before_pao2,
        w.match_window_minutes,

        CASE
            WHEN p.lab_fio2_fraction IS NOT NULL
                THEN p.lab_fio2_fraction
            WHEN c.exact_chart_fio2 IS NOT NULL
                THEN c.exact_chart_fio2
            WHEN c.previous_chart_fio2 IS NOT NULL
             AND c.previous_chart_minutes_before_pao2
                 <= w.match_window_minutes
                THEN c.previous_chart_fio2
            ELSE NULL
        END AS selected_fio2_fraction,

        CASE
            WHEN p.lab_fio2_fraction IS NOT NULL
                THEN 'SAME_PANEL_LAB'
            WHEN c.exact_chart_fio2 IS NOT NULL
                THEN 'SAME_TIMESTAMP_CHART'
            WHEN c.previous_chart_fio2 IS NOT NULL
             AND c.previous_chart_minutes_before_pao2
                 <= w.match_window_minutes
                THEN 'PREVIOUS_CHART'
            ELSE 'NO_MATCH'
        END AS selected_fio2_source,

        CASE
            WHEN p.lab_fio2_fraction IS NOT NULL
                THEN 0.0
            WHEN c.exact_chart_fio2 IS NOT NULL
                THEN 0.0
            WHEN c.previous_chart_fio2 IS NOT NULL
             AND c.previous_chart_minutes_before_pao2
                 <= w.match_window_minutes
                THEN c.previous_chart_minutes_before_pao2
            ELSE NULL
        END AS selected_fio2_minutes_before_pao2

    FROM arterial_context p
    LEFT JOIN arterial_chart_candidates c
        ON p.stay_id = c.stay_id
       AND p.pao2_charttime = c.pao2_charttime
    CROSS JOIN match_windows w
    """
)

con.execute(
    f"""
    CREATE TABLE primary_matches AS
    SELECT
        stay_id,
        subject_id,
        hadm_id,
        pao2_charttime,
        pao2_mmHg,
        specimen_values,
        specimen_class,

        respiratory_state,
        respiratory_interval_start,
        respiratory_interval_end,
        respiratory_source_time,

        lab_fio2_fraction,
        exact_chart_fio2,
        previous_chart_fio2_time,
        previous_chart_fio2,
        previous_chart_minutes_before_pao2,

        selected_fio2_fraction,
        selected_fio2_source,
        selected_fio2_minutes_before_pao2,

        CASE
            WHEN selected_fio2_fraction IS NOT NULL THEN
                pao2_mmHg / selected_fio2_fraction
            ELSE NULL
        END AS pf_ratio,

        CASE
            WHEN selected_fio2_fraction IS NULL
                THEN 'NO_MATCH'
            WHEN selected_fio2_source = 'SAME_PANEL_LAB'
                THEN 'DIRECT_SAME_PANEL'
            WHEN selected_fio2_source = 'SAME_TIMESTAMP_CHART'
                THEN 'DIRECT_SAME_TIMESTAMP'
            ELSE 'PREVIOUS_WITHIN_120_MIN'
        END AS matching_status,

        {PRIMARY_MATCH_WINDOW_MINUTES}::INTEGER
            AS primary_match_window_minutes

    FROM all_match_policies
    WHERE match_window_minutes = {PRIMARY_MATCH_WINDOW_MINUTES}
    """
)


# =============================================================================
# Canonical output 1: event-level primary matches
# =============================================================================

con.execute(
    f"""
    COPY (
        SELECT *
        FROM primary_matches
        ORDER BY stay_id, pao2_charttime
    )
    TO '{qpath(OUT_MATCHES)}'
    (FORMAT PARQUET, COMPRESSION ZSTD)
    """
)


# =============================================================================
# Canonical output 2: policy sensitivity
# =============================================================================

con.execute(
    """
    CREATE TABLE matching_sensitivity AS
    SELECT
        match_window_minutes,
        CASE
            WHEN match_window_minutes = 120 THEN 1
            ELSE 0
        END AS primary_policy_flag,

        COUNT(*) AS arterial_pao2_events_n,
        COUNT(DISTINCT stay_id) AS arterial_pao2_stays_n,

        COUNT(*) FILTER (
            WHERE selected_fio2_source <> 'NO_MATCH'
        ) AS matched_events_n,

        COUNT(DISTINCT stay_id) FILTER (
            WHERE selected_fio2_source <> 'NO_MATCH'
        ) AS matched_stays_n,

        100.0 * COUNT(*) FILTER (
            WHERE selected_fio2_source <> 'NO_MATCH'
        ) / COUNT(*) AS matched_events_pct,

        COUNT(*) FILTER (
            WHERE selected_fio2_source = 'SAME_PANEL_LAB'
        ) AS same_panel_lab_n,

        COUNT(*) FILTER (
            WHERE selected_fio2_source = 'SAME_TIMESTAMP_CHART'
        ) AS same_timestamp_chart_n,

        COUNT(*) FILTER (
            WHERE selected_fio2_source = 'PREVIOUS_CHART'
        ) AS previous_chart_n,

        COUNT(*) FILTER (
            WHERE selected_fio2_source = 'NO_MATCH'
        ) AS no_match_n,

        median(selected_fio2_minutes_before_pao2) FILTER (
            WHERE selected_fio2_source = 'PREVIOUS_CHART'
        ) AS median_previous_chart_minutes,

        quantile_cont(
            selected_fio2_minutes_before_pao2,
            0.95
        ) FILTER (
            WHERE selected_fio2_source = 'PREVIOUS_CHART'
        ) AS p95_previous_chart_minutes,

        median(
            pao2_mmHg / selected_fio2_fraction
        ) FILTER (
            WHERE selected_fio2_fraction IS NOT NULL
        ) AS median_pf_ratio,

        quantile_cont(
            pao2_mmHg / selected_fio2_fraction,
            0.25
        ) FILTER (
            WHERE selected_fio2_fraction IS NOT NULL
        ) AS p25_pf_ratio,

        quantile_cont(
            pao2_mmHg / selected_fio2_fraction,
            0.75
        ) FILTER (
            WHERE selected_fio2_fraction IS NOT NULL
        ) AS p75_pf_ratio,

        COUNT(*) FILTER (
            WHERE pao2_mmHg / selected_fio2_fraction < 300
        ) AS pf_below_300_n,

        COUNT(*) FILTER (
            WHERE pao2_mmHg / selected_fio2_fraction < 200
        ) AS pf_below_200_n,

        COUNT(*) FILTER (
            WHERE pao2_mmHg / selected_fio2_fraction < 100
        ) AS pf_below_100_n

    FROM all_match_policies
    GROUP BY match_window_minutes
    """
)

con.execute(
    f"""
    COPY (
        SELECT *
        FROM matching_sensitivity
        ORDER BY match_window_minutes
    )
    TO '{qpath(OUT_SENSITIVITY)}'
    (HEADER, DELIMITER ',')
    """
)


# =============================================================================
# Canonical output 3: long-format audit
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

# Design and cohort.
con.executemany(
    "INSERT INTO audit VALUES (?, ?, ?, ?, ?)",
    [
        ("DESIGN", None, "internal_version", 13, "v13"),
        ("DESIGN", None, "primary_match_window_minutes", 120, None),
        (
            "DESIGN",
            None,
            "fio2_role",
            None,
            "Technical input for PaO2/FiO2 trajectory construction",
        ),
        (
            "DESIGN",
            None,
            "future_to_past_assignment_flag",
            0,
            "No backward fill from future FiO2 values",
        ),
        (
            "DESIGN",
            None,
            "cross_respiratory_interval_matching_flag",
            0,
            "Previous chart FiO2 constrained to the same respiratory interval",
        ),
        (
            "DESIGN",
            None,
            "mixed_specimen_primary_inclusion_flag",
            0,
            "Only clearly arterial specimen panels retained",
        ),
        ("COHORT", None, "cohort_n", EXPECTED_COHORT_N, None),
    ],
)

# Event-level completeness.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'PRIMARY_MATCH',
        NULL,
        'arterial_pao2_events_n',
        COUNT(*)::DOUBLE,
        NULL
    FROM primary_matches
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'PRIMARY_MATCH',
        NULL,
        'arterial_pao2_stays_n',
        COUNT(DISTINCT stay_id)::DOUBLE,
        NULL
    FROM primary_matches
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'PRIMARY_MATCH',
        NULL,
        'matched_events_n',
        COUNT(*) FILTER (
            WHERE selected_fio2_source <> 'NO_MATCH'
        )::DOUBLE,
        NULL
    FROM primary_matches
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'PRIMARY_MATCH',
        NULL,
        'matched_stays_n',
        COUNT(DISTINCT stay_id) FILTER (
            WHERE selected_fio2_source <> 'NO_MATCH'
        )::DOUBLE,
        NULL
    FROM primary_matches
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'PRIMARY_MATCH',
        NULL,
        'matched_events_pct',
        100.0 * COUNT(*) FILTER (
            WHERE selected_fio2_source <> 'NO_MATCH'
        ) / COUNT(*),
        NULL
    FROM primary_matches
    """
)

# Primary source counts.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'MATCH_SOURCE',
        selected_fio2_source,
        'events_n',
        COUNT(*)::DOUBLE,
        NULL
    FROM primary_matches
    GROUP BY selected_fio2_source
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'MATCH_SOURCE',
        selected_fio2_source,
        'stays_n',
        COUNT(DISTINCT stay_id)::DOUBLE,
        NULL
    FROM primary_matches
    GROUP BY selected_fio2_source
    """
)

# Respiratory-state context.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'RESPIRATORY_STATE',
        coalesce(respiratory_state, 'UNCOVERED'),
        'arterial_pao2_events_n',
        COUNT(*)::DOUBLE,
        NULL
    FROM primary_matches
    GROUP BY respiratory_state
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'RESPIRATORY_STATE',
        coalesce(respiratory_state, 'UNCOVERED'),
        'matched_events_pct',
        100.0 * COUNT(*) FILTER (
            WHERE selected_fio2_source <> 'NO_MATCH'
        ) / COUNT(*),
        NULL
    FROM primary_matches
    GROUP BY respiratory_state
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'RESPIRATORY_STATE',
        coalesce(respiratory_state, 'UNCOVERED'),
        'median_pf_ratio',
        median(pf_ratio),
        NULL
    FROM primary_matches
    GROUP BY respiratory_state
    """
)

# Chart normalization.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'NORMALIZATION_CHART',
        value_class,
        'rows_n',
        COUNT(*)::DOUBLE,
        NULL
    FROM chart_fio2_raw
    GROUP BY value_class
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'NORMALIZATION_CHART',
        value_class,
        'stays_n',
        COUNT(DISTINCT stay_id)::DOUBLE,
        NULL
    FROM chart_fio2_raw
    GROUP BY value_class
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'NORMALIZATION_CHART',
        value_class,
        'retained_rows_n',
        COUNT(*) FILTER (
            WHERE fio2_fraction IS NOT NULL
        )::DOUBLE,
        NULL
    FROM chart_fio2_raw
    GROUP BY value_class
    """
)

# Lab FiO2 normalization.
con.execute(
    f"""
    CREATE TABLE lab_fio2_normalization AS
    SELECT
        CASE
            WHEN valuenum IS NULL THEN 'NONNUMERIC_OR_NULL'
            WHEN valuenum <= 0 THEN 'ZERO_OR_NEGATIVE'
            WHEN valuenum > 0 AND valuenum < 0.21
                THEN 'FRACTION_LOW'
            WHEN valuenum >= 0.21 AND valuenum <= 1
                THEN 'FRACTION_VALID'
            WHEN valuenum > 1 AND valuenum < 21
                THEN 'LOW_INTEGER_SUSPECT'
            WHEN valuenum >= 21 AND valuenum <= 100
                THEN 'PERCENT_VALID'
            WHEN valuenum > 100 THEN 'ABOVE_100'
            ELSE 'UNRESOLVED'
        END AS value_class,
        *
    FROM lab_selected
    WHERE itemid = {FIO2_LAB_ITEMID}
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'NORMALIZATION_LAB',
        value_class,
        'rows_n',
        COUNT(*)::DOUBLE,
        NULL
    FROM lab_fio2_normalization
    GROUP BY value_class
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'NORMALIZATION_LAB',
        value_class,
        'retained_rows_n',
        COUNT(*) FILTER (
            WHERE
                (valuenum >= 0.21 AND valuenum <= 1)
                OR
                (valuenum >= 21 AND valuenum <= 100)
        )::DOUBLE,
        NULL
    FROM lab_fio2_normalization
    GROUP BY value_class
    """
)

# Specimen audit.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'SPECIMEN',
        specimen_class,
        'panels_n',
        COUNT(*)::DOUBLE,
        NULL
    FROM lab_panels_classified
    GROUP BY specimen_class
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'SPECIMEN',
        specimen_class,
        'panels_with_pao2_n',
        COUNT(*) FILTER (
            WHERE pao2_mmHg IS NOT NULL
        )::DOUBLE,
        NULL
    FROM lab_panels_classified
    GROUP BY specimen_class
    """
)

# Same-panel lab versus exact chart agreement.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'SOURCE_AGREEMENT',
        NULL,
        'same_timestamp_pairs_n',
        COUNT(*) FILTER (
            WHERE lab_fio2_fraction IS NOT NULL
              AND exact_chart_fio2 IS NOT NULL
        )::DOUBLE,
        NULL
    FROM primary_matches
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'SOURCE_AGREEMENT',
        NULL,
        'median_absolute_difference',
        median(
            abs(lab_fio2_fraction - exact_chart_fio2)
        ) FILTER (
            WHERE lab_fio2_fraction IS NOT NULL
              AND exact_chart_fio2 IS NOT NULL
        ),
        NULL
    FROM primary_matches
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'SOURCE_AGREEMENT',
        NULL,
        'p95_absolute_difference',
        quantile_cont(
            abs(lab_fio2_fraction - exact_chart_fio2),
            0.95
        ) FILTER (
            WHERE lab_fio2_fraction IS NOT NULL
              AND exact_chart_fio2 IS NOT NULL
        ),
        NULL
    FROM primary_matches
    """
)

# P/F summary is descriptive only.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'PF_DESCRIPTIVE',
        NULL,
        'median_pf_ratio',
        median(pf_ratio),
        NULL
    FROM primary_matches
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'PF_DESCRIPTIVE',
        NULL,
        'p25_pf_ratio',
        quantile_cont(pf_ratio, 0.25),
        NULL
    FROM primary_matches
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'PF_DESCRIPTIVE',
        NULL,
        'p75_pf_ratio',
        quantile_cont(pf_ratio, 0.75),
        NULL
    FROM primary_matches
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

arterial_event_n = con.execute(
    "SELECT COUNT(*) FROM primary_matches"
).fetchone()[0]

if arterial_event_n == 0:
    raise RuntimeError("No clearly arterial PaO2 events were identified.")

if arterial_event_n != con.execute(
    "SELECT COUNT(*) FROM arterial_panels"
).fetchone()[0]:
    raise RuntimeError("Arterial event conservation failed.")

duplicate_primary_events = con.execute(
    """
    SELECT COUNT(*)
    FROM (
        SELECT
            stay_id,
            pao2_charttime,
            COUNT(*) AS rows_n
        FROM primary_matches
        GROUP BY stay_id, pao2_charttime
        HAVING COUNT(*) > 1
    )
    """
).fetchone()[0]

if duplicate_primary_events:
    raise RuntimeError(
        f"Duplicate primary PaO2 events: {duplicate_primary_events}."
    )

invalid_selected_fio2 = con.execute(
    """
    SELECT COUNT(*)
    FROM primary_matches
    WHERE selected_fio2_fraction IS NOT NULL
      AND (
            selected_fio2_fraction < 0.21
            OR selected_fio2_fraction > 1.0
      )
    """
).fetchone()[0]

if invalid_selected_fio2:
    raise RuntimeError(
        f"Invalid selected FiO2 values: {invalid_selected_fio2}."
    )

invalid_pf = con.execute(
    """
    SELECT COUNT(*)
    FROM primary_matches
    WHERE pf_ratio IS NOT NULL
      AND pf_ratio <= 0
    """
).fetchone()[0]

if invalid_pf:
    raise RuntimeError(f"Invalid P/F ratios: {invalid_pf}.")

future_matches = con.execute(
    """
    SELECT COUNT(*)
    FROM primary_matches
    WHERE selected_fio2_source = 'PREVIOUS_CHART'
      AND previous_chart_fio2_time > pao2_charttime
    """
).fetchone()[0]

if future_matches:
    raise RuntimeError(f"Future FiO2 matches: {future_matches}.")

cross_interval_matches = con.execute(
    """
    SELECT COUNT(*)
    FROM primary_matches
    WHERE selected_fio2_source = 'PREVIOUS_CHART'
      AND (
            respiratory_interval_start IS NULL
            OR respiratory_interval_end IS NULL
            OR previous_chart_fio2_time < respiratory_interval_start
            OR previous_chart_fio2_time >= respiratory_interval_end
      )
    """
).fetchone()[0]

if cross_interval_matches:
    raise RuntimeError(
        f"Cross-interval previous FiO2 matches: {cross_interval_matches}."
    )

excessive_gap_matches = con.execute(
    f"""
    SELECT COUNT(*)
    FROM primary_matches
    WHERE selected_fio2_source = 'PREVIOUS_CHART'
      AND selected_fio2_minutes_before_pao2
          > {PRIMARY_MATCH_WINDOW_MINUTES}
    """
).fetchone()[0]

if excessive_gap_matches:
    raise RuntimeError(
        f"Previous FiO2 matches beyond primary window: {excessive_gap_matches}."
    )

sensitivity_rows_n = con.execute(
    "SELECT COUNT(*) FROM matching_sensitivity"
).fetchone()[0]

if sensitivity_rows_n != len(SENSITIVITY_WINDOWS_MINUTES):
    raise RuntimeError(
        f"Unexpected sensitivity rows: {sensitivity_rows_n}."
    )


# =============================================================================
# Console report
# =============================================================================

print("=" * 108)
print("TRAJECTORIES - CANONICAL FIO2 NORMALIZATION AND PAO2 MATCHING V13")
print("=" * 108)

print("Primary 120-minute matching policy")
print("----------------------------------")
print(
    con.execute(
        """
        SELECT
            COUNT(*) AS arterial_pao2_events_n,
            COUNT(DISTINCT stay_id) AS arterial_pao2_stays_n,
            COUNT(*) FILTER (
                WHERE selected_fio2_source <> 'NO_MATCH'
            ) AS matched_events_n,
            COUNT(DISTINCT stay_id) FILTER (
                WHERE selected_fio2_source <> 'NO_MATCH'
            ) AS matched_stays_n,
            round(
                100.0 * COUNT(*) FILTER (
                    WHERE selected_fio2_source <> 'NO_MATCH'
                ) / COUNT(*),
                2
            ) AS matched_events_pct,
            COUNT(*) FILTER (
                WHERE selected_fio2_source = 'SAME_PANEL_LAB'
            ) AS same_panel_lab_n,
            COUNT(*) FILTER (
                WHERE selected_fio2_source = 'SAME_TIMESTAMP_CHART'
            ) AS same_timestamp_chart_n,
            COUNT(*) FILTER (
                WHERE selected_fio2_source = 'PREVIOUS_CHART'
            ) AS previous_chart_n,
            COUNT(*) FILTER (
                WHERE selected_fio2_source = 'NO_MATCH'
            ) AS no_match_n,
            round(median(pf_ratio), 1) AS median_pf_ratio,
            round(quantile_cont(pf_ratio, 0.25), 1) AS p25_pf_ratio,
            round(quantile_cont(pf_ratio, 0.75), 1) AS p75_pf_ratio
        FROM primary_matches
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Sensitivity policies")
print("--------------------")
print(
    con.execute(
        """
        SELECT
            match_window_minutes,
            primary_policy_flag,
            arterial_pao2_events_n,
            matched_events_n,
            matched_stays_n,
            round(matched_events_pct, 2) AS matched_events_pct,
            same_panel_lab_n,
            same_timestamp_chart_n,
            previous_chart_n,
            no_match_n,
            round(median_previous_chart_minutes, 1)
                AS median_previous_chart_minutes,
            round(p95_previous_chart_minutes, 1)
                AS p95_previous_chart_minutes,
            round(median_pf_ratio, 1) AS median_pf_ratio,
            round(p25_pf_ratio, 1) AS p25_pf_ratio,
            round(p75_pf_ratio, 1) AS p75_pf_ratio
        FROM matching_sensitivity
        ORDER BY match_window_minutes
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Primary matching by respiratory state")
print("-------------------------------------")
print(
    con.execute(
        """
        SELECT
            coalesce(respiratory_state, 'UNCOVERED')
                AS respiratory_state,
            COUNT(*) AS arterial_pao2_events_n,
            COUNT(*) FILTER (
                WHERE selected_fio2_source <> 'NO_MATCH'
            ) AS matched_events_n,
            round(
                100.0 * COUNT(*) FILTER (
                    WHERE selected_fio2_source <> 'NO_MATCH'
                ) / COUNT(*),
                2
            ) AS matched_events_pct,
            round(median(pf_ratio), 1) AS median_pf_ratio
        FROM primary_matches
        GROUP BY respiratory_state
        ORDER BY arterial_pao2_events_n DESC
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Canonical outputs")
print("-----------------")
for path in (OUT_MATCHES, OUT_SENSITIVITY, OUT_AUDIT):
    print(path)

print()
print("Canonical FiO2 component completed successfully.")
print("The subsequent component may construct PaO2/FiO2 trajectories.")
