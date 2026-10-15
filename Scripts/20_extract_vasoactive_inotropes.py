#!/usr/bin/env python3
"""
Step 01.18 - canonical postoperative vasoactive/inotropic infusion trajectories
for the frozen TRAJECTORIES cohort in MIMIC-IV 3.1.

This production extractor creates complete 1-hour and 6-hour trajectory grids,
a 24-hour stay summary, and reproducibility audits. Primary values remain
uncapped; p99.9-capped values and within-drug SUM values are retained only as
prespecified sensitivity fields.

Frozen primary rules
--------------------
* observation horizon: postoperative time zero to the earlier of 24 hours and
  the frozen observation end;
* source intervals are half-open [starttime, endtime);
* positive-duration rows with positive rates and expected units are retained;
* ChangeDose/Rate, FinishedRunning, Stopped, and Paused rows are retained for
  their recorded interval; rows explicitly labelled Bolus are excluded;
* concurrent rows for the same drug are represented by their maximum dose;
* all seven drugs remain individually available;
* standard VIS and prespecified NEE are computed separately;
* the primary trajectory is uncapped and not winsorized;
* SUM-within-drug overlap and empirical p99.9 dose caps are exported only as
  sensitivity variables;
* capped contributions are recalculated only when a row is truly above its
  drug-specific p99.9 threshold. Rows at or below the threshold retain the
  original contribution exactly.

Standard VIS
------------
VIS = dopamine + dobutamine + 100*epinephrine + 10*milrinone
      + 10,000*vasopressin + 100*norepinephrine

All non-vasopressin rates are in mcg/kg/min. Vasopressin is normalized to
units/kg/min for VIS.

Prespecified NEE
----------------
NEE = norepinephrine + epinephrine + dopamine/100 + phenylephrine/10
      + 2.5*vasopressin

Vasopressin is in units/min for NEE. Dobutamine and milrinone do not enter NEE.

TWA is the primary within-window intensity representation. Peak is retained as
a secondary clinically interpretable and diagnostic representation. The script
reads only the frozen cohort and raw MIMIC-IV inputevents. Inputs are never
modified or deleted.

Inputs
------
data/00_04_final_analytic_cohort.csv
Raw MIMIC-IV 3.1 files relative to MIMIC_ROOT:
  icu/inputevents.csv.gz

Configuration uses the existing --cohort, --mimic-root, and --output-dir
options; --mimic-root must identify the local MIMIC-IV 3.1 directory.

Outputs
-------
01_18_vasoactive_inotrope_hourly_trajectory.parquet
01_18_vasoactive_inotrope_six_hour_trajectory.parquet
01_18_vasoactive_inotrope_stay_summary.csv
01_18_vasoactive_inotrope_trajectory_audit.csv
01_18_vasoactive_inotrope_extreme_windows.csv
All outputs are written to --output-dir (default: the project data directory).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    import duckdb
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "Missing dependency 'duckdb'. Install it in the project environment."
    ) from exc


EXPECTED_COHORT_N = 9305

CANDIDATE_DRUGS = (
    (221906, "NOREPINEPHRINE", "norepinephrine", "VASOPRESSOR_NEE", 1),
    (221289, "EPINEPHRINE", "epinephrine", "VASOPRESSOR_NEE", 1),
    (221749, "PHENYLEPHRINE", "phenylephrine", "VASOPRESSOR_NEE", 0),
    (221662, "DOPAMINE", "dopamine", "VASOPRESSOR_NEE", 1),
    (222315, "VASOPRESSIN", "vasopressin", "VASOPRESSOR_NEE", 1),
    (221653, "DOBUTAMINE", "dobutamine", "INOTROPE_INODILATOR", 1),
    (221986, "MILRINONE", "milrinone", "INOTROPE_INODILATOR", 1),
)


def parse_args() -> argparse.Namespace:
    script_path = Path(__file__).resolve()
    project_root = (
        script_path.parent.parent
        if script_path.parent.name.lower() == "scripts"
        else script_path.parent
    )
    parser = argparse.ArgumentParser(
        description="Construct canonical 0-24 h vasoactive trajectories."
    )
    parser.add_argument(
        "--cohort",
        type=Path,
        default=project_root / "data" / "00_04_final_analytic_cohort.csv",
        help="Frozen final cohort CSV.",
    )
    parser.add_argument(
        "--mimic-root",
        type=Path,
        default=Path("path/to/mimic-iv-3.1"),
        help="MIMIC-IV 3.1 root containing the icu directory.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=project_root / "data",
        help="Directory for canonical outputs.",
    )
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--memory-limit", default="16GB")
    return parser.parse_args()


def sql_string(value: Path | str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def require_file(path: Path, label: str) -> Path:
    path = path.expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"{label} not found: {path}")
    return path


def source_columns(con: "duckdb.DuckDBPyConnection", path: Path) -> set[str]:
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_csv_auto({sql_string(path)}, header=true)"
    ).fetchall()
    return {str(row[0]).lower() for row in rows}


def require_columns(actual: set[str], required: set[str], label: str) -> None:
    missing = sorted(required - actual)
    if missing:
        raise RuntimeError(f"{label} is missing columns: {', '.join(missing)}")


def copy_query_to_file(
    con: "duckdb.DuckDBPyConnection",
    query: str,
    output_path: Path,
    file_format: str,
) -> None:
    if file_format == "PARQUET":
        options = "FORMAT PARQUET, COMPRESSION ZSTD"
    elif file_format == "CSV":
        options = "FORMAT CSV, HEADER TRUE, DELIMITER ','"
    else:  # pragma: no cover
        raise ValueError(f"Unsupported output format: {file_format}")
    con.execute(f"COPY ({query}) TO {sql_string(output_path)} ({options})")


def terminal_value(value: object) -> str:
    if value is None:
        return "<NULL>"
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value).replace("\t", " ").replace("\r", " ").replace("\n", " ")


def print_query_table(
    con: "duckdb.DuckDBPyConnection",
    title: str,
    query: str,
    max_rows: int = 200,
) -> None:
    relation = con.execute(query)
    columns = [str(column[0]) for column in relation.description]
    rows = relation.fetchmany(max_rows + 1)
    print(f"\n=== {title} ===")
    print("\t".join(columns))
    for row in rows[:max_rows]:
        print("\t".join(terminal_value(value) for value in row))
    if len(rows) > max_rows:
        print(f"... OUTPUT TRUNCATED AFTER {max_rows} ROWS ...")


def main() -> int:
    args = parse_args()
    if args.threads < 1:
        raise ValueError("--threads must be at least 1")

    cohort_path = require_file(args.cohort, "Frozen cohort")
    inputevents_path = require_file(
        args.mimic_root / "icu" / "inputevents.csv.gz", "inputevents"
    )
    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    outputs = {
        "hourly": output_dir / "01_18_vasoactive_inotrope_hourly_trajectory.parquet",
        "six_hour": output_dir / "01_18_vasoactive_inotrope_six_hour_trajectory.parquet",
        "stay_summary": output_dir / "01_18_vasoactive_inotrope_stay_summary.csv",
        "audit": output_dir / "01_18_vasoactive_inotrope_trajectory_audit.csv",
        "extreme_windows": output_dir / "01_18_vasoactive_inotrope_extreme_windows.csv",
    }

    print("Step 01.18 - canonical vasoactive/inotrope trajectories")
    print(f"Cohort:      {cohort_path}")
    print(f"Inputevents: {inputevents_path}")
    print(f"Output dir:  {output_dir}")

    con = duckdb.connect(database=":memory:")
    con.execute(f"SET threads = {int(args.threads)}")
    con.execute(f"SET memory_limit = {sql_string(args.memory_limit)}")
    con.execute("SET preserve_insertion_order = false")

    cohort_required = {
        "subject_id",
        "hadm_id",
        "stay_id",
        "surgery_group",
        "postoperative_time_zero",
        "observation_end_time",
        "observation_end_type",
    }
    inputevents_required = {
        "subject_id",
        "hadm_id",
        "stay_id",
        "starttime",
        "endtime",
        "itemid",
        "rate",
        "rateuom",
        "orderid",
        "linkorderid",
        "patientweight",
        "statusdescription",
    }
    print("Checking input schemas...")
    require_columns(source_columns(con, cohort_path), cohort_required, "Cohort")
    require_columns(
        source_columns(con, inputevents_path), inputevents_required, "inputevents"
    )

    con.execute(
        f"""
        CREATE TEMP TABLE cohort AS
        SELECT
            CAST(subject_id AS BIGINT) AS subject_id,
            CAST(hadm_id AS BIGINT) AS hadm_id,
            CAST(stay_id AS BIGINT) AS stay_id,
            CAST(surgery_group AS VARCHAR) AS surgery_group,
            CAST(postoperative_time_zero AS TIMESTAMP) AS postoperative_time_zero,
            CAST(observation_end_time AS TIMESTAMP) AS observation_end_time,
            CAST(observation_end_type AS VARCHAR) AS observation_end_type,
            LEAST(
                CAST(postoperative_time_zero AS TIMESTAMP) + INTERVAL '24 hours',
                CAST(observation_end_time AS TIMESTAMP)
            ) AS analysis_end_time
        FROM read_csv_auto({sql_string(cohort_path)}, header=true)
        """
    )
    cohort_n, unique_stays_n, invalid_windows_n = con.execute(
        """
        SELECT
            COUNT(*),
            COUNT(DISTINCT stay_id),
            COUNT(*) FILTER (
                WHERE postoperative_time_zero IS NULL
                   OR analysis_end_time IS NULL
                   OR analysis_end_time <= postoperative_time_zero
            )
        FROM cohort
        """
    ).fetchone()
    if cohort_n != EXPECTED_COHORT_N:
        raise RuntimeError(
            f"Cohort has {cohort_n:,} rows; expected {EXPECTED_COHORT_N:,}."
        )
    if unique_stays_n != cohort_n:
        raise RuntimeError("Cohort stay_id is not unique.")
    if invalid_windows_n:
        raise RuntimeError(f"Cohort contains {invalid_windows_n:,} invalid windows.")

    values_sql = ",\n".join(
        f"({itemid}, '{drug}', '{slug}', '{drug_class}', {vis_flag})"
        for itemid, drug, slug, drug_class, vis_flag in CANDIDATE_DRUGS
    )
    con.execute(
        f"""
        CREATE TEMP TABLE candidate_drugs (
            itemid BIGINT,
            drug_name VARCHAR,
            drug_slug VARCHAR,
            drug_class VARCHAR,
            vis_standard_component_flag INTEGER
        );
        INSERT INTO candidate_drugs VALUES {values_sql}
        """
    )

    print("Extracting, clipping, validating, and normalizing source intervals...")
    con.execute(
        f"""
        CREATE TEMP TABLE window_events AS
        WITH selected AS (
            SELECT
                CAST(subject_id AS BIGINT) AS subject_id,
                CAST(hadm_id AS BIGINT) AS hadm_id,
                CAST(stay_id AS BIGINT) AS stay_id,
                CAST(starttime AS TIMESTAMP) AS starttime,
                CAST(endtime AS TIMESTAMP) AS endtime,
                CAST(itemid AS BIGINT) AS itemid,
                CAST(rate AS DOUBLE) AS rate,
                CAST(rateuom AS VARCHAR) AS rateuom,
                CAST(orderid AS BIGINT) AS orderid,
                CAST(linkorderid AS BIGINT) AS linkorderid,
                CAST(patientweight AS DOUBLE) AS patientweight,
                CAST(statusdescription AS VARCHAR) AS statusdescription
            FROM read_csv_auto({sql_string(inputevents_path)}, header=true)
            WHERE CAST(itemid AS BIGINT) IN (
                {', '.join(str(row[0]) for row in CANDIDATE_DRUGS)}
            )
        )
        SELECT
            c.subject_id,
            c.hadm_id,
            c.stay_id,
            c.surgery_group,
            c.postoperative_time_zero,
            c.analysis_end_time,
            c.observation_end_type,
            d.drug_name,
            d.drug_slug,
            d.drug_class,
            d.vis_standard_component_flag,
            s.* EXCLUDE (subject_id, hadm_id, stay_id),
            GREATEST(s.starttime, c.postoperative_time_zero) AS clipped_starttime,
            LEAST(s.endtime, c.analysis_end_time) AS clipped_endtime
        FROM selected AS s
        INNER JOIN cohort AS c
            ON s.subject_id = c.subject_id
           AND s.hadm_id = c.hadm_id
           AND s.stay_id = c.stay_id
        INNER JOIN candidate_drugs AS d USING (itemid)
        WHERE s.starttime < c.analysis_end_time
          AND s.endtime > c.postoperative_time_zero
        """
    )

    con.execute(
        """
        CREATE TEMP TABLE classified_events AS
        SELECT
            *,
            date_diff('microsecond', clipped_starttime, clipped_endtime)
                / 60000000.0 AS clipped_duration_minutes,
            concat_ws(
                '|',
                CASE WHEN endtime IS NULL OR endtime <= starttime
                     THEN 'INVALID_RECORDED_DURATION' END,
                CASE WHEN clipped_endtime <= clipped_starttime
                     THEN 'INVALID_CLIPPED_DURATION' END,
                CASE WHEN rate IS NULL THEN 'MISSING_RATE' END,
                CASE WHEN rate <= 0 THEN 'NONPOSITIVE_RATE' END,
                CASE
                    WHEN drug_name IN (
                        'NOREPINEPHRINE', 'EPINEPHRINE', 'PHENYLEPHRINE',
                        'DOPAMINE', 'DOBUTAMINE', 'MILRINONE'
                    ) AND coalesce(rateuom, '<NULL>') <> 'mcg/kg/min'
                    THEN 'UNEXPECTED_RATE_UNIT'
                    WHEN drug_name = 'VASOPRESSIN'
                     AND coalesce(rateuom, '<NULL>') NOT IN (
                         'units/hour', 'units/min'
                     )
                    THEN 'UNEXPECTED_RATE_UNIT'
                END,
                CASE
                    WHEN drug_name = 'VASOPRESSIN'
                     AND (patientweight IS NULL OR patientweight <= 0)
                    THEN 'INVALID_WEIGHT_FOR_VIS'
                END,
                CASE WHEN statusdescription = 'Bolus' THEN 'BOLUS_STATUS' END
            ) AS exclusion_reason
        FROM window_events
        """
    )

    con.execute(
        """
        CREATE TEMP TABLE normalized_events_pre_cap AS
        SELECT
            *,
            CASE
                WHEN drug_name = 'VASOPRESSIN' AND rateuom = 'units/hour'
                    THEN rate / 60.0 / patientweight
                WHEN drug_name = 'VASOPRESSIN' AND rateuom = 'units/min'
                    THEN rate / patientweight
                ELSE rate
            END AS normalized_dose,
            CASE
                WHEN drug_name = 'DOPAMINE' THEN rate
                WHEN drug_name = 'DOBUTAMINE' THEN rate
                WHEN drug_name = 'EPINEPHRINE' THEN 100.0 * rate
                WHEN drug_name = 'MILRINONE' THEN 10.0 * rate
                WHEN drug_name = 'NOREPINEPHRINE' THEN 100.0 * rate
                WHEN drug_name = 'VASOPRESSIN' AND rateuom = 'units/hour'
                    THEN 10000.0 * rate / 60.0 / patientweight
                WHEN drug_name = 'VASOPRESSIN' AND rateuom = 'units/min'
                    THEN 10000.0 * rate / patientweight
                ELSE 0.0
            END AS vis_row_contribution,
            CASE
                WHEN drug_name = 'NOREPINEPHRINE' THEN rate
                WHEN drug_name = 'EPINEPHRINE' THEN rate
                WHEN drug_name = 'DOPAMINE' THEN rate / 100.0
                WHEN drug_name = 'PHENYLEPHRINE' THEN rate / 10.0
                WHEN drug_name = 'VASOPRESSIN' AND rateuom = 'units/hour'
                    THEN 2.5 * rate / 60.0
                WHEN drug_name = 'VASOPRESSIN' AND rateuom = 'units/min'
                    THEN 2.5 * rate
                ELSE 0.0
            END AS nee_row_contribution
        FROM classified_events
        WHERE exclusion_reason = ''
        """
    )

    con.execute(
        """
        CREATE TEMP TABLE dose_tail_thresholds AS
        SELECT
            drug_name,
            quantile_cont(normalized_dose, 0.999) AS dose_p999
        FROM normalized_events_pre_cap
        GROUP BY drug_name
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE normalized_events AS
        SELECT
            e.*,
            t.dose_p999,
            least(e.normalized_dose, t.dose_p999) AS normalized_dose_p999_capped,
            CASE
                WHEN e.normalized_dose > t.dose_p999
                    THEN e.vis_row_contribution * t.dose_p999
                        / e.normalized_dose
                ELSE e.vis_row_contribution
            END AS vis_row_contribution_p999_capped,
            CASE
                WHEN e.normalized_dose > t.dose_p999
                    THEN e.nee_row_contribution * t.dose_p999
                        / e.normalized_dose
                ELSE e.nee_row_contribution
            END AS nee_row_contribution_p999_capped,
            CAST(e.normalized_dose > t.dose_p999 AS INTEGER)
                AS above_p999_flag
        FROM normalized_events_pre_cap AS e
        INNER JOIN dose_tail_thresholds AS t USING (drug_name)
        """
    )

    print("Constructing atomic MAX-policy dose and score segments...")
    con.execute(
        """
        CREATE TEMP TABLE global_boundaries AS
        SELECT stay_id, postoperative_time_zero AS boundary_time FROM cohort
        UNION
        SELECT stay_id, analysis_end_time AS boundary_time FROM cohort
        UNION
        SELECT stay_id, clipped_starttime AS boundary_time FROM normalized_events
        UNION
        SELECT stay_id, clipped_endtime AS boundary_time FROM normalized_events
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE global_segments AS
        WITH ordered AS (
            SELECT
                stay_id,
                boundary_time AS segment_start,
                lead(boundary_time) OVER (
                    PARTITION BY stay_id ORDER BY boundary_time
                ) AS segment_end
            FROM global_boundaries
        )
        SELECT
            stay_id,
            segment_start,
            segment_end,
            date_diff('microsecond', segment_start, segment_end)
                / 60000000.0 AS segment_minutes
        FROM ordered
        WHERE segment_end > segment_start
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE segment_drug AS
        SELECT
            s.stay_id,
            s.segment_start,
            s.segment_end,
            e.drug_name,
            e.drug_slug,
            max(e.normalized_dose) AS dose_max_policy,
            sum(e.normalized_dose) AS dose_sum_overlap_sensitivity,
            max(e.normalized_dose_p999_capped) AS dose_p999_capped,
            max(e.vis_row_contribution) AS vis_max_component,
            sum(e.vis_row_contribution) AS vis_sum_overlap_component,
            max(e.vis_row_contribution_p999_capped) AS vis_p999_capped_component,
            max(e.nee_row_contribution) AS nee_max_component,
            sum(e.nee_row_contribution) AS nee_sum_overlap_component,
            max(e.nee_row_contribution_p999_capped) AS nee_p999_capped_component,
            count(*) AS concurrent_rows_n,
            max(e.above_p999_flag) AS above_p999_flag
        FROM global_segments AS s
        INNER JOIN normalized_events AS e
            ON s.stay_id = e.stay_id
           AND e.clipped_starttime < s.segment_end
           AND e.clipped_endtime > s.segment_start
        GROUP BY
            s.stay_id, s.segment_start, s.segment_end,
            e.drug_name, e.drug_slug
        """
    )

    drug_segment_columns = []
    for _, drug, slug, _, _ in CANDIDATE_DRUGS:
        drug_segment_columns.extend(
            [
                f"coalesce(max(sd.dose_max_policy) FILTER "
                f"(WHERE sd.drug_name = '{drug}'), 0.0) AS {slug}_dose",
                f"coalesce(max(sd.dose_p999_capped) FILTER "
                f"(WHERE sd.drug_name = '{drug}'), 0.0) "
                f"AS {slug}_dose_p999_capped",
                f"coalesce(max(sd.concurrent_rows_n) FILTER "
                f"(WHERE sd.drug_name = '{drug}'), 0) "
                f"AS {slug}_concurrent_rows_n",
            ]
        )
    con.execute(
        f"""
        CREATE TEMP TABLE score_segments AS
        SELECT
            g.stay_id,
            g.segment_start,
            g.segment_end,
            g.segment_minutes,
            coalesce(sum(sd.vis_max_component), 0.0) AS vis,
            coalesce(sum(sd.vis_sum_overlap_component), 0.0)
                AS vis_sum_overlap_sensitivity,
            coalesce(sum(sd.vis_p999_capped_component), 0.0)
                AS vis_p999_capped_sensitivity,
            coalesce(sum(sd.nee_max_component), 0.0) AS nee,
            coalesce(sum(sd.nee_sum_overlap_component), 0.0)
                AS nee_sum_overlap_sensitivity,
            coalesce(sum(sd.nee_p999_capped_component), 0.0)
                AS nee_p999_capped_sensitivity,
            coalesce(max(sd.concurrent_rows_n), 0) AS max_concurrent_rows,
            coalesce(max(sd.above_p999_flag), 0) AS any_above_p999_flag,
            {', '.join(drug_segment_columns)}
        FROM global_segments AS g
        LEFT JOIN segment_drug AS sd
            ON g.stay_id = sd.stay_id
           AND g.segment_start = sd.segment_start
           AND g.segment_end = sd.segment_end
        GROUP BY
            g.stay_id, g.segment_start, g.segment_end, g.segment_minutes
        """
    )

    print("Aggregating complete hourly and six-hour grids...")
    con.execute(
        """
        CREATE TEMP TABLE window_grid AS
        WITH resolution_spec AS (
            SELECT 'HOURLY' AS resolution, 60 AS width_minutes, 24 AS windows_n
            UNION ALL
            SELECT 'SIX_HOUR', 360, 4
        ), expanded AS (
            SELECT
                c.*,
                r.resolution,
                r.width_minutes,
                i AS window_index,
                c.postoperative_time_zero
                    + i * r.width_minutes * INTERVAL '1 minute' AS window_start,
                c.postoperative_time_zero
                    + (i + 1) * r.width_minutes * INTERVAL '1 minute'
                    AS window_end
            FROM cohort AS c
            CROSS JOIN resolution_spec AS r
            CROSS JOIN range(r.windows_n) AS x(i)
        )
        SELECT
            *,
            least(window_end, analysis_end_time) AS observed_window_end,
            CASE
                WHEN window_start >= analysis_end_time THEN 0.0
                ELSE date_diff(
                    'microsecond', window_start,
                    least(window_end, analysis_end_time)
                ) / 60000000.0
            END AS observable_minutes,
            CASE
                WHEN window_start >= analysis_end_time THEN 'NOT_OBSERVABLE'
                WHEN analysis_end_time < window_end THEN 'PARTIALLY_OBSERVABLE'
                ELSE 'FULLY_OBSERVABLE'
            END AS window_status,
            CASE
                WHEN resolution = 'HOURLY'
                    THEN printf('H%02d_H%02d', window_index, window_index + 1)
                ELSE printf('H%02d_H%02d', window_index * 6, (window_index + 1) * 6)
            END AS window_label
        FROM expanded
        """
    )
    con.execute(
        """
        CREATE TEMP TABLE window_segment_overlap AS
        SELECT
            w.resolution,
            w.stay_id,
            w.window_index,
            s.* EXCLUDE (stay_id),
            date_diff(
                'microsecond',
                greatest(w.window_start, s.segment_start),
                least(w.observed_window_end, s.segment_end)
            ) / 60000000.0 AS overlap_minutes
        FROM window_grid AS w
        INNER JOIN score_segments AS s
            ON w.stay_id = s.stay_id
           AND w.observable_minutes > 0
           AND s.segment_start < w.observed_window_end
           AND s.segment_end > w.window_start
        """
    )

    window_drug_columns = []
    for _, _, slug, _, _ in CANDIDATE_DRUGS:
        window_drug_columns.extend(
            [
                f"CASE WHEN w.observable_minutes > 0 THEN "
                f"coalesce(sum(o.{slug}_dose * o.overlap_minutes), 0.0) "
                f"/ w.observable_minutes END AS {slug}_twa_dose",
                f"CASE WHEN w.observable_minutes > 0 THEN "
                f"coalesce(max(o.{slug}_dose), 0.0) END AS {slug}_peak_dose",
                f"CASE WHEN w.observable_minutes > 0 THEN coalesce(sum(" 
                f"o.overlap_minutes) FILTER (WHERE o.{slug}_dose > 0), 0.0) "
                f"END AS {slug}_exposure_minutes",
                f"CASE WHEN w.observable_minutes > 0 THEN CAST(coalesce(max(" 
                f"CAST(o.{slug}_dose > 0 AS INTEGER)), 0) AS INTEGER) "
                f"END AS {slug}_exposed_flag",
            ]
        )

    con.execute(
        f"""
        CREATE TEMP TABLE trajectory_windows AS
        SELECT
            w.subject_id,
            w.hadm_id,
            w.stay_id,
            w.surgery_group,
            w.observation_end_type,
            w.resolution,
            CAST(w.window_index AS INTEGER) AS window_index,
            w.window_label,
            w.window_start,
            w.window_end,
            CASE WHEN w.observable_minutes > 0
                 THEN w.observed_window_end ELSE NULL END AS observed_window_end,
            w.window_status,
            w.observable_minutes,
            CASE WHEN w.observable_minutes > 0 THEN
                coalesce(sum(o.vis * o.overlap_minutes), 0.0)
                    / w.observable_minutes END AS vis_twa,
            CASE WHEN w.observable_minutes > 0
                 THEN coalesce(max(o.vis), 0.0) END AS vis_peak,
            CASE WHEN w.observable_minutes > 0 THEN coalesce(
                sum(o.overlap_minutes) FILTER (WHERE o.vis > 0), 0.0
            ) END AS vis_positive_minutes,
            CASE WHEN w.observable_minutes > 0 THEN
                coalesce(sum(o.nee * o.overlap_minutes), 0.0)
                    / w.observable_minutes END AS nee_twa,
            CASE WHEN w.observable_minutes > 0
                 THEN coalesce(max(o.nee), 0.0) END AS nee_peak,
            CASE WHEN w.observable_minutes > 0 THEN coalesce(
                sum(o.overlap_minutes) FILTER (WHERE o.nee > 0), 0.0
            ) END AS nee_positive_minutes,
            CASE WHEN w.observable_minutes > 0 THEN coalesce(
                sum(o.overlap_minutes) FILTER (
                    WHERE o.dobutamine_dose > 0 OR o.milrinone_dose > 0
                ), 0.0
            ) END AS inotrope_inodilator_minutes,
            CASE WHEN w.observable_minutes > 0 THEN coalesce(
                sum(o.overlap_minutes) FILTER (
                    WHERE o.norepinephrine_dose > 0
                       OR o.epinephrine_dose > 0
                       OR o.phenylephrine_dose > 0
                       OR o.dopamine_dose > 0
                       OR o.vasopressin_dose > 0
                ), 0.0
            ) END AS vasopressor_minutes,
            CASE WHEN w.observable_minutes > 0 THEN coalesce(
                sum(o.overlap_minutes) FILTER (
                    WHERE o.norepinephrine_dose > 0
                       OR o.epinephrine_dose > 0
                       OR o.phenylephrine_dose > 0
                       OR o.dopamine_dose > 0
                       OR o.vasopressin_dose > 0
                       OR o.dobutamine_dose > 0
                       OR o.milrinone_dose > 0
                ), 0.0
            ) END AS any_candidate_minutes,
            CASE WHEN w.observable_minutes > 0 THEN CAST(
                coalesce(max(CAST(o.vis > 0 AS INTEGER)), 0) AS INTEGER
            ) END AS vis_exposed_flag,
            CASE WHEN w.observable_minutes > 0 THEN CAST(
                coalesce(max(CAST(o.nee > 0 AS INTEGER)), 0) AS INTEGER
            ) END AS nee_exposed_flag,
            CASE WHEN w.observable_minutes > 0 THEN CAST(coalesce(max(CAST(
                o.dobutamine_dose > 0 OR o.milrinone_dose > 0 AS INTEGER
            )), 0) AS INTEGER) END AS inotrope_inodilator_exposed_flag,
            CASE WHEN w.observable_minutes > 0 THEN CAST(coalesce(max(CAST(
                o.norepinephrine_dose > 0
                OR o.epinephrine_dose > 0
                OR o.phenylephrine_dose > 0
                OR o.dopamine_dose > 0
                OR o.vasopressin_dose > 0
                OR o.dobutamine_dose > 0
                OR o.milrinone_dose > 0 AS INTEGER
            )), 0) AS INTEGER) END AS any_candidate_exposed_flag,
            CASE WHEN w.observable_minutes > 0 THEN
                coalesce(sum(o.vis_sum_overlap_sensitivity * o.overlap_minutes), 0.0)
                    / w.observable_minutes END AS vis_twa_sum_overlap_sensitivity,
            CASE WHEN w.observable_minutes > 0 THEN coalesce(
                max(o.vis_sum_overlap_sensitivity), 0.0
            ) END AS vis_peak_sum_overlap_sensitivity,
            CASE WHEN w.observable_minutes > 0 THEN
                coalesce(sum(o.nee_sum_overlap_sensitivity * o.overlap_minutes), 0.0)
                    / w.observable_minutes END AS nee_twa_sum_overlap_sensitivity,
            CASE WHEN w.observable_minutes > 0 THEN coalesce(
                max(o.nee_sum_overlap_sensitivity), 0.0
            ) END AS nee_peak_sum_overlap_sensitivity,
            CASE WHEN w.observable_minutes > 0 THEN
                coalesce(sum(o.vis_p999_capped_sensitivity * o.overlap_minutes), 0.0)
                    / w.observable_minutes END AS vis_twa_p999_capped_sensitivity,
            CASE WHEN w.observable_minutes > 0 THEN coalesce(
                max(o.vis_p999_capped_sensitivity), 0.0
            ) END AS vis_peak_p999_capped_sensitivity,
            CASE WHEN w.observable_minutes > 0 THEN
                coalesce(sum(o.nee_p999_capped_sensitivity * o.overlap_minutes), 0.0)
                    / w.observable_minutes END AS nee_twa_p999_capped_sensitivity,
            CASE WHEN w.observable_minutes > 0 THEN coalesce(
                max(o.nee_p999_capped_sensitivity), 0.0
            ) END AS nee_peak_p999_capped_sensitivity,
            CASE WHEN w.observable_minutes > 0 THEN coalesce(
                sum(o.overlap_minutes) FILTER (WHERE o.max_concurrent_rows > 1), 0.0
            ) END AS within_drug_overlap_minutes,
            CASE WHEN w.observable_minutes > 0 THEN coalesce(
                sum(o.overlap_minutes) FILTER (WHERE o.any_above_p999_flag = 1), 0.0
            ) END AS above_p999_minutes,
            {', '.join(window_drug_columns)}
        FROM window_grid AS w
        LEFT JOIN window_segment_overlap AS o
            ON w.resolution = o.resolution
           AND w.stay_id = o.stay_id
           AND w.window_index = o.window_index
        GROUP BY
            w.subject_id,
            w.hadm_id,
            w.stay_id,
            w.surgery_group,
            w.observation_end_type,
            w.resolution,
            w.window_index,
            w.window_label,
            w.window_start,
            w.window_end,
            w.observed_window_end,
            w.window_status,
            w.observable_minutes
        """
    )

    hourly_n = con.execute(
        "SELECT COUNT(*) FROM trajectory_windows WHERE resolution = 'HOURLY'"
    ).fetchone()[0]
    six_hour_n = con.execute(
        "SELECT COUNT(*) FROM trajectory_windows WHERE resolution = 'SIX_HOUR'"
    ).fetchone()[0]
    if hourly_n != cohort_n * 24:
        raise RuntimeError(
            f"Hourly grid has {hourly_n:,} rows; expected {cohort_n * 24:,}."
        )
    if six_hour_n != cohort_n * 4:
        raise RuntimeError(
            f"Six-hour grid has {six_hour_n:,} rows; expected {cohort_n * 4:,}."
        )

    null_semantics_failures = con.execute(
        """
        SELECT COUNT(*)
        FROM trajectory_windows
        WHERE (window_status = 'NOT_OBSERVABLE' AND (
                   vis_twa IS NOT NULL OR nee_twa IS NOT NULL
                   OR any_candidate_exposed_flag IS NOT NULL
               ))
           OR (window_status <> 'NOT_OBSERVABLE' AND (
                   vis_twa IS NULL OR nee_twa IS NULL
                   OR any_candidate_exposed_flag IS NULL
               ))
        """
    ).fetchone()[0]
    if null_semantics_failures:
        raise RuntimeError(
            f"Found {null_semantics_failures:,} window null-semantics failures."
        )

    print("Building 24-hour stay summaries and validation audit...")
    drug_stay_columns = []
    for _, _, slug, _, _ in CANDIDATE_DRUGS:
        drug_stay_columns.extend(
            [
                f"sum({slug}_twa_dose * observable_minutes) "
                f"/ nullif(sum(observable_minutes), 0) AS {slug}_twa_24h",
                f"max({slug}_peak_dose) AS {slug}_peak_24h",
                f"sum({slug}_exposure_minutes) AS {slug}_exposure_minutes_24h",
                f"max({slug}_exposed_flag) AS {slug}_exposed_flag_24h",
            ]
        )
    con.execute(
        f"""
        CREATE TEMP TABLE stay_summary AS
        SELECT
            subject_id,
            hadm_id,
            stay_id,
            surgery_group,
            max(observation_end_type) AS observation_end_type,
            sum(observable_minutes) AS observable_minutes_24h,
            sum(vis_twa * observable_minutes)
                / nullif(sum(observable_minutes), 0) AS vis_twa_24h,
            max(vis_peak) AS vis_peak_24h,
            sum(vis_positive_minutes) AS vis_positive_minutes_24h,
            sum(nee_twa * observable_minutes)
                / nullif(sum(observable_minutes), 0) AS nee_twa_24h,
            max(nee_peak) AS nee_peak_24h,
            sum(nee_positive_minutes) AS nee_positive_minutes_24h,
            sum(inotrope_inodilator_minutes) AS inotrope_inodilator_minutes_24h,
            sum(vasopressor_minutes) AS vasopressor_minutes_24h,
            sum(any_candidate_minutes) AS any_candidate_minutes_24h,
            max(vis_exposed_flag) AS vis_exposed_flag_24h,
            max(nee_exposed_flag) AS nee_exposed_flag_24h,
            max(inotrope_inodilator_exposed_flag)
                AS inotrope_inodilator_exposed_flag_24h,
            max(any_candidate_exposed_flag) AS any_candidate_exposed_flag_24h,
            sum(vis_twa_sum_overlap_sensitivity * observable_minutes)
                / nullif(sum(observable_minutes), 0)
                AS vis_twa_24h_sum_overlap_sensitivity,
            max(vis_peak_sum_overlap_sensitivity)
                AS vis_peak_24h_sum_overlap_sensitivity,
            sum(nee_twa_sum_overlap_sensitivity * observable_minutes)
                / nullif(sum(observable_minutes), 0)
                AS nee_twa_24h_sum_overlap_sensitivity,
            max(nee_peak_sum_overlap_sensitivity)
                AS nee_peak_24h_sum_overlap_sensitivity,
            sum(vis_twa_p999_capped_sensitivity * observable_minutes)
                / nullif(sum(observable_minutes), 0)
                AS vis_twa_24h_p999_capped_sensitivity,
            max(vis_peak_p999_capped_sensitivity)
                AS vis_peak_24h_p999_capped_sensitivity,
            sum(nee_twa_p999_capped_sensitivity * observable_minutes)
                / nullif(sum(observable_minutes), 0)
                AS nee_twa_24h_p999_capped_sensitivity,
            max(nee_peak_p999_capped_sensitivity)
                AS nee_peak_24h_p999_capped_sensitivity,
            sum(within_drug_overlap_minutes) AS within_drug_overlap_minutes_24h,
            sum(above_p999_minutes) AS above_p999_minutes_24h,
            {', '.join(drug_stay_columns)}
        FROM trajectory_windows
        WHERE resolution = 'HOURLY'
        GROUP BY subject_id, hadm_id, stay_id, surgery_group
        """
    )

    con.execute(
        """
        CREATE TEMP TABLE conservation_audit AS
        WITH segment_by_stay AS (
            SELECT
                stay_id,
                sum(segment_minutes) AS observable_minutes,
                sum(vis * segment_minutes) AS vis_integral,
                sum(nee * segment_minutes) AS nee_integral
            FROM score_segments
            GROUP BY stay_id
        ), hourly_by_stay AS (
            SELECT
                stay_id,
                sum(observable_minutes) AS observable_minutes,
                sum(vis_twa * observable_minutes) AS vis_integral,
                sum(nee_twa * observable_minutes) AS nee_integral
            FROM trajectory_windows
            WHERE resolution = 'HOURLY'
            GROUP BY stay_id
        ), six_hour_by_stay AS (
            SELECT
                stay_id,
                sum(observable_minutes) AS observable_minutes,
                sum(vis_twa * observable_minutes) AS vis_integral,
                sum(nee_twa * observable_minutes) AS nee_integral
            FROM trajectory_windows
            WHERE resolution = 'SIX_HOUR'
            GROUP BY stay_id
        ), stay_comparison AS (
            SELECT
                s.stay_id,
                s.observable_minutes AS segment_observable_minutes,
                h.observable_minutes AS hourly_observable_minutes,
                x.observable_minutes AS six_hour_observable_minutes,
                s.vis_integral AS segment_vis_integral,
                h.vis_integral AS hourly_vis_integral,
                x.vis_integral AS six_hour_vis_integral,
                s.nee_integral AS segment_nee_integral,
                h.nee_integral AS hourly_nee_integral,
                x.nee_integral AS six_hour_nee_integral
            FROM segment_by_stay AS s
            INNER JOIN hourly_by_stay AS h USING (stay_id)
            INNER JOIN six_hour_by_stay AS x USING (stay_id)
        ), segment_integrals AS (
            SELECT
                sum(segment_observable_minutes) AS observable_minutes,
                sum(segment_vis_integral) AS vis_integral,
                sum(segment_nee_integral) AS nee_integral
            FROM stay_comparison
        ), hourly_integrals AS (
            SELECT
                sum(hourly_observable_minutes) AS observable_minutes,
                sum(hourly_vis_integral) AS vis_integral,
                sum(hourly_nee_integral) AS nee_integral
            FROM stay_comparison
        ), six_hour_integrals AS (
            SELECT
                sum(six_hour_observable_minutes) AS observable_minutes,
                sum(six_hour_vis_integral) AS vis_integral,
                sum(six_hour_nee_integral) AS nee_integral
            FROM stay_comparison
        ), stay_quality AS (
            SELECT
                count(*) FILTER (
                    WHERE abs(segment_observable_minutes
                              - hourly_observable_minutes) > 1e-6
                       OR abs(segment_observable_minutes
                              - six_hour_observable_minutes) > 1e-6
                       OR abs(segment_vis_integral
                              - hourly_vis_integral) > 1e-6
                       OR abs(segment_vis_integral
                              - six_hour_vis_integral) > 1e-6
                       OR abs(segment_nee_integral
                              - hourly_nee_integral) > 1e-6
                       OR abs(segment_nee_integral
                              - six_hour_nee_integral) > 1e-6
                ) AS stays_failing_conservation_n,
                max(abs(segment_vis_integral - hourly_vis_integral))
                    AS max_stay_hourly_vis_integral_abs_diff,
                max(abs(segment_vis_integral - six_hour_vis_integral))
                    AS max_stay_six_hour_vis_integral_abs_diff,
                max(abs(segment_nee_integral - hourly_nee_integral))
                    AS max_stay_hourly_nee_integral_abs_diff,
                max(abs(segment_nee_integral - six_hour_nee_integral))
                    AS max_stay_six_hour_nee_integral_abs_diff
            FROM stay_comparison
        )
        SELECT
            s.observable_minutes AS segment_observable_minutes,
            h.observable_minutes AS hourly_observable_minutes,
            x.observable_minutes AS six_hour_observable_minutes,
            abs(s.observable_minutes - h.observable_minutes)
                AS hourly_observable_minutes_abs_diff,
            abs(s.observable_minutes - x.observable_minutes)
                AS six_hour_observable_minutes_abs_diff,
            s.vis_integral AS segment_vis_integral,
            h.vis_integral AS hourly_vis_integral,
            x.vis_integral AS six_hour_vis_integral,
            abs(s.vis_integral - h.vis_integral) AS hourly_vis_integral_abs_diff,
            abs(s.vis_integral - x.vis_integral) AS six_hour_vis_integral_abs_diff,
            s.nee_integral AS segment_nee_integral,
            h.nee_integral AS hourly_nee_integral,
            x.nee_integral AS six_hour_nee_integral,
            abs(s.nee_integral - h.nee_integral) AS hourly_nee_integral_abs_diff,
            abs(s.nee_integral - x.nee_integral) AS six_hour_nee_integral_abs_diff,
            q.stays_failing_conservation_n,
            q.max_stay_hourly_vis_integral_abs_diff,
            q.max_stay_six_hour_vis_integral_abs_diff,
            q.max_stay_hourly_nee_integral_abs_diff,
            q.max_stay_six_hour_nee_integral_abs_diff
        FROM segment_integrals AS s
        CROSS JOIN hourly_integrals AS h
        CROSS JOIN six_hour_integrals AS x
        CROSS JOIN stay_quality AS q
        """
    )
    conservation = con.execute("SELECT * FROM conservation_audit").fetchone()
    for index in (3, 4, 8, 9, 13, 14):
        if conservation[index] is None or conservation[index] > 1e-5:
            raise RuntimeError(
                "Dose-time conservation failed; inspect conservation_audit."
            )
    if conservation[15] != 0:
        raise RuntimeError(
            f"Dose-time conservation failed in {conservation[15]:,} stays."
        )

    drug_conservation_parts = []
    for _, drug, slug, _, _ in CANDIDATE_DRUGS:
        drug_conservation_parts.append(
            f"""
            SELECT
                '{drug}' AS drug_name,
                s.segment_integral,
                h.hourly_integral,
                x.six_hour_integral,
                abs(s.segment_integral - h.hourly_integral)
                    AS hourly_integral_abs_diff,
                abs(s.segment_integral - x.six_hour_integral)
                    AS six_hour_integral_abs_diff
            FROM (
                SELECT coalesce(sum(d.dose_max_policy * g.segment_minutes), 0.0)
                    AS segment_integral
                FROM segment_drug AS d
                INNER JOIN global_segments AS g
                    ON d.stay_id = g.stay_id
                   AND d.segment_start = g.segment_start
                   AND d.segment_end = g.segment_end
                WHERE d.drug_name = '{drug}'
            ) AS s
            CROSS JOIN (
                SELECT coalesce(sum({slug}_twa_dose * observable_minutes), 0.0)
                    AS hourly_integral
                FROM trajectory_windows
                WHERE resolution = 'HOURLY'
            ) AS h
            CROSS JOIN (
                SELECT coalesce(sum({slug}_twa_dose * observable_minutes), 0.0)
                    AS six_hour_integral
                FROM trajectory_windows
                WHERE resolution = 'SIX_HOUR'
            ) AS x
            """
        )
    con.execute(
        "CREATE TEMP TABLE drug_conservation_audit AS "
        + " UNION ALL ".join(drug_conservation_parts)
    )
    drug_conservation_failures = con.execute(
        """
        SELECT COUNT(*)
        FROM drug_conservation_audit
        WHERE hourly_integral_abs_diff > 1e-5
           OR six_hour_integral_abs_diff > 1e-5
        """
    ).fetchone()[0]
    if drug_conservation_failures:
        raise RuntimeError(
            f"Drug dose-time conservation failed for "
            f"{drug_conservation_failures:,} drugs."
        )

    con.execute(
        """
        CREATE TEMP TABLE audit_metrics AS
        SELECT 'cohort_stays_n' AS metric, CAST(COUNT(*) AS DOUBLE) AS value
        FROM cohort
        UNION ALL
        SELECT 'source_rows_overlapping_window_n', CAST(COUNT(*) AS DOUBLE)
        FROM window_events
        UNION ALL
        SELECT 'retained_continuous_rows_n', CAST(COUNT(*) AS DOUBLE)
        FROM normalized_events
        UNION ALL
        SELECT 'excluded_rows_n', CAST(COUNT(*) AS DOUBLE)
        FROM classified_events WHERE exclusion_reason <> ''
        UNION ALL
        SELECT 'hourly_rows_n', CAST(COUNT(*) AS DOUBLE)
        FROM trajectory_windows WHERE resolution = 'HOURLY'
        UNION ALL
        SELECT 'six_hour_rows_n', CAST(COUNT(*) AS DOUBLE)
        FROM trajectory_windows WHERE resolution = 'SIX_HOUR'
        UNION ALL
        SELECT 'null_semantics_failures_n', CAST(0 AS DOUBLE)
        UNION ALL
        SELECT 'hourly_vis_integral_abs_diff', hourly_vis_integral_abs_diff
        FROM conservation_audit
        UNION ALL
        SELECT 'six_hour_vis_integral_abs_diff', six_hour_vis_integral_abs_diff
        FROM conservation_audit
        UNION ALL
        SELECT 'hourly_nee_integral_abs_diff', hourly_nee_integral_abs_diff
        FROM conservation_audit
        UNION ALL
        SELECT 'six_hour_nee_integral_abs_diff', six_hour_nee_integral_abs_diff
        FROM conservation_audit
        """
    )

    export_columns = "* EXCLUDE (resolution)"
    copy_query_to_file(
        con,
        f"""
        SELECT {export_columns}
        FROM trajectory_windows
        WHERE resolution = 'HOURLY'
        ORDER BY stay_id, window_index
        """,
        outputs["hourly"],
        "PARQUET",
    )
    copy_query_to_file(
        con,
        f"""
        SELECT {export_columns}
        FROM trajectory_windows
        WHERE resolution = 'SIX_HOUR'
        ORDER BY stay_id, window_index
        """,
        outputs["six_hour"],
        "PARQUET",
    )
    copy_query_to_file(
        con,
        "SELECT * FROM stay_summary ORDER BY stay_id",
        outputs["stay_summary"],
        "CSV",
    )
    copy_query_to_file(
        con,
        "SELECT * FROM audit_metrics ORDER BY metric",
        outputs["audit"],
        "CSV",
    )
    copy_query_to_file(
        con,
        """
        SELECT *
        FROM trajectory_windows
        WHERE resolution = 'HOURLY'
          AND (above_p999_minutes > 0 OR within_drug_overlap_minutes > 0)
        ORDER BY stay_id, window_index
        """,
        outputs["extreme_windows"],
        "CSV",
    )

    print_query_table(
        con,
        "SOURCE RULE DECISIONS",
        """
        SELECT
            d.drug_name,
            count(c.stay_id) AS overlapping_rows_n,
            count(c.stay_id) FILTER (WHERE c.exclusion_reason = '')
                AS retained_continuous_rows_n,
            count(c.stay_id) FILTER (
                WHERE contains(c.exclusion_reason, 'BOLUS_STATUS')
            ) AS excluded_bolus_rows_n,
            count(c.stay_id) FILTER (
                WHERE c.exclusion_reason <> ''
                  AND NOT contains(c.exclusion_reason, 'BOLUS_STATUS')
            ) AS other_excluded_rows_n,
            count(DISTINCT c.stay_id) FILTER (WHERE c.exclusion_reason = '')
                AS retained_stays_n,
            t.dose_p999
        FROM candidate_drugs AS d
        LEFT JOIN classified_events AS c USING (drug_name)
        LEFT JOIN dose_tail_thresholds AS t USING (drug_name)
        GROUP BY d.drug_name, t.dose_p999
        ORDER BY d.drug_name
        """,
    )

    print_query_table(
        con,
        "HOURLY OBSERVABILITY AND EXPOSURE",
        """
        SELECT
            window_label,
            count(*) FILTER (WHERE window_status <> 'NOT_OBSERVABLE')
                AS observable_stays_n,
            count(*) FILTER (WHERE window_status = 'FULLY_OBSERVABLE')
                AS fully_observable_stays_n,
            count(*) FILTER (WHERE window_status = 'PARTIALLY_OBSERVABLE')
                AS partially_observable_stays_n,
            count(*) FILTER (WHERE any_candidate_exposed_flag = 1)
                AS any_candidate_stays_n,
            count(*) FILTER (WHERE vis_exposed_flag = 1) AS vis_stays_n,
            count(*) FILTER (WHERE nee_exposed_flag = 1) AS nee_stays_n,
            count(*) FILTER (WHERE inotrope_inodilator_exposed_flag = 1)
                AS inotrope_inodilator_stays_n
        FROM trajectory_windows
        WHERE resolution = 'HOURLY'
        GROUP BY window_index, window_label
        ORDER BY window_index
        """,
    )

    print_query_table(
        con,
        "SIX-HOUR OBSERVABILITY, EXPOSURE, AND SCORES",
        """
        SELECT
            window_label,
            count(*) FILTER (WHERE window_status <> 'NOT_OBSERVABLE')
                AS observable_stays_n,
            count(*) FILTER (WHERE window_status = 'PARTIALLY_OBSERVABLE')
                AS partially_observable_stays_n,
            count(*) FILTER (WHERE any_candidate_exposed_flag = 1)
                AS any_candidate_stays_n,
            count(*) FILTER (WHERE vis_exposed_flag = 1) AS vis_stays_n,
            count(*) FILTER (WHERE nee_exposed_flag = 1) AS nee_stays_n,
            median(vis_twa) FILTER (WHERE vis_exposed_flag = 1)
                AS vis_twa_median_exposed,
            quantile_cont(vis_twa, 0.9) FILTER (WHERE vis_exposed_flag = 1)
                AS vis_twa_p90_exposed,
            median(nee_twa) FILTER (WHERE nee_exposed_flag = 1)
                AS nee_twa_median_exposed,
            quantile_cont(nee_twa, 0.9) FILTER (WHERE nee_exposed_flag = 1)
                AS nee_twa_p90_exposed
        FROM trajectory_windows
        WHERE resolution = 'SIX_HOUR'
        GROUP BY window_index, window_label
        ORDER BY window_index
        """,
    )

    print_query_table(
        con,
        "24-HOUR DRUG EXPOSURE SUMMARY",
        """
        SELECT
            d.drug_name,
            count(DISTINCT sd.stay_id) AS exposed_stays_n,
            sum(g.segment_minutes) FILTER (WHERE sd.dose_max_policy > 0)
                AS exposed_minutes,
            median(sd.dose_max_policy) AS segment_dose_median,
            quantile_cont(sd.dose_max_policy, 0.9) AS segment_dose_p90,
            max(sd.dose_max_policy) AS segment_dose_max,
            count(DISTINCT sd.stay_id) FILTER (WHERE sd.above_p999_flag = 1)
                AS above_p999_stays_n
        FROM candidate_drugs AS d
        LEFT JOIN segment_drug AS sd USING (drug_name)
        LEFT JOIN global_segments AS g
            ON sd.stay_id = g.stay_id
           AND sd.segment_start = g.segment_start
           AND sd.segment_end = g.segment_end
        GROUP BY d.drug_name
        ORDER BY d.drug_name
        """,
    )

    print_query_table(
        con,
        "24-HOUR SCORE SUMMARY",
        """
        SELECT
            count(*) AS stays_n,
            count(*) FILTER (WHERE any_candidate_exposed_flag_24h = 1)
                AS any_candidate_stays_n,
            count(*) FILTER (WHERE vis_exposed_flag_24h = 1) AS vis_stays_n,
            count(*) FILTER (WHERE nee_exposed_flag_24h = 1) AS nee_stays_n,
            median(vis_twa_24h) AS vis_twa_median_all,
            quantile_cont(vis_twa_24h, 0.9) AS vis_twa_p90_all,
            median(vis_peak_24h) FILTER (WHERE vis_exposed_flag_24h = 1)
                AS vis_peak_median_exposed,
            quantile_cont(vis_peak_24h, 0.9)
                FILTER (WHERE vis_exposed_flag_24h = 1)
                AS vis_peak_p90_exposed,
            max(vis_peak_24h) AS vis_peak_max,
            median(nee_twa_24h) AS nee_twa_median_all,
            quantile_cont(nee_twa_24h, 0.9) AS nee_twa_p90_all,
            median(nee_peak_24h) FILTER (WHERE nee_exposed_flag_24h = 1)
                AS nee_peak_median_exposed,
            quantile_cont(nee_peak_24h, 0.9)
                FILTER (WHERE nee_exposed_flag_24h = 1)
                AS nee_peak_p90_exposed,
            max(nee_peak_24h) AS nee_peak_max
        FROM stay_summary
        """,
    )

    print_query_table(
        con,
        "EMPIRICAL P99.9 CAP IMPACT BY DRUG",
        """
        SELECT
            d.drug_name,
            t.dose_p999,
            count(DISTINCT sd.stay_id) FILTER (WHERE sd.above_p999_flag = 1)
                AS above_p999_stays_n,
            count(*) FILTER (WHERE sd.above_p999_flag = 1)
                AS above_p999_atomic_segments_n,
            coalesce(sum(g.segment_minutes) FILTER (
                WHERE sd.above_p999_flag = 1
            ), 0.0) AS above_p999_minutes,
            max(sd.dose_max_policy) AS dose_max,
            max(sd.dose_max_policy) / nullif(t.dose_p999, 0)
                AS max_to_p999_ratio,
            coalesce(sum(
                (sd.vis_max_component - sd.vis_p999_capped_component)
                * g.segment_minutes
            ), 0.0) AS vis_integral_removed_by_cap,
            coalesce(sum(
                (sd.nee_max_component - sd.nee_p999_capped_component)
                * g.segment_minutes
            ), 0.0) AS nee_integral_removed_by_cap
        FROM candidate_drugs AS d
        LEFT JOIN dose_tail_thresholds AS t USING (drug_name)
        LEFT JOIN segment_drug AS sd USING (drug_name)
        LEFT JOIN global_segments AS g
            ON sd.stay_id = g.stay_id
           AND sd.segment_start = g.segment_start
           AND sd.segment_end = g.segment_end
        GROUP BY d.drug_name, t.dose_p999
        ORDER BY d.drug_name
        """,
    )

    print_query_table(
        con,
        "TOP FIVE SOURCE DOSES PER DRUG",
        """
        WITH ranked AS (
            SELECT
                drug_name,
                stay_id,
                clipped_starttime,
                clipped_endtime,
                date_diff('second', clipped_starttime, clipped_endtime) / 60.0
                    AS clipped_duration_min,
                rate,
                rateuom,
                patientweight,
                normalized_dose,
                dose_p999,
                normalized_dose / nullif(dose_p999, 0) AS dose_to_p999_ratio,
                statusdescription,
                orderid,
                linkorderid,
                row_number() OVER (
                    PARTITION BY drug_name
                    ORDER BY normalized_dose DESC, stay_id, clipped_starttime
                ) AS dose_rank
            FROM normalized_events
        )
        SELECT * EXCLUDE (dose_rank)
        FROM ranked
        WHERE dose_rank <= 5
        ORDER BY drug_name, dose_rank
        """,
        max_rows=40,
    )

    print_query_table(
        con,
        "MAX-VERSUS-SUM OVERLAP SENSITIVITY",
        """
        SELECT
            resolution,
            count(*) FILTER (
                WHERE vis_twa_sum_overlap_sensitivity <> vis_twa
                   OR nee_twa_sum_overlap_sensitivity <> nee_twa
            ) AS discordant_windows_n,
            count(DISTINCT stay_id) FILTER (
                WHERE vis_twa_sum_overlap_sensitivity <> vis_twa
                   OR nee_twa_sum_overlap_sensitivity <> nee_twa
            ) AS discordant_stays_n,
            max(abs(vis_twa_sum_overlap_sensitivity - vis_twa))
                AS max_abs_vis_twa_diff,
            max(abs(nee_twa_sum_overlap_sensitivity - nee_twa))
                AS max_abs_nee_twa_diff,
            max(within_drug_overlap_minutes) AS max_overlap_minutes_per_window
        FROM trajectory_windows
        GROUP BY resolution
        ORDER BY resolution
        """,
    )

    print_query_table(
        con,
        "P99.9 CAP SENSITIVITY",
        """
        SELECT
            resolution,
            count(*) FILTER (
                WHERE greatest(
                    abs(vis_twa_p999_capped_sensitivity - vis_twa),
                    abs(nee_twa_p999_capped_sensitivity - nee_twa)
                ) > 1e-12
            ) AS changed_windows_n,
            count(DISTINCT stay_id) FILTER (
                WHERE greatest(
                    abs(vis_twa_p999_capped_sensitivity - vis_twa),
                    abs(nee_twa_p999_capped_sensitivity - nee_twa)
                ) > 1e-12
            ) AS changed_stays_n,
            count(*) FILTER (WHERE above_p999_minutes > 0)
                AS windows_with_true_above_p999_exposure_n,
            count(DISTINCT stay_id) FILTER (WHERE above_p999_minutes > 0)
                AS stays_with_true_above_p999_exposure_n,
            max(abs(vis_twa_p999_capped_sensitivity - vis_twa))
                AS max_abs_vis_twa_diff,
            max(abs(vis_peak_p999_capped_sensitivity - vis_peak))
                AS max_abs_vis_peak_diff,
            max(abs(nee_twa_p999_capped_sensitivity - nee_twa))
                AS max_abs_nee_twa_diff,
            max(abs(nee_peak_p999_capped_sensitivity - nee_peak))
                AS max_abs_nee_peak_diff,
            max(above_p999_minutes) AS max_above_p999_minutes_per_window
        FROM trajectory_windows
        GROUP BY resolution
        ORDER BY resolution
        """,
    )

    print_query_table(
        con,
        "DOSE-TIME CONSERVATION",
        "SELECT * FROM conservation_audit",
    )

    print_query_table(
        con,
        "PER-DRUG DOSE-TIME CONSERVATION",
        "SELECT * FROM drug_conservation_audit ORDER BY drug_name",
    )

    print_query_table(
        con,
        "OUTPUT GRID AND NULL SEMANTICS",
        """
        SELECT
            resolution,
            count(*) AS rows_n,
            count(DISTINCT stay_id) AS stays_n,
            count(*) FILTER (WHERE window_status = 'FULLY_OBSERVABLE')
                AS fully_observable_rows_n,
            count(*) FILTER (WHERE window_status = 'PARTIALLY_OBSERVABLE')
                AS partially_observable_rows_n,
            count(*) FILTER (WHERE window_status = 'NOT_OBSERVABLE')
                AS not_observable_rows_n,
            count(*) FILTER (
                WHERE window_status <> 'NOT_OBSERVABLE'
                  AND any_candidate_exposed_flag = 0
            ) AS observed_zero_treatment_rows_n
        FROM trajectory_windows
        GROUP BY resolution
        ORDER BY resolution
        """,
    )

    retained_n = con.execute("SELECT COUNT(*) FROM normalized_events").fetchone()[0]
    excluded_n = con.execute(
        "SELECT COUNT(*) FROM classified_events WHERE exclusion_reason <> ''"
    ).fetchone()[0]
    exposed_n = con.execute(
        "SELECT COUNT(*) FROM stay_summary WHERE any_candidate_exposed_flag_24h = 1"
    ).fetchone()[0]
    vis_n = con.execute(
        "SELECT COUNT(*) FROM stay_summary WHERE vis_exposed_flag_24h = 1"
    ).fetchone()[0]
    nee_n = con.execute(
        "SELECT COUNT(*) FROM stay_summary WHERE nee_exposed_flag_24h = 1"
    ).fetchone()[0]

    print("\nCompleted canonical vasoactive/inotrope trajectory extraction")
    print(f"Frozen cohort stays:                         {cohort_n:,}")
    print(f"Retained continuous source rows:             {retained_n:,}")
    print(f"Excluded source rows:                        {excluded_n:,}")
    print(f"Stays with any candidate exposure:           {exposed_n:,}")
    print(f"Stays with >=1 standard VIS component:       {vis_n:,}")
    print(f"Stays with >=1 NEE vasopressor:              {nee_n:,}")
    print(f"Hourly trajectory rows:                      {hourly_n:,}")
    print(f"Six-hour trajectory rows:                    {six_hour_n:,}")
    print("Primary within-drug overlap policy:          MAX")
    print("Primary dose-tail policy:                    UNCAPPED")
    print("Post-observation windows:                    NULL / NOT_OBSERVABLE")
    print("Observable minutes without infusion:         ZERO DOSE")
    print("\nCanonical outputs:")
    for output in outputs.values():
        print(f"  {output}")
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
