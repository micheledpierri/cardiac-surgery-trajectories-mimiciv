#!/usr/bin/env python3
"""
Create the minimal starting cohort of adult MIMIC-IV ICU patients undergoing
standard open cardiac surgery: CABG, valve surgery, or ascending aorta/aortic
arch surgery.

The script reads the original MIMIC-IV 3.1 CSV.GZ files directly and creates
one canonical output only:
    data/00_cardiac_surgery_starting_cohort.csv

Required raw MIMIC-IV inputs (relative to MIMIC_ROOT):
    hosp/patients.csv.gz
    hosp/admissions.csv.gz
    hosp/procedures_icd.csv.gz
    hosp/d_icd_procedures.csv.gz
    hosp/services.csv.gz
    icu/icustays.csv.gz

Run directly from Visual Studio Code with "Run Python File".
"""

from pathlib import Path
import sys
import duckdb


SCRIPT_PATH = Path(__file__).resolve()
PROJECT_ROOT = (
    SCRIPT_PATH.parent.parent
    if SCRIPT_PATH.parent.name.lower() == "scripts"
    else SCRIPT_PATH.parent
)
DATA_DIR = PROJECT_ROOT / "data"

# Set this path to your local MIMIC-IV 3.1 directory (containing hosp/ and icu/).
MIMIC_ROOT = Path("path/to/mimic-iv-3.1")
HOSP = MIMIC_ROOT / "hosp"
ICU = MIMIC_ROOT / "icu"

OUTPUT = DATA_DIR / "00_cardiac_surgery_starting_cohort.csv"

FILES = {
    "patients": HOSP / "patients.csv.gz",
    "admissions": HOSP / "admissions.csv.gz",
    "procedures": HOSP / "procedures_icd.csv.gz",
    "procedure_dictionary": HOSP / "d_icd_procedures.csv.gz",
    "services": HOSP / "services.csv.gz",
    "icustays": ICU / "icustays.csv.gz",
}


def p(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def main() -> int:
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    missing = [str(path) for path in FILES.values() if not path.is_file()]
    if missing:
        print("Missing source files:", file=sys.stderr)
        for path in missing:
            print(f"  {path}", file=sys.stderr)
        return 1

    con = duckdb.connect(":memory:")

    try:
        con.execute(f"""
            CREATE VIEW patients AS
            SELECT
                subject_id::BIGINT AS subject_id,
                gender::VARCHAR AS sex,
                anchor_age::INTEGER AS anchor_age,
                anchor_year::INTEGER AS anchor_year,
                anchor_year_group::VARCHAR AS anchor_year_group,
                dod::DATE AS recorded_date_of_death
            FROM read_csv_auto('{p(FILES['patients'])}',
                               header=true, all_varchar=true, sample_size=-1)
        """)

        con.execute(f"""
            CREATE VIEW admissions AS
            SELECT
                subject_id::BIGINT AS subject_id,
                hadm_id::BIGINT AS hadm_id,
                admittime::TIMESTAMP AS hospital_admittime,
                dischtime::TIMESTAMP AS hospital_dischtime,
                deathtime::TIMESTAMP AS hospital_deathtime,
                admission_type::VARCHAR AS admission_type,
                admission_location::VARCHAR AS admission_location,
                discharge_location::VARCHAR AS discharge_location,
                hospital_expire_flag::INTEGER AS hospital_expire_flag
            FROM read_csv_auto('{p(FILES['admissions'])}',
                               header=true, all_varchar=true, sample_size=-1)
        """)

        con.execute(f"""
            CREATE VIEW procedures AS
            SELECT
                subject_id::BIGINT AS subject_id,
                hadm_id::BIGINT AS hadm_id,
                chartdate::DATE AS chartdate,
                UPPER(REPLACE(TRIM(icd_code::VARCHAR), '.', '')) AS icd_code,
                icd_version::INTEGER AS icd_version
            FROM read_csv_auto('{p(FILES['procedures'])}',
                               header=true, all_varchar=true, sample_size=-1)
        """)

        con.execute(f"""
            CREATE VIEW procedure_dictionary AS
            SELECT
                UPPER(REPLACE(TRIM(icd_code::VARCHAR), '.', '')) AS icd_code,
                icd_version::INTEGER AS icd_version,
                long_title::VARCHAR AS long_title
            FROM read_csv_auto('{p(FILES['procedure_dictionary'])}',
                               header=true, all_varchar=true, sample_size=-1)
        """)

        con.execute(f"""
            CREATE VIEW services AS
            SELECT
                hadm_id::BIGINT AS hadm_id,
                transfertime::TIMESTAMP AS transfertime,
                UPPER(TRIM(curr_service::VARCHAR)) AS curr_service
            FROM read_csv_auto('{p(FILES['services'])}',
                               header=true, all_varchar=true, sample_size=-1)
        """)

        # time_zero_icu is the source ICU admission timestamp; the validated
        # postoperative time zero is assigned separately by script 00_03.
        con.execute(f"""
            CREATE VIEW icustays AS
            SELECT
                subject_id::BIGINT AS subject_id,
                hadm_id::BIGINT AS hadm_id,
                stay_id::BIGINT AS stay_id,
                first_careunit::VARCHAR AS first_careunit,
                last_careunit::VARCHAR AS last_careunit,
                intime::TIMESTAMP AS time_zero_icu,
                outtime::TIMESTAMP AS icu_outtime,
                los::DOUBLE AS source_icu_los_days
            FROM read_csv_auto('{p(FILES['icustays'])}',
                               header=true, all_varchar=true, sample_size=-1)
        """)

        # Conservative, prespecified procedure definitions.
        con.execute("""
            CREATE TABLE qualifying_procedures AS
            SELECT *
            FROM (
                SELECT
                    p.subject_id,
                    p.hadm_id,
                    p.chartdate,
                    p.icd_code,
                    p.icd_version,
                    d.long_title,
                    CASE
                        WHEN p.icd_version = 9
                             AND p.icd_code LIKE '361%'
                            THEN 'CABG'
                        WHEN p.icd_version = 10
                             AND regexp_full_match(p.icd_code, '021[0-3]0..')
                            THEN 'CABG'
                        WHEN p.icd_version = 9
                             AND (p.icd_code LIKE '351%'
                                  OR p.icd_code LIKE '352%')
                            THEN 'VALVE'
                        WHEN p.icd_version = 10
                             AND regexp_full_match(p.icd_code,
                                                   '02[QRU][FGHJ]0..')
                            THEN 'VALVE'
                        WHEN p.icd_version = 9
                             AND p.icd_code = '3845'
                            THEN 'ASCENDING_AORTA'
                        WHEN p.icd_version = 10
                             AND regexp_full_match(p.icd_code,
                                                   '02[QRU]X0..')
                            THEN 'ASCENDING_AORTA'
                        ELSE NULL
                    END AS procedure_group
                FROM procedures p
                LEFT JOIN procedure_dictionary d
                  ON p.icd_code = d.icd_code
                 AND p.icd_version = d.icd_version
            ) x
            WHERE procedure_group IS NOT NULL
        """)

        # Require a CSURG service record and use the earliest qualifying procedure
        # date in each admission, combining qualifying procedures on that date.
        # Select one ICU stay within +/-1 calendar day: nearest date, then prefer
        # nonnegative offsets, cardiac care units, earlier ICU time, and stay_id.
        # Retain adults with derived age_at_icu_admission >= 18.
        con.execute("""
            CREATE TABLE final_cohort AS
            WITH csurg AS (
                SELECT
                    hadm_id,
                    MIN(transfertime) AS first_csurg_transfertime
                FROM services
                WHERE curr_service = 'CSURG'
                GROUP BY hadm_id
            ),
            first_date AS (
                SELECT
                    subject_id,
                    hadm_id,
                    MIN(chartdate) AS index_operation_date,
                    COUNT(DISTINCT chartdate) AS qualifying_date_count
                FROM qualifying_procedures
                GROUP BY subject_id, hadm_id
            ),
            operation AS (
                SELECT
                    f.subject_id,
                    f.hadm_id,
                    f.index_operation_date,
                    f.qualifying_date_count,
                    MAX(q.procedure_group = 'CABG')::INTEGER AS cabg_flag,
                    MAX(q.procedure_group = 'VALVE')::INTEGER AS valve_flag,
                    MAX(q.procedure_group = 'ASCENDING_AORTA')::INTEGER
                        AS ascending_aorta_flag,
                    STRING_AGG(DISTINCT q.icd_version || ':' || q.icd_code,
                               ' | ') AS qualifying_icd_codes,
                    STRING_AGG(DISTINCT COALESCE(q.long_title,
                                                 '[TITLE NOT FOUND]'),
                               ' | ') AS qualifying_titles
                FROM first_date f
                JOIN qualifying_procedures q
                  ON f.subject_id = q.subject_id
                 AND f.hadm_id = q.hadm_id
                 AND f.index_operation_date = q.chartdate
                GROUP BY f.subject_id, f.hadm_id,
                         f.index_operation_date, f.qualifying_date_count
            ),
            candidates AS (
                SELECT
                    o.*,
                    c.first_csurg_transfertime,
                    i.*,
                    DATE_DIFF('day', o.index_operation_date,
                              CAST(i.time_zero_icu AS DATE)) AS day_offset,
                    COUNT(*) OVER (PARTITION BY o.hadm_id)
                        AS candidate_icu_count,
                    ROW_NUMBER() OVER (
                        PARTITION BY o.hadm_id
                        ORDER BY
                            ABS(DATE_DIFF('day', o.index_operation_date,
                                          CAST(i.time_zero_icu AS DATE))),
                            CASE WHEN DATE_DIFF('day', o.index_operation_date,
                                                CAST(i.time_zero_icu AS DATE))
                                      >= 0 THEN 0 ELSE 1 END,
                            CASE WHEN LOWER(COALESCE(i.first_careunit, ''))
                                      LIKE '%cardiac%' THEN 0 ELSE 1 END,
                            i.time_zero_icu,
                            i.stay_id
                    ) AS selection_rank
                FROM operation o
                JOIN csurg c USING (hadm_id)
                JOIN icustays i
                  ON o.subject_id = i.subject_id
                 AND o.hadm_id = i.hadm_id
                WHERE DATE_DIFF('day', o.index_operation_date,
                                CAST(i.time_zero_icu AS DATE)) BETWEEN -1 AND 1
            ),
            selected AS (
                SELECT * FROM candidates WHERE selection_rank = 1
            ),
            assembled AS (
                SELECT
                    s.subject_id,
                    s.hadm_id,
                    s.stay_id,
                    s.index_operation_date,
                    CONCAT_WS('+',
                        CASE WHEN s.cabg_flag = 1 THEN 'CABG' END,
                        CASE WHEN s.valve_flag = 1 THEN 'VALVE' END,
                        CASE WHEN s.ascending_aorta_flag = 1
                             THEN 'ASCENDING_AORTA' END
                    ) AS surgery_group,
                    s.cabg_flag,
                    s.valve_flag,
                    s.ascending_aorta_flag,
                    s.qualifying_icd_codes,
                    s.qualifying_titles,
                    s.qualifying_date_count,
                    (s.qualifying_date_count > 1)::INTEGER
                        AS multiple_qualifying_dates_flag,
                    s.first_csurg_transfertime,
                    s.first_careunit,
                    s.last_careunit,
                    s.time_zero_icu,
                    s.icu_outtime,
                    s.day_offset AS icu_start_day_offset_from_operation,
                    s.candidate_icu_count,
                    p.sex,
                    p.anchor_age
                      + (EXTRACT(YEAR FROM s.time_zero_icu) - p.anchor_year)
                        AS age_at_icu_admission,
                    (p.anchor_age = 91)::INTEGER
                        AS age_over_89_deidentified_flag,
                    p.anchor_year_group,
                    a.hospital_admittime,
                    a.hospital_dischtime,
                    a.hospital_deathtime,
                    p.recorded_date_of_death,
                    a.admission_type,
                    a.admission_location,
                    a.discharge_location,
                    a.hospital_expire_flag,
                    CASE WHEN a.hospital_expire_flag = 1
                         THEN 'DIED_IN_HOSPITAL'
                         ELSE 'DISCHARGED_ALIVE' END
                        AS hospital_discharge_status,
                    ROUND(DATE_DIFF('second', s.time_zero_icu,
                                    s.icu_outtime) / 3600.0, 3)
                        AS icu_length_of_stay_hours,
                    ROUND(DATE_DIFF('second', s.time_zero_icu,
                                    s.icu_outtime) / 86400.0, 3)
                        AS icu_length_of_stay_days,
                    ROUND(DATE_DIFF('second', a.hospital_admittime,
                                    a.hospital_dischtime) / 86400.0, 3)
                        AS hospital_length_of_stay_days,
                    CASE WHEN a.hospital_deathtime IS NOT NULL
                         THEN ROUND(DATE_DIFF('second', s.time_zero_icu,
                                              a.hospital_deathtime)
                                    / 3600.0, 3) END
                        AS hours_from_icu_to_inhospital_death,
                    CASE WHEN p.recorded_date_of_death IS NOT NULL
                         THEN DATE_DIFF('day', CAST(s.time_zero_icu AS DATE),
                                        p.recorded_date_of_death) END
                        AS days_from_icu_to_recorded_death
                FROM selected s
                JOIN patients p USING (subject_id)
                JOIN admissions a USING (subject_id, hadm_id)
            )
            SELECT *
            FROM assembled
            WHERE age_at_icu_admission >= 18
            ORDER BY index_operation_date, subject_id, hadm_id
        """)

        n = con.execute("SELECT COUNT(*) FROM final_cohort").fetchone()[0]
        if n == 0:
            raise RuntimeError("The cohort is empty.")

        duplicate_hadm = con.execute("""
            SELECT COUNT(*) FROM (
                SELECT hadm_id FROM final_cohort
                GROUP BY hadm_id HAVING COUNT(*) > 1
            )
        """).fetchone()[0]
        if duplicate_hadm:
            raise RuntimeError("Duplicate hospital admissions were produced.")

        invalid_time = con.execute("""
            SELECT COUNT(*) FROM final_cohort
            WHERE time_zero_icu IS NULL
               OR icu_outtime <= time_zero_icu
               OR hospital_dischtime < hospital_admittime
        """).fetchone()[0]
        if invalid_time:
            raise RuntimeError("Invalid admission/discharge timing was found.")

        if OUTPUT.exists():
            OUTPUT.unlink()

        con.execute(f"COPY final_cohort TO '{p(OUTPUT)}' (HEADER, DELIMITER ',')")

        print("=" * 88)
        print("MIMIC-IV STANDARD CARDIAC SURGERY STARTING COHORT")
        print("=" * 88)
        print(f"Final adult ICU surgical admissions: {n:,}")
        print(f"Unique patients: {con.execute('SELECT COUNT(DISTINCT subject_id) FROM final_cohort').fetchone()[0]:,}")

        deaths, mortality = con.execute("""
            SELECT SUM(hospital_expire_flag),
                   100.0 * AVG(hospital_expire_flag)
            FROM final_cohort
        """).fetchone()
        print(f"In-hospital deaths: {int(deaths):,} ({mortality:.2f}%)")

        print("\nSurgery groups")
        for group, count in con.execute("""
            SELECT surgery_group, COUNT(*)
            FROM final_cohort
            GROUP BY surgery_group
            ORDER BY COUNT(*) DESC, surgery_group
        """).fetchall():
            print(f"  {group:<45} {count:>7,}")

        print("\nIndex ICU first care units")
        for unit, count in con.execute("""
            SELECT COALESCE(first_careunit, '[MISSING]'), COUNT(*)
            FROM final_cohort
            GROUP BY first_careunit
            ORDER BY COUNT(*) DESC, first_careunit
        """).fetchall():
            print(f"  {unit:<55} {count:>7,}")

        print("\nICU start day offset from operation date")
        for offset, count in con.execute("""
            SELECT icu_start_day_offset_from_operation, COUNT(*)
            FROM final_cohort
            GROUP BY icu_start_day_offset_from_operation
            ORDER BY icu_start_day_offset_from_operation
        """).fetchall():
            print(f"  {offset:>3} day(s): {count:>7,}")

        print("\nQualifying ICD codes in the final cohort")
        for row in con.execute("""
            SELECT
                q.procedure_group,
                q.icd_version,
                q.icd_code,
                COALESCE(q.long_title, '[TITLE NOT FOUND]'),
                COUNT(DISTINCT f.hadm_id)
            FROM qualifying_procedures q
            JOIN final_cohort f
              ON q.hadm_id = f.hadm_id
             AND q.chartdate = f.index_operation_date
            GROUP BY q.procedure_group, q.icd_version,
                     q.icd_code, q.long_title
            ORDER BY q.procedure_group, COUNT(DISTINCT f.hadm_id) DESC,
                     q.icd_version, q.icd_code
        """).fetchall():
            group, version, code, title, count = row
            print(f"  {group:<17} ICD-{version:<2} {code:<8} {count:>5,}  {title}")

        print(f"\nOutput: {OUTPUT}")
        print("=" * 88)
        return 0

    except Exception as exc:
        print("=" * 88, file=sys.stderr)
        print("COHORT BUILD FAILED", file=sys.stderr)
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        print("=" * 88, file=sys.stderr)
        return 2
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
