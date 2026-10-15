"""
01_17_extract_revision_for_bleeding_outcome.py

Trajectories project
Bleeding outcomes - surgical revision/re-exploration for bleeding
FROZEN PRODUCTION COMPONENT

Purpose
-------
Construct the final conservative outcome for early surgical revision /
re-exploration for clinically relevant intrathoracic bleeding after cardiac
surgery.

The implemented outcome uses the two accepted ICD pathways described below.

Final source-closure logic
--------------------------
The final rule reflects the completed source-closure validation.

A) ICD-9-CM reopening pathway
   Accept ICD-9-CM 34.03 "Reopening of recent thoracotomy site" occurring on
   calendar day 0, 1, or 2 relative to postoperative time zero.

   This signal is accepted because its semantics explicitly identify reopening
   of a recent thoracotomy. In the prior manual source-closure audit, all 9 cases
   that had escaped the generic index-operation matcher nevertheless showed a
   distinct cardiac index operation in the same admission.

B) ICD-10-PCS bleeding-control pathway
   Consider only open control of bleeding in the target intrathoracic domains:

       0W3C0ZZ  Control Bleeding in Mediastinum, Open Approach
       0W3D0ZZ  Control Bleeding in Pericardial Cavity, Open Approach
       0W390ZZ  Control Bleeding in Right Pleural Cavity, Open Approach
       0W3B0ZZ  Control Bleeding in Left Pleural Cavity, Open Approach

   occurring on calendar day 0, 1, or 2 relative to t0.

   These codes are accepted only when a DISTINCT cardiac index procedure is
   independently visible in PROCEDURES_ICD using the same semantic index matcher
   validated during source-closure review.

   Two reviewed cases with isolated same-day mediastinal Control Bleeding that were
   not resolved by that matcher remain excluded. This deliberately avoids
   treating possible index-operative hemostasis as re-exploration.

Expected source-closure count
-----------------------------
Fixed reproduction targets from completed source-closure validation:

    ICD-9 reopening candidates accepted                 66
    ICD-10 bleeding-control + visible index accepted    51
                                                        --
    Expected final positives                           117

The script asserts this count. A mismatch stops execution and requires review.

Outcome role
------------
This is a BLEEDING OUTCOME and is independent of:
- chest-drain trajectory;
- transfusion outcome.

Timing
------
ICD procedures provide calendar-date resolution, not exact event time.
Therefore the primary outcome is a binary early re-exploration flag.
PROCEDUREEVENTS itemid 225467 "Chest Opened" is retained only as supportive
exact-time information and does NOT determine outcome status.

Outputs
-------
Canonical outputs:
data/01_17_revision_for_bleeding_outcomes.parquet
data/01_17_revision_for_bleeding_outcomes_audit.csv

These filenames identify the frozen production outputs.

Inputs
------
data/00_04_final_analytic_cohort.csv
Raw MIMIC-IV 3.1 files relative to MIMIC_ROOT:
  icu/procedureevents.csv.gz
  hosp/procedures_icd.csv.gz
  hosp/d_icd_procedures.csv.gz

Prior manual source-closure review established the frozen rules; no manual
case list or historical audit file is required at runtime.
"""

from pathlib import Path
import duckdb


# =============================================================================
# Paths / constants
# =============================================================================

# Project directory containing the Scripts and data directories.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
# Set this path to the local MIMIC-IV 3.1 directory.
MIMIC_ROOT = Path("path/to/mimic-iv-3.1")

COHORT_FILE = PROJECT_ROOT / "data" / "00_04_final_analytic_cohort.csv"

PROCEDUREEVENTS_FILE = MIMIC_ROOT / "icu" / "procedureevents.csv.gz"
PROCEDURES_ICD_FILE = MIMIC_ROOT / "hosp" / "procedures_icd.csv.gz"
D_ICD_PROCEDURES_FILE = MIMIC_ROOT / "hosp" / "d_icd_procedures.csv.gz"

OUT_OUTCOMES = (
    PROJECT_ROOT / "data" / "01_17_revision_for_bleeding_outcomes.parquet"
)
OUT_AUDIT = (
    PROJECT_ROOT / "data" / "01_17_revision_for_bleeding_outcomes_audit.csv"
)

EXPECTED_COHORT_N = 9305
EXPECTED_FINAL_POSITIVES_N = 117

CHEST_OPENED_ITEMID = 225467

ICD9_REOPENING_CODE = "3403"

ICD10_BLEEDING_CODES = (
    "0W3C0ZZ",
    "0W3D0ZZ",
    "0W390ZZ",
    "0W3B0ZZ",
)

# IMPORTANT:
# This is the frozen semantic index-operation matcher used in source-closure
# validation. It is intentionally NOT expanded with subsequently reviewed titles.
#
# Prior manual review resolved the nine missed ICD-9 reopening cases.
# For ICD-10 Control Bleeding, failure of this matcher is intentionally
# conservative because the bleeding-control code may represent index-operative
# hemostasis rather than a separate re-exploration.
INDEX_CABG_REGEX = (
    "coronary artery bypass|bypass coronary|"
    "aortocoronary bypass|internal mammary.*coronary"
)

INDEX_VALVE_REGEX = (
    "replacement of .* valve|repair of .* valve|"
    "replacement of aortic valve|replacement of mitral valve|"
    "repair of aortic valve|repair of mitral valve|"
    "replacement of tricuspid valve|repair of tricuspid valve|"
    "aortic valve replacement|mitral valve replacement|"
    "mitral valve repair|tricuspid valve repair"
)

INDEX_AORTA_REGEX = (
    "replacement of thoracic aorta|"
    "replacement of ascending aorta|"
    "repair of thoracic aorta|"
    "repair of ascending aorta|"
    "resection of thoracic aorta|"
    "resection of ascending aorta|"
    "aortic arch|"
    "thoracic aortic aneurysm.*repair"
)

INDEX_ANY_REGEX = (
    f"({INDEX_CABG_REGEX})|"
    f"({INDEX_VALVE_REGEX})|"
    f"({INDEX_AORTA_REGEX})"
)


def qpath(path: Path) -> str:
    return str(path).replace("'", "''")


def sql_strings(values) -> str:
    return ", ".join("'" + str(x).replace("'", "''") + "'" for x in values)


for required in [
    COHORT_FILE,
    PROCEDUREEVENTS_FILE,
    PROCEDURES_ICD_FILE,
    D_ICD_PROCEDURES_FILE,
]:
    if not required.exists():
        raise FileNotFoundError(f"Required file not found: {required}")

OUT_OUTCOMES.parent.mkdir(parents=True, exist_ok=True)

ICD10_BLEED_SQL = sql_strings(ICD10_BLEEDING_CODES)

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
        postoperative_time_zero::TIMESTAMP AS postoperative_time_zero,
        observation_end_time::TIMESTAMP AS observation_end_time,
        observation_end_type
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
# ICD procedure dictionary and all cohort procedures
# =============================================================================

con.execute(
    f"""
    CREATE TABLE d_icd_procedures AS
    SELECT
        icd_code,
        try_cast(icd_version AS INTEGER) AS icd_version,
        long_title
    FROM read_csv_auto(
        '{qpath(D_ICD_PROCEDURES_FILE)}',
        header=true,
        all_varchar=true
    )
    """
)

con.execute(
    f"""
    CREATE TABLE cohort_procedures AS
    SELECT
        c.subject_id,
        c.hadm_id,
        c.stay_id,
        c.surgery_group,
        c.postoperative_time_zero,

        p.seq_num,
        p.chartdate,
        p.icd_code,
        p.icd_version::INTEGER AS icd_version,
        d.long_title,

        date_diff(
            'day',
            cast(c.postoperative_time_zero AS DATE),
            p.chartdate
        ) AS day_from_t0

    FROM read_csv_auto(
        '{qpath(PROCEDURES_ICD_FILE)}',
        header=true,
        all_varchar=false
    ) p

    INNER JOIN cohort c
        ON p.subject_id = c.subject_id
       AND p.hadm_id = c.hadm_id

    LEFT JOIN d_icd_procedures d
        ON p.icd_code = d.icd_code
       AND p.icd_version = d.icd_version
    """
)


# =============================================================================
# ICD-9 reopening pathway
# =============================================================================

con.execute(
    f"""
    CREATE TABLE icd9_reopening AS
    SELECT
        stay_id,
        1 AS icd9_reopening_flag,
        min(day_from_t0) AS first_reopening_day,
        min(seq_num) FILTER (
            WHERE day_from_t0 = (
                SELECT min(cp2.day_from_t0)
                FROM cohort_procedures cp2
                WHERE cp2.stay_id = cohort_procedures.stay_id
                  AND cp2.icd_version = 9
                  AND cp2.icd_code = '{ICD9_REOPENING_CODE}'
                  AND cp2.day_from_t0 BETWEEN 0 AND 2
            )
        ) AS first_reopening_seq_num

    FROM cohort_procedures

    WHERE icd_version = 9
      AND icd_code = '{ICD9_REOPENING_CODE}'
      AND day_from_t0 BETWEEN 0 AND 2

    GROUP BY stay_id
    """
)


# =============================================================================
# ICD-10 open bleeding-control candidates
# =============================================================================

con.execute(
    f"""
    CREATE TABLE icd10_bleeding_candidates AS
    SELECT
        *,
        CASE icd_code
            WHEN '0W3C0ZZ' THEN 'MEDIASTINUM'
            WHEN '0W3D0ZZ' THEN 'PERICARDIUM'
            WHEN '0W390ZZ' THEN 'RIGHT_PLEURA'
            WHEN '0W3B0ZZ' THEN 'LEFT_PLEURA'
            ELSE 'OTHER'
        END AS bleeding_site
    FROM cohort_procedures
    WHERE icd_version = 10
      AND icd_code IN ({ICD10_BLEED_SQL})
      AND day_from_t0 BETWEEN 0 AND 2
    """
)


# =============================================================================
# Frozen semantic matcher for distinct index operation
# =============================================================================

con.execute(
    f"""
    CREATE TABLE index_proc_candidates AS
    SELECT
        *,
        CASE
            WHEN regexp_matches(
                lower(coalesce(long_title, '')),
                '{INDEX_CABG_REGEX}'
            ) THEN 'CABG'
            WHEN regexp_matches(
                lower(coalesce(long_title, '')),
                '{INDEX_VALVE_REGEX}'
            ) THEN 'VALVE'
            WHEN regexp_matches(
                lower(coalesce(long_title, '')),
                '{INDEX_AORTA_REGEX}'
            ) THEN 'AORTA'
            ELSE 'OTHER'
        END AS index_semantic_group
    FROM cohort_procedures
    WHERE regexp_matches(
        lower(coalesce(long_title, '')),
        '{INDEX_ANY_REGEX}'
    )
      AND day_from_t0 BETWEEN -1 AND 0
    """
)

con.execute(
    """
    CREATE TABLE icd10_bleeding_with_index AS
    SELECT
        b.stay_id,
        b.icd_code,
        b.long_title AS bleeding_title,
        b.bleeding_site,
        b.chartdate AS bleeding_chartdate,
        b.day_from_t0 AS bleeding_day_from_t0,
        b.seq_num AS bleeding_seq_num,

        i.icd_code AS index_icd_code,
        i.icd_version AS index_icd_version,
        i.long_title AS index_title,
        i.index_semantic_group,
        i.chartdate AS index_chartdate,
        i.day_from_t0 AS index_day_from_t0,
        i.seq_num AS index_seq_num,

        CASE
            WHEN i.stay_id IS NOT NULL
             AND NOT (
                    i.icd_version = b.icd_version
                AND i.icd_code = b.icd_code
             )
                THEN 1
            ELSE 0
        END AS distinct_index_visible_flag

    FROM icd10_bleeding_candidates b

    LEFT JOIN index_proc_candidates i
        ON b.stay_id = i.stay_id
       AND i.chartdate <= b.chartdate
    """
)

con.execute(
    """
    CREATE TABLE icd10_accepted AS
    SELECT
        stay_id,
        1 AS icd10_control_bleeding_with_index_flag,
        min(bleeding_day_from_t0) AS first_icd10_bleeding_day,

        string_agg(
            DISTINCT bleeding_site,
            ' | '
            ORDER BY bleeding_site
        ) AS accepted_bleeding_sites,

        string_agg(
            DISTINCT bleeding_title,
            ' | '
            ORDER BY bleeding_title
        ) AS accepted_bleeding_titles,

        string_agg(
            DISTINCT index_semantic_group || ':' || index_title,
            ' | '
            ORDER BY index_semantic_group || ':' || index_title
        ) FILTER (
            WHERE distinct_index_visible_flag = 1
        ) AS visible_index_procedures

    FROM icd10_bleeding_with_index

    WHERE distinct_index_visible_flag = 1

    GROUP BY stay_id
    """
)


# =============================================================================
# Supportive exact-time Chest Opened
# =============================================================================

con.execute(
    f"""
    CREATE TABLE chest_opened AS
    SELECT
        c.stay_id,
        p.starttime,

        date_diff(
            'minute',
            c.postoperative_time_zero,
            p.starttime
        ) AS minutes_from_t0

    FROM read_csv_auto(
        '{qpath(PROCEDUREEVENTS_FILE)}',
        header=true,
        all_varchar=false
    ) p

    INNER JOIN cohort c
        ON p.stay_id = c.stay_id

    WHERE p.itemid = {CHEST_OPENED_ITEMID}
      AND lower(trim(coalesce(p.statusdescription, ''))) <> 'rewritten'
      AND p.starttime >= c.postoperative_time_zero
      AND p.starttime < c.postoperative_time_zero + INTERVAL '72 hours'
    """
)

con.execute(
    """
    CREATE TABLE chest_opened_patient AS
    SELECT
        stay_id,
        1 AS chest_opened_0_72h_flag,
        min(minutes_from_t0) AS first_chest_opened_min
    FROM chest_opened
    GROUP BY stay_id
    """
)


# =============================================================================
# Final patient-level outcome
# =============================================================================

con.execute(
    """
    CREATE TABLE outcomes AS
    SELECT
        c.subject_id,
        c.hadm_id,
        c.stay_id,
        c.surgery_group,
        c.postoperative_time_zero,

        coalesce(r.icd9_reopening_flag, 0)
            AS icd9_reopening_flag,
        r.first_reopening_day,
        r.first_reopening_seq_num,

        coalesce(b.icd10_control_bleeding_with_index_flag, 0)
            AS icd10_control_bleeding_with_index_flag,
        b.first_icd10_bleeding_day,
        b.accepted_bleeding_sites,
        b.accepted_bleeding_titles,
        b.visible_index_procedures,

        coalesce(ch.chest_opened_0_72h_flag, 0)
            AS chest_opened_0_72h_flag,
        ch.first_chest_opened_min,

        CASE
            WHEN coalesce(r.icd9_reopening_flag, 0) = 1
                THEN 1
            WHEN coalesce(b.icd10_control_bleeding_with_index_flag, 0) = 1
                THEN 1
            ELSE 0
        END AS revision_for_bleeding_early_flag,

        CASE
            WHEN coalesce(r.icd9_reopening_flag, 0) = 1
                THEN 'ICD9_REOPENING_THORACOTOMY'
            WHEN coalesce(b.icd10_control_bleeding_with_index_flag, 0) = 1
                THEN 'ICD10_CONTROL_BLEEDING_WITH_DISTINCT_INDEX'
            ELSE 'NO_ACCEPTED_REVISION_SIGNAL'
        END AS revision_for_bleeding_source

    FROM cohort c
    LEFT JOIN icd9_reopening r USING (stay_id)
    LEFT JOIN icd10_accepted b USING (stay_id)
    LEFT JOIN chest_opened_patient ch USING (stay_id)
    """
)


# =============================================================================
# Validation invariants
# =============================================================================

out_n, out_unique_n = con.execute(
    "SELECT COUNT(*), COUNT(DISTINCT stay_id) FROM outcomes"
).fetchone()

if out_n != EXPECTED_COHORT_N or out_unique_n != EXPECTED_COHORT_N:
    raise RuntimeError(
        f"Unexpected outcome dimensions: {out_n} / {out_unique_n}"
    )

final_positive_n = con.execute(
    """
    SELECT COUNT(*)
    FROM outcomes
    WHERE revision_for_bleeding_early_flag = 1
    """
).fetchone()[0]

if final_positive_n != EXPECTED_FINAL_POSITIVES_N:
    raise RuntimeError(
        f"Expected {EXPECTED_FINAL_POSITIVES_N} final positives from "
        f"v6/v7 source closure, found {final_positive_n}. "
        "Do not canonicalize; inspect source counts."
    )

icd9_positive_n = con.execute(
    """
    SELECT COUNT(*)
    FROM outcomes
    WHERE revision_for_bleeding_source = 'ICD9_REOPENING_THORACOTOMY'
    """
).fetchone()[0]

icd10_positive_n = con.execute(
    """
    SELECT COUNT(*)
    FROM outcomes
    WHERE revision_for_bleeding_source
          = 'ICD10_CONTROL_BLEEDING_WITH_DISTINCT_INDEX'
    """
).fetchone()[0]

if icd9_positive_n != 66:
    raise RuntimeError(
        f"Expected 66 ICD-9 reopening positives, found {icd9_positive_n}."
    )

if icd10_positive_n != 51:
    raise RuntimeError(
        f"Expected 51 ICD-10 accepted positives, found {icd10_positive_n}."
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

con.executemany(
    "INSERT INTO audit VALUES (?, ?, ?, ?, ?)",
    [
        ("OVERALL", None, "cohort_n", cohort_n, None),
        ("OUTCOME", "ALL", "positive_n", final_positive_n, None),
        ("OUTCOME", "ICD9_REOPENING", "positive_n", icd9_positive_n, None),
        ("OUTCOME", "ICD10_CONTROL_WITH_INDEX", "positive_n", icd10_positive_n, None),
    ],
)

# Raw candidate counts.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'RAW_CANDIDATE',
        'ICD9_REOPENING',
        'patients_n',
        COUNT(DISTINCT stay_id)::DOUBLE,
        'calendar_day_0_2'
    FROM cohort_procedures
    WHERE icd_version = 9
      AND icd_code = '3403'
      AND day_from_t0 BETWEEN 0 AND 2
    """
)

con.execute(
    """
    INSERT INTO audit
    SELECT
        'RAW_CANDIDATE',
        'ICD10_CONTROL_BLEEDING',
        'patients_n',
        COUNT(DISTINCT stay_id)::DOUBLE,
        'calendar_day_0_2'
    FROM icd10_bleeding_candidates
    """
)

# Accepted ICD-10 by site.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'ICD10_ACCEPTED_SITE',
        bleeding_site,
        'patients_n',
        COUNT(DISTINCT stay_id)::DOUBLE,
        NULL
    FROM icd10_bleeding_with_index
    WHERE distinct_index_visible_flag = 1
    GROUP BY bleeding_site
    """
)

# Excluded unresolved ICD-10 candidates.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'ICD10_EXCLUDED_AMBIGUOUS',
        cast(b.stay_id AS VARCHAR),
        'candidate',
        NULL,
        'code=' || b.icd_code
            || '; title=' || coalesce(b.long_title, '<NULL>')
            || '; day=' || cast(b.day_from_t0 AS VARCHAR)
    FROM icd10_bleeding_candidates b
    LEFT JOIN icd10_accepted a USING (stay_id)
    WHERE a.stay_id IS NULL
    """
)

# Positive detail.
con.execute(
    """
    INSERT INTO audit
    SELECT
        'POSITIVE_DETAIL',
        cast(stay_id AS VARCHAR),
        revision_for_bleeding_source,
        NULL,
        'surgery_group=' || coalesce(surgery_group, '<NULL>')
            || '; reopening_day='
            || coalesce(cast(first_reopening_day AS VARCHAR), '<NULL>')
            || '; icd10_bleeding_day='
            || coalesce(cast(first_icd10_bleeding_day AS VARCHAR), '<NULL>')
            || '; bleeding_sites='
            || coalesce(accepted_bleeding_sites, '<NULL>')
            || '; chest_opened='
            || cast(chest_opened_0_72h_flag AS VARCHAR)
            || '; chest_first_min='
            || coalesce(cast(first_chest_opened_min AS VARCHAR), '<NULL>')
    FROM outcomes
    WHERE revision_for_bleeding_early_flag = 1
    """
)


# =============================================================================
# Export development validation outputs
# =============================================================================

con.execute(
    f"""
    COPY (
        SELECT *
        FROM outcomes
        ORDER BY stay_id
    )
    TO '{qpath(OUT_OUTCOMES)}'
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
                WHEN 'RAW_CANDIDATE' THEN 2
                WHEN 'OUTCOME' THEN 3
                WHEN 'ICD10_ACCEPTED_SITE' THEN 4
                WHEN 'ICD10_EXCLUDED_AMBIGUOUS' THEN 5
                WHEN 'POSITIVE_DETAIL' THEN 6
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

print("=" * 122)
print("TRAJECTORIES - FINAL REVISION FOR BLEEDING OUTCOME v2")
print("=" * 122)
print(f"Cohort rows / unique stays              : {cohort_n} / {cohort_unique_n}")
print()

print("Final outcome")
print("-------------")
print(
    con.execute(
        """
        SELECT
            COUNT(*) AS cohort_n,
            COUNT(*) FILTER (
                WHERE revision_for_bleeding_early_flag = 1
            ) AS revision_n,
            round(
                100.0 * COUNT(*) FILTER (
                    WHERE revision_for_bleeding_early_flag = 1
                ) / COUNT(*),
                2
            ) AS revision_percent
        FROM outcomes
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Accepted source")
print("---------------")
print(
    con.execute(
        """
        SELECT
            revision_for_bleeding_source,
            COUNT(*) AS patients_n
        FROM outcomes
        WHERE revision_for_bleeding_early_flag = 1
        GROUP BY revision_for_bleeding_source
        ORDER BY patients_n DESC
        """
    ).fetchdf().to_string(index=False)
)
print()

print("ICD-10 accepted by bleeding site")
print("--------------------------------")
print(
    con.execute(
        """
        SELECT
            bleeding_site,
            COUNT(DISTINCT stay_id) AS patients_n
        FROM icd10_bleeding_with_index
        WHERE distinct_index_visible_flag = 1
        GROUP BY bleeding_site
        ORDER BY patients_n DESC, bleeding_site
        """
    ).fetchdf().to_string(index=False)
)
print()

print("ICD-10 candidates excluded as unresolved/ambiguous")
print("--------------------------------------------------")
print(
    con.execute(
        """
        SELECT
            b.stay_id,
            b.icd_code,
            b.long_title,
            b.day_from_t0,
            b.seq_num
        FROM icd10_bleeding_candidates b
        LEFT JOIN icd10_accepted a USING (stay_id)
        WHERE a.stay_id IS NULL
        ORDER BY b.stay_id
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Supportive exact Chest Opened timing among final positives")
print("----------------------------------------------------------")
print(
    con.execute(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE revision_for_bleeding_early_flag = 1
                  AND first_chest_opened_min IS NOT NULL
            ) AS positives_with_exact_time_n,
            round(
                median(first_chest_opened_min) FILTER (
                    WHERE revision_for_bleeding_early_flag = 1
                      AND first_chest_opened_min IS NOT NULL
                ),
                1
            ) AS median_min,
            round(
                quantile_cont(first_chest_opened_min, 0.25) FILTER (
                    WHERE revision_for_bleeding_early_flag = 1
                      AND first_chest_opened_min IS NOT NULL
                ),
                1
            ) AS p25_min,
            round(
                quantile_cont(first_chest_opened_min, 0.75) FILTER (
                    WHERE revision_for_bleeding_early_flag = 1
                      AND first_chest_opened_min IS NOT NULL
                ),
                1
            ) AS p75_min
        FROM outcomes
        """
    ).fetchdf().to_string(index=False)
)
print()

print("Validation outputs")
print("------------------")
print(OUT_OUTCOMES)
print(OUT_AUDIT)
print()
print("Expected and observed final positives agree:", final_positive_n)
print("Final source-closure extraction v2 complete.")
