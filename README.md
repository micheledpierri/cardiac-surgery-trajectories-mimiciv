# TRAJECTORIES

Early usual-care and favorable-recovery trajectories after adult cardiac surgery: a multidomain reference study.

This repository documents cohort construction, clinical variable extraction, and construction and descriptive analysis of multidomain postoperative trajectories using MIMIC-IV v3.1. Analyses cover usual care, favorable recovery, clinical strata, trajectory features, observability and cross-domain associations. Records are anchored to ICU stays and do not necessarily represent unique patients.

## Data and authorized access

Obtain [MIMIC-IV v3.1 from PhysioNet](https://physionet.org/content/mimiciv/3.1/). Access requires PhysioNet credentialing, completion of the required training, and acceptance of the dataset's data use agreement. Follow these conditions when handling raw and derived data. No MIMIC-IV data or patient-level results are included here.

Scripts read compressed CSV tables from the local MIMIC-IV `hosp/` and `icu/` directories. Each script identifies its required tables. Use version 3.1; fixed cohort and source checks remain part of the code.

## Structure

```text
github_release/
    README.md
    Scripts/
        01_identify_standard_cardiac_surgery_cohort.py
        ...
        27_cross_domain_observability.py
```

The release contains only this README and 27 Python scripts. Scripts read and generate local analytical files beneath `<repository>/data/` when run; generated data are not part of this release.

## Python dependencies

Use Python supporting the script syntax (Python 3.10 or later). Actual third-party imports are `duckdb` for scripts 01-26 and `numpy`, `pandas`, and `scipy` for script 27. For example, install them in your chosen environment with:

```sh
python -m pip install duckdb numpy pandas scipy
```

No tested package-version combination is asserted. Scientific scripts were not executed during release preparation.

## Configuration

Keep the scripts in `<repository>/Scripts/`. Their project-root logic resolves to its parent and uses `<repository>/data/`. Run commands from the repository root.

In scripts reading raw MIMIC-IV, replace `Path("path/to/mimic-iv-3.1")` with your authorized local dataset root containing `hosp/` and `icu/`. Prefer an absolute local path; a relative raw-data path resolves against the current working directory. Script 16 uses generated inputs only. Script 25 also needs the raw-data root for admissions.

Script 20 uses the existing CLI:

```sh
python Scripts/20_extract_vasoactive_inotropes.py --mimic-root "/your/local/mimic-iv-3.1"
```

Its other options are `--cohort`, `--output-dir`, `--threads` and `--memory-limit`. Keep the default cohort and output locations unless subsequent scripts are configured to find the same canonical files.

## Execution order

Run scripts one at a time in the following order, allowing each to finish successfully before proceeding. For example:

```sh
python Scripts/01_identify_standard_cardiac_surgery_cohort.py
python Scripts/02_define_postoperative_time_zero.py
python Scripts/03_build_final_cohort_summary.py
```

Independent extractors are listed in a stable order; each does not necessarily depend on the preceding extractor.

| Step | Release script | Original component filename |
| --- | --- | --- |
| 01 | `01_identify_standard_cardiac_surgery_cohort.py` | `0.00_identify_standard_cardiac_surgery_cohort.py` |
| 02 | `02_define_postoperative_time_zero.py` | `00_03_define_postoperative_time_zero.py` |
| 03 | `03_build_final_cohort_summary.py` | `00_04_build_final_cohort_summary.py` |
| 04 | `04_extract_map.py` | `01_00_extract_map.py` |
| 05 | `05_extract_heart_rate.py` | `01_01_extract_heart_rate.py` |
| 06 | `06_extract_lactate.py` | `01_02_extract_lactate.py` |
| 07 | `07_extract_ph.py` | `01_03_extract_ph.py` |
| 08 | `08_extract_glucose.py` | `01_04_extract_glucose.py` |
| 09 | `09_extract_base_excess.py` | `01_06_extract_base_excess.py` |
| 10 | `10_extract_urine_output.py` | `01_07_extract_urine_output.py` |
| 11 | `11_extract_diuretic_exposure.py` | `01_08_extract_diuretic_exposure.py` |
| 12 | `12_extract_creatinine_outcomes.py` | `01_09_extract_creatinine_outcomes.py` |
| 13 | `13_extract_fluid_balance.py` | `01_10_extract_fluid_balance.py` |
| 14 | `14_extract_respiratory_support.py` | `01_11_extract_respiratory_support.py` |
| 15 | `15_extract_fio2.py` | `01_12_extract_fio2.py` |
| 16 | `16_extract_pf.py` | `01_13_extract_pf.py` |
| 17 | `17_extract_respiratory_outcomes.py` | `01_14_extract_respiratory_outcomes.py` |
| 18 | `18_extract_bleeding_trajectory.py` | `01_15_extract_bleeding_trajectory.py` |
| 19 | `19_extract_revision_for_bleeding_outcome.py` | `01_17_extract_revision_for_bleeding_outcome.py` |
| 20 | `20_extract_vasoactive_inotropes.py` | `01_18_extract_vasoactive_inotropes.py` |
| 21 | `21_build_study1_dataset.py` | `02_01_build_study1_dataset.py` |
| 22 | `22_describe_usual_care_trajectories.py` | `02_02_describe_usual_care_trajectories.py` |
| 23 | `23_describe_clinical_strata.py` | `02_03_describe_clinical_strata.py` |
| 24 | `24_engineer_trajectory_features.py` | `02_04_engineer_trajectory_features.py` |
| 25 | `25_build_favorable_recovery.py` | `02_05_build_favorable_recovery.py` |
| 26 | `26_describe_favorable_recovery_trajectories.py` | `02_06_describe_favorable_recovery_trajectories.py` |
| 27 | `27_cross_domain_observability.py` | `02_07_cross_domain_observability.py` |

Essential dependencies:

- 01 -> 02 -> 03: starting cohort, postoperative time zero and final analytical cohort. All clinical extractors use the final cohort.
- 13 (fluid balance) requires both urine trajectories from 10. 18 (bleeding) requires the six-hour urine output from 10 for baseline weight.
- 16 (P/F) requires respiratory-support outputs from 14 and FiO2/PaO2 matches from 15. Steps 15 and 17 implement respiratory-state rules internally from raw events, rather than loading step 14 outputs.
- 21 assembles the cohort and hourly/six-hour trajectories from 04-11, 13-14, 16, 18 and 20.
- 22 and 23 use 21 plus six-hour diuretic and vasoactive sources from 11 and 20; 23 also reads the cohort. 24 uses 21 plus the hourly diuretic source from 11.
- 25 requires the cohort, creatinine outcomes (12), fluid trajectories (13), respiratory outcomes (17), revision-for-bleeding outcomes (19), and raw admissions. Complication inputs provide construct-validation context; they do not define favorable-recovery classification.
- 26 requires favorable-recovery membership from 25, master datasets from 21, and six-hour sources from 11 and 20. Usual-care continuous and treatment summaries from 22 are optional comparison inputs: absence omits the corresponding comparison rows; absence of both produces an empty comparison table.
- 27 requires both feature tables from 24 and the availability summary from 22.

## Input and output conventions

Only Python filenames have sequential release numbering. Dataset filenames retain their established Group 00/01/02 prefixes to preserve producer/consumer compatibility. Some source documentation retains original component identifiers shown above; these are not imports or commands that execute another script.

Cohort files are `00_cardiac_surgery_starting_cohort.csv`, `00_03_postoperative_time_zero.csv` and `00_04_final_analytic_cohort.csv` under `data/`. Extractors produce canonical Parquet trajectories/outcomes and CSV extraction audits. Script 21 produces `02_01_study1_hourly.parquet` and `02_01_study1_six_hour.parquet`. Subsequent scripts produce descriptive CSV summaries, features, favorable-recovery outputs and the cross-domain text report. Each script declares exact filenames.

Windows use the validated postoperative time origin and observation endpoint. The main trajectory horizon is 24 hours; chest-drainage trajectories cover 12 hours. Eligibility, missingness, exposure zeros and coverage are variable-specific. Follow the documented definitions rather than treating missing measurements as zero. Existing validation checks may stop execution if their conditions are not met. Reruns may replace declared outputs.

## Scope limitations

This release covers cohort construction, clinical extraction and descriptive trajectory analysis. It excludes the transfusion extractor and Group 03, S1, S2 and S3 scripts. It does not include final publication figure rendering, editorial table assembly, or every supplementary methodological check. It is not presented as independently reproducing all manuscript figures and tables. No data, generated results or license file are included.
