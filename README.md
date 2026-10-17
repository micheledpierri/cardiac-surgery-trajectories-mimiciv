# TRAJECTORIES

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Early usual-care and favorable-recovery trajectories after adult cardiac surgery: a multidomain reference study**

## Overview

TRAJECTORIES is a reproducible analysis workflow for describing the early postoperative course of adult cardiac-surgery patients using MIMIC-IV v3.1. The Study 1 analyses construct an ICU-stay cohort and summarize clinical measurements, respiratory and cardiovascular support, fluid-related variables, observability, and short-term temporal patterns during the first 24 postoperative hours. Chest-drainage trajectories are evaluated over the first 12 hours.

The principal outputs describe **usual-care** and **favorable-recovery** reference trajectories. The analyses are descriptive and do not establish individual prognosis, causality, or the temporal precedence of adverse outcomes. The ICU stay is the analytic unit; multiple stays may belong to the same patient.

## Data access

The analyses require [MIMIC-IV v3.1](https://physionet.org/content/mimiciv/3.1/) from PhysioNet. Access is subject to credentialing, required training, and acceptance of the applicable data use agreement. Authorized users should obtain the dataset through PhysioNet and maintain it in a secure local environment.

**No MIMIC-IV records, patient-level extracts, or generated analysis datasets are distributed in this repository.** Scripts accessing the source database expect the local `hosp/` and `icu/` directories supplied with MIMIC-IV.

## Repository contents

```text
TRAJECTORIES/
├── README.md
├── LICENSE
├── .gitignore
└── Scripts/
    ├── 01_identify_standard_cardiac_surgery_cohort.py
    ├── ...
    ├── 27_cross_domain_observability.py
    └── 28_trajectory_transitions.py
```

The repository includes **28 sequentially numbered Python scripts**, organized into the following stages:

| Scripts | Analysis stage |
| --- | --- |
| 01–03 | Cardiac-surgery cohort identification, postoperative time origin, and final cohort |
| 04–20 | Clinical-variable and outcome extraction, including hemodynamic, metabolic, renal, fluid, respiratory, bleeding, and treatment domains |
| 21 | Assembly of hourly and six-hour analytical datasets |
| 22–23 | Usual-care trajectory summaries and descriptive clinical stratification |
| 24 | Patient-level trajectory features |
| 25–26 | Favorable-recovery classification and reference trajectories |
| 27 | Cross-domain observability and descriptive analysis |
| 28 | Respiratory–vasoactive support transitions and four-window sequences |

Script numbering is specific to this public release. Intermediate dataset filenames retain the project's established identifiers (such as `01_11_*`, `02_01_*`, and `03_08_*`) because downstream scripts depend on those filenames.

## Requirements

Python **3.10 or later** is required. The scripts use the following third-party packages:

- `duckdb` for scripts 01–26;
- `numpy`, `pandas`, and `scipy` for script 27;
- `numpy`, `pandas`, and `pyarrow` for script 28.

A basic environment can be prepared with:

```bash
python -m pip install duckdb numpy pandas scipy pyarrow
```

Package versions and platform-specific configurations are not pinned. Successful execution may depend on the installed database readers, available memory, and the local organization of authorized MIMIC-IV files. The entire pipeline has not been independently tested as a single end-to-end installation.

## Configuration and execution

Clone the repository and preserve the `Scripts/` directory relative to its root. Scripts write derived files under the project's local `data/` directory unless otherwise specified; these files are **not** included in the repository.

For scripts accessing raw MIMIC-IV tables, configure the provided `path/to/mimic-iv-3.1` placeholder to point to the authorized local dataset root containing `hosp/` and `icu/`. Script 20 additionally supports an explicit command-line option:

```bash
python Scripts/20_extract_vasoactive_inotropes.py --mimic-root "/path/to/mimic-iv-3.1"
```

Run the scripts in numerical order where their dependencies require it. For example:

```bash
python Scripts/01_identify_standard_cardiac_surgery_cohort.py
python Scripts/02_define_postoperative_time_zero.py
python Scripts/03_build_final_cohort_summary.py
```

Clinical extraction scripts do not all depend on each other; some require outputs from specific earlier stages. In particular:

- Fluid-balance extraction (13) requires urine-output extraction (10); bleeding-trajectory extraction (18) also uses the six-hour urine-output data.
- P/F extraction (16) requires respiratory-support (14) and FiO₂/PaO₂-related (15) outputs.
- Dataset assembly (21) combines the cohort and selected clinical extractions; downstream descriptive scripts (22–28) depend on its outputs as specified in their source code.
- Favorable-recovery construction (25) additionally uses outcome datasets and the authorized MIMIC-IV admissions table.
- Support-transition analysis (28) uses the six-hour analytical dataset (21) and respiratory-support data (14), independently of scripts 22–27.

Each script declares its input and output filenames. The source code is the authoritative reference for individual file dependencies and command-line parameters. Some scripts enforce cohort-specific integrity checks and will terminate if the required inputs are missing or inconsistent.

## Support-transition analysis

`Scripts/28_trajectory_transitions.py` implements the descriptive support-transition analysis (`03_08`). It requires the following **locally generated** inputs in the chosen project's `data/` directory:

- `02_01_study1_six_hour.parquet`
- `01_11_respiratory_support_six_hour.parquet`

Example:

```bash
python Scripts/28_trajectory_transitions.py \
  --project "/path/to/project" \
  --output-dir "/path/to/output"
```

By default, `--project` refers to the repository root and outputs are written to `<project>/data/`. Specify a separate `--output-dir` to avoid replacing existing results.

The analysis produces:

- `03_08_joint_transitions_wilson95.csv`: adjacent six-hour joint respiratory–vasoactive state transitions;
- `03_08_support_sequences_wilson95.csv`: four-window exposure sequences;
- `03_08_directional_summary_wilson95.csv`: descriptive joint directions of change.

Invasive ventilation is defined as documented invasive-ventilation time within a window. A zero-ventilation state requires positive classified respiratory time; windows with no classified respiratory time are treated as missing. Partially classified windows remain eligible based on observed documentation, with the corresponding coverage limitation. Vasoactive exposure follows the canonical exposure flag. Estimates use descriptive Wilson 95% confidence intervals.

The analysis does **not** infer extubation, reintubation, treatment cessation, or drug restart from changes in six-hour exposure states.

## Outputs and reproducibility scope

Scripts produce local Parquet and CSV files, including cohort definitions, variable trajectories, availability summaries, descriptive stratifications, favorable-recovery references, and transition tables. Observability and missingness are variable-specific; missing measurements must not be interpreted as clinical zeros.

This repository provides the analytical workflow, **not** the underlying restricted data or a complete turnkey reproduction of the published manuscript. Figure-rendering scripts, editorial table assembly, some supplementary checks, and selected exploratory or auxiliary components are not part of this release. End-to-end results require authorized MIMIC-IV access, compatible input data, and execution of the relevant dependency chain.

## Citation

If you use this repository, cite the associated TRAJECTORIES Study 1 publication when available and acknowledge the MIMIC-IV database and PhysioNet in accordance with their citation guidance. See [MIMIC-IV v3.1 on PhysioNet](https://physionet.org/content/mimiciv/3.1/) for the official dataset citation and access conditions.

## License

The original source code in this repository is licensed under the [MIT License](LICENSE). Copyright (c) 2026 Michele Danilo Pierri.

This software license applies to the repository code only. MIMIC-IV data and any restricted derivatives remain subject to PhysioNet credentialing, data use agreements, and applicable access conditions; they are not licensed or distributed under MIT.
