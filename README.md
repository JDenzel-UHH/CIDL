<div align="center">
  <img width="512" height="512" alt="CIDL_left_bottom_extracted" src="https://github.com/user-attachments/assets/5210f427-15b5-433c-8de3-df331794ca56" />
</div>
# ACIC 2022 Data Access Package

This module provides an easy and efficient way to access and explore the **ACIC Data Challenge 2022** datasets. The ACIC 2022 competition serves as a benchmark for evaluating novel methods in causal inference.  

The module was developed as part of a Master’s Thesis project at the University of Hamburg (UHH) and aims to support researchers working on causal inference and predictive modeling.

> *Future versions may include additional datasets and functionality for related simulation studies.*


---


## Purpose

This module is designed for researchers developing new causal estimation methods and seeking ready-to-use benchmark data for validation.


---


## About the data

The ACIC 2022 datasets are **synthetic simulation datasets** designed to resemble real-world evaluations of large-scale U.S. health care interventions. Although the data are simulated, the marginal and joint distributions of many covariates are constructed to mirror those observed in a real Medicare dataset, using empirical summaries such as CDFs, relationships between covariates, and intra-class correlations.

### Intervention and causal question

The intervention is defined at the **practice level**:

- `Z` indicates whether a practice participates in the intervention (not randomly assigned).
- Treated practices receive additional financial support (a “bonus”).
- The intended mechanism is that additional resources (e.g., staffing, equipment, processes) improve care delivery and, over time, reduce **avoidable medical expenditures** (e.g., preventable hospital stays).

The outcome of interest is:

- `Y`: **monthly Medicare expenditures** at the patient-year level.

The modeling task is explicitly causal: you should estimate/predict how expenditures would have differed **because of** the intervention (`Z`), not merely how well `Y` can be predicted.

### Why this is challenging (and useful for benchmarking)

These simulations intentionally include several features that make causal effect estimation difficult and realistic:

- **Non-randomized participation:** since practices are not randomly assigned to treatment, treated and untreated groups can differ systematically (confounding / selection bias).
- **Heterogeneous effects:** the intervention may reduce spending for some patient groups or practices while increasing it for others.
- **Hierarchical and longitudinal structure:** patients are observed repeatedly over time and are clustered within practices.
- **Skewed, high-variance outcome:** Medicare spending is highly variable and right-skewed (many moderate-cost patients, few extremely high-cost patients).

In summary, each simulation represents a realistic observational-style setting in which the goal is to recover **the causal impact of a practice-level intervention on patient-level expenditures** under confounding, heterogeneity, clustering, and time dynamics.

### Size and structure

The full collection contains **3,400 independent simulations** generated from **17 data-generating processes (DGPs)** (200 simulations per DGP).

Each simulation contains:

- ~40,000 patients  
- 500 practices  
- up to 4 years (`year = 1..4`)  

### Key variables (high level)

- Identifiers: `id.patient`, `id.practice`, `year`
- Treatment indicators: `Z` (practice-level treatment group), `post` (post-intervention period indicator)
- Outcome: `Y` (monthly Medicare expenditures)
- Practice-level covariates: `X1`–`X9` (with `X1`–`X5` defining subgroup SATTs)
- Patient-level covariates: `V1`–`V5`

For exact typing and definitions, refer to [`Acic22 Data Dictionary`](https://github.com/JDenzel-UHH/CIDL/blob/main/src/cidl/metadata/acic22_data_dict.json).

### Data-generating process (DGP) metadata

DGP properties (confounding strength/source, impact heterogeneity, idiosyncrasy) and a heuristic difficulty tier
are provided in [`Acic22 DGP Info`](https://github.com/JDenzel-UHH/CIDL/blob/main/src/cidl/metadata/acic22_dgp_info.json), along with the mapping between DGPs and simulation indices.


---


## How to use it

This package provides **index-based access** to ready-to-use ACIC 2022 datasets and their **matched ground-truth SATT values**. A recommended workflow is:

1. Load simulations (by index).
2. Load the corresponding ground truth (matched by index).
3. Fit your causal method and produce SATT predictions (overall / subgroup / year 3 and 4 / practice).
4. Compare predictions to ground truth.

#### 0) Configure S3 access (required)

The data are hosted on UHH’s S3-compatible storage (LZS). To enable access, set the following environment variables on your local machine:

- `UHH_S3_ACCESS`: `...`
- `UHH_S3_SECRET`: `...`

#### 1) Load simulations by index

Simulations are addressed by a single integer index (`1..3400`). Use this index as the canonical identifier throughout your workflow.

Recommended practice:
- If you load more than one simulation, store them in a structure that preserves the index (e.g., a dictionary keyed by index).
- Do not rely on file names to identify corresponding ground truth.

#### 2) Load matched ground truth

Ground-truth files use the **same index** as the simulations. Load ground truth via the package’s matching logic to ensure that indices align and mismatches are surfaced early (missing / extra indices).

Ground truth includes **SATT values** in the following forms:
- overall SATT,
- subgroup SATTs defined by covariates `X1`–`X5`,
- year-specific SATT for post-intervention years (3 and 4),
- practice-level SATT.

#### 3) Estimate and format results

Run your estimation.

You may deviate from the workflow above if you prefer. However, to enable the (planned) built-in evaluation tools, format your results as:

- one `.csv` file,
- with the columns: `dataset.num`, `variable`, `level`, `year`, `id.practice`, `satt`, `lower90`, `upper90`.

For an example results table, see:  
[Example Result Table](https://github.com/JDenzel-UHH/CIDL/blob/main/documentation/example_results_table.md)

#### 4) Evaluate

If you follow the standardized results schema, you can use the (planned) built-in evaluation functionality (TBD).



---


## Notes and Clarifications

### Academic Context

This package was developed as part of a **Master’s Thesis at the University of Hamburg (UHH)**.  
Its goal is to simplify access to standardized simulation datasets for causal inference research,  
facilitating reproducible testing and comparison of new methods.

### Contributing

Contributions and suggestions are welcome!  

### License

This project is licensed under the **MIT License**.

### Author

**Julian Denzel**  
Master’s Thesis Project, University of Hamburg  

julian.denzel@studium.uni-hamburg.de  
martin.spindler@uni-hamburg.de  

First issued: **April 2026**
