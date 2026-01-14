<div align="center">
  <img width="512" height="512" alt="CIDL_left_bottom_extracted" src="https://github.com/user-attachments/assets/5210f427-15b5-433c-8de3-df331794ca56" />
</div>


This repository contains a modular Python backend for structured, reproducible access to the
**ACIC Data Challenge 2022** datasets, including corresponding ground-truth effects,
as well as metadata on the data-generating processes (DGPs) and the data itself.

The project is developed as part of a Master’s Thesis and currently represents an **intermediate
development stage**. The focus at this stage lies on data infrastructure, correctness, and
reproducibility rather than on a finalized public API.

---

## Motivation

The ACIC 2022 datasets are a widely used benchmark for evaluating causal inference methods.
While the raw data are publicly available, practical usage in Python is cumbersome due to

- distribution across multiple ZIP archives,
- heterogeneous file structures,
- lack of direct links between simulations, ground truth, and DGP metadata,
- and repetitive, error-prone data preparation steps.

This module addresses this gap by providing a **clean abstraction layer** between the raw data
and downstream causal analysis, enabling reproducible and standardized experimentation.

---

## Scope of This Repository

At its current stage, this repository provides:

- A **robust S3 backend** for accessing ACIC data hosted on institutional infrastructure
- **Index-based loading** of simulation datasets
- **Index-consistent loading and validation** of ground-truth estimands
- Centralized access to **DGP metadata** (confounding strength, heterogeneity, difficulty tiers)
- A unified, column-consistent **Parquet-based data format**

The repository **does not yet** provide:
- evaluation methods,
- distribution via PyPI,
- or a finalized public API.

---

## Data Overview (ACIC 2022)

The ACIC 2022 dataset consists of **3,400 independent simulations**, generated from
**17 data-generating processes (DGPs)** with **200 simulations per DGP**.

Each simulation contains approximately:

- 40,000 patients
- 500 medical practices
- Up to 4 years of follow-up per patient and practice

The data mimic large-scale evaluations of U.S. healthcare programs with

- voluntary program participation (non-randomized treatment),
- strong observed confounding,
- hierarchical and longitudinal structure,
- and substantial treatment-effect heterogeneity.

Ground-truth SATT values are available and included in the module (overall, practice-level, and for all covariate-defined subgroups).

For full background on the data-generating design, see the official  
[ACIC 2022 documentation](https://acic2022.mathematica.org/data).

---

## Data Storage

All processed data are stored on the University of Hamburg’s  
*object and long-term storage (LZS)*, an S3-compatible infrastructure.

The storage layout separates simulations, ground truth, and metadata:

- **Simulations:** merged, column-consistent Parquet files (one file per simulation)
- **Ground truth:** Parquet files indexed identically to simulations
- **Metadata:** JSON files describing simulation indices, DGP properties, and variable definitions

To access the data, you must set two keys as environment variables on your local machine.

---

## Development Status

This repository is currently under **development**. Significant changes should be expected at this stage.

The current focus is on:
- correctness of data handling,
- robustness of index-based access,
- and transparent linkage between simulations, ground truth, and metadata.

Planned next steps include:
- stabilization of the public API,
- improved documentation and examples,
- optional PyPI packaging,
- and extensions for evaluation utilities.

---

## Notes and Clarifications

### Academic Context

This project is developed as part of a **Master’s Thesis at the University of Hamburg (UHH)**.
Its primary goal is to create a reusable and reliable data-access layer for causal inference
research using the ACIC 2022 datasets.

### Intended Use

At present, this repository is intended for:
- thesis supervision and review,
- internal experimentation,
- and documentation of data preprocessing.

### Contributing

Suggestions, issues, and conceptual feedback are welcome.

---

## License

MIT License

---

## Author

**Julian Denzel**  
Master’s Thesis Project  
University of Hamburg  

julian.denzel@studium.uni-hamburg.de  

martin.spindler@uni-hamburg.de


First issued: **April 2026**
