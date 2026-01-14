<img width="1024" height="1024" alt="CIDL_showstopper_futuristic_slate" src="https://github.com/user-attachments/assets/ed0cbf89-1976-42d4-a055-21eade1d3c01" />

# Causal Inference Dtaa Library

This module provides an easy and efficient way to access and explore the **ACIC Data Challenge 2022** datasets. The ACIC 2022 competition serves as a benchmark for evaluating novel methods in causal inference.  

The module was developed as part of a Master’s Thesis project at the University of Hamburg (UHH) and aims to support researchers working on causal inference and predictive modeling.

> *Future versions may include additional datasets and functionality for related simulation studies.*

---

## Purpose and Audience

This module is designed for Researchers developing new causal estimation methods and seeking ready-to-use benchmark data for validation and experimentation  

---

## About the Data

The ACIC 2022 dataset consists of 3,400 independent simulations of empirical healthcare data.  
Each simulation includes:

- ~40,000 individuals  
- 500 practices  
- Up to 4 years of follow-up per individual and practice  

Each dataset is based on a specific data generating process (DGP). There are 17 different DGPs in total, which vary in difficulty due to differences within followoing areas: 

![alt text](DGPs.png)

Each DGP is respinsible for 200 diffenrent datasets.

The ground truth data for each simulation is also available within this module and is as easyly accessible as tge simulations.

For detailed information on the data, please refer to the official [ACIC 2022 documentation](https://acic2022.mathematica.org/data).

### Data Formats

The data is presented in 

2. **Merged data** (in `.parquet` format), which combines all sources into a single dataset ready for analysis. Each simulation can be loaded seperately.

### Data Hosting

The data is hosted on the so called *Objekt- und Langzeitspeicher (LZS)* of the University of Hamburg —  
a high-performance S3-compatible data storage infrastructure.  
The first three example simulations are shipped directly with the package for easy offline exploration.

### Methodological Notes

Splitting, sampling, and cross-fitting are method-specific steps that depend on the causal inference approach used.
Therefore, no predefined training/test partitions are included.
Users should implement their own data-splitting logic consistent with their chosen method.






See the example notebook examples/load_and_split.ipynb for guidance !!!!!!!!!!! ANPASSEN und ERSTELLEN




---

## How to Access the Data

 --> infos abaout how to use functionalites of the module (TBD)



---

## Notes and Clarifications

### Academic Context

This package was developed as part of a **Master’s Thesis at the University of Hamburg (UHH)**.  
Its goal is to simplify access to standardized simulation datasets for causal inference research,  
facilitating reproducible testing and comparison of new methods.

### Contributing

Contributions and suggestions are welcome!  
Please open an issue or submit a pull request on [GitHub](https://github.com/yourusername/package).

### License

This project is licensed under the **MIT License**.

### Author

**Julian Denzel**  
Master’s Thesis Project, University of Hamburg  

julian.denzel@studium.uni-hamburg.de  
martin.spindler@uni-hamburg.de  

First issued: **April 2026**

