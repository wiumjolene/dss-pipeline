Jolene PhD
==============================

Pipeline module to synch data to data warehouse

Project Organization
------------


├── README.md          <- The top-level README for developers using this project.
├── data
│   ├── external       <- Data from third party sources.
│   ├── interim        <- Intermediate data that has been transformed.
│   ├── processed      <- The final, canonical data sets for modeling.
│   └── raw            <- The original, immutable data dump.
│
├── docs               <- A default Sphinx project; see sphinx-doc.org for details
│
├── references         <- Data dictionaries, manuals, and all other explanatory materials.
│
└── src                <- Source code for use in this project.
    ├── __init__.py    <- Python module
    │
    ├── get_data       <- Scripts to generate and manipulate data
    │
    ├── make_features  <- Scripts to turn raw data into features for modeling
    │
    └── utils         <- Utility functions
        ├── confiq.py
        ├── connect.py
        ├── controller.py
        └── visualize.py
