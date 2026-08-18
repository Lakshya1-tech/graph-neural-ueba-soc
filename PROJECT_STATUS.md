# Graph-Neural UEBA Capstone - Project Status

## Project Title

Autonomous Graph-Neural UEBA for AI-Driven SOC

## Core Pipeline

LANL Logs
-> PySpark Data Pipeline
-> UEBA Graph Engine
-> Graph Autoencoder / GNN
-> Anomaly Detection
-> SOC Triage Engine
-> Local LLM Explanation
-> Streamlit Dashboard


# Phase 0 - Project Initialization

Status: COMPLETED

- Project folder structure created
- auth.txt.gz downloaded
- proc.txt.gz downloaded
- redteam.txt.gz downloaded
- Raw datasets stored under Data/raw/
- .gitignore configured


# Phase 1 - Development Environment

Status: COMPLETED

- Python 3.12.10 configured
- Java 17 configured
- JAVA_HOME configured
- Git installed
- Virtual environment (.venv) created
- PySpark 4.2.0 installed
- pandas configured
- NumPy installed
- scikit-learn installed
- matplotlib installed
- Jupyter installed
- SparkSession successfully tested
- Spark DataFrame successfully tested


# Phase 2 - Dataset Understanding

Status: COMPLETED


## Phase 2A - Red-Team Dataset

Status: COMPLETED

Completed:

- redteam.txt.gz successfully read
- Dataset schema verified
- Missing values checked
- Red-team events inspected
- Small reusable sample generated

Outputs:

Data/samples/redteam_sample.csv
Logs/redteam_inspection_summary.json


## Phase 2B - Authentication Dataset

Status: COMPLETED

Completed:

- auth.txt.gz successfully read using PySpark
- LANL authentication schema verified
- Missing values handled
- Authentication events inspected
- User and computer entities examined
- Authentication types examined
- Logon types examined
- Success/failure values examined
- Small reusable authentication sample generated

Outputs:

Data/samples/auth_sample.csv
Logs/auth_inspection_summary.json


## Phase 2C - Process Dataset

Status: COMPLETED

Completed:

- proc.txt.gz successfully read using PySpark
- Process dataset schema verified
- Process Start/End events inspected
- Users, computers and process identifiers examined
- Small reusable process sample generated

Outputs:

Data/samples/proc_sample.csv
Logs/proc_inspection_summary.json


## Phase 2D - Cross-Dataset Validation

Status: COMPLETED

Validation results:

- Red-team events tested: 10
- Exact authentication matches: 10
- Authentication match rate: 100%
- Process context events found: 148
- Same-user process events: 4
- Process context window: +/- 300 seconds

Important:

The 100% result represents dataset consistency validation.
It is NOT machine-learning detection accuracy.

Outputs:

Data/samples/redteam_auth_matches.csv
Data/samples/redteam_proc_context.csv
Logs/cross_dataset_validation_summary.json


## Phase 2E - Dataset Documentation

Status: COMPLETED

Completed:

- Dataset roles documented
- Dataset schemas documented
- Cross-dataset relationships documented
- Cross-dataset validation documented

Output:

Docs/DATASET_DOCUMENTATION.md


# Phase 3 - Authentication Preprocessing Pipeline

Status: IN PROGRESS


## Phase 3A - Daily Authentication Staging

Status: COMPLETED

Development window:

7 LANL days

Timestamp range:

0 - 604799 seconds

Authentication events staged:

Day 00: 15,740,768
Day 01: 17,562,411
Day 02: 15,585,892
Day 03: 13,473,124
Day 04: 13,398,148
Day 05: 18,557,382
Day 06: 19,381,578

Total authentication events:

113,699,303

Generated staging files:

Data/processed/staging/auth_daily/auth_day_00.csv.gz
Data/processed/staging/auth_daily/auth_day_01.csv.gz
Data/processed/staging/auth_daily/auth_day_02.csv.gz
Data/processed/staging/auth_daily/auth_day_03.csv.gz
Data/processed/staging/auth_daily/auth_day_04.csv.gz
Data/processed/staging/auth_daily/auth_day_05.csv.gz
Data/processed/staging/auth_daily/auth_day_06.csv.gz

Approximate total compressed staging size:

769 MB

Script:

src/preprocessing/stage_auth_daily.py

Summary:

Logs/auth_daily_staging_summary.json


## Phase 3B - PySpark Cleaning and Normalization

Status: COMPLETED

Planned:

- Read daily staged authentication files
- Apply LANL authentication schema
- Normalize user identifiers
- Normalize computer identifiers
- Normalize categorical fields
- Handle missing/unknown values
- Remove records missing critical graph identifiers

Script:

src/preprocessing/preprocess_auth.py


## Phase 3C - Structural Field Generation

Status: COMPLETED

Fields to generate:

Temporal features:
- day_index
- second_of_day
- hour

User features:
- src_user_account
- src_user_domain
- dst_user_account
- dst_user_domain

Authentication features:
- success_flag
- is_failure
- is_logon
- is_logoff

Account features:
- src_is_machine_account
- dst_is_machine_account

Relationship features:
- same_user
- same_computer
- cross_host


## Phase 3D - Parquet Materialization

Status: COMPLETED

Target output:

Data/processed/auth_clean/

Expected structure:

day_00/
day_01/
day_02/
day_03/
day_04/
day_05/
day_06/

Format:

Parquet with Snappy compression


## Phase 3E - Processed Dataset Validation

Status: COMPLETED

Planned:

- Verify processed schema
- Validate row integrity
- Check generated fields
- Check timestamp ranges
- Check null values
- Check daily outputs
- Verify known red-team events remain present


## Phase 3F - Documentation and Git Checkpoint

Status: NOT STARTED

Planned:

- Document preprocessing
- Save statistics
- Update project status
- Commit Phase 3 changes
- Push to GitHub


# Phase 4 - UEBA Behavioral Feature Engineering

Status: NOT STARTED

Planned features:

- Authentication frequency
- User-host access frequency
- Unique destination hosts
- New destination host
- Failed authentication frequency
- Successful authentication frequency
- Cross-host activity
- Login-hour behavior
- Historical user-host frequency
- Behavioral baseline features


# Phase 5 - Red-Team Mapping and Label Preparation

Status: NOT STARTED

Planned:

- Map red-team events to processed authentication data
- Prepare evaluation labels
- Preserve ground truth separately from training data


# Phase 6 - UEBA User-Host Graph Construction

Status: NOT STARTED

Planned:

- User nodes
- Host nodes
- Authentication edges
- Edge attributes
- Behavioral edge features


# Phase 7 - Graph Dataset Preparation

Status: NOT STARTED

Planned:

- Node indexing
- Edge indexing
- Feature matrices
- Train/test graph preparation
- PyTorch Geometric representation


# Phase 8 - Graph Autoencoder / GNN

Status: NOT STARTED

Planned:

- GNN encoder
- Graph embeddings
- Graph Autoencoder decoder
- Normal relationship learning
- Reconstruction modelling


# Phase 9 - Anomaly Detection

Status: NOT STARTED

Planned:

- Reconstruction/anomaly scores
- Suspicious user-host relationships
- Detection threshold
- Ranked anomalies


# Phase 10 - Initial Model Evaluation

Status: NOT STARTED

Planned:

- Compare anomalies with LANL red-team events
- Precision
- Recall
- F1-score
- Detection rate
- False-positive analysis

Review 2 target:

Completion through Phase 10 represents approximately
50-60% implementation.


# Phase 11 - Process Context Enrichment

Status: NOT STARTED

Planned:

- Retrieve relevant process activity
- Associate process behavior with suspicious users/hosts
- Add SOC contextual evidence


# Phase 12 - SOC Triage Engine

Status: NOT STARTED

Planned:

- Alert severity
- Risk prioritization
- Context aggregation
- Structured SOC alert generation


# Phase 13 - Local LLM Explanation

Status: NOT STARTED

Planned:

- Explain suspicious behavior
- Summarize evidence
- Recommend investigation actions
- Generate analyst-readable alert notes

The LLM will NOT perform core anomaly detection.


# Phase 14 - Streamlit SOC Dashboard

Status: NOT STARTED

Planned:

- Alert dashboard
- Anomaly scores
- Risk levels
- Suspicious users
- Affected hosts
- Authentication context
- Process context
- LLM explanation
- Investigation recommendations


# Phase 15 - Full Integration and Evaluation

Status: NOT STARTED


# Git / GitHub

Local Git:

Status: CONFIGURED

Branch:

main

GitHub:

Status: CONFIGURED

Repository:

Lakshya1-tech/graph-neural-ueba-soc

Visibility:

Private

Raw and processed large datasets are excluded from GitHub.


# Current Implementation Progress

Approximate completion:

25%

Currently working on:

Phase 3B-3D

PySpark Authentication Cleaning
-> Structural Feature Generation
-> Parquet Materialization

Next major milestone:

Behavioral Features
-> User-Host Graph
-> Graph Autoencoder / GNN
-> Anomaly Detection
-> Red-Team Evaluation