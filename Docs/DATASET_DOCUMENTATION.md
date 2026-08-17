# LANL Dataset Documentation

## Project

Autonomous Graph-Neural UEBA for AI-Driven SOC

## Dataset Source

Los Alamos National Laboratory (LANL)
Comprehensive Multi-Source Cyber-Security Events Dataset

The project uses three data files from the LANL dataset:

1. auth.txt.gz
2. redteam.txt.gz
3. proc.txt.gz

---

# 1. Authentication Dataset

File:

auth.txt.gz

Role:

Primary telemetry source for UEBA graph construction and compromised-account / lateral-movement detection.

Schema:

| Field | Description |
|---|---|
| time | Event timestamp in seconds |
| src_user | Source user/account |
| dst_user | Destination user/account |
| src_computer | Source computer |
| dst_computer | Destination computer |
| auth_type | Authentication protocol |
| logon_type | Type of logon |
| auth_orientation | LogOn or LogOff |
| success | Success or failure status |

Example relationship:

User -> authenticates from/to -> Computer

The authentication dataset is processed using PySpark because of its large size.

---

# 2. Red-Team Dataset

File:

redteam.txt.gz

Role:

Ground-truth malicious authentication events used to validate detection performance.

Schema:

| Field | Description |
|---|---|
| time | Timestamp of malicious authentication |
| user | Compromised / red-team user |
| src_computer | Source computer |
| dst_computer | Destination computer |

The red-team events can be linked directly to authentication events using:

time + user + source computer + destination computer

Cross-dataset validation performed during Phase 2 showed:

- Red-team events tested: 10
- Exact authentication matches: 10
- Match rate: 100%

This validates the use of the red-team file as ground truth for later anomaly-detection evaluation.

---

# 3. Process Dataset

File:

proc.txt.gz

Role:

Secondary telemetry source used for contextual enrichment of suspicious authentication events.

Schema:

| Field | Description |
|---|---|
| time | Process event timestamp |
| user | User associated with process |
| computer | Computer on which process activity occurred |
| process | De-identified process identifier |
| event_type | Process Start or End |

Process identifiers are anonymized by LANL.

Process information is therefore used primarily as behavioral context rather than for direct executable-name-based detection.

---

# Cross-Dataset Relationship

The three datasets are integrated as follows:

Red-Team Event
        |
        v
Authentication Event
        |
        v
User-Host Behavioral Relationship
        |
        v
Graph-Neural UEBA Detection
        |
        v
Process Context Enrichment
        |
        v
SOC Triage and Alert Explanation

---

# Cross-Dataset Validation Results

A validation experiment was conducted using the first ten LANL red-team events.

Results:

| Metric | Result |
|---|---:|
| Red-team events tested | 10 |
| Exact authentication matches | 10 |
| Authentication match rate | 100% |
| Process context events found | 148 |
| Same-user process events | 4 |
| Process context window | ±300 seconds |

Important:

The 100% authentication match rate represents dataset consistency validation and must not be interpreted as machine-learning detection accuracy.

---

# Dataset Usage in Final Architecture

## Primary Detection

auth.txt.gz

Used for:

- PySpark preprocessing
- behavioral feature engineering
- user-host graph construction
- Graph Autoencoder / GNN anomaly detection

## Ground Truth

redteam.txt.gz

Used for:

- detection validation
- precision / recall evaluation
- red-team event comparison

## Contextual Enrichment

proc.txt.gz

Used for:

- process activity around suspicious events
- additional SOC analyst context
- alert enrichment
- LLM explanation input

---

# Data Handling

Raw LANL datasets are stored locally under:

Data/raw/

Raw datasets are excluded from Git using .gitignore because of their size.

Only small reusable samples, scripts, summaries, and generated results are stored in the Git repository.