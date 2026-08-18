# Authentication Preprocessing Documentation

## Phase

Phase 3 - LANL Authentication Preprocessing Pipeline

## Input Dataset

Primary authentication dataset:

Data/raw/auth.txt.gz

Development window:

7 LANL days

Timestamp range:

0 - 604799 seconds

Total staged authentication events:

113,699,303


## Phase 3A - Daily Authentication Staging

The original LANL authentication dataset is stored as a large compressed
gzip stream.

To improve downstream processing, the first seven days of authentication
telemetry were divided into daily compressed staging files.

Generated staging files:

Data/processed/staging/auth_daily/auth_day_00.csv.gz
Data/processed/staging/auth_daily/auth_day_01.csv.gz
Data/processed/staging/auth_daily/auth_day_02.csv.gz
Data/processed/staging/auth_daily/auth_day_03.csv.gz
Data/processed/staging/auth_daily/auth_day_04.csv.gz
Data/processed/staging/auth_daily/auth_day_05.csv.gz
Data/processed/staging/auth_daily/auth_day_06.csv.gz


## Phase 3B - Cleaning and Normalization

PySpark was used to normalize the LANL authentication telemetry.

Operations included:

- user identifier normalization
- computer identifier normalization
- categorical value normalization
- handling unknown values
- removal of records missing critical graph identifiers


## Phase 3C - Structural Field Generation

The following fields were generated.

Temporal:

- day_index
- second_of_day
- hour

Identity:

- src_user_account
- src_user_domain
- dst_user_account
- dst_user_domain

Authentication:

- success_flag
- is_failure
- is_logon
- is_logoff

Account:

- src_is_machine_account
- dst_is_machine_account

Relationship:

- same_user
- same_computer
- cross_host


## Phase 3D - Parquet Materialization

The cleaned authentication telemetry was written using:

Format: Parquet

Compression: Snappy

The processing environment used:

- WSL2
- Ubuntu 24.04 LTS
- Java 17
- Python
- PySpark 3.5.9

Large Parquet outputs are stored in WSL native Linux storage:

/home/lakshya/ueba_data/auth_clean

This avoids Windows Hadoop filesystem compatibility issues and provides
more appropriate storage for Spark workloads.


## Phase 3E - Validation

The processed dataset was validated before UEBA feature engineering.

Validation checks:

- physical Parquet output
- schema correctness
- seven-day coverage
- row integrity
- critical null validation
- binary field validation
- temporal range validation
- logical consistency
- LANL red-team event preservation

Results:

physical_output: PASS
schema: PASS
day_coverage: PASS
row_integrity: PASS
critical_nulls: PASS
binary_fields: PASS
temporal_ranges: PASS
logical_consistency: PASS
redteam_preservation: PASS

Overall validation:

PASS


## Validation Outputs

Logs/processed_auth_validation_summary.json

Data/samples/redteam_processed_auth_matches.csv


## Conclusion

The processed authentication dataset successfully passed all validation
checks and is suitable for downstream UEBA behavioral feature engineering,
user-host graph construction and Graph Neural Network modelling.