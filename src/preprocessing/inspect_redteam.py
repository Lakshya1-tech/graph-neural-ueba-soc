import csv
import gzip
import json
from pathlib import Path

import pandas as pd


# ---------------------------------------------------------
# PROJECT PATHS
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Support your current folder capitalization
DATA_DIR = PROJECT_ROOT / "Data"

RAW_DIR = DATA_DIR / "raw"
SAMPLE_DIR = DATA_DIR / "samples"

LOG_DIR = PROJECT_ROOT / "Logs"

REDTEAM_FILE = RAW_DIR / "redteam.txt.gz"

SAMPLE_OUTPUT = SAMPLE_DIR / "redteam_sample.csv"
SUMMARY_OUTPUT = LOG_DIR / "redteam_inspection_summary.json"


# ---------------------------------------------------------
# EXPECTED LANL REDTEAM SCHEMA
# ---------------------------------------------------------

COLUMNS = [
    "time",
    "user",
    "src_computer",
    "dst_computer"
]


def main():

    print("=" * 70)
    print("LANL RED-TEAM DATASET INSPECTION")
    print("=" * 70)

    print(f"\nProject root : {PROJECT_ROOT}")
    print(f"Dataset file : {REDTEAM_FILE}")

    # -----------------------------------------------------
    # 1. CHECK FILE EXISTS
    # -----------------------------------------------------

    if not REDTEAM_FILE.exists():
        raise FileNotFoundError(
            f"\nredteam.txt.gz was not found at:\n{REDTEAM_FILE}"
        )

    print("\n[OK] redteam.txt.gz found.")

    # -----------------------------------------------------
    # 2. READ GZIP FILE
    # -----------------------------------------------------

    valid_rows = []
    malformed_rows = []

    with gzip.open(
        REDTEAM_FILE,
        mode="rt",
        encoding="utf-8",
        newline=""
    ) as file:

        reader = csv.reader(file)

        for line_number, row in enumerate(reader, start=1):

            if len(row) != 4:
                malformed_rows.append(
                    {
                        "line_number": line_number,
                        "content": row
                    }
                )
                continue

            valid_rows.append(row)

    # -----------------------------------------------------
    # 3. CREATE DATAFRAME
    # -----------------------------------------------------

    df = pd.DataFrame(valid_rows, columns=COLUMNS)

    df["time"] = pd.to_numeric(
        df["time"],
        errors="coerce"
    )

    # -----------------------------------------------------
    # 4. BASIC DATASET INFORMATION
    # -----------------------------------------------------

    print("\n" + "=" * 70)
    print("DATASET SUMMARY")
    print("=" * 70)

    print(f"Total valid events      : {len(df):,}")
    print(f"Malformed rows          : {len(malformed_rows):,}")
    print(f"Unique users            : {df['user'].nunique():,}")
    print(f"Unique source computers : {df['src_computer'].nunique():,}")
    print(f"Unique dest computers   : {df['dst_computer'].nunique():,}")

    print(f"\nMinimum timestamp: {df['time'].min()}")
    print(f"Maximum timestamp: {df['time'].max()}")

    # -----------------------------------------------------
    # 5. CHECK MISSING VALUES
    # -----------------------------------------------------

    print("\n" + "=" * 70)
    print("MISSING VALUES")
    print("=" * 70)

    print(df.isnull().sum())

    # -----------------------------------------------------
    # 6. SHOW FIRST 10 EVENTS
    # -----------------------------------------------------

    print("\n" + "=" * 70)
    print("FIRST 10 RED-TEAM EVENTS")
    print("=" * 70)

    print(df.head(10).to_string(index=False))

    # -----------------------------------------------------
    # 7. SAVE SAMPLE
    # -----------------------------------------------------

    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    df.head(20).to_csv(
        SAMPLE_OUTPUT,
        index=False
    )

    # -----------------------------------------------------
    # 8. SAVE SUMMARY
    # -----------------------------------------------------

    LOG_DIR.mkdir(parents=True, exist_ok=True)

    summary = {
        "dataset": "LANL redteam.txt.gz",
        "schema": COLUMNS,
        "total_valid_events": int(len(df)),
        "malformed_rows": int(len(malformed_rows)),
        "unique_users": int(df["user"].nunique()),
        "unique_source_computers": int(df["src_computer"].nunique()),
        "unique_destination_computers": int(df["dst_computer"].nunique()),
        "minimum_timestamp": (
            int(df["time"].min())
            if not df.empty else None
        ),
        "maximum_timestamp": (
            int(df["time"].max())
            if not df.empty else None
        )
    }

    with open(
        SUMMARY_OUTPUT,
        mode="w",
        encoding="utf-8"
    ) as output_file:

        json.dump(
            summary,
            output_file,
            indent=4
        )

    # -----------------------------------------------------
    # 9. COMPLETION MESSAGE
    # -----------------------------------------------------

    print("\n" + "=" * 70)
    print("OUTPUT FILES CREATED")
    print("=" * 70)

    print(f"Sample  : {SAMPLE_OUTPUT}")
    print(f"Summary : {SUMMARY_OUTPUT}")

    print("\nRed-team dataset inspection completed successfully.")


if __name__ == "__main__":
    main()