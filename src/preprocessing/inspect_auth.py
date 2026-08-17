from pathlib import Path
import json

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
)


# ---------------------------------------------------------
# PROJECT PATHS
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "Data"
RAW_DIR = DATA_DIR / "raw"
SAMPLE_DIR = DATA_DIR / "samples"

LOG_DIR = PROJECT_ROOT / "Logs"

AUTH_FILE = RAW_DIR / "auth.txt.gz"

SAMPLE_OUTPUT = SAMPLE_DIR / "auth_sample.csv"
SUMMARY_OUTPUT = LOG_DIR / "auth_inspection_summary.json"


# ---------------------------------------------------------
# SETTINGS
# ---------------------------------------------------------

INSPECTION_ROWS = 50000
OUTPUT_SAMPLE_ROWS = 10000


# ---------------------------------------------------------
# AUTHENTICATION SCHEMA
# ---------------------------------------------------------

AUTH_SCHEMA = StructType([
    StructField("time", LongType(), True),
    StructField("src_user", StringType(), True),
    StructField("dst_user", StringType(), True),
    StructField("src_computer", StringType(), True),
    StructField("dst_computer", StringType(), True),
    StructField("auth_type", StringType(), True),
    StructField("logon_type", StringType(), True),
    StructField("auth_orientation", StringType(), True),
    StructField("success", StringType(), True),
])


def main():

    print("=" * 75)
    print("LANL AUTHENTICATION DATASET INSPECTION")
    print("=" * 75)

    print(f"\nProject root : {PROJECT_ROOT}")
    print(f"Dataset file : {AUTH_FILE}")

    # -----------------------------------------------------
    # 1. CHECK FILE
    # -----------------------------------------------------

    if not AUTH_FILE.exists():
        raise FileNotFoundError(
            f"\nauth.txt.gz was not found at:\n{AUTH_FILE}"
        )

    print("\n[OK] auth.txt.gz found.")

    # -----------------------------------------------------
    # 2. START SPARK
    # -----------------------------------------------------

    print("\nStarting SparkSession...")

    spark = (
        SparkSession.builder
        .appName("LANL_Auth_Inspection")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"[OK] Spark version: {spark.version}")

    # -----------------------------------------------------
    # 3. READ AUTHENTICATION DATA
    # -----------------------------------------------------

    print("\nReading authentication dataset using PySpark...")

    auth_df = (
        spark.read
        .option("header", "false")
        .option("sep", ",")
        .option("nullValue", "?")
        .schema(AUTH_SCHEMA)
        .csv(str(AUTH_FILE))
    )

    # IMPORTANT:
    # We intentionally inspect only a limited subset first.
    sample_df = (
        auth_df
        .limit(INSPECTION_ROWS)
        .cache()
    )

    sample_count = sample_df.count()

    print(f"[OK] Loaded {sample_count:,} rows for inspection.")

    # -----------------------------------------------------
    # 4. PRINT SCHEMA
    # -----------------------------------------------------

    print("\n" + "=" * 75)
    print("AUTHENTICATION DATASET SCHEMA")
    print("=" * 75)

    sample_df.printSchema()

    # -----------------------------------------------------
    # 5. SHOW FIRST 10 RECORDS
    # -----------------------------------------------------

    print("\n" + "=" * 75)
    print("FIRST 10 AUTHENTICATION EVENTS")
    print("=" * 75)

    sample_df.show(
        10,
        truncate=False
    )

    # -----------------------------------------------------
    # 6. MISSING VALUE ANALYSIS
    # -----------------------------------------------------

    print("\n" + "=" * 75)
    print("MISSING VALUES IN INSPECTION SAMPLE")
    print("=" * 75)

    missing_expr = [
        F.sum(
            F.col(column).isNull().cast("int")
        ).alias(column)
        for column in sample_df.columns
    ]

    missing_row = (
        sample_df
        .select(missing_expr)
        .first()
        .asDict()
    )

    for column, count in missing_row.items():
        print(f"{column:20s}: {count}")

    # -----------------------------------------------------
    # 7. SAMPLE ENTITY STATISTICS
    # -----------------------------------------------------

    print("\n" + "=" * 75)
    print("SAMPLE ENTITY STATISTICS")
    print("=" * 75)

    unique_src_users = (
        sample_df
        .select("src_user")
        .distinct()
        .count()
    )

    unique_dst_users = (
        sample_df
        .select("dst_user")
        .distinct()
        .count()
    )

    unique_src_computers = (
        sample_df
        .select("src_computer")
        .distinct()
        .count()
    )

    unique_dst_computers = (
        sample_df
        .select("dst_computer")
        .distinct()
        .count()
    )

    print(f"Unique source users       : {unique_src_users:,}")
    print(f"Unique destination users  : {unique_dst_users:,}")
    print(f"Unique source computers   : {unique_src_computers:,}")
    print(f"Unique destination computers: {unique_dst_computers:,}")

    # -----------------------------------------------------
    # 8. AUTHENTICATION RESULT DISTRIBUTION
    # -----------------------------------------------------

    print("\n" + "=" * 75)
    print("SUCCESS / FAILURE DISTRIBUTION")
    print("=" * 75)

    sample_df.groupBy("success") \
        .count() \
        .orderBy(F.desc("count")) \
        .show(truncate=False)

    # -----------------------------------------------------
    # 9. AUTHENTICATION TYPE DISTRIBUTION
    # -----------------------------------------------------

    print("\n" + "=" * 75)
    print("AUTHENTICATION TYPE DISTRIBUTION")
    print("=" * 75)

    sample_df.groupBy("auth_type") \
        .count() \
        .orderBy(F.desc("count")) \
        .show(truncate=False)

    # -----------------------------------------------------
    # 10. LOGON TYPE DISTRIBUTION
    # -----------------------------------------------------

    print("\n" + "=" * 75)
    print("LOGON TYPE DISTRIBUTION")
    print("=" * 75)

    sample_df.groupBy("logon_type") \
        .count() \
        .orderBy(F.desc("count")) \
        .show(truncate=False)

    # -----------------------------------------------------
    # 11. TIMESTAMP RANGE
    # -----------------------------------------------------

    time_row = (
        sample_df
        .agg(
            F.min("time").alias("minimum_timestamp"),
            F.max("time").alias("maximum_timestamp")
        )
        .first()
    )

    minimum_timestamp = time_row["minimum_timestamp"]
    maximum_timestamp = time_row["maximum_timestamp"]

    print("\n" + "=" * 75)
    print("TIMESTAMP RANGE OF SAMPLE")
    print("=" * 75)

    print(f"Minimum timestamp: {minimum_timestamp}")
    print(f"Maximum timestamp: {maximum_timestamp}")

    # -----------------------------------------------------
    # 12. SAVE SMALL REUSABLE SAMPLE
    # -----------------------------------------------------

    print("\nCreating reusable authentication sample...")

    SAMPLE_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    pandas_sample = (
        sample_df
        .limit(OUTPUT_SAMPLE_ROWS)
        .toPandas()
    )

    pandas_sample.to_csv(
        SAMPLE_OUTPUT,
        index=False
    )

    print(
        f"[OK] Saved {len(pandas_sample):,} rows to "
        f"{SAMPLE_OUTPUT}"
    )

    # -----------------------------------------------------
    # 13. SAVE SUMMARY JSON
    # -----------------------------------------------------

    LOG_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    summary = {
        "dataset": "LANL auth.txt.gz",
        "inspection_rows": int(sample_count),
        "schema": [
            "time",
            "src_user",
            "dst_user",
            "src_computer",
            "dst_computer",
            "auth_type",
            "logon_type",
            "auth_orientation",
            "success"
        ],
        "unique_source_users_in_sample":
            int(unique_src_users),
        "unique_destination_users_in_sample":
            int(unique_dst_users),
        "unique_source_computers_in_sample":
            int(unique_src_computers),
        "unique_destination_computers_in_sample":
            int(unique_dst_computers),
        "minimum_timestamp":
            int(minimum_timestamp)
            if minimum_timestamp is not None
            else None,
        "maximum_timestamp":
            int(maximum_timestamp)
            if maximum_timestamp is not None
            else None,
        "missing_values_in_sample": {
            key: int(value)
            for key, value in missing_row.items()
        }
    }

    with open(
        SUMMARY_OUTPUT,
        "w",
        encoding="utf-8"
    ) as output_file:

        json.dump(
            summary,
            output_file,
            indent=4
        )

    print(f"[OK] Summary written to {SUMMARY_OUTPUT}")

    # -----------------------------------------------------
    # 14. CLEANUP
    # -----------------------------------------------------

    sample_df.unpersist()

    spark.stop()

    print("\n" + "=" * 75)
    print("AUTHENTICATION DATASET INSPECTION COMPLETED")
    print("=" * 75)


if __name__ == "__main__":
    main()