from pathlib import Path
import json

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

PROC_FILE = RAW_DIR / "proc.txt.gz"

SAMPLE_OUTPUT = SAMPLE_DIR / "proc_sample.csv"
SUMMARY_OUTPUT = LOG_DIR / "proc_inspection_summary.json"


# ---------------------------------------------------------
# SETTINGS
# ---------------------------------------------------------

INSPECTION_ROWS = 50000
OUTPUT_SAMPLE_ROWS = 10000


# ---------------------------------------------------------
# LANL PROCESS DATASET SCHEMA
# ---------------------------------------------------------

PROC_SCHEMA = StructType([
    StructField("time", LongType(), True),
    StructField("user", StringType(), True),
    StructField("computer", StringType(), True),
    StructField("process", StringType(), True),
    StructField("event_type", StringType(), True),
])


def main():

    print("=" * 75)
    print("LANL PROCESS DATASET INSPECTION")
    print("=" * 75)

    print(f"\nProject root : {PROJECT_ROOT}")
    print(f"Dataset file : {PROC_FILE}")

    # -----------------------------------------------------
    # 1. CHECK FILE EXISTS
    # -----------------------------------------------------

    if not PROC_FILE.exists():
        raise FileNotFoundError(
            f"\nproc.txt.gz was not found at:\n{PROC_FILE}"
        )

    print("\n[OK] proc.txt.gz found.")

    # -----------------------------------------------------
    # 2. START SPARK
    # -----------------------------------------------------

    print("\nStarting SparkSession...")

    spark = (
        SparkSession.builder
        .appName("LANL_Proc_Inspection")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print(f"[OK] Spark version: {spark.version}")

    # -----------------------------------------------------
    # 3. READ PROCESS DATA
    # -----------------------------------------------------

    print("\nReading process dataset using PySpark...")

    proc_df = (
        spark.read
        .option("header", "false")
        .option("sep", ",")
        .option("nullValue", "?")
        .schema(PROC_SCHEMA)
        .csv(str(PROC_FILE))
    )

    # Inspect only a controlled sample.
    sample_df = (
        proc_df
        .limit(INSPECTION_ROWS)
        .cache()
    )

    sample_count = sample_df.count()

    print(f"[OK] Loaded {sample_count:,} rows for inspection.")

    # -----------------------------------------------------
    # 4. PRINT SCHEMA
    # -----------------------------------------------------

    print("\n" + "=" * 75)
    print("PROCESS DATASET SCHEMA")
    print("=" * 75)

    sample_df.printSchema()

    # -----------------------------------------------------
    # 5. FIRST 10 RECORDS
    # -----------------------------------------------------

    print("\n" + "=" * 75)
    print("FIRST 10 PROCESS EVENTS")
    print("=" * 75)

    sample_df.show(
        10,
        truncate=False
    )

    # -----------------------------------------------------
    # 6. MISSING VALUES
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
    # 7. ENTITY STATISTICS
    # -----------------------------------------------------

    print("\n" + "=" * 75)
    print("SAMPLE ENTITY STATISTICS")
    print("=" * 75)

    unique_users = (
        sample_df
        .select("user")
        .distinct()
        .count()
    )

    unique_computers = (
        sample_df
        .select("computer")
        .distinct()
        .count()
    )

    unique_processes = (
        sample_df
        .select("process")
        .distinct()
        .count()
    )

    print(f"Unique users       : {unique_users:,}")
    print(f"Unique computers   : {unique_computers:,}")
    print(f"Unique processes   : {unique_processes:,}")

    # -----------------------------------------------------
    # 8. START / END DISTRIBUTION
    # -----------------------------------------------------

    print("\n" + "=" * 75)
    print("PROCESS EVENT TYPE DISTRIBUTION")
    print("=" * 75)

    sample_df \
        .groupBy("event_type") \
        .count() \
        .orderBy(F.desc("count")) \
        .show(truncate=False)

    # -----------------------------------------------------
    # 9. MOST FREQUENT PROCESS IDENTIFIERS
    # -----------------------------------------------------

    print("\n" + "=" * 75)
    print("TOP 15 PROCESS IDENTIFIERS")
    print("=" * 75)

    sample_df \
        .groupBy("process") \
        .count() \
        .orderBy(F.desc("count")) \
        .show(15, truncate=False)

    # -----------------------------------------------------
    # 10. MOST ACTIVE USERS
    # -----------------------------------------------------

    print("\n" + "=" * 75)
    print("TOP 10 USERS BY PROCESS ACTIVITY")
    print("=" * 75)

    sample_df \
        .groupBy("user") \
        .count() \
        .orderBy(F.desc("count")) \
        .show(10, truncate=False)

    # -----------------------------------------------------
    # 11. MOST ACTIVE COMPUTERS
    # -----------------------------------------------------

    print("\n" + "=" * 75)
    print("TOP 10 COMPUTERS BY PROCESS ACTIVITY")
    print("=" * 75)

    sample_df \
        .groupBy("computer") \
        .count() \
        .orderBy(F.desc("count")) \
        .show(10, truncate=False)

    # -----------------------------------------------------
    # 12. TIMESTAMP RANGE
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
    # 13. SAVE REUSABLE SAMPLE
    # -----------------------------------------------------

    print("\nCreating reusable process sample...")

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
    # 14. SAVE INSPECTION SUMMARY
    # -----------------------------------------------------

    LOG_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    summary = {
        "dataset": "LANL proc.txt.gz",
        "inspection_rows": int(sample_count),

        "schema": [
            "time",
            "user",
            "computer",
            "process",
            "event_type"
        ],

        "unique_users_in_sample":
            int(unique_users),

        "unique_computers_in_sample":
            int(unique_computers),

        "unique_processes_in_sample":
            int(unique_processes),

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
        mode="w",
        encoding="utf-8"
    ) as output_file:

        json.dump(
            summary,
            output_file,
            indent=4
        )

    print(f"[OK] Summary written to {SUMMARY_OUTPUT}")

    # -----------------------------------------------------
    # 15. CLEANUP
    # -----------------------------------------------------

    sample_df.unpersist()

    spark.stop()

    print("\n" + "=" * 75)
    print("PROCESS DATASET INSPECTION COMPLETED")
    print("=" * 75)


if __name__ == "__main__":
    main()