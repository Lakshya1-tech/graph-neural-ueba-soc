import argparse
import json
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
)


# =========================================================
# PROJECT PATHS
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "Data"

STAGING_DIR = (
    DATA_DIR
    / "processed"
    / "staging"
    / "auth_daily"
)

OUTPUT_DIR = (
    DATA_DIR
    / "processed"
    / "auth_clean"
)

LOG_DIR = PROJECT_ROOT / "Logs"

SUMMARY_OUTPUT = (
    LOG_DIR
    / "auth_preprocessing_summary.json"
)


# =========================================================
# LANL AUTHENTICATION SCHEMA
# =========================================================

AUTH_SCHEMA = StructType([
    StructField(
        "time",
        LongType(),
        True
    ),

    StructField(
        "src_user",
        StringType(),
        True
    ),

    StructField(
        "dst_user",
        StringType(),
        True
    ),

    StructField(
        "src_computer",
        StringType(),
        True
    ),

    StructField(
        "dst_computer",
        StringType(),
        True
    ),

    StructField(
        "auth_type",
        StringType(),
        True
    ),

    StructField(
        "logon_type",
        StringType(),
        True
    ),

    StructField(
        "auth_orientation",
        StringType(),
        True
    ),

    StructField(
        "success",
        StringType(),
        True
    ),
])


SECONDS_PER_DAY = 86400


# =========================================================
# COMMAND-LINE ARGUMENTS
# =========================================================

def parse_arguments():

    parser = argparse.ArgumentParser(
        description=(
            "Clean and normalize staged LANL "
            "authentication data and write "
            "optimized Parquet output."
        )
    )

    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of staged LANL days to process. Default: 7"
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Delete existing processed output before running."
    )

    return parser.parse_args()


# =========================================================
# STRING NORMALIZATION
# =========================================================

def normalize_required(column_name):
    """
    Normalize identifiers that are essential for graph construction.

    Empty values remain NULL so that records missing critical
    graph identifiers can be removed later.
    """

    normalized_value = F.upper(
        F.trim(
            F.col(column_name)
        )
    )

    return F.when(
        F.col(column_name).isNull()
        | (F.trim(F.col(column_name)) == ""),
        F.lit(None)
    ).otherwise(
        normalized_value
    )


def normalize_optional(column_name):
    """
    Normalize optional categorical values.

    Missing/empty values become UNKNOWN rather than being removed.
    """

    normalized_value = F.upper(
        F.trim(
            F.col(column_name)
        )
    )

    return F.when(
        F.col(column_name).isNull()
        | (F.trim(F.col(column_name)) == ""),
        F.lit("UNKNOWN")
    ).otherwise(
        normalized_value
    )


# =========================================================
# USER ACCOUNT PARSING
# =========================================================

def add_user_components(
    dataframe,
    column_name,
    prefix
):
    """
    Split LANL identities such as:

        U748@DOM1

    into:

        account = U748
        domain  = DOM1

    Machine accounts such as:

        C101$@DOM1

    remain identifiable because the $ symbol is preserved.
    """

    account_component = F.regexp_extract(
        F.col(column_name),
        r"^([^@]+)",
        1
    )

    domain_component = F.regexp_extract(
        F.col(column_name),
        r"@(.+)$",
        1
    )

    dataframe = dataframe.withColumn(
        f"{prefix}_account",
        F.when(
            account_component == "",
            F.lit("UNKNOWN")
        ).otherwise(
            account_component
        )
    )

    dataframe = dataframe.withColumn(
        f"{prefix}_domain",
        F.when(
            domain_component == "",
            F.lit("UNKNOWN")
        ).otherwise(
            domain_component
        )
    )

    return dataframe


# =========================================================
# CLEANING + STRUCTURAL FEATURE GENERATION
# =========================================================

def clean_auth_dataframe(dataframe):

    # -----------------------------------------------------
    # 1. NORMALIZE IDENTIFIERS
    # -----------------------------------------------------

    dataframe = (
        dataframe
        .withColumn(
            "src_user",
            normalize_required("src_user")
        )
        .withColumn(
            "dst_user",
            normalize_optional("dst_user")
        )
        .withColumn(
            "src_computer",
            normalize_required("src_computer")
        )
        .withColumn(
            "dst_computer",
            normalize_required("dst_computer")
        )
        .withColumn(
            "auth_type",
            normalize_optional("auth_type")
        )
        .withColumn(
            "logon_type",
            normalize_optional("logon_type")
        )
        .withColumn(
            "auth_orientation",
            normalize_optional(
                "auth_orientation"
            )
        )
        .withColumn(
            "success",
            normalize_optional("success")
        )
    )

    # -----------------------------------------------------
    # 2. REMOVE RECORDS MISSING CRITICAL GRAPH DATA
    # -----------------------------------------------------

    dataframe = dataframe.filter(
        F.col("time").isNotNull()
        & F.col("src_user").isNotNull()
        & F.col("src_computer").isNotNull()
        & F.col("dst_computer").isNotNull()
    )

    # -----------------------------------------------------
    # 3. TEMPORAL STRUCTURAL FEATURES
    # -----------------------------------------------------

    dataframe = dataframe.withColumn(
        "day_index",
        F.floor(
            F.col("time")
            / F.lit(SECONDS_PER_DAY)
        ).cast("int")
    )

    dataframe = dataframe.withColumn(
        "second_of_day",
        F.pmod(
            F.col("time"),
            F.lit(SECONDS_PER_DAY)
        ).cast("int")
    )

    dataframe = dataframe.withColumn(
        "hour",
        F.floor(
            F.col("second_of_day")
            / F.lit(3600)
        ).cast("int")
    )

    # -----------------------------------------------------
    # 4. SPLIT USER ACCOUNT / DOMAIN
    # -----------------------------------------------------

    dataframe = add_user_components(
        dataframe,
        "src_user",
        "src_user"
    )

    dataframe = add_user_components(
        dataframe,
        "dst_user",
        "dst_user"
    )

    # -----------------------------------------------------
    # 5. AUTHENTICATION RESULT FLAGS
    # -----------------------------------------------------

    dataframe = dataframe.withColumn(
        "success_flag",
        F.when(
            F.col("success") == "SUCCESS",
            F.lit(1)
        )
        .when(
            F.col("success").isin(
                "FAIL",
                "FAILURE"
            ),
            F.lit(0)
        )
        .otherwise(
            F.lit(-1)
        )
        .cast("int")
    )

    dataframe = dataframe.withColumn(
        "is_failure",
        F.when(
            F.col("success_flag") == 0,
            F.lit(1)
        )
        .otherwise(
            F.lit(0)
        )
        .cast("int")
    )

    # -----------------------------------------------------
    # 6. LOGON / LOGOFF FLAGS
    # -----------------------------------------------------

    dataframe = dataframe.withColumn(
        "is_logon",
        F.when(
            F.col("auth_orientation") == "LOGON",
            F.lit(1)
        )
        .otherwise(
            F.lit(0)
        )
        .cast("int")
    )

    dataframe = dataframe.withColumn(
        "is_logoff",
        F.when(
            F.col("auth_orientation") == "LOGOFF",
            F.lit(1)
        )
        .otherwise(
            F.lit(0)
        )
        .cast("int")
    )

    # -----------------------------------------------------
    # 7. MACHINE ACCOUNT FLAGS
    # -----------------------------------------------------

    dataframe = dataframe.withColumn(
        "src_is_machine_account",
        F.when(
            F.col("src_user_account").endswith("$"),
            F.lit(1)
        )
        .otherwise(
            F.lit(0)
        )
        .cast("int")
    )

    dataframe = dataframe.withColumn(
        "dst_is_machine_account",
        F.when(
            F.col("dst_user_account").endswith("$"),
            F.lit(1)
        )
        .otherwise(
            F.lit(0)
        )
        .cast("int")
    )

    # -----------------------------------------------------
    # 8. USER RELATIONSHIP FLAG
    # -----------------------------------------------------

    dataframe = dataframe.withColumn(
        "same_user",
        F.when(
            F.col("src_user")
            == F.col("dst_user"),
            F.lit(1)
        )
        .otherwise(
            F.lit(0)
        )
        .cast("int")
    )

    # -----------------------------------------------------
    # 9. HOST RELATIONSHIP FLAGS
    # -----------------------------------------------------

    dataframe = dataframe.withColumn(
        "same_computer",
        F.when(
            F.col("src_computer")
            == F.col("dst_computer"),
            F.lit(1)
        )
        .otherwise(
            F.lit(0)
        )
        .cast("int")
    )

    dataframe = dataframe.withColumn(
        "cross_host",
        F.when(
            F.col("src_computer")
            != F.col("dst_computer"),
            F.lit(1)
        )
        .otherwise(
            F.lit(0)
        )
        .cast("int")
    )

    # -----------------------------------------------------
    # 10. FINAL COLUMN ORDER
    # -----------------------------------------------------

    dataframe = dataframe.select(

        # Time
        "time",
        "day_index",
        "second_of_day",
        "hour",

        # Source identity
        "src_user",
        "src_user_account",
        "src_user_domain",

        # Destination identity
        "dst_user",
        "dst_user_account",
        "dst_user_domain",

        # Computers
        "src_computer",
        "dst_computer",

        # Authentication metadata
        "auth_type",
        "logon_type",
        "auth_orientation",
        "success",

        # Authentication flags
        "success_flag",
        "is_failure",
        "is_logon",
        "is_logoff",

        # Account flags
        "src_is_machine_account",
        "dst_is_machine_account",

        # Relationship flags
        "same_user",
        "same_computer",
        "cross_host"
    )

    return dataframe


# =========================================================
# CALCULATE DIRECTORY SIZE
# =========================================================

def directory_size_mb(directory):

    total_bytes = 0

    for file_path in directory.rglob("*"):

        if file_path.is_file():

            total_bytes += (
                file_path.stat().st_size
            )

    return (
        total_bytes
        / (1024 * 1024)
    )


# =========================================================
# MAIN PIPELINE
# =========================================================

def main():

    args = parse_arguments()

    if args.days <= 0:

        raise ValueError(
            "--days must be greater than zero."
        )

    print("=" * 75)
    print("LANL AUTHENTICATION PREPROCESSING PIPELINE")
    print("=" * 75)

    print(
        f"\nInput directory : "
        f"{STAGING_DIR}"
    )

    print(
        f"Output directory: "
        f"{OUTPUT_DIR}"
    )

    print(
        f"Days            : "
        f"{args.days}"
    )

    # -----------------------------------------------------
    # 1. VERIFY DAILY STAGING FILES
    # -----------------------------------------------------

    input_files = []

    for day in range(args.days):

        file_path = (
            STAGING_DIR
            / f"auth_day_{day:02d}.csv.gz"
        )

        if not file_path.exists():

            raise FileNotFoundError(
                f"\nMissing staged authentication file:\n"
                f"{file_path}"
            )

        input_files.append(
            file_path
        )

    print(
        f"\n[OK] Found "
        f"{len(input_files)} "
        f"daily staging files."
    )

    # -----------------------------------------------------
    # 2. HANDLE EXISTING OUTPUT
    # -----------------------------------------------------

    if (
        OUTPUT_DIR.exists()
        and args.overwrite
    ):

        print(
            "\nRemoving previous processed "
            "authentication output..."
        )

        shutil.rmtree(
            OUTPUT_DIR
        )

    elif (
        OUTPUT_DIR.exists()
        and any(OUTPUT_DIR.iterdir())
        and not args.overwrite
    ):

        raise RuntimeError(
            "\nProcessed authentication output already exists.\n"
            "Use --overwrite if you want to recreate it."
        )

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    LOG_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    # -----------------------------------------------------
    # 3. START SPARK
    # -----------------------------------------------------

    print("\nStarting SparkSession...")

    spark = (
        SparkSession.builder

        .appName(
            "LANL_Auth_Preprocessing"
        )

        # Two cores is deliberate for this laptop.
        .master("local[2]")

        .config(
            "spark.sql.shuffle.partitions",
            "8"
        )

        .config(
            "spark.default.parallelism",
            "4"
        )

        # Keep the local job conservative for a 16 GB machine.
        .config(
            "spark.driver.memory",
            "4g"
        )

        .config(
            "spark.sql.adaptive.enabled",
            "true"
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print(
        f"[OK] Spark version: "
        f"{spark.version}"
    )

    # -----------------------------------------------------
    # 4. TRACK PROCESSED DAYS
    # -----------------------------------------------------

    processed_days = []

    # -----------------------------------------------------
    # 5. PROCESS ONE DAY AT A TIME
    # -----------------------------------------------------

    for day, input_file in enumerate(
        input_files
    ):

        print("\n" + "-" * 75)

        print(
            f"Processing Day {day:02d}"
        )

        print(
            f"Input: "
            f"{input_file.name}"
        )

        # -------------------------------------------------
        # READ DAILY CSV.GZ
        # -------------------------------------------------

        raw_df = (
            spark.read

            .option(
                "header",
                "false"
            )

            .option(
                "sep",
                ","
            )

            .option(
                "nullValue",
                "?"
            )

            .schema(
                AUTH_SCHEMA
            )

            .csv(
                str(input_file)
            )
        )

        # -------------------------------------------------
        # CLEAN + GENERATE STRUCTURAL FIELDS
        # -------------------------------------------------

        clean_df = (
            clean_auth_dataframe(
                raw_df
            )
        )

        # -------------------------------------------------
        # OUTPUT DIRECTORY FOR CURRENT DAY
        # -------------------------------------------------

        output_day_dir = (
            OUTPUT_DIR
            / f"day_{day:02d}"
        )

        if output_day_dir.exists():

            shutil.rmtree(
                output_day_dir
            )

        # -------------------------------------------------
        # WRITE AS PARQUET
        # -------------------------------------------------

        print(
            "Writing cleaned Parquet..."
        )

        (
            clean_df

            # Two files per day keeps file count low
            # while still enabling parallel reads later.
            .repartition(2)

            .write

            .mode(
                "overwrite"
            )

            .option(
                "compression",
                "snappy"
            )

            .parquet(
                str(output_day_dir)
            )
        )

        # -------------------------------------------------
        # CALCULATE OUTPUT SIZE
        # -------------------------------------------------

        output_size = (
            directory_size_mb(
                output_day_dir
            )
        )

        processed_days.append({
            "day":
                day,

            "input_file":
                str(input_file),

            "output_directory":
                str(output_day_dir),

            "output_size_mb":
                round(
                    output_size,
                    2
                )
        })

        print(
            f"[OK] Day {day:02d} "
            f"written successfully."
        )

        print(
            f"Output size: "
            f"{output_size:.2f} MB"
        )

    # -----------------------------------------------------
    # 6. STOP SPARK
    # -----------------------------------------------------

    spark.stop()

    # -----------------------------------------------------
    # 7. SAVE PIPELINE SUMMARY
    # -----------------------------------------------------

    summary = {

        "phase":
            "3B-3D Authentication Preprocessing",

        "days_processed":
            args.days,

        "input_directory":
            str(STAGING_DIR),

        "output_directory":
            str(OUTPUT_DIR),

        "output_format":
            "Parquet",

        "compression":
            "Snappy",

        "generated_columns": [

            "day_index",
            "second_of_day",
            "hour",

            "src_user_account",
            "src_user_domain",

            "dst_user_account",
            "dst_user_domain",

            "success_flag",
            "is_failure",

            "is_logon",
            "is_logoff",

            "src_is_machine_account",
            "dst_is_machine_account",

            "same_user",
            "same_computer",
            "cross_host"
        ],

        "processed_days":
            processed_days
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
    # 8. COMPLETION MESSAGE
    # -----------------------------------------------------

    print("\n" + "=" * 75)

    print(
        "AUTHENTICATION PREPROCESSING COMPLETED"
    )

    print("=" * 75)

    print(
        f"\nSummary written to:\n"
        f"{SUMMARY_OUTPUT}"
    )


# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    main()