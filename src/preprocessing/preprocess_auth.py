import argparse
import json
import platform
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

# Default output path.
# On WSL we will override this using --output-dir.
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
            "Clean and normalize staged LANL authentication "
            "data and write optimized Parquet output."
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

    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(OUTPUT_DIR),
        help=(
            "Directory where processed Parquet data will be written. "
            "For WSL, use native Linux storage such as "
            "/home/lakshya/ueba_data/auth_clean"
        )
    )

    return parser.parse_args()


# =========================================================
# STRING NORMALIZATION
# =========================================================

def normalize_required(column_name):
    """
    Normalize identifiers required for graph construction.

    Missing/empty values remain NULL so that invalid graph
    events can be filtered later.
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
    Normalize optional/categorical values.

    Missing values become UNKNOWN.
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
    Example:

    U748@DOM1

    becomes:

    account = U748
    domain  = DOM1
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
# CLEANING + STRUCTURAL FIELD GENERATION
# =========================================================

def clean_auth_dataframe(dataframe):

    # -----------------------------------------------------
    # 1. NORMALIZE IDENTIFIERS / CATEGORICAL VALUES
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
            normalize_optional("auth_orientation")
        )
        .withColumn(
            "success",
            normalize_optional("success")
        )
    )

    # -----------------------------------------------------
    # 2. REMOVE RECORDS MISSING CRITICAL GRAPH IDENTIFIERS
    # -----------------------------------------------------

    dataframe = dataframe.filter(
        F.col("time").isNotNull()
        & F.col("src_user").isNotNull()
        & F.col("src_computer").isNotNull()
        & F.col("dst_computer").isNotNull()
    )

    # -----------------------------------------------------
    # 3. TEMPORAL STRUCTURAL FIELDS
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
    # 4. USER ACCOUNT / DOMAIN COMPONENTS
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
    # 5. AUTH RESULT FLAGS
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
    # 8. RELATIONSHIP FLAGS
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
    # 9. FINAL COLUMN ORDER
    # -----------------------------------------------------

    dataframe = dataframe.select(

        "time",
        "day_index",
        "second_of_day",
        "hour",

        "src_user",
        "src_user_account",
        "src_user_domain",

        "dst_user",
        "dst_user_account",
        "dst_user_domain",

        "src_computer",
        "dst_computer",

        "auth_type",
        "logon_type",
        "auth_orientation",
        "success",

        "success_flag",
        "is_failure",

        "is_logon",
        "is_logoff",

        "src_is_machine_account",
        "dst_is_machine_account",

        "same_user",
        "same_computer",
        "cross_host"
    )

    return dataframe


# =========================================================
# DIRECTORY SIZE
# =========================================================

def directory_size_mb(directory):

    total_bytes = 0

    for file_path in directory.rglob("*"):

        if file_path.is_file():
            total_bytes += file_path.stat().st_size

    return total_bytes / (1024 * 1024)


# =========================================================
# OUTPUT LOCATION CHECK
# =========================================================

def validate_output_location(output_dir):
    """
    Hadoop/Spark can have chmod problems when writing Parquet
    from WSL directly to /mnt/c.

    Prevent accidental large writes to Windows-mounted storage.
    """

    if platform.system() == "Linux":

        output_string = str(output_dir)

        if output_string.startswith("/mnt/"):

            raise RuntimeError(
                "\nINVALID WSL OUTPUT LOCATION\n\n"
                "Do not write Spark Parquet output directly under /mnt/c.\n"
                "Use native Linux storage instead, for example:\n\n"
                "/home/lakshya/ueba_data/auth_clean\n"
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

    # Expand ~ and convert to absolute path.
    output_dir = Path(
        args.output_dir
    ).expanduser().resolve()

    validate_output_location(
        output_dir
    )

    print("=" * 75)
    print("LANL AUTHENTICATION PREPROCESSING PIPELINE")
    print("=" * 75)

    print(
        f"\nProject root    : "
        f"{PROJECT_ROOT}"
    )

    print(
        f"Input directory : "
        f"{STAGING_DIR}"
    )

    print(
        f"Output directory: "
        f"{output_dir}"
    )

    print(
        f"Days            : "
        f"{args.days}"
    )

    # -----------------------------------------------------
    # 1. VERIFY STAGING FILES
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
    # 2. PREPARE OUTPUT
    # -----------------------------------------------------

    if (
        output_dir.exists()
        and args.overwrite
    ):

        print(
            "\nRemoving previous processed "
            "authentication output..."
        )

        shutil.rmtree(
            output_dir
        )

    elif (
        output_dir.exists()
        and any(output_dir.iterdir())
        and not args.overwrite
    ):

        raise RuntimeError(
            "\nProcessed authentication output already exists.\n"
            "Run again with --overwrite if you want to recreate it."
        )

    output_dir.mkdir(
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

        # Conservative configuration for local laptop.
        .master("local[2]")

        .config(
            "spark.sql.shuffle.partitions",
            "8"
        )

        .config(
            "spark.default.parallelism",
            "4"
        )

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

    processed_days = []

    # -----------------------------------------------------
    # 4. PROCESS EACH DAY
    # -----------------------------------------------------

    for day, input_file in enumerate(
        input_files
    ):

        print("\n" + "-" * 75)

        print(
            f"Processing Day {day:02d}"
        )

        print(
            f"Input: {input_file.name}"
        )

        # -------------------------------------------------
        # READ DAILY STAGED CSV.GZ
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

        clean_df = clean_auth_dataframe(
            raw_df
        )

        # -------------------------------------------------
        # DAILY OUTPUT LOCATION
        # -------------------------------------------------

        output_day_dir = (
            output_dir
            / f"day_{day:02d}"
        )

        if output_day_dir.exists():

            shutil.rmtree(
                output_day_dir
            )

        # -------------------------------------------------
        # WRITE PARQUET
        # -------------------------------------------------

        print(
            "Writing cleaned Parquet..."
        )

        (
            clean_df

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
        # OUTPUT SIZE
        # -------------------------------------------------

        output_size = directory_size_mb(
            output_day_dir
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
    # 5. STOP SPARK
    # -----------------------------------------------------

    spark.stop()

    # -----------------------------------------------------
    # 6. SAVE SUMMARY
    # -----------------------------------------------------

    summary = {

        "phase":
            "3B-3D Authentication Preprocessing",

        "days_processed":
            args.days,

        "input_directory":
            str(STAGING_DIR),

        "output_directory":
            str(output_dir),

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
    # 7. COMPLETION MESSAGE
    # -----------------------------------------------------

    print("\n" + "=" * 75)

    print(
        "AUTHENTICATION PREPROCESSING COMPLETED"
    )

    print("=" * 75)

    print(
        f"\nProcessed data:\n"
        f"{output_dir}"
    )

    print(
        f"\nSummary written to:\n"
        f"{SUMMARY_OUTPUT}"
    )


# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    main()