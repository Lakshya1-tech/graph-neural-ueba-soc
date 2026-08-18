import argparse
import csv
import gzip
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    StringType,
)


# =========================================================
# PROJECT PATHS
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "Data"
RAW_DIR = DATA_DIR / "raw"
SAMPLES_DIR = DATA_DIR / "samples"
LOG_DIR = PROJECT_ROOT / "Logs"

REDTEAM_FILE = RAW_DIR / "redteam.txt.gz"

STAGING_SUMMARY = (
    LOG_DIR
    / "auth_daily_staging_summary.json"
)

VALIDATION_SUMMARY = (
    LOG_DIR
    / "processed_auth_validation_summary.json"
)

REDTEAM_MATCH_OUTPUT = (
    SAMPLES_DIR
    / "redteam_processed_auth_matches.csv"
)

SECONDS_PER_DAY = 86400


# =========================================================
# EXPECTED PROCESSED SCHEMA
# =========================================================

EXPECTED_SCHEMA = {

    "time": "bigint",

    "day_index": "int",
    "second_of_day": "int",
    "hour": "int",

    "src_user": "string",
    "src_user_account": "string",
    "src_user_domain": "string",

    "dst_user": "string",
    "dst_user_account": "string",
    "dst_user_domain": "string",

    "src_computer": "string",
    "dst_computer": "string",

    "auth_type": "string",
    "logon_type": "string",
    "auth_orientation": "string",
    "success": "string",

    "success_flag": "int",
    "is_failure": "int",

    "is_logon": "int",
    "is_logoff": "int",

    "src_is_machine_account": "int",
    "dst_is_machine_account": "int",

    "same_user": "int",
    "same_computer": "int",
    "cross_host": "int",
}


CRITICAL_COLUMNS = [

    "time",
    "day_index",
    "second_of_day",
    "hour",

    "src_user",
    "src_computer",
    "dst_computer",
]


BINARY_COLUMNS = [

    "is_failure",

    "is_logon",
    "is_logoff",

    "src_is_machine_account",
    "dst_is_machine_account",

    "same_user",
    "same_computer",
    "cross_host",
]


# =========================================================
# ARGUMENTS
# =========================================================

def parse_arguments():

    parser = argparse.ArgumentParser(
        description=(
            "Validate processed LANL authentication "
            "Parquet data before UEBA feature engineering."
        )
    )

    parser.add_argument(
        "--input-dir",
        type=str,
        default=str(
            Path.home()
            / "ueba_data"
            / "auth_clean"
        ),
        help=(
            "Processed authentication Parquet directory. "
            "Default: ~/ueba_data/auth_clean"
        )
    )

    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help=(
            "Number of processed LANL days "
            "to validate. Default: 7"
        )
    )

    return parser.parse_args()


# =========================================================
# HELPER FUNCTIONS
# =========================================================

def count_when(condition, alias):

    return F.sum(
        F.when(
            condition,
            F.lit(1)
        ).otherwise(
            F.lit(0)
        )
    ).cast("long").alias(alias)


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


def load_staging_summary():

    if not STAGING_SUMMARY.exists():
        return None

    with open(
        STAGING_SUMMARY,
        mode="r",
        encoding="utf-8"
    ) as file:

        return json.load(file)


# =========================================================
# LOAD RED-TEAM EVENTS
# =========================================================

def load_redteam_events(days):

    cutoff_time = (
        days
        * SECONDS_PER_DAY
    )

    records = []

    if not REDTEAM_FILE.exists():

        raise FileNotFoundError(
            f"\nRed-team dataset not found:\n"
            f"{REDTEAM_FILE}"
        )

    with gzip.open(
        REDTEAM_FILE,
        mode="rt",
        encoding="utf-8",
        newline=""
    ) as file:

        reader = csv.reader(file)

        for source_row, row in enumerate(
            reader,
            start=1
        ):

            if len(row) != 4:
                continue

            try:

                event_time = int(
                    row[0]
                )

            except ValueError:

                continue

            # Only evaluate red-team events
            # inside the current development window.
            if event_time >= cutoff_time:
                continue

            records.append({

                "event_id":
                    len(records),

                "source_row":
                    source_row,

                "time":
                    event_time,

                "user":
                    row[1]
                    .strip()
                    .upper(),

                "src_computer":
                    row[2]
                    .strip()
                    .upper(),

                "dst_computer":
                    row[3]
                    .strip()
                    .upper(),
            })

    return records


# =========================================================
# CSV WRITER FOR SMALL VALIDATION OUTPUT
# =========================================================

def write_csv(
    output_path,
    rows,
    fieldnames
):

    output_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        output_path,
        mode="w",
        encoding="utf-8",
        newline=""
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames
        )

        writer.writeheader()

        for row in rows:
            writer.writerow(row)


# =========================================================
# MAIN
# =========================================================

def main():

    args = parse_arguments()

    if args.days <= 0:

        raise ValueError(
            "--days must be greater than zero."
        )

    input_dir = Path(
        args.input_dir
    ).expanduser().resolve()

    print("=" * 78)
    print("PHASE 3E - PROCESSED AUTHENTICATION VALIDATION")
    print("=" * 78)

    print(
        f"\nProcessed input : "
        f"{input_dir}"
    )

    print(
        f"Days to validate: "
        f"{args.days}"
    )

    # =====================================================
    # 1. VERIFY PHYSICAL DAY OUTPUTS
    # =====================================================

    day_directories = [

        input_dir
        / f"day_{day:02d}"

        for day
        in range(args.days)
    ]

    missing_directories = [

        str(day_directory)

        for day_directory
        in day_directories

        if not day_directory.exists()
    ]

    if missing_directories:

        raise FileNotFoundError(
            "\nMissing processed day directories:\n"
            + "\n".join(
                missing_directories
            )
        )

    day_sizes = {}

    missing_parquet_days = []

    for day, day_directory in enumerate(
        day_directories
    ):

        parquet_files = list(
            day_directory.glob(
                "*.parquet"
            )
        )

        if not parquet_files:

            missing_parquet_days.append(
                day
            )

        day_sizes[str(day)] = round(
            directory_size_mb(
                day_directory
            ),
            2
        )

    if missing_parquet_days:

        raise FileNotFoundError(
            "\nParquet files missing for day(s): "
            + ", ".join(
                str(day)
                for day
                in missing_parquet_days
            )
        )

    print(
        "\n[OK] All seven processed "
        "day directories contain Parquet files."
    )

    # =====================================================
    # 2. START SPARK
    # =====================================================

    print(
        "\nStarting SparkSession..."
    )

    spark = (
        SparkSession.builder

        .appName(
            "LANL_Processed_Auth_Validation"
        )

        .master(
            "local[2]"
        )

        .config(
            "spark.driver.memory",
            "4g"
        )

        .config(
            "spark.sql.shuffle.partitions",
            "8"
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

    # =====================================================
    # 3. READ PROCESSED PARQUET
    # =====================================================

    auth_df = (
        spark.read.parquet(
            *[
                str(day_directory)

                for day_directory
                in day_directories
            ]
        )
    )

    print(
        "\n[OK] Processed Parquet "
        "dataset loaded."
    )

    # =====================================================
    # 4. SCHEMA VALIDATION
    # =====================================================

    actual_schema = {

        field.name:
            field.dataType.simpleString()

        for field
        in auth_df.schema.fields
    }

    missing_columns = [

        column

        for column
        in EXPECTED_SCHEMA

        if column
        not in actual_schema
    ]

    unexpected_columns = [

        column

        for column
        in actual_schema

        if column
        not in EXPECTED_SCHEMA
    ]

    type_mismatches = {}

    for (
        column,
        expected_type
    ) in EXPECTED_SCHEMA.items():

        if (
            column in actual_schema
            and
            actual_schema[column]
            != expected_type
        ):

            type_mismatches[
                column
            ] = {

                "expected":
                    expected_type,

                "actual":
                    actual_schema[column]
            }

    schema_pass = (

        len(missing_columns) == 0
        and
        len(unexpected_columns) == 0
        and
        len(type_mismatches) == 0
    )

    print(
        f"\nExpected columns: "
        f"{len(EXPECTED_SCHEMA)}"
    )

    print(
        f"Actual columns  : "
        f"{len(actual_schema)}"
    )

    print(
        f"Schema result   : "
        f"{'PASS' if schema_pass else 'FAIL'}"
    )

    # =====================================================
    # 5. GLOBAL DATA INTEGRITY CHECKS
    # =====================================================

    print(
        "\nRunning global integrity checks..."
    )

    expressions = [

        F.count(
            F.lit(1)
        ).cast(
            "long"
        ).alias(
            "processed_rows"
        ),

        F.min(
            "time"
        ).alias(
            "minimum_time"
        ),

        F.max(
            "time"
        ).alias(
            "maximum_time"
        ),

        F.min(
            "day_index"
        ).alias(
            "minimum_day_index"
        ),

        F.max(
            "day_index"
        ).alias(
            "maximum_day_index"
        ),

        F.min(
            "second_of_day"
        ).alias(
            "minimum_second_of_day"
        ),

        F.max(
            "second_of_day"
        ).alias(
            "maximum_second_of_day"
        ),

        F.min(
            "hour"
        ).alias(
            "minimum_hour"
        ),

        F.max(
            "hour"
        ).alias(
            "maximum_hour"
        ),
    ]

    # Critical NULL checks.
    for column in CRITICAL_COLUMNS:

        expressions.append(

            count_when(

                F.col(
                    column
                ).isNull(),

                f"null_{column}"
            )
        )

    # Binary columns must contain only 0 / 1.
    for column in BINARY_COLUMNS:

        expressions.append(

            count_when(

                F.col(
                    column
                ).isNull()
                |
                (
                    ~F.col(
                        column
                    ).isin(
                        0,
                        1
                    )
                ),

                f"invalid_{column}"
            )
        )

    # success_flag must be -1, 0 or 1.
    expressions.append(

        count_when(

            F.col(
                "success_flag"
            ).isNull()
            |
            (
                ~F.col(
                    "success_flag"
                ).isin(
                    -1,
                    0,
                    1
                )
            ),

            "invalid_success_flag"
        )
    )

    # Day index range.
    expressions.append(

        count_when(

            (
                F.col(
                    "day_index"
                ) < 0
            )
            |
            (
                F.col(
                    "day_index"
                )
                >= args.days
            ),

            "invalid_day_index"
        )
    )

    # Second of day range.
    expressions.append(

        count_when(

            (
                F.col(
                    "second_of_day"
                ) < 0
            )
            |
            (
                F.col(
                    "second_of_day"
                )
                >= SECONDS_PER_DAY
            ),

            "invalid_second_of_day"
        )
    )

    # Hour range.
    expressions.append(

        count_when(

            (
                F.col(
                    "hour"
                ) < 0
            )
            |
            (
                F.col(
                    "hour"
                ) > 23
            ),

            "invalid_hour"
        )
    )

    # same_computer and cross_host
    # should always be complementary.
    expressions.append(

        count_when(

            (
                F.col(
                    "same_computer"
                )
                +
                F.col(
                    "cross_host"
                )
            )
            != 1,

            "invalid_host_relationship"
        )
    )

    # is_failure must agree with success_flag.
    expressions.append(

        count_when(

            (
                F.col(
                    "success_flag"
                ) == 0
            )
            &
            (
                F.col(
                    "is_failure"
                ) != 1
            ),

            "failure_flag_mismatch_1"
        )
    )

    expressions.append(

        count_when(

            (
                F.col(
                    "success_flag"
                ) != 0
            )
            &
            (
                F.col(
                    "is_failure"
                ) != 0
            ),

            "failure_flag_mismatch_2"
        )
    )

    # Event cannot simultaneously
    # be marked LogOn and LogOff.
    expressions.append(

        count_when(

            (
                F.col(
                    "is_logon"
                ) == 1
            )
            &
            (
                F.col(
                    "is_logoff"
                ) == 1
            ),

            "simultaneous_logon_logoff"
        )
    )

    global_stats = (
        auth_df
        .agg(
            *expressions
        )
        .first()
        .asDict()
    )

    processed_rows = int(
        global_stats[
            "processed_rows"
        ]
    )

    print(
        f"\nProcessed rows: "
        f"{processed_rows:,}"
    )

    print(
        f"Timestamp range: "
        f"{global_stats['minimum_time']} "
        f"to "
        f"{global_stats['maximum_time']}"
    )

    # =====================================================
    # 6. DAILY EVENT COUNTS
    # =====================================================

    print(
        "\nCalculating processed "
        "rows per day..."
    )

    daily_rows = (
        auth_df

        .groupBy(
            "day_index"
        )

        .count()

        .orderBy(
            "day_index"
        )

        .collect()
    )

    processed_rows_per_day = {}

    present_days = []

    for row in daily_rows:

        day_value = row[
            "day_index"
        ]

        if day_value is None:
            continue

        day_value = int(
            day_value
        )

        present_days.append(
            day_value
        )

        processed_rows_per_day[
            str(day_value)
        ] = int(
            row["count"]
        )

    expected_days = list(
        range(
            args.days
        )
    )

    day_coverage_pass = (
        sorted(
            present_days
        )
        ==
        expected_days
    )

    # =====================================================
    # 7. COMPARE WITH PHASE 3A STAGING COUNTS
    # =====================================================

    staging_summary = (
        load_staging_summary()
    )

    input_staged_rows = None
    removed_rows = None
    retention_rate = None
    daily_retention = {}

    row_integrity_pass = (
        processed_rows > 0
    )

    if staging_summary is not None:

        input_staged_rows = int(
            staging_summary.get(
                "total_valid_rows",
                0
            )
        )

        removed_rows = (
            input_staged_rows
            - processed_rows
        )

        if input_staged_rows > 0:

            retention_rate = (
                processed_rows
                /
                input_staged_rows
            ) * 100

        row_integrity_pass = (

            processed_rows > 0

            and

            processed_rows
            <= input_staged_rows
        )

        staged_daily = (
            staging_summary.get(
                "rows_per_day",
                {}
            )
        )

        for day in expected_days:

            key = str(
                day
            )

            staged_count = int(
                staged_daily.get(
                    key,
                    0
                )
            )

            processed_count = int(
                processed_rows_per_day.get(
                    key,
                    0
                )
            )

            if staged_count > 0:

                rate = (
                    processed_count
                    /
                    staged_count
                ) * 100

            else:

                rate = None

            daily_retention[
                key
            ] = {

                "staged_rows":
                    staged_count,

                "processed_rows":
                    processed_count,

                "removed_rows":
                    staged_count
                    - processed_count,

                "retention_rate_percent":
                    (
                        round(
                            rate,
                            6
                        )
                        if rate is not None
                        else None
                    )
            }

    # =====================================================
    # 8. NULL / FEATURE VALIDATION RESULTS
    # =====================================================

    critical_null_counts = {

        column:
            int(
                global_stats[
                    f"null_{column}"
                ]
            )

        for column
        in CRITICAL_COLUMNS
    }

    invalid_binary_counts = {

        column:
            int(
                global_stats[
                    f"invalid_{column}"
                ]
            )

        for column
        in BINARY_COLUMNS
    }

    critical_null_pass = all(

        value == 0

        for value
        in critical_null_counts.values()
    )

    binary_field_pass = (

        all(

            value == 0

            for value
            in invalid_binary_counts.values()
        )

        and

        int(
            global_stats[
                "invalid_success_flag"
            ]
        ) == 0
    )

    temporal_pass = (

        int(
            global_stats[
                "invalid_day_index"
            ]
        ) == 0

        and

        int(
            global_stats[
                "invalid_second_of_day"
            ]
        ) == 0

        and

        int(
            global_stats[
                "invalid_hour"
            ]
        ) == 0
    )

    logical_counts = {

        "invalid_host_relationship":

            int(
                global_stats[
                    "invalid_host_relationship"
                ]
            ),

        "failure_flag_mismatch_1":

            int(
                global_stats[
                    "failure_flag_mismatch_1"
                ]
            ),

        "failure_flag_mismatch_2":

            int(
                global_stats[
                    "failure_flag_mismatch_2"
                ]
            ),

        "simultaneous_logon_logoff":

            int(
                global_stats[
                    "simultaneous_logon_logoff"
                ]
            ),
    }

    logical_pass = all(

        value == 0

        for value
        in logical_counts.values()
    )

    # =====================================================
    # 9. RED-TEAM PRESERVATION VALIDATION
    # =====================================================

    print(
        "\nChecking known red-team "
        "events after preprocessing..."
    )

    redteam_records = (
        load_redteam_events(
            args.days
        )
    )

    redteam_total = len(
        redteam_records
    )

    redteam_schema = StructType([

        StructField(
            "event_id",
            IntegerType(),
            False
        ),

        StructField(
            "time",
            LongType(),
            False
        ),

        StructField(
            "user",
            StringType(),
            False
        ),

        StructField(
            "src_computer",
            StringType(),
            False
        ),

        StructField(
            "dst_computer",
            StringType(),
            False
        ),
    ])

    redteam_for_spark = [

        (
            record[
                "event_id"
            ],

            record[
                "time"
            ],

            record[
                "user"
            ],

            record[
                "src_computer"
            ],

            record[
                "dst_computer"
            ],
        )

        for record
        in redteam_records
    ]

    match_counts = {}

    if redteam_for_spark:

        redteam_df = (
            spark.createDataFrame(
                redteam_for_spark,
                schema=redteam_schema
            )
        )

        auth_keys = (

            auth_df

            .select(
                "time",
                "src_user",
                "src_computer",
                "dst_computer"
            )

            .alias(
                "a"
            )
        )

        redteam_small = (

            F.broadcast(
                redteam_df.alias(
                    "r"
                )
            )
        )

        matches = (

            auth_keys

            .join(

                redteam_small,

                (
                    F.col(
                        "a.time"
                    )
                    ==
                    F.col(
                        "r.time"
                    )
                )

                &

                (
                    F.col(
                        "a.src_user"
                    )
                    ==
                    F.col(
                        "r.user"
                    )
                )

                &

                (
                    F.col(
                        "a.src_computer"
                    )
                    ==
                    F.col(
                        "r.src_computer"
                    )
                )

                &

                (
                    F.col(
                        "a.dst_computer"
                    )
                    ==
                    F.col(
                        "r.dst_computer"
                    )
                ),

                "inner"
            )

            .select(
                F.col(
                    "r.event_id"
                ).alias(
                    "event_id"
                )
            )

            .groupBy(
                "event_id"
            )

            .count()

            .collect()
        )

        match_counts = {

            int(
                row[
                    "event_id"
                ]
            ):
            int(
                row[
                    "count"
                ]
            )

            for row
            in matches
        }

    matched_events = 0

    redteam_output_rows = []

    for record in redteam_records:

        match_count = (
            match_counts.get(
                record[
                    "event_id"
                ],
                0
            )
        )

        preserved = (
            match_count > 0
        )

        if preserved:

            matched_events += 1

        redteam_output_rows.append({

            "event_id":
                record[
                    "event_id"
                ],

            "source_row":
                record[
                    "source_row"
                ],

            "time":
                record[
                    "time"
                ],

            "user":
                record[
                    "user"
                ],

            "src_computer":
                record[
                    "src_computer"
                ],

            "dst_computer":
                record[
                    "dst_computer"
                ],

            "processed_match_count":
                match_count,

            "preserved":
                1
                if preserved
                else 0,
        })

    if redteam_total > 0:

        redteam_match_rate = (

            matched_events
            /
            redteam_total

        ) * 100

    else:

        redteam_match_rate = 0.0

    redteam_pass = (

        redteam_total > 0

        and

        matched_events
        == redteam_total
    )

    write_csv(

        REDTEAM_MATCH_OUTPUT,

        redteam_output_rows,

        [
            "event_id",
            "source_row",
            "time",
            "user",
            "src_computer",
            "dst_computer",
            "processed_match_count",
            "preserved",
        ]
    )

    print(
        f"\nRed-team events in "
        f"7-day window: "
        f"{redteam_total:,}"
    )

    print(
        f"Events preserved: "
        f"{matched_events:,}"
    )

    print(
        f"Preservation rate: "
        f"{redteam_match_rate:.2f}%"
    )

    # =====================================================
    # 10. OVERALL VALIDATION
    # =====================================================

    validation_checks = {

        "physical_output":
            (
                len(
                    missing_directories
                ) == 0

                and

                len(
                    missing_parquet_days
                ) == 0
            ),

        "schema":
            schema_pass,

        "day_coverage":
            day_coverage_pass,

        "row_integrity":
            row_integrity_pass,

        "critical_nulls":
            critical_null_pass,

        "binary_fields":
            binary_field_pass,

        "temporal_ranges":
            temporal_pass,

        "logical_consistency":
            logical_pass,

        "redteam_preservation":
            redteam_pass,
    }

    overall_pass = all(
        validation_checks.values()
    )

    # =====================================================
    # 11. SAVE JSON SUMMARY
    # =====================================================

    summary = {

        "phase":
            "3E Processed Authentication Validation",

        "validation_status":
            (
                "PASS"
                if overall_pass
                else "FAIL"
            ),

        "input_directory":
            str(
                input_dir
            ),

        "days_validated":
            args.days,

        "output_sizes_mb":
            day_sizes,

        "schema": {

            "expected":
                EXPECTED_SCHEMA,

            "actual":
                actual_schema,

            "missing_columns":
                missing_columns,

            "unexpected_columns":
                unexpected_columns,

            "type_mismatches":
                type_mismatches,

            "pass":
                schema_pass,
        },

        "row_integrity": {

            "staged_rows":
                input_staged_rows,

            "processed_rows":
                processed_rows,

            "removed_rows":
                removed_rows,

            "retention_rate_percent":
                (
                    round(
                        retention_rate,
                        6
                    )
                    if retention_rate
                    is not None
                    else None
                ),

            "processed_rows_per_day":
                processed_rows_per_day,

            "daily_retention":
                daily_retention,

            "pass":
                row_integrity_pass,
        },

        "ranges": {

            "minimum_time":
                global_stats[
                    "minimum_time"
                ],

            "maximum_time":
                global_stats[
                    "maximum_time"
                ],

            "minimum_day_index":
                global_stats[
                    "minimum_day_index"
                ],

            "maximum_day_index":
                global_stats[
                    "maximum_day_index"
                ],

            "minimum_second_of_day":
                global_stats[
                    "minimum_second_of_day"
                ],

            "maximum_second_of_day":
                global_stats[
                    "maximum_second_of_day"
                ],

            "minimum_hour":
                global_stats[
                    "minimum_hour"
                ],

            "maximum_hour":
                global_stats[
                    "maximum_hour"
                ],
        },

        "critical_null_counts":
            critical_null_counts,

        "invalid_binary_counts":
            invalid_binary_counts,

        "invalid_success_flag":

            int(
                global_stats[
                    "invalid_success_flag"
                ]
            ),

        "invalid_temporal_counts": {

            "day_index":

                int(
                    global_stats[
                        "invalid_day_index"
                    ]
                ),

            "second_of_day":

                int(
                    global_stats[
                        "invalid_second_of_day"
                    ]
                ),

            "hour":

                int(
                    global_stats[
                        "invalid_hour"
                    ]
                ),
        },

        "logical_consistency_counts":
            logical_counts,

        "day_coverage": {

            "expected_days":
                expected_days,

            "present_days":
                sorted(
                    present_days
                ),

            "pass":
                day_coverage_pass,
        },

        "redteam_preservation": {

            "events_in_window":
                redteam_total,

            "events_preserved":
                matched_events,

            "preservation_rate_percent":
                round(
                    redteam_match_rate,
                    6
                ),

            "pass":
                redteam_pass,

            "details_csv":
                str(
                    REDTEAM_MATCH_OUTPUT
                ),
        },

        "checks":
            validation_checks,
    }

    LOG_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        VALIDATION_SUMMARY,
        mode="w",
        encoding="utf-8"
    ) as file:

        json.dump(
            summary,
            file,
            indent=4
        )

    # =====================================================
    # 12. STOP SPARK
    # =====================================================

    spark.stop()

    # =====================================================
    # 13. FINAL REPORT
    # =====================================================

    print(
        "\n"
        + "=" * 78
    )

    print(
        "PHASE 3E VALIDATION RESULT"
    )

    print(
        "=" * 78
    )

    for (
        check_name,
        passed
    ) in validation_checks.items():

        print(
            f"{check_name:24s}: "
            f"{'PASS' if passed else 'FAIL'}"
        )

    print(
        "-" * 78
    )

    print(
        f"OVERALL VALIDATION        : "
        f"{'PASS' if overall_pass else 'FAIL'}"
    )

    print(
        f"\nValidation summary:\n"
        f"{VALIDATION_SUMMARY}"
    )

    print(
        f"\nRed-team match details:\n"
        f"{REDTEAM_MATCH_OUTPUT}"
    )

    print(
        "=" * 78
    )


# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":
    main()