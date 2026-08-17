import argparse
import csv
import gzip
import json
import shutil
from pathlib import Path


# =========================================================
# PROJECT PATHS
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "Data"
RAW_DIR = DATA_DIR / "raw"

PROCESSED_DIR = DATA_DIR / "processed"
STAGING_DIR = PROCESSED_DIR / "staging" / "auth_daily"

LOG_DIR = PROJECT_ROOT / "Logs"

AUTH_FILE = RAW_DIR / "auth.txt.gz"

SUMMARY_OUTPUT = LOG_DIR / "auth_daily_staging_summary.json"


# =========================================================
# CONSTANTS
# =========================================================

SECONDS_PER_DAY = 86400
EXPECTED_COLUMNS = 9


# =========================================================
# ARGUMENT PARSING
# =========================================================

def parse_arguments():

    parser = argparse.ArgumentParser(
        description=(
            "Split the chronological LANL authentication "
            "gzip file into smaller daily gzip files."
        )
    )

    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of LANL days to prepare. Default: 7"
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help=(
            "Delete existing authentication staging files "
            "before recreating them."
        )
    )

    return parser.parse_args()


# =========================================================
# MAIN
# =========================================================

def main():

    args = parse_arguments()

    number_of_days = args.days

    if number_of_days <= 0:
        raise ValueError("--days must be greater than zero.")

    cutoff_time = number_of_days * SECONDS_PER_DAY

    print("=" * 75)
    print("LANL AUTHENTICATION DAILY STAGING")
    print("=" * 75)

    print(f"\nProject root : {PROJECT_ROOT}")
    print(f"Source file  : {AUTH_FILE}")
    print(f"Output dir   : {STAGING_DIR}")
    print(f"Days         : {number_of_days}")
    print(f"Time cutoff  : {cutoff_time:,} seconds")

    # -----------------------------------------------------
    # 1. CHECK INPUT FILE
    # -----------------------------------------------------

    if not AUTH_FILE.exists():

        raise FileNotFoundError(
            f"\nAuthentication dataset not found:\n{AUTH_FILE}"
        )

    print("\n[OK] Authentication dataset found.")

    # -----------------------------------------------------
    # 2. PREPARE OUTPUT DIRECTORY
    # -----------------------------------------------------

    if STAGING_DIR.exists():

        existing_files = list(
            STAGING_DIR.glob("*.csv.gz")
        )

        if existing_files:

            if args.overwrite:

                print(
                    "\nExisting staging data found. "
                    "Removing because --overwrite was used."
                )

                shutil.rmtree(STAGING_DIR)

            else:

                raise RuntimeError(
                    "\nStaging directory already contains files.\n"
                    "Run again with --overwrite if you want "
                    "to recreate them."
                )

    STAGING_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    LOG_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    # -----------------------------------------------------
    # 3. TRACKING VARIABLES
    # -----------------------------------------------------

    output_handles = {}
    csv_writers = {}

    rows_per_day = {
        day: 0
        for day in range(number_of_days)
    }

    total_valid_rows = 0
    malformed_rows = 0

    minimum_timestamp = None
    maximum_timestamp = None

    # -----------------------------------------------------
    # 4. CREATE DAILY WRITER ONLY WHEN NEEDED
    # -----------------------------------------------------

    def get_writer(day_index):

        if day_index not in csv_writers:

            output_path = (
                STAGING_DIR
                / f"auth_day_{day_index:02d}.csv.gz"
            )

            handle = gzip.open(
                output_path,
                mode="wt",
                encoding="utf-8",
                newline="",
                compresslevel=5
            )

            output_handles[day_index] = handle
            csv_writers[day_index] = csv.writer(handle)

            print(
                f"[OPEN] Day {day_index:02d} -> "
                f"{output_path.name}"
            )

        return csv_writers[day_index]

    # -----------------------------------------------------
    # 5. STREAM THROUGH ORIGINAL AUTH FILE
    # -----------------------------------------------------

    print("\nReading raw authentication stream...")

    try:

        with gzip.open(
            AUTH_FILE,
            mode="rt",
            encoding="utf-8",
            newline=""
        ) as input_file:

            reader = csv.reader(input_file)

            for row in reader:

                # -----------------------------------------
                # CHECK COLUMN COUNT
                # -----------------------------------------

                if len(row) != EXPECTED_COLUMNS:

                    malformed_rows += 1
                    continue

                # -----------------------------------------
                # PARSE TIMESTAMP
                # -----------------------------------------

                try:

                    event_time = int(row[0])

                except ValueError:

                    malformed_rows += 1
                    continue

                # -----------------------------------------
                # STOP AFTER REQUESTED NUMBER OF DAYS
                # -----------------------------------------

                if event_time >= cutoff_time:
                    break

                # -----------------------------------------
                # CALCULATE DAY INDEX
                # -----------------------------------------

                day_index = (
                    event_time // SECONDS_PER_DAY
                )

                if day_index >= number_of_days:
                    break

                # -----------------------------------------
                # WRITE ORIGINAL LANL ROW UNCHANGED
                # -----------------------------------------

                writer = get_writer(day_index)

                writer.writerow(row)

                rows_per_day[day_index] += 1
                total_valid_rows += 1

                # -----------------------------------------
                # TRACK TIMESTAMP RANGE
                # -----------------------------------------

                if minimum_timestamp is None:
                    minimum_timestamp = event_time

                maximum_timestamp = event_time

                # -----------------------------------------
                # PROGRESS LOG
                # -----------------------------------------

                if (
                    total_valid_rows % 5_000_000
                    == 0
                ):

                    print(
                        f"[PROGRESS] "
                        f"{total_valid_rows:,} rows staged..."
                    )

    finally:

        for handle in output_handles.values():
            handle.close()

    # -----------------------------------------------------
    # 6. PRINT SUMMARY
    # -----------------------------------------------------

    print("\n" + "=" * 75)
    print("STAGING SUMMARY")
    print("=" * 75)

    print(
        f"Total valid rows : "
        f"{total_valid_rows:,}"
    )

    print(
        f"Malformed rows   : "
        f"{malformed_rows:,}"
    )

    print(
        f"Minimum timestamp: "
        f"{minimum_timestamp}"
    )

    print(
        f"Maximum timestamp: "
        f"{maximum_timestamp}"
    )

    print("\nROWS PER DAY")

    for day, count in rows_per_day.items():

        print(
            f"Day {day:02d}: "
            f"{count:,} rows"
        )

    # -----------------------------------------------------
    # 7. SAVE SUMMARY JSON
    # -----------------------------------------------------

    summary = {

        "source_dataset":
            "LANL auth.txt.gz",

        "number_of_days":
            number_of_days,

        "cutoff_seconds":
            cutoff_time,

        "total_valid_rows":
            total_valid_rows,

        "malformed_rows":
            malformed_rows,

        "minimum_timestamp":
            minimum_timestamp,

        "maximum_timestamp":
            maximum_timestamp,

        "rows_per_day": {
            str(day): count
            for day, count in rows_per_day.items()
        },

        "output_directory":
            str(STAGING_DIR)
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

    print(
        f"\n[OK] Summary saved to:\n"
        f"{SUMMARY_OUTPUT}"
    )

    # -----------------------------------------------------
    # 8. DISPLAY GENERATED FILES
    # -----------------------------------------------------

    print("\nGenerated files:")

    generated_files = sorted(
        STAGING_DIR.glob("*.csv.gz")
    )

    for file_path in generated_files:

        size_mb = (
            file_path.stat().st_size
            / (1024 * 1024)
        )

        print(
            f"  {file_path.name}"
            f"  ({size_mb:.2f} MB)"
        )

    print("\n" + "=" * 75)
    print("AUTHENTICATION DAILY STAGING COMPLETED")
    print("=" * 75)


if __name__ == "__main__":
    main()