import csv
import gzip
import json
from pathlib import Path

import pandas as pd


# =========================================================
# PROJECT PATHS
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "Data"
RAW_DIR = DATA_DIR / "raw"
SAMPLE_DIR = DATA_DIR / "samples"

LOG_DIR = PROJECT_ROOT / "Logs"

REDTEAM_FILE = RAW_DIR / "redteam.txt.gz"
AUTH_FILE = RAW_DIR / "auth.txt.gz"
PROC_FILE = RAW_DIR / "proc.txt.gz"

AUTH_MATCH_OUTPUT = SAMPLE_DIR / "redteam_auth_matches.csv"
PROC_CONTEXT_OUTPUT = SAMPLE_DIR / "redteam_proc_context.csv"

SUMMARY_OUTPUT = LOG_DIR / "cross_dataset_validation_summary.json"


# =========================================================
# SETTINGS
# =========================================================

# Only a small number is needed to validate cross-file relationships.
MAX_REDTEAM_EVENTS = 10

# Process context window:
# 5 minutes before/after each known red-team authentication.
PROC_WINDOW_SECONDS = 300


# =========================================================
# LOAD RED-TEAM EVENTS
# =========================================================

def load_redteam_events():

    events = []

    with gzip.open(
        REDTEAM_FILE,
        mode="rt",
        encoding="utf-8",
        newline=""
    ) as file:

        reader = csv.reader(file)

        for row in reader:

            if len(row) != 4:
                continue

            try:
                time_value = int(row[0])
            except ValueError:
                continue

            events.append({
                "redteam_id": len(events) + 1,
                "time": time_value,
                "user": row[1],
                "src_computer": row[2],
                "dst_computer": row[3]
            })

            if len(events) >= MAX_REDTEAM_EVENTS:
                break

    return events


# =========================================================
# FIND MATCHING AUTHENTICATION EVENTS
# =========================================================

def find_auth_matches(redteam_events):

    print("\n" + "=" * 75)
    print("VALIDATING RED-TEAM EVENTS AGAINST AUTHENTICATION DATA")
    print("=" * 75)

    target_lookup = {}

    for event in redteam_events:

        key = (
            event["time"],
            event["user"],
            event["src_computer"],
            event["dst_computer"]
        )

        target_lookup[key] = event

    maximum_target_time = max(
        event["time"]
        for event in redteam_events
    )

    matches = []
    rows_scanned = 0

    with gzip.open(
        AUTH_FILE,
        mode="rt",
        encoding="utf-8",
        newline=""
    ) as file:

        reader = csv.reader(file)

        for row in reader:

            if len(row) != 9:
                continue

            try:
                time_value = int(row[0])
            except ValueError:
                continue

            rows_scanned += 1

            # LANL logs are chronological.
            # Stop once the first 10 red-team timestamps have passed.
            if time_value > maximum_target_time:
                break

            key = (
                time_value,
                row[1],
                row[3],
                row[4]
            )

            if key not in target_lookup:
                continue

            redteam_event = target_lookup[key]

            matches.append({
                "redteam_id": redteam_event["redteam_id"],
                "redteam_time": redteam_event["time"],
                "redteam_user": redteam_event["user"],

                "src_user": row[1],
                "dst_user": row[2],

                "src_computer": row[3],
                "dst_computer": row[4],

                "auth_type": row[5],
                "logon_type": row[6],
                "auth_orientation": row[7],
                "success": row[8]
            })

    print(f"Authentication rows scanned : {rows_scanned:,}")
    print(f"Red-team events tested       : {len(redteam_events):,}")
    print(f"Exact authentication matches : {len(matches):,}")

    return matches, rows_scanned


# =========================================================
# FIND PROCESS CONTEXT
# =========================================================

def find_process_context(redteam_events):

    print("\n" + "=" * 75)
    print("SEARCHING PROCESS CONTEXT AROUND RED-TEAM EVENTS")
    print("=" * 75)

    minimum_time = min(
        event["time"]
        for event in redteam_events
    ) - PROC_WINDOW_SECONDS

    maximum_time = max(
        event["time"]
        for event in redteam_events
    ) + PROC_WINDOW_SECONDS

    context_rows = []
    rows_scanned = 0

    with gzip.open(
        PROC_FILE,
        mode="rt",
        encoding="utf-8",
        newline=""
    ) as file:

        reader = csv.reader(file)

        for row in reader:

            if len(row) != 5:
                continue

            try:
                process_time = int(row[0])
            except ValueError:
                continue

            rows_scanned += 1

            if process_time < minimum_time:
                continue

            if process_time > maximum_time:
                break

            process_user = row[1]
            process_computer = row[2]

            for event in redteam_events:

                time_difference = abs(
                    process_time - event["time"]
                )

                if time_difference > PROC_WINDOW_SECONDS:
                    continue

                target_hosts = {
                    event["src_computer"],
                    event["dst_computer"]
                }

                if process_computer not in target_hosts:
                    continue

                if process_computer == event["src_computer"]:
                    host_role = "source_host"
                else:
                    host_role = "destination_host"

                same_user = (
                    process_user == event["user"]
                )

                context_rows.append({
                    "redteam_id": event["redteam_id"],
                    "redteam_time": event["time"],
                    "redteam_user": event["user"],

                    "redteam_src_computer":
                        event["src_computer"],

                    "redteam_dst_computer":
                        event["dst_computer"],

                    "process_time": process_time,

                    "time_difference_seconds":
                        time_difference,

                    "process_user": process_user,

                    "computer": process_computer,

                    "host_role": host_role,

                    "same_user_as_redteam":
                        same_user,

                    "process": row[3],

                    "event_type": row[4]
                })

    print(f"Process rows scanned       : {rows_scanned:,}")
    print(f"Relevant process events    : {len(context_rows):,}")

    return context_rows, rows_scanned


# =========================================================
# MAIN
# =========================================================

def main():

    print("=" * 75)
    print("LANL CROSS-DATASET VALIDATION")
    print("=" * 75)

    print(f"\nRed-team file : {REDTEAM_FILE}")
    print(f"Auth file     : {AUTH_FILE}")
    print(f"Process file  : {PROC_FILE}")

    # -----------------------------------------------------
    # CHECK REQUIRED FILES
    # -----------------------------------------------------

    for file_path in [
        REDTEAM_FILE,
        AUTH_FILE,
        PROC_FILE
    ]:

        if not file_path.exists():

            raise FileNotFoundError(
                f"Required file not found:\n{file_path}"
            )

    print("\n[OK] All three LANL files found.")

    # -----------------------------------------------------
    # LOAD RED-TEAM EVENTS
    # -----------------------------------------------------

    redteam_events = load_redteam_events()

    print(
        f"\nLoaded {len(redteam_events)} "
        f"red-team events for validation."
    )

    for event in redteam_events:

        print(
            f"#{event['redteam_id']:02d} | "
            f"time={event['time']} | "
            f"user={event['user']} | "
            f"{event['src_computer']} -> "
            f"{event['dst_computer']}"
        )

    # -----------------------------------------------------
    # AUTHENTICATION VALIDATION
    # -----------------------------------------------------

    auth_matches, auth_rows_scanned = (
        find_auth_matches(redteam_events)
    )

    # -----------------------------------------------------
    # PROCESS CONTEXT
    # -----------------------------------------------------

    proc_context, proc_rows_scanned = (
        find_process_context(redteam_events)
    )

    # -----------------------------------------------------
    # CREATE OUTPUT DIRECTORIES
    # -----------------------------------------------------

    SAMPLE_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    LOG_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    # -----------------------------------------------------
    # SAVE AUTH MATCHES
    # -----------------------------------------------------

    auth_df = pd.DataFrame(auth_matches)

    auth_df.to_csv(
        AUTH_MATCH_OUTPUT,
        index=False
    )

    # -----------------------------------------------------
    # SAVE PROCESS CONTEXT
    # -----------------------------------------------------

    proc_df = pd.DataFrame(proc_context)

    proc_df.to_csv(
        PROC_CONTEXT_OUTPUT,
        index=False
    )

    # -----------------------------------------------------
    # CALCULATE SUMMARY
    # -----------------------------------------------------

    matched_redteam_ids = set(
        match["redteam_id"]
        for match in auth_matches
    )

    unmatched_ids = [
        event["redteam_id"]
        for event in redteam_events
        if event["redteam_id"]
        not in matched_redteam_ids
    ]

    exact_match_rate = (
        len(matched_redteam_ids)
        / len(redteam_events)
        * 100
        if redteam_events
        else 0
    )

    same_user_process_context = sum(
        1
        for row in proc_context
        if row["same_user_as_redteam"]
    )

    summary = {

        "phase":
            "2D Cross-Dataset Validation",

        "redteam_events_tested":
            len(redteam_events),

        "redteam_events_with_exact_auth_match":
            len(matched_redteam_ids),

        "exact_auth_match_rate_percent":
            round(exact_match_rate, 2),

        "unmatched_redteam_ids":
            unmatched_ids,

        "auth_rows_scanned":
            auth_rows_scanned,

        "process_rows_scanned":
            proc_rows_scanned,

        "process_context_events_found":
            len(proc_context),

        "same_user_process_context_events":
            same_user_process_context,

        "process_context_window_seconds":
            PROC_WINDOW_SECONDS
    }

    # -----------------------------------------------------
    # SAVE JSON SUMMARY
    # -----------------------------------------------------

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
    # DISPLAY FINAL SUMMARY
    # -----------------------------------------------------

    print("\n" + "=" * 75)
    print("CROSS-DATASET VALIDATION SUMMARY")
    print("=" * 75)

    print(
        f"Red-team events tested       : "
        f"{len(redteam_events)}"
    )

    print(
        f"Events with exact auth match : "
        f"{len(matched_redteam_ids)}"
    )

    print(
        f"Exact authentication match   : "
        f"{exact_match_rate:.2f}%"
    )

    print(
        f"Process context events found  : "
        f"{len(proc_context)}"
    )

    print(
        f"Same-user process events      : "
        f"{same_user_process_context}"
    )

    print("\nOutputs created:")

    print(
        f"\nAuthentication matches:\n"
        f"{AUTH_MATCH_OUTPUT}"
    )

    print(
        f"\nProcess context:\n"
        f"{PROC_CONTEXT_OUTPUT}"
    )

    print(
        f"\nValidation summary:\n"
        f"{SUMMARY_OUTPUT}"
    )

    print("\n" + "=" * 75)
    print("CROSS-DATASET VALIDATION COMPLETED")
    print("=" * 75)


if __name__ == "__main__":
    main()