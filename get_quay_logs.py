import requests
import os
import json
import csv
import sqlite3
import argparse
from datetime import datetime, timedelta, timezone

# --- Configuration ---
QUAY_API_BASE_URL = "https://quay.io/api/v1"
REPOSITORIES = [
    "fedora/fedora-bootc",
    "fedora/fedora-coreos",
]  # List of repositories to fetch logs from
DATABASE_PATH = "quay_logs.db"


def init_database():
    """Create database tables and indexes if they don't exist."""
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()

        # Create main logs table
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS quay_logs (
                timestamp INTEGER NOT NULL,
                repo TEXT NOT NULL,
                datetime_str TEXT,
                kind TEXT,
                namespace TEXT,
                manifest_digest TEXT,
                tag TEXT,
                provider TEXT,
                service TEXT,
                country_code TEXT,
                continent TEXT,
                aws_region TEXT,
                PRIMARY KEY (timestamp, repo)
            )
        """
        )

        # Create indexes
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_quay_logs_timestamp
            ON quay_logs(timestamp)
        """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_quay_logs_tag
            ON quay_logs(repo, tag)
        """
        )

        conn.commit()


def insert_logs_to_database(flattened_logs, repo_name):
    """
    Insert log entries into the database.
    Returns tuple of (inserted_count, duplicate_count)
    """
    inserted_count = 0
    duplicate_count = 0

    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()

        for log in flattened_logs:
            try:
                cursor.execute(
                    """
                    INSERT INTO quay_logs (
                        timestamp, repo, datetime_str, kind, namespace,
                        manifest_digest, tag, provider, service, country_code, continent,
                        aws_region
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        log.get("timestamp", ""),
                        log.get("repo", ""),
                        log.get("datetime", ""),
                        log.get("kind", ""),
                        log.get("namespace", ""),
                        log.get("manifest_digest", ""),
                        log.get("tag", ""),
                        log.get("provider", ""),
                        log.get("service", ""),
                        log.get("country_code", ""),
                        log.get("continent", ""),
                        log.get("aws_region", ""),
                    ),
                )
                inserted_count += 1
            except sqlite3.IntegrityError:
                # Primary key violation - duplicate entry
                duplicate_count += 1

        conn.commit()

    return inserted_count, duplicate_count


def export_database_to_csv(csv_filename="quay_logs.csv"):
    """
    Export all data from the SQLite database to a CSV file.

    Args:
        csv_filename (str): Filename for the CSV. Defaults to "quay_logs.csv".

    Returns:
        str: The filename of the created CSV file, or None if no data found.
    """
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()

        # Get all data from the database
        cursor.execute("SELECT * FROM quay_logs ORDER BY timestamp DESC, repo")
        rows = cursor.fetchall()

        if not rows:
            print("No data found in database to export.")
            return None

        # Get column names
        cursor.execute("PRAGMA table_info(quay_logs)")
        columns = [column[1] for column in cursor.fetchall()]

        # Use the provided filename (defaults to "quay_logs.csv")

        # Write to CSV
        with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)  # Header
            writer.writerows(rows)  # Data

        print(f"Database exported to CSV: {csv_filename}")
        print(f"  Total records exported: {len(rows)}")

        # Show summary by repository
        repo_counts = {}
        for row in rows:
            repo_col_index = columns.index("repo")
            repo = row[repo_col_index]
            repo_counts[repo] = repo_counts.get(repo, 0) + 1

        for repo, count in sorted(repo_counts.items()):
            print(f"  {repo}: {count} records")

        return csv_filename


def flatten_log_entry(entry):
    """
    Flatten a single log entry into a dictionary suitable for CSV.

    Args:
        entry (dict): Single log entry from the JSON response

    Returns:
        dict: Flattened dictionary with all relevant fields
    """
    flattened = {}

    # Top-level fields
    flattened["kind"] = entry.get("kind", "")
    flattened["datetime"] = entry.get("datetime", "")

    # Parse datetime to extract timestamp
    if flattened["datetime"]:
        try:
            # Parse the datetime string: "Thu, 04 Dec 2025 11:54:54 -0000"
            dt = datetime.strptime(flattened["datetime"], "%a, %d %b %Y %H:%M:%S %z")
            flattened["timestamp"] = int(dt.timestamp())
        except (ValueError, TypeError):
            flattened["timestamp"] = ""

    # Metadata fields
    metadata = entry.get("metadata", {})
    flattened["repo"] = metadata.get("repo", "")
    flattened["namespace"] = metadata.get("namespace", "")
    flattened["manifest_digest"] = metadata.get("manifest_digest", "")
    flattened["tag"] = metadata.get("tag", "")

    # Resolved IP information
    resolved_ip = metadata.get("resolved_ip", {})
    flattened["provider"] = resolved_ip.get("provider", "")
    flattened["service"] = resolved_ip.get("service", "")
    flattened["country_code"] = resolved_ip.get("country_iso_code", "")
    flattened["continent"] = resolved_ip.get("continent", "")
    flattened["aws_region"] = resolved_ip.get("aws_region", "")

    return flattened


def get_quay_repository_logs(
    api_token, repository_path, start_time_str=None, end_time_str=None
):
    """
    Fetches usage logs for a specific Quay.io repository.
    Relies on the API's default page size.

    Args:
        api_token (str): Your Quay.io OAuth2 access token.
        repository_path (str): The full path of the repository (e.g., "namespace/reponame").
        start_time_str (str, optional): The start time for logs (e.g., "MM/DD/YYYY" in UTC).
        end_time_str (str, optional): The end time for logs (e.g., "MM/DD/YYYY" in UTC).

    Returns:
        list: A list of log entries (dictionaries), or None if an error occurs.
    """
    logs_url = f"{QUAY_API_BASE_URL}/repository/{repository_path}/logs"
    headers = {"Authorization": f"Bearer {api_token}", "Accept": "application/json"}

    params = {}
    if start_time_str:
        params["starttime"] = start_time_str
    if end_time_str:
        params["endtime"] = end_time_str  # Add endtime to params if provided

    all_logs = []
    page_count = 0

    print(f"Fetching logs for repository: {repository_path}")
    if params:
        print(f"Initial request parameters: {params}")
    else:
        # This case should ideally not happen if we always set start/end times
        print(
            "Initial request with no specific start/end time parameters (fetching recent logs based on API default)."
        )

    while True:
        page_count += 1
        # For logging, truncate long next_page tokens
        current_request_params_display = params.copy()
        if (
            "next_page" in current_request_params_display
            and len(current_request_params_display["next_page"]) > 20
        ):
            current_request_params_display["next_page"] = (
                current_request_params_display["next_page"][:10]
                + "..."
                + current_request_params_display["next_page"][-10:]
            )

        if page_count > 1:
            print(
                f"Fetching page {page_count} with params: {current_request_params_display}..."
            )
        else:
            print(f"Fetching page {page_count}...")

        try:
            response = requests.get(logs_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            if "logs" in data and data["logs"]:
                retrieved_count = len(data["logs"])
                all_logs.extend(data["logs"])
                print(f" starttime {data['start_time']}, endtime {data['end_time']}")
                # print(f"  Retrieved {retrieved_count} log entries from this page.")
            else:
                print(
                    "  No more log entries found on this page (or 'logs' key missing/empty)."
                )

            if "next_page" in data and data["next_page"]:
                # For subsequent requests, only use the next_page token.
                # The starttime and endtime context should be carried by the next_page token.
                params = {"next_page": data["next_page"]}
            else:
                print(f"{data}")
                print("No Next page found")
                break

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            print(f"Response content: {response.text}")
            if response.status_code == 401:
                print(
                    "Authentication error (401): Please ensure your QUAY_API_TOKEN is valid and has correct permissions."
                )
            elif response.status_code == 403:
                print(
                    "Authorization error (403): Your token may not have permission to access logs for this repository, or the specified range/format for time might be incorrect."
                )
            return None
        except requests.exceptions.RequestException as req_err:
            print(f"Request error occurred: {req_err}")
            return None
        except json.JSONDecodeError:
            print(f"Failed to decode JSON from response: {response.text}")
            return None

    print(f"\nTotal log entries retrieved: {len(all_logs)}")
    return all_logs


def parse_arguments():
    """Parse command line arguments for date range."""
    parser = argparse.ArgumentParser(description="Fetch Quay.io repository usage logs")
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date in YYYY-MM-DD format. If not provided, uses automatic logic based on existing data.",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date in YYYY-MM-DD format. If not provided, uses current date.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    # Parse command line arguments
    args = parse_arguments()

    # Initialize database
    print("Initializing database...")
    init_database()

    quay_token = os.environ.get("QUAY_API_TOKEN")

    if not quay_token:
        print("Error: QUAY_API_TOKEN environment variable not set.")
        print("Please set it to your Quay.io OAuth2 access token.")
        print("Example (Linux/macOS): export QUAY_API_TOKEN='your_token_here'")
        exit(1)

    print(f"=== Processing {len(REPOSITORIES)} repositories ===")

    # Process each repository
    for repo_index, repository_path in enumerate(REPOSITORIES, 1):
        print(
            f"\n[{repo_index}/{len(REPOSITORIES)}] Processing repository: {repository_path}"
        )

        # Determine date range based on command line arguments or existing data
        if args.start_date or args.end_date:
            # Use command line arguments
            if args.start_date:
                try:
                    start_datetime_utc = datetime.strptime(
                        args.start_date, "%Y-%m-%d"
                    ).replace(tzinfo=timezone.utc)
                    print(f"Using command line start date: {start_datetime_utc.date()}")
                except ValueError:
                    print(
                        f"Error: Invalid start date format '{args.start_date}'. Use YYYY-MM-DD format."
                    )
                    exit(1)
            else:
                # No start date provided, use automatic logic
                with sqlite3.connect(DATABASE_PATH) as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT MAX(timestamp) FROM quay_logs WHERE repo = ?",
                        (repository_path,),
                    )
                    result = cursor.fetchone()
                    last_timestamp = result[0] if result and result[0] else None

                if last_timestamp:
                    last_fetch_dt = datetime.fromtimestamp(
                        last_timestamp, tz=timezone.utc
                    )
                    start_datetime_utc = last_fetch_dt - timedelta(hours=1)
                    print(
                        f"Using last fetch timestamp as start: {start_datetime_utc.isoformat()}"
                    )
                else:
                    start_datetime_utc = datetime.now(timezone.utc) - timedelta(days=7)
                    print(
                        f"No existing data, using 7 days ago as start: {start_datetime_utc.date()}"
                    )

            if args.end_date:
                try:
                    end_datetime_utc = datetime.strptime(
                        args.end_date, "%Y-%m-%d"
                    ).replace(tzinfo=timezone.utc) + timedelta(days=1, microseconds=-1)
                    print(f"Using command line end date: {args.end_date} (end of day)")
                except ValueError:
                    print(
                        f"Error: Invalid end date format '{args.end_date}'. Use YYYY-MM-DD format."
                    )
                    exit(1)
            else:
                end_datetime_utc = datetime.now(timezone.utc)
                print(f"Using current time as end date: {end_datetime_utc.isoformat()}")
        else:
            # Use automatic logic based on existing data
            with sqlite3.connect(DATABASE_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT MAX(timestamp) FROM quay_logs WHERE repo = ?",
                    (repository_path,),
                )
                result = cursor.fetchone()
                last_timestamp = result[0] if result and result[0] else None

            if last_timestamp:
                # Convert timestamp to datetime for calculation
                last_fetch_dt = datetime.fromtimestamp(last_timestamp, tz=timezone.utc)
                print(f"Found existing data for {repository_path}")
                print(f"Latest data timestamp: {last_fetch_dt.isoformat()}")

                # Fetch logs from the last known timestamp to now
                # Add a small buffer to avoid missing logs
                start_datetime_utc = last_fetch_dt - timedelta(hours=1)
                end_datetime_utc = datetime.now(timezone.utc)
            else:
                print(f"No existing data found for {repository_path}")
                # First time fetch - get last 7 days
                days_to_fetch_for_start = 7
                end_datetime_utc = datetime.now(timezone.utc)
                start_datetime_utc = end_datetime_utc - timedelta(
                    days=days_to_fetch_for_start
                )

        # Format times for API
        start_time_param_for_api = start_datetime_utc.strftime("%m/%d/%Y")
        end_time_param_for_api = end_datetime_utc.strftime("%m/%d/%Y")

        print(f"--- Log Fetch Configuration ---")
        print(f"Repository: {repository_path}")
        print(f"Database: {DATABASE_PATH}")
        print(
            f"Fetch range: {start_datetime_utc.isoformat()} to {end_datetime_utc.isoformat()}"
        )
        print(
            f"API parameters: starttime={start_time_param_for_api}, endtime={end_time_param_for_api}"
        )
        print(f"-----------------------------")

        # --- Fetch Logs ---
        logs = get_quay_repository_logs(
            quay_token,
            repository_path,
            start_time_str=start_time_param_for_api,
            end_time_str=end_time_param_for_api,
        )

        if logs is not None:
            if logs:
                # Flatten all log entries
                print("Processing log entries...")
                flattened_logs = [flatten_log_entry(log) for log in logs]

                # Insert into database
                inserted_count, duplicate_count = insert_logs_to_database(
                    flattened_logs, repository_path
                )

                print(f"\nDatabase update complete for {repository_path}:")
                print(f"  New records inserted: {inserted_count}")
                print(f"  Duplicates skipped: {duplicate_count}")
                print(f"  Total records processed: {len(logs)}")

            else:
                print(
                    f"No new log entries found for {repository_path} in the specified time range."
                )
        else:
            print(f"Failed to retrieve logs for {repository_path} due to an error.")

    print(f"\n=== Completed processing all {len(REPOSITORIES)} repositories ===")

    # Export all data to CSV
    print("\n=== Exporting database to CSV ===")
    export_database_to_csv()
