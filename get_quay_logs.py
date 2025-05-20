import requests
import os
import json
from datetime import datetime, timedelta, timezone

# --- Configuration ---
QUAY_API_BASE_URL = "https://quay.io/api/v1"
REPOSITORY_PATH = "fedora/fedora-bootc" # The repository you want to get logs from

def get_quay_repository_logs(api_token, repository_path, start_time_str=None, end_time_str=None):
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
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Accept": "application/json"
    }

    params = {}
    if start_time_str:
        params["starttime"] = start_time_str
    if end_time_str:
        params["endtime"] = end_time_str # Add endtime to params if provided

    all_logs = []
    page_count = 0

    print(f"Fetching logs for repository: {repository_path}")
    if params:
        print(f"Initial request parameters: {params}")
    else:
        # This case should ideally not happen if we always set start/end times
        print("Initial request with no specific start/end time parameters (fetching recent logs based on API default).")


    while True:
        page_count += 1
        # For logging, truncate long next_page tokens
        current_request_params_display = params.copy()
        if "next_page" in current_request_params_display and len(current_request_params_display["next_page"]) > 20:
            current_request_params_display["next_page"] = current_request_params_display["next_page"][:10] + "..." + current_request_params_display["next_page"][-10:]

        if page_count > 1:
             print(f"Fetching page {page_count} with params: {current_request_params_display}...")
        else:
             print(f"Fetching page {page_count}...")

        try:
            response = requests.get(logs_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()


            if "logs" in data and data["logs"]:
                retrieved_count = len(data['logs'])
                all_logs.extend(data["logs"])
                print(f" starttime {data['start_time']}, endtime {data['end_time']}")
                # print(f"  Retrieved {retrieved_count} log entries from this page.")
            else:
                print("  No more log entries found on this page (or 'logs' key missing/empty).")

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
                print("Authentication error (401): Please ensure your QUAY_API_TOKEN is valid and has correct permissions.")
            elif response.status_code == 403:
                print("Authorization error (403): Your token may not have permission to access logs for this repository, or the specified range/format for time might be incorrect.")
            return None
        except requests.exceptions.RequestException as req_err:
            print(f"Request error occurred: {req_err}")
            return None
        except json.JSONDecodeError:
            print(f"Failed to decode JSON from response: {response.text}")
            return None

    print(f"\nTotal log entries retrieved: {len(all_logs)}")
    return all_logs

if __name__ == "__main__":
    quay_token = os.environ.get("QUAY_API_TOKEN")

    if not quay_token:
        print("Error: QUAY_API_TOKEN environment variable not set.")
        print("Please set it to your Quay.io OAuth2 access token.")
        print("Example (Linux/macOS): export QUAY_API_TOKEN='your_token_here'")
        exit(1)

    # --- Define Time Range: Calculate 30 days ago and current date, format as MM/DD/YYYY in UTC ---
    days_to_fetch_for_start = 5 # This defines the start date relative to the end date

    # End time will be the current UTC date
    end_datetime_utc = datetime.now(timezone.utc)
    end_time_param_for_api = end_datetime_utc.strftime('%m/%d/%Y')
    
    # Start time will be 'days_to_fetch_for_start' days before the end_datetime_utc
    start_datetime_utc = end_datetime_utc - timedelta(days=days_to_fetch_for_start)
    start_time_param_for_api = start_datetime_utc.strftime('%m/%d/%Y')


    print(f"--- Log Fetch Configuration ---")
    print(f"Requesting logs for an approximate {days_to_fetch_for_start}-day window.")
    print(f"Calculated start datetime (UTC): {start_datetime_utc.isoformat()}")
    print(f"Using starttime API parameter (MM/DD/YYYY format): \"{start_time_param_for_api}\"")
    print(f"Calculated end datetime (UTC): {end_datetime_utc.isoformat()}")
    print(f"Using endtime API parameter (MM/DD/YYYY format): \"{end_time_param_for_api}\"")
    print(f"Relying on API's default page size.")
    print(f"-----------------------------")

    # --- Define Output Filename ---
    # Filename still reflects the intent of fetching approximately 30 days
    time_range_str = f"_from_{start_datetime_utc.strftime('%Y%m%d')}_to_{end_datetime_utc.strftime('%Y%m%d')}"
    output_filename = f"quay_{REPOSITORY_PATH.replace('/', '_')}_logs{time_range_str}.json"

    # --- Fetch Logs ---
    logs = get_quay_repository_logs(quay_token, REPOSITORY_PATH, 
                                    start_time_str=start_time_param_for_api,
                                    end_time_str=end_time_param_for_api)

    if logs is not None:
        if logs:
            try:
                with open(output_filename, "w") as f:
                    json.dump(logs, f)
                print(f"\nSuccessfully saved {len(logs)} log entries to: {output_filename}")
            except IOError as e:
                print(f"\nError saving logs to file: {e}")
        else:
            print("No log entries were returned by the API for the specified criteria. An empty file will not be written.")
    else:
        print("Failed to retrieve logs due to an error. No file written.")