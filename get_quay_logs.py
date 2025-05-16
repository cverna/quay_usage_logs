import requests
import os
import json
from datetime import datetime, timedelta

# --- Configuration ---
QUAY_API_BASE_URL = "https://quay.io/api/v1"
REPOSITORY_PATH = "fedora/fedora-bootc" # The repository you want to get logs from
LOGS_PAGE_SIZE = 100 # Desired number of log entries per page (try 100, API max may vary)

def get_quay_repository_logs(api_token, repository_path, start_time_str=None, end_time_str=None, page_size=100):
    """
    Fetches usage logs for a specific Quay.io repository.

    Args:
        api_token (str): Your Quay.io OAuth2 access token.
        repository_path (str): The full path of the repository (e.g., "namespace/reponame").
        start_time_str (str, optional): The start time for logs.
        end_time_str (str, optional): The end time for logs.
        page_size (int, optional): The desired number of logs per page.

    Returns:
        list: A list of log entries (dictionaries), or None if an error occurs.
    """
    logs_url = f"{QUAY_API_BASE_URL}/repository/{repository_path}/logs"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Accept": "application/json"
    }

    # Parameters for the initial request
    params = {"limit": page_size}
    if start_time_str:
        params["starttime"] = start_time_str
    if end_time_str:
        params["endtime"] = end_time_str

    all_logs = []
    page_count = 0

    print(f"Fetching logs for repository: {repository_path}")
    if params: # Will now include limit on first call
        print(f"Initial request parameters: {params}")

    while True:
        page_count += 1
        if page_count > 1: # For subsequent pages, params might be different
             print(f"Fetching page {page_count} with params: {params}...")
        else:
             print(f"Fetching page {page_count}...") # Initial request params already printed

        try:
            response = requests.get(logs_url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            if "logs" in data and data["logs"]:
                retrieved_count = len(data['logs'])
                all_logs.extend(data["logs"])
                print(f"  Retrieved {retrieved_count} log entries from this page.")
                # If the API returns fewer than requested, even if there's no next_page,
                # it might be the end or the API's own internal paging behavior.
            else:
                print("  No more log entries found on this page (or 'logs' key missing/empty).")
                break

            if "next_page" in data and data["next_page"]:
                # For subsequent requests, only use the next_page token.
                # The next_page token should preserve the context of the original query,
                # including any limit, starttime, and endtime filters.
                params = {"next_page": data["next_page"]}
            else:
                break
        
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            print(f"Response content: {response.text}")
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
        # ... (rest of the error message)
        exit(1)

    start_time = "30d"
    end_time = None
    
    time_range_str = f"_last_{start_time.replace('/', '_')}" if start_time else "" # Sanitize start_time for filename
    output_filename = f"quay_{REPOSITORY_PATH.replace('/', '_')}_logs{time_range_str}.json"

    print(f"Attempting to fetch logs for the period: starttime='{start_time}', endtime='{end_time}'. Page size: {LOGS_PAGE_SIZE}")
    logs = get_quay_repository_logs(quay_token, REPOSITORY_PATH, 
                                    start_time_str=start_time, 
                                    end_time_str=end_time, 
                                    page_size=LOGS_PAGE_SIZE) # Pass page_size

    if logs is not None:
        if logs:
            try:
                with open(output_filename, "w") as f:
                    json.dump(logs, f)
                print(f"\nSuccessfully saved {len(logs)} log entries to: {output_filename}")
            except IOError as e:
                print(f"\nError saving logs to file: {e}")
        else:
            print("No logs found for the specified criteria. No file written.")
    else:
        print("Failed to retrieve logs. No file written.")