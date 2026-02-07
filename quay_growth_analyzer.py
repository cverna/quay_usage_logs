#!/usr/bin/env python3
"""
Comprehensive Quay.io Growth Analyzer
- Fetches aggregated logs for fedora-bootc and fedora-coreos
- Stores data in CSV format
- Creates monthly growth charts for each repository
"""

import requests
import os
import json
import csv
import argparse
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

# --- Configuration ---
QUAY_API_BASE_URL = "https://quay.io/api/v1"
REPOSITORIES = [
    "fedora/fedora-bootc",
    "fedora/fedora-coreos",
]
CSV_FILENAME = "quay_growth_data.csv"
SUMMARY_FILENAME = "monthly_growth_summary.json"


def get_quay_repository_aggregated_logs(api_token, repository_path, start_time_str, end_time_str):
    """
    Fetches aggregated usage logs for a specific Quay.io repository.

    Args:
        api_token (str): Quay.io OAuth2 access token
        repository_path (str): Repository path (e.g., "fedora/fedora-coreos")
        start_time_str (str): Start time in MM/DD/YYYY format
        end_time_str (str): End time in MM/DD/YYYY format

    Returns:
        list: Aggregated log entries or None if error
    """
    aggregatelogs_url = f"{QUAY_API_BASE_URL}/repository/{repository_path}/aggregatelogs"
    headers = {"Authorization": f"Bearer {api_token}", "Accept": "application/json"}

    params = {
        "starttime": start_time_str,
        "endtime": end_time_str
    }

    print(f"📡 Fetching aggregated logs for {repository_path}")
    print(f"    Time range: {start_time_str} to {end_time_str}")

    try:
        response = requests.get(aggregatelogs_url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        if "aggregated" in data:
            aggregated_logs = data["aggregated"]
            print(f"    ✅ Retrieved {len(aggregated_logs)} aggregated entries")

            # Process the aggregated logs to add repo info and parse dates
            for log in aggregated_logs:
                log["repo"] = repository_path
                # Parse datetime to extract date
                if "datetime" in log:
                    try:
                        dt = datetime.strptime(log["datetime"], "%a, %d %b %Y %H:%M:%S %z")
                        log["date"] = dt.strftime("%Y-%m-%d")
                    except (ValueError, TypeError):
                        log["date"] = ""

            return aggregated_logs
        else:
            print(f"    ❌ No 'aggregated' key found in response")
            return []

    except requests.exceptions.HTTPError as http_err:
        print(f"    ❌ HTTP error: {http_err}")
        if response.status_code == 404:
            print("    The aggregatelogs endpoint may not be available for this repository")
        return None
    except requests.exceptions.RequestException as req_err:
        print(f"    ❌ Request error: {req_err}")
        return None
    except json.JSONDecodeError:
        print(f"    ❌ Failed to decode JSON response")
        return None


def fetch_all_repositories(api_token, start_time_str, end_time_str):
    """Fetch aggregated logs for all repositories"""
    print("🚀 Starting aggregated log collection...")
    print(f"📊 Repositories: {', '.join(REPOSITORIES)}")
    print(f"📅 Date range: {start_time_str} to {end_time_str}")
    print("-" * 60)

    all_logs = []

    for repo_index, repository_path in enumerate(REPOSITORIES, 1):
        print(f"\n[{repo_index}/{len(REPOSITORIES)}] Processing {repository_path}")

        logs = get_quay_repository_aggregated_logs(
            api_token, repository_path, start_time_str, end_time_str
        )

        if logs is not None and logs:
            all_logs.extend(logs)

            # Show summary
            total_pulls = sum(log.get("count", 0) for log in logs if log.get("kind") == "pull_repo")
            pull_entries = [log for log in logs if log.get("kind") == "pull_repo"]

            print(f"    📈 Total pulls: {total_pulls:,}")
            print(f"    📊 Pull events: {len(pull_entries)}")

        elif logs is not None:
            print(f"    ⚠️  No logs found for this time range")
        else:
            print(f"    ❌ Failed to fetch logs")

    print(f"\n✅ Collection complete! Total entries: {len(all_logs)}")
    return all_logs


def load_existing_csv_data(filename=CSV_FILENAME):
    """Load existing CSV data for merging purposes"""
    if not os.path.exists(filename):
        print(f"📄 No existing CSV file found at {filename}")
        return []

    print(f"📖 Loading existing data from {filename}")
    existing_data = []

    try:
        with open(filename, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            existing_data = list(reader)

        print(f"✅ Loaded {len(existing_data)} existing records")
        return existing_data

    except Exception as e:
        print(f"⚠️  Error loading existing CSV: {e}")
        return []


def merge_and_save_csv(new_logs, filename=CSV_FILENAME):
    """Merge new logs with existing data and save to CSV file"""
    if not new_logs:
        print("❌ No new data to save")
        return False

    print(f"💾 Merging and saving data to {filename}")

    # Load existing data
    existing_data = load_existing_csv_data(filename)

    # Create a set of existing record keys for fast lookup
    # Key format: "date|repo|kind"
    existing_keys = set()
    for record in existing_data:
        key = f"{record.get('date', '')}|{record.get('repo', '')}|{record.get('kind', '')}"
        existing_keys.add(key)

    # Filter new logs to only include records not already in existing data
    new_records = []
    duplicate_count = 0

    for log in new_logs:
        key = f"{log.get('date', '')}|{log.get('repo', '')}|{log.get('kind', '')}"
        if key not in existing_keys:
            new_records.append(log)
        else:
            duplicate_count += 1

    print(f"📊 Found {len(new_records)} new records, {duplicate_count} duplicates skipped")

    if not new_records and existing_data:
        print("✅ No new data to add - all records already exist in CSV")
        return True

    # Combine existing and new data
    all_records = existing_data + new_records

    # Define column order
    columns = ['date', 'repo', 'kind', 'count', 'datetime_str']

    # Save combined data
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()

        # Write existing records
        for record in existing_data:
            writer.writerow(record)

        # Write new records
        for log in new_records:
            row = {
                'date': log.get('date', ''),
                'repo': log.get('repo', ''),
                'kind': log.get('kind', ''),
                'count': log.get('count', 0),
                'datetime_str': log.get('datetime', '')
            }
            writer.writerow(row)

    total_records = len(all_records)
    print(f"✅ Saved {total_records} total records ({len(existing_data)} existing + {len(new_records)} new) to {filename}")
    return True


def save_to_csv(logs, filename=CSV_FILENAME):
    """Save aggregated logs to CSV file (legacy function - now calls merge_and_save_csv)"""
    return merge_and_save_csv(logs, filename)


def load_and_prepare_data(filename=CSV_FILENAME):
    """Load CSV data and prepare for analysis"""
    print(f"📖 Loading data from {filename}")

    try:
        df = pd.read_csv(filename)
        df['date'] = pd.to_datetime(df['date'])

        # Filter to only pull events for growth analysis
        pull_data = df[df['kind'] == 'pull_repo'].copy()

        # Add month-year column for aggregation
        pull_data['month'] = pull_data['date'].dt.to_period('M')

        print(f"📊 Loaded {len(df)} total records")
        print(f"📈 Pull events: {len(pull_data)}")
        print(f"📅 Date range: {pull_data['date'].min().strftime('%Y-%m-%d')} to {pull_data['date'].max().strftime('%Y-%m-%d')}")

        return df, pull_data

    except FileNotFoundError:
        print(f"❌ File {filename} not found")
        return None, None
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return None, None


def create_monthly_growth_charts(pull_data):
    """Create monthly growth charts for each repository"""
    print("📊 Creating monthly growth charts...")

    # Aggregate by month and repository
    monthly_data = pull_data.groupby(['repo', 'month'])['count'].sum().reset_index()
    monthly_data['month_str'] = monthly_data['month'].astype(str)

    # Set up the plotting style
    sns.set_style("whitegrid")
    plt.style.use('default')

    # Define colors for repositories
    repo_colors = {
        'fedora/fedora-coreos': 'steelblue',
        'fedora/fedora-bootc': 'darkorange'
    }

    # Create separate chart for each repository
    for repo in sorted(pull_data['repo'].unique()):
        repo_data = monthly_data[monthly_data['repo'] == repo].copy()

        if repo_data.empty:
            print(f"⚠️  No data for {repo}")
            continue

        plt.figure(figsize=(12, 6))

        # Create line plot with markers
        plt.plot(repo_data['month_str'], repo_data['count'],
                marker='o', linewidth=3, markersize=8,
                color=repo_colors.get(repo, 'blue'),
                markerfacecolor='white', markeredgewidth=2)

        # Customize the plot
        repo_name = repo.replace('fedora/', '').title()
        plt.title(f'{repo_name} - Monthly Pull Growth', fontsize=16, fontweight='bold')
        plt.xlabel('Month', fontsize=12)
        plt.ylabel('Total Monthly Pulls', fontsize=12)

        # Add value labels on points
        for _, row in repo_data.iterrows():
            plt.annotate(f'{row["count"]:,}',
                        (row['month_str'], row['count']),
                        textcoords="offset points",
                        xytext=(0,10),
                        ha='center', fontweight='bold')

        # Format y-axis with commas
        ax = plt.gca()
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: format(int(x), ',')))

        # Rotate x-axis labels for better readability
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()

        # Save plot to file
        filename = f"growth_chart_{repo.replace('/', '_')}.png"
        plt.savefig(filename, dpi=150, bbox_inches='tight')
        print(f"📈 Generated growth chart for {repo} → {filename}")
        plt.close()


def load_existing_summary(filename=SUMMARY_FILENAME):
    """Load existing monthly summary JSON for merging"""
    if not os.path.exists(filename):
        print(f"📄 No existing summary file found at {filename}")
        return {}

    print(f"📖 Loading existing summary from {filename}")
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"✅ Loaded summary with {len(data.get('repositories', {}))} repositories")
        return data
    except Exception as e:
        print(f"⚠️  Error loading existing summary: {e}")
        return {}


def save_monthly_summary(pull_data, filename=SUMMARY_FILENAME):
    """Generate and save monthly summary JSON file for git tracking"""
    print(f"\n💾 Generating monthly summary to {filename}")

    # Load existing summary to merge with
    existing_summary = load_existing_summary(filename)
    existing_repos = existing_summary.get('repositories', {})

    # Aggregate by month and repository
    monthly_data = pull_data.groupby(['repo', 'month'])['count'].sum().reset_index()

    # Build summary structure
    repositories = {}

    for repo in sorted(pull_data['repo'].unique()):
        repo_data = monthly_data[monthly_data['repo'] == repo].sort_values('month')

        # Start with existing data for this repo
        monthly_pulls = existing_repos.get(repo, {}).get('monthly_pulls', {})

        # Add/update with new data
        for _, row in repo_data.iterrows():
            month_str = str(row['month'])
            monthly_pulls[month_str] = int(row['count'])

        # Sort by month
        monthly_pulls = dict(sorted(monthly_pulls.items()))

        # Calculate statistics
        pulls_list = list(monthly_pulls.values())
        total_pulls = sum(pulls_list)

        repositories[repo] = {
            'monthly_pulls': monthly_pulls,
            'total_pulls': total_pulls,
            'months_tracked': len(monthly_pulls)
        }

        # Add growth percentage if we have more than one month
        if len(pulls_list) > 1:
            months = list(monthly_pulls.keys())
            first_month_pulls = monthly_pulls[months[0]]
            last_month_pulls = monthly_pulls[months[-1]]
            if first_month_pulls > 0:
                growth_pct = ((last_month_pulls - first_month_pulls) / first_month_pulls) * 100
                repositories[repo]['overall_growth_pct'] = round(growth_pct, 2)

    summary = {
        'last_updated': datetime.now(timezone.utc).isoformat(),
        'repositories': repositories
    }

    # Save to JSON
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)

    print(f"✅ Saved monthly summary to {filename}")
    return summary


def print_monthly_summary(pull_data):
    """Print monthly summary statistics"""
    monthly_data = pull_data.groupby(['repo', 'month'])['count'].sum().reset_index()

    print(f"\n{'=' * 60}")
    print("📈 MONTHLY GROWTH SUMMARY")
    print(f"{'=' * 60}")

    for repo in sorted(pull_data['repo'].unique()):
        repo_data = monthly_data[monthly_data['repo'] == repo].sort_values('month')
        repo_name = repo.replace('fedora/', '').upper()

        print(f"\n🔹 {repo_name}:")

        total_pulls = repo_data['count'].sum()
        print(f"   Total pulls: {total_pulls:,}")

        if len(repo_data) > 1:
            first_month = repo_data.iloc[0]['count']
            last_month = repo_data.iloc[-1]['count']
            growth_pct = ((last_month - first_month) / first_month) * 100 if first_month > 0 else 0

            trend_emoji = "📈" if growth_pct > 0 else "📉" if growth_pct < 0 else "➡️"
            print(f"   Growth: {first_month:,} → {last_month:,} ({growth_pct:+.1f}%) {trend_emoji}")

        print(f"   Monthly breakdown:")
        for _, row in repo_data.iterrows():
            month_str = str(row['month'])
            count = row['count']
            print(f"     {month_str}: {count:,}")


def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Quay.io Growth Analyzer")
    parser.add_argument(
        "--days", type=int, default=7,
        help="Number of days to fetch (default: 7 for one week)"
    )
    parser.add_argument(
        "--start-date", type=str,
        help="Start date in YYYY-MM-DD format (overrides --days)"
    )
    parser.add_argument(
        "--end-date", type=str,
        help="End date in YYYY-MM-DD format (default: today)"
    )
    parser.add_argument(
        "--analyze-only", action="store_true",
        help="Skip data fetching, only analyze existing CSV"
    )
    return parser.parse_args()


def main():
    """Main function"""
    print("🚀 QUAY.IO GROWTH ANALYZER")
    print("=" * 60)

    # Load environment variables
    load_dotenv()

    # Parse arguments
    args = parse_arguments()

    if not args.analyze_only:
        # Data collection phase
        quay_token = os.environ.get("QUAY_API_TOKEN")
        if not quay_token:
            print("❌ Error: QUAY_API_TOKEN environment variable not set")
            print("Please set it to your Quay.io OAuth2 access token")
            exit(1)

        # Determine date range
        if args.start_date and args.end_date:
            try:
                start_date = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                end_date = datetime.strptime(args.end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                print("❌ Error: Invalid date format. Use YYYY-MM-DD")
                exit(1)
        elif args.start_date:
            try:
                start_date = datetime.strptime(args.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                end_date = datetime.now(timezone.utc)
            except ValueError:
                print("❌ Error: Invalid start date format. Use YYYY-MM-DD")
                exit(1)
        else:
            # Use --days parameter (default 7)
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=args.days)

        # Format for API
        start_time_str = start_date.strftime("%m/%d/%Y")
        end_time_str = end_date.strftime("%m/%d/%Y")

        # Fetch data
        logs = fetch_all_repositories(quay_token, start_time_str, end_time_str)

        # Save to CSV
        if logs:
            save_to_csv(logs)
        else:
            print("❌ No data collected, exiting")
            exit(1)

    # Analysis phase
    print(f"\n{'=' * 60}")
    print("📊 STARTING GROWTH ANALYSIS")
    print(f"{'=' * 60}")

    df, pull_data = load_and_prepare_data()

    if pull_data is None or pull_data.empty:
        print("❌ No data available for analysis")
        exit(1)

    # Print summary
    print_monthly_summary(pull_data)

    # Save monthly summary JSON (for git tracking)
    save_monthly_summary(pull_data)

    # Create visualizations
    create_monthly_growth_charts(pull_data)

    print(f"\n✅ Analysis complete! Individual growth charts generated.")
    print(f"📁 Raw data saved in: {CSV_FILENAME} (local only)")
    print(f"📁 Monthly summary saved in: {SUMMARY_FILENAME} (tracked in git)")


if __name__ == "__main__":
    main()