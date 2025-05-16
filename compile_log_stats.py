import json
import argparse
from collections import Counter
from datetime import datetime # Added back for datetime parsing

def load_log_data(filepath):
    """Loads log data from a JSON file."""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        if not isinstance(data, list):
            print(f"Error: Expected a JSON list in {filepath}, but got {type(data)}.")
            return None
        return data
    except FileNotFoundError:
        print(f"Error: File not found at '{filepath}'.")
        return None
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{filepath}'. Ensure it's a valid JSON file.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while loading the file: {e}")
        return None

def compile_statistics(log_data, top_n=10):
    """Compiles various statistics from the log data."""
    if not log_data:
        return None

    total_entries = len(log_data)
    event_kinds = Counter()
    user_agents = Counter()
    ip_addresses = Counter()
    pulled_tags = Counter()
    countries = Counter()
    performers = Counter()

    earliest_event_time = None
    latest_event_time = None

    for entry in log_data:
        # Event Kind
        kind = entry.get("kind")
        if kind:
            event_kinds[kind] += 1

        # IP Address
        ip = entry.get("ip")
        if ip:
            ip_addresses[ip] += 1

        # Datetime processing for time range
        dt_str = entry.get("datetime")
        if dt_str:
            try:
                # Example: "Fri, 16 May 2025 06:15:07 -0000"
                dt_obj = datetime.strptime(dt_str, '%a, %d %b %Y %H:%M:%S %z')
                
                if earliest_event_time is None or dt_obj < earliest_event_time:
                    earliest_event_time = dt_obj
                if latest_event_time is None or dt_obj > latest_event_time:
                    latest_event_time = dt_obj
            except ValueError:
                print(f"Warning: Could not parse datetime string for time range: {dt_str}")

        # Metadata based stats
        metadata = entry.get("metadata", {})
        
        user_agent = metadata.get("user-agent")
        if user_agent:
            user_agents[user_agent] += 1

        resolved_ip_info = metadata.get("resolved_ip", {})
        country = resolved_ip_info.get("country_iso_code")
        if country:
            countries[country] += 1
        
        if kind == "pull_repo":
            tag = metadata.get("tag")
            if tag:
                pulled_tags[tag] += 1

        # Performer (user/robot)
        performer_info = entry.get("performer")
        if performer_info and isinstance(performer_info, dict):
            performer_name = performer_info.get("name")
            if performer_name:
                performers[performer_name] += 1
        elif "username" in metadata: 
            username = metadata.get("username")
            if username:
                performers[username] +=1

    stats = {
        "total_log_entries": total_entries,
        "earliest_event_time": earliest_event_time.isoformat() if earliest_event_time else "N/A",
        "latest_event_time": latest_event_time.isoformat() if latest_event_time else "N/A",
        "event_kind_breakdown": dict(event_kinds),
        "top_user_agents": user_agents.most_common(top_n),
        "top_ip_addresses": ip_addresses.most_common(top_n),
        "top_pulled_tags": pulled_tags.most_common(top_n),
        "activity_by_country": countries.most_common(top_n),
        "top_performers": performers.most_common(top_n),
    }
    return stats

def print_statistics(stats, filename):
    """Prints the compiled statistics in a readable format."""
    if not stats:
        print("No statistics were compiled.")
        return

    print(f"\n--- Statistics for Log File: {filename} ---")
    
    print(f"\n1. Log Entries Overview:")
    print(f"   - Total Log Entries: {stats['total_log_entries']}")
    print(f"   - Earliest Event Time: {stats['earliest_event_time']}")
    print(f"   - Latest Event Time:   {stats['latest_event_time']}")


    print("\n2. Event Kind Breakdown:")
    if stats['event_kind_breakdown']:
        for kind, count in stats['event_kind_breakdown'].items():
            print(f"  - {kind}: {count}")
    else:
        print("  No event kind data available.")

    print(f"\n3. Top {len(stats['top_user_agents'])} User Agents:")
    if stats['top_user_agents']:
        for agent, count in stats['top_user_agents']:
            print(f"  - \"{agent}\": {count}")
    else:
        print("  No user agent data available.")

    print(f"\n4. Top {len(stats['top_ip_addresses'])} IP Addresses:")
    if stats['top_ip_addresses']:
        for ip, count in stats['top_ip_addresses']:
            print(f"  - {ip}: {count}")
    else:
        print("  No IP address data available.")

    print(f"\n5. Top {len(stats['top_pulled_tags'])} Pulled Tags (for 'pull_repo' events):")
    if stats['top_pulled_tags']:
        for tag, count in stats['top_pulled_tags']:
            print(f"  - {tag}: {count}")
    else:
        print("  No pulled tag data available or no 'pull_repo' events with tags.")

    print(f"\n6. Top {len(stats['activity_by_country'])} Countries (by IP location):")
    if stats['activity_by_country']:
        for country, count in stats['activity_by_country']:
            print(f"  - {country}: {count}")
    else:
        print("  No country data available.")

    print(f"\n7. Top {len(stats['top_performers'])} Performers (Users/Robots):")
    if stats['top_performers']:
        for performer, count in stats['top_performers']:
            print(f"  - {performer}: {count}")
    else:
        print("  No performer data available.")
        
    print("\n--- End of Statistics ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compile statistics from a Quay.io JSON log file.")
    parser.add_argument("filepath", help="Path to the JSON log file.")
    parser.add_argument("--top-n", type=int, default=10, help="Number of top items to display for relevant statistics (e.g., IPs, User Agents). Default is 10.")
    
    args = parser.parse_args()

    log_data = load_log_data(args.filepath)
    if log_data:
        compiled_stats = compile_statistics(log_data, top_n=args.top_n)
        if compiled_stats:
            print_statistics(compiled_stats, args.filepath)