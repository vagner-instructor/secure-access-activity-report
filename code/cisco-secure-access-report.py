#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cisco Secure Access
Author: Vagner Silva
Credits: Victor Azevedo

UPDATED: Automatic region detection, robust redirect handling,
and auto-incremented CSV filename creation (_001, _002, ...)
"""

import requests
import time
import calendar
from datetime import datetime, timedelta
import csv
import json
import re
import os
import configparser
import sys

# =============================
# INITIAL AUTH / FALLBACKS
# =============================
AUTH_HOST = "https://api.sse.cisco.com"
AUTH_URL = f"{AUTH_HOST}/auth/v2/token"
# default reports base (will be overridden after auth)
REPORTS_BASE = "https://api.umbrella.com/reports.us"

CATEGORIES_PATH = "/reports/v2/categories"
ACTIVITY_PATH = "/reports/v2/activity"

# =============================
# TIME UTILITIES
# =============================
def dt_to_epoch_millis(dt: datetime) -> int:
    return int(time.mktime(dt.timetuple()) * 1000)

def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")

def elapsed(start_ts: float) -> str:
    return str(timedelta(seconds=int(time.time() - start_ts)))

# =============================
# UNIQUE FILENAME GENERATOR
# =============================
def get_unique_filename(filename: str) -> str:
    """
    If filename exists, append _001, _002, etc., before the extension.
    Example:
        report.csv -> report_001.csv
    """
    if not os.path.exists(filename):
        return filename
    base, ext = os.path.splitext(filename)
    counter = 1
    while True:
        new_filename = f"{base}_{counter:03d}{ext}"
        if not os.path.exists(new_filename):
            return new_filename
        counter += 1

# =============================
# RATE LIMITER
# =============================
class RateLimiter:
    def __init__(self, max_requests=18000, per_seconds=3600):
        self.max_requests = max_requests
        self.per_seconds = per_seconds
        self.window_start = time.time()
        self.count = 0

    def check(self):
        now = time.time()
        if now - self.window_start >= self.per_seconds:
            self.window_start = now
            self.count = 0
        if self.count >= self.max_requests:
            wait = int(self.per_seconds - (now - self.window_start))
            if wait < 0:
                wait = 0
            print(f"\n⏸️ Rate limit reached ({self.max_requests}/hour). Waiting {wait}s...")
            time.sleep(wait)
            self.window_start = time.time()
            self.count = 0
        self.count += 1

# =============================
# AUTHENTICATION
# =============================
def discover_region_from_headers(headers: dict) -> str:
    """
    Read known headers that may indicate the region to use (e.g. x-region-redirect).
    Returns a short region code (e.g. 'us', 'eu') or a hostname if present.
    """
    for h in ("x-region-redirect", "x-region", "x-region-host", "x-region-name"):
        v = headers.get(h)
        if v:
            v = v.strip()
            if v.startswith("reports."):
                v = v.replace("reports.", "")
            if v.startswith("https://") or v.startswith("http://"):
                try:
                    import urllib.parse as _up
                    parsed = _up.urlparse(v)
                    host = parsed.hostname or "us"
                    if host.startswith("reports."):
                        region = host.split(".", 1)[1] if "." in host else host
                        return region
                except Exception:
                    pass
            return v
    return "us"

def get_token_and_reports_base(client_id: str, client_secret: str, timeout: int = 30) -> tuple[str, str]:
    """
    Request client credentials token and discover the proper reports base URL.
    Returns: (access_token, reports_base_url)
    """
    try:
        resp = requests.post(
            AUTH_URL,
            data={"grant_type": "client_credentials"},
            auth=(client_id, client_secret),
            timeout=timeout,
            allow_redirects=False,
        )
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to obtain token: {e}")

    data = resp.json()
    token = data.get("access_token")
    if not token:
        raise RuntimeError(f"No access_token in auth response: {data}")

    region = discover_region_from_headers(resp.headers)
    reports_base = "https://api.umbrella.com/reports.us"
    if region:
        # if header is simple region code like 'us' or 'eu'
        if re.fullmatch(r"[a-zA-Z]{2,8}", region):
            reports_base = f"https://api.umbrella.com/reports.{region}"
        else:
            # region may be a hostname or reports.<region>
            if region.startswith("reports."):
                reports_base = f"https://{region}"
            elif "." in region:
                reports_base = f"https://{region}"
            else:
                reports_base = f"https://api.umbrella.com/reports.{region}"

    return token, reports_base

def prompt_credentials_with_test() -> tuple[str, str, str]:
    # global must be declared at top because we assign to it inside
    global REPORTS_BASE
    while True:
        client_id = input("🔑 CLIENT_ID: ").strip()
        client_secret = input("🔑 CLIENT_SECRET: ").strip()
        try:
            token, reports_base = get_token_and_reports_base(client_id, client_secret)
            print(f"✅ Authentication OK. Reports base: {reports_base}\n")
            REPORTS_BASE = reports_base
            return client_id, client_secret, token
        except Exception as e:
            print(f"❌ Authentication failed: {e}")
            print("Please try again.\n")

# =============================
# READ CATEGORIES FROM INI FILE
# =============================
def read_category_list_from_ini(file_path: str) -> dict:
    config = configparser.ConfigParser()
    config.read(file_path)
    categories = {}
    if 'Categories' in config:
        for key, value in config['Categories'].items():
            try:
                categories[int(key)] = value
            except Exception:
                pass
    return categories

# =============================
# INTERACTIVE DATE PROMPT
# =============================
def interactive_prompt_dates() -> tuple[int, int, list[int], str]:
    years = [2026, 2025, 2024, 2023, 2022, 2021]
    print("Select the year:")
    for i, y in enumerate(years, 1):
        print(f"{i}. {y}")
    while True:
        try:
            year_idx = int(input("Year (number): "))
            if 1 <= year_idx <= len(years):
                year = years[year_idx - 1]
                break
        except ValueError:
            pass
        print("Invalid input. Please try again.")

    months = ["January","February","March","April","May","June",
              "July","August","September","October","November","December"]
    print("\nSelect the month:")
    for i, m in enumerate(months, 1):
        print(f"{i}. {m}")
    while True:
        try:
            month = int(input("Month (number): "))
            if 1 <= month <= 12:
                break
        except ValueError:
            pass
        print("Invalid input. Please try again.")

    max_day = calendar.monthrange(year, month)[1]
    print(f"\nSelect day(s) (1-{max_day}):")
    print("0. All days of the month")
    print("Or enter multiple days separated by commas, e.g., 8,15,26")
    while True:
        try:
            day_input = input("Day(s): ").strip()
            if day_input == "0":
                days_to_process = list(range(1, max_day + 1))
                selected_day_for_filename = "all"
                break
            else:
                days_to_process = sorted({int(d.strip()) for d in day_input.split(",") if d.strip().isdigit()})
                if all(1 <= d <= max_day for d in days_to_process):
                    selected_day_for_filename = "_".join(str(d) for d in days_to_process)
                    break
        except ValueError:
            pass
        print("Invalid input. Please enter numbers between 1 and", max_day)

    return year, month, days_to_process, selected_day_for_filename

# =============================
# GET ALL AVAILABLE CATEGORIES - HANDLE REDIRECT
# =============================
def get_all_available_categories(token: str) -> list[dict]:
    headers = {"Authorization": f"Bearer {token}"}
    endpoint = f"{REPORTS_BASE}{CATEGORIES_PATH}"
    try:
        resp = requests.get(endpoint, headers=headers, timeout=30, allow_redirects=False)
        if resp.status_code == 302:
            redirected_url = resp.headers.get("Location")
            if not redirected_url:
                print("❌ 302 redirect without Location header")
                return []
            print(f"🔹 Following redirect to: {redirected_url}")
            resp = requests.get(redirected_url, headers=headers, timeout=30)
        resp.raise_for_status()
        all_categories_response = resp.json()
        all_categories = all_categories_response.get("data", [])
        if isinstance(all_categories, list):
            print("\n--- All Available Categories from API (ID, Type, Label) ---")
            sorted_categories = sorted(all_categories, key=lambda x: (x.get("type", ""), x.get("label", "")))
            for cat in sorted_categories:
                if isinstance(cat, dict):
                    print(f"  ID: {cat.get('id')}, Type: '{cat.get('type')}', Label: '{cat.get('label')}'")
            print("-----------------------------------------------------------")
            return all_categories
        else:
            print(f"❌ API response does not contain a list in 'data'. Type: {type(all_categories)}")
            return []
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching categories from API: {e}")
        return []

# =============================
# FETCH ACTIVITY WINDOW WITH REDIRECT AND 403 HANDLING
# =============================
def fetch_activity_window(token, client_id, client_secret, from_ts, to_ts,
                          limit=1000, offset_ceiling=None, verbose=False,
                          rate_limiter=None, filters=None,
                          activity_endpoint_url=None):
    # we may update REPORTS_BASE on token refresh; declare global at top
    global REPORTS_BASE

    # activity_endpoint_url is expected to be a fully qualified URL (no params)
    offset = 0
    events = []
    need_minute_fallback = False
    consecutive_403 = 0
    max_403_attempts = 5
    max_retries_conn = 5

    while True:
        if offset_ceiling is not None and offset >= offset_ceiling:
            need_minute_fallback = True
            if verbose:
                print(f"   ⚠️ Offset {offset} >= ceiling {offset_ceiling}. Activating fallback minute-by-minute.")
            break

        if rate_limiter:
            rate_limiter.check()

        params = {"from": str(from_ts), "to": str(to_ts), "limit": limit, "offset": offset}
        if filters:
            params.update(filters)

        headers = {"Authorization": f"Bearer {token}"}
        resp = None

        # Connection retry loop
        for attempt in range(max_retries_conn):
            try:
                resp = requests.get(activity_endpoint_url, headers=headers, params=params, timeout=60, allow_redirects=False)
                break
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.ChunkedEncodingError,
                    requests.exceptions.ReadTimeout) as e:
                wait = 2 ** attempt
                print(f"   ⚠️ Connection error ({e}). Retrying in {wait}s...")
                time.sleep(wait)

        if resp is None:
            print("   🚨 Repeated connection failures. Aborting this interval.")
            break

        # Handle redirect (302)
        if resp.status_code == 302:
            redirected_url = resp.headers.get("Location")
            if redirected_url:
                if verbose:
                    print(f"🔹 Following redirect to: {redirected_url}")
                try:
                    # follow the redirect with the same headers/params
                    resp = requests.get(redirected_url, headers=headers, params=params, timeout=60)
                except Exception as e:
                    print(f"   ❌ Failed to fetch redirected URL: {e}")
                    break
            else:
                print("   ❌ 302 redirect received but no Location header.")
                break

        # Success
        if resp.status_code == 200:
            try:
                payload = resp.json()
            except Exception as e:
                print(f"   ⚠️ Failed to parse JSON: {e}, response: {resp.text[:200]}")
                break
            batch = payload.get("data", [])
            if not batch:
                break
            events.extend(batch)
            offset += len(batch)
            if verbose:
                print(f"      🔹 {len(batch)} events fetched (offset now {offset})")
            consecutive_403 = 0
            if len(batch) < limit:
                break
            continue

        # 403 handling -> refresh token and possibly region
        if resp.status_code == 403:
            consecutive_403 += 1
            print(f"   ⚠️ HTTP 403 detected ({consecutive_403}/{max_403_attempts}). Refreshing token...")
            try:
                new_token, new_reports_base = get_token_and_reports_base(client_id, client_secret)
                token = new_token
                REPORTS_BASE = new_reports_base
            except Exception as e:
                print(f"   ❌ Failed to refresh token: {e}")
                time.sleep(5)
            if consecutive_403 >= max_403_attempts:
                print("   🚨 Persistent 403 after several retries. Stopping this interval.")
                break
            continue

        # Client errors trigger minute fallback
        if resp.status_code in (400, 404):
            need_minute_fallback = True
            print(f"   ⚠️ HTTP {resp.status_code} — activating minute-by-minute fallback for this hour.")
            break

        # Other errors
        print(f"   ⚠️ HTTP {resp.status_code} returned. Message: {resp.text[:200]}")
        break

    return events, need_minute_fallback, token

# =============================
# FETCH HOUR WITH MINUTE FALLBACK
# =============================
def fetch_hour_with_minute_fallback(token, client_id, client_secret, hour_start_dt,
                                    limit=1000, offset_ceiling=10000, verbose=True,
                                    rate_limiter=None, filters=None,
                                    activity_endpoint_url=None):
    hour_end_dt = hour_start_dt + timedelta(hours=1) - timedelta(milliseconds=1)
    from_ts = dt_to_epoch_millis(hour_start_dt)
    to_ts = dt_to_epoch_millis(hour_end_dt)

    if verbose:
        print(f"\n⏳ Hourly: {fmt_dt(hour_start_dt)} to {fmt_dt(hour_end_dt)}")

    hour_events_from_api, need_minute_fallback, token = fetch_activity_window(
        token, client_id, client_secret, from_ts, to_ts,
        limit=limit, offset_ceiling=offset_ceiling, verbose=verbose,
        rate_limiter=rate_limiter, filters=filters, activity_endpoint_url=activity_endpoint_url
    )

    if not need_minute_fallback:
        if verbose:
            print(f"   ✅ Hour OK: {len(hour_events_from_api)} events")
        return hour_events_from_api, token

    collected = []
    if verbose:
        print("   ↪️ Starting minute-by-minute fallback (60 minutes).")
    for m in range(60):
        minute_start = hour_start_dt + timedelta(minutes=m)
        minute_end = minute_start + timedelta(minutes=1) - timedelta(milliseconds=1)
        m_from = dt_to_epoch_millis(minute_start)
        m_to = dt_to_epoch_millis(minute_end)

        if verbose:
            print(f"      ➤ Minute: {fmt_dt(minute_start)} to {fmt_dt(minute_end)} ... ", end="")

        minute_events, _, token = fetch_activity_window(
            token, client_id, client_secret, m_from, m_to,
            limit=limit, offset_ceiling=None, verbose=False,
            rate_limiter=rate_limiter, filters=filters, activity_endpoint_url=activity_endpoint_url
        )
        collected.extend(minute_events)
        if verbose:
            print(f"   {len(minute_events)} events")

    if verbose:
        print(f"   ✅ Fallback minute total: {len(collected)} events for hour {hour_start_dt.strftime('%Y-%m-%d %H:00')}")
    return collected, token

# =============================
# CSV UTILITIES
# =============================
def _parse_event_datetime(ev):
    ts_val = ev.get("timestamp")
    if isinstance(ts_val, str) and ts_val:
        try:
            return datetime.fromisoformat(ts_val.replace("Z", "+00:00"))
        except ValueError:
            pass
    elif isinstance(ts_val, (int, float)):
        try:
            return datetime.fromtimestamp(ts_val / 1000)
        except ValueError:
            pass
    d = ev.get("date")
    t = ev.get("time")
    if isinstance(d, str) and isinstance(t, str):
        try:
            return datetime.fromisoformat(f"{d}T{t}")
        except ValueError:
            pass
    return None

def save_to_csv_custom_format(events, writer):
    for ev in events:
        dt = _parse_event_datetime(ev)
        policy_identity = ''
        if ev.get('rule', {}) and ev.get('rule', {}).get('label'):
            policy_identity = ev['rule']['label']
        elif ev.get('policy', {}) and ev.get('policy', {}).get('name'):
            policy_identity = ev['policy']['name']
        elif ev.get('policyName'):
            policy_identity = ev['policyName']
        else:
            for id_data in ev.get('identities', []):
                if isinstance(id_data, dict) and id_data.get('policyIdentity'):
                    policy_identity = id_data['policyIdentity']
                    break
        identities_data = ev.get('identities', [])
        identity_labels = []
        identity_types = []
        for id_data in identities_data:
            if isinstance(id_data, dict):
                label = id_data.get('label')
                if isinstance(label, str):
                    identity_labels.append(label)
                id_type_obj = id_data.get('type')
                if isinstance(id_type_obj, dict):
                    id_type_label = id_type_obj.get('label')
                    if isinstance(id_type_label, str):
                        identity_types.append(id_type_label)
                elif isinstance(id_type_obj, str):
                    identity_types.append(id_type_obj)
        categories_data = ev.get('categories', [])
        category_labels = [cat_data.get('label', '') for cat_data in categories_data if isinstance(cat_data, dict) and cat_data.get('label')]
        writer.writerow({
            "Date": dt.strftime("%Y-%m-%d") if dt else "",
            "Time": dt.strftime("%H:%M:%S") if dt else "",
            "Policy Identity": policy_identity,
            "Identity Type": identity_types[0] if identity_types else "",
            "Identities": "; ".join(identity_labels),
            "Identity Types": "; ".join(identity_types),
            "Record Type": ev.get('recordType', ev.get('type', '')),
            "Internal Ip Address": ev.get('internalip', ''),
            "External Ip Address": ev.get('externalip', ''),
            "Action": ev.get('verdict', ''),
            "Destination": ev.get('domain', ev.get('dest', ev.get('url', ''))),
            "Categories": "; ".join(category_labels),
            "Full Event JSON": json.dumps(ev, ensure_ascii=False)
        })

def save_raw_events_to_csv(events, writer):
    for ev in events:
        dt = _parse_event_datetime(ev)
        writer.writerow({
            "timestamp": dt.isoformat() if dt else "",
            "full_event_json": json.dumps(ev, ensure_ascii=False)
        })

def sanitize_filename(text: str) -> str:
    text = text.replace(" ", "_")
    text = re.sub(r'[^\w.\-]', '', text)
    return text.lower()

# =============================
# MAIN FUNCTION
# =============================
def main():
    # 1) Credentials with test (this will also set REPORTS_BASE global)
    client_id, client_secret, token = prompt_credentials_with_test()

    # 2) Prompt for year/month/day(s)
    year, month, days_to_process, selected_day_for_filename = interactive_prompt_dates()

    # 3) Event Type Selection
    valid_event_types = ["dns", "proxy", "firewall", "ip", "ztna", "remote-access", "intrusion"]
    print("\n--- Select Event Type ---")
    for i, etype in enumerate(valid_event_types, 1):
        print(f"{i}. {etype}")
    while True:
        try:
            type_choice_idx = int(input(f"Enter the number for the event type (1-{len(valid_event_types)}): ").strip())
            if 1 <= type_choice_idx <= len(valid_event_types):
                event_type_filter = valid_event_types[type_choice_idx - 1]
                break
        except ValueError:
            pass
        print("Invalid input. Please enter a valid number.")

    # 4) Read category list from .ini file
    categories_file_path = "category_list.ini"
    predefined_categories = read_category_list_from_ini(categories_file_path)

    # 5) Category selection
    categories_to_process_list = []
    category_selection_choice = '1'  # default
    print("\n--- Category Filtering Options ---")
    print("1. Predefined list of categories in the category_list.ini file (each category will have its own CSV generated)")
    while True:
        category_selection_choice = input("Enter your choice (1): ").strip()
        if category_selection_choice in ['0', '1', '2', '3']:
            break
        print("Invalid input. Please enter 0, 1, 2, or 3.")

    if category_selection_choice == '1':
        for cat_id, cat_name in predefined_categories.items():
            categories_to_process_list.append(
                ({"categories": str(cat_id)}, sanitize_filename(cat_name))
            )

    # 6) CSV format selection
    print("\n--- CSV Output Format Options ---")
    print("1. Custom formatted CSV (Date;Time;Policy Identity;...)")
    print("2. All Data (Raw JSON event in a column)")
    while True:
        csv_format_choice = input("Enter your choice (1 or 2): ").strip()
        if csv_format_choice in ['1', '2']:
            break
        print("Invalid input. Enter 1 or 2.")

    if csv_format_choice == '1':
        csv_fieldnames = [
            "Date", "Time", "Policy Identity", "Identity Type", "Identities",
            "Identity Types", "Record Type", "Internal Ip Address",
            "External Ip Address", "Action", "Destination", "Categories", "Full Event JSON"
        ]
        save_events_function = save_to_csv_custom_format
        csv_format_suffix = "custom"
    else:
        csv_fieldnames = ["timestamp", "full_event_json"]
        save_events_function = save_raw_events_to_csv
        csv_format_suffix = "raw_json"

    # 7) Exclusion filters
    excluded_identity_names = {"user_a", "user_b", "service_account_1"}

    # 8) Optional action filter: "allowed" or "blocked"
    action_filter = None
    print("\n--- Action Filter ---")
    print("1. No filter")
    print("2. Only allowed (Action=Allowed)")
    print("3. Only blocked (Action=Blocked)")
    while True:
        af_choice = input("Enter your choice (1/2/3): ").strip()
        if af_choice == "2":
            action_filter = "allowed"
            break
        elif af_choice == "3":
            action_filter = "blocked"
            break
        elif af_choice == "1":
            break
        print("Invalid input. Please enter 1, 2, or 3.")

    # DEBUG info
    print(f"\nDEBUG: days_to_process={days_to_process}")
    print(f"DEBUG: categories_to_process_list={categories_to_process_list}")

    # 9) Main fetching loop
    total_categories = len(categories_to_process_list)

    for cat_idx, (current_api_filters, current_category_filename_segment) in enumerate(categories_to_process_list, start=1):
        csv_file = f"activity_{year}_{month:02d}_{selected_day_for_filename}_{current_category_filename_segment}_{event_type_filter}_{csv_format_suffix}.csv"
        # ensure unique filename using _001, _002 pattern
        csv_file = get_unique_filename(csv_file)

        print(f"\n🚀 Starting collection for category {cat_idx}/{total_categories}: '{current_category_filename_segment}'")
        print(f"📂 Saving into file: '{csv_file}'")
        file_exists = os.path.exists(csv_file)

        with open(csv_file, "a", newline="", encoding="utf-8") as f:
            csv_writer = csv.DictWriter(f, fieldnames=csv_fieldnames, delimiter=';')
            if not file_exists:
                csv_writer.writeheader()

            rate_limiter = RateLimiter(max_requests=18000, per_seconds=3600)
            start_time = time.time()
            total_events_for_this_category = 0

            for idx, current_day in enumerate(days_to_process):
                print(f"\n📅 Day: {current_day} ({idx + 1}/{len(days_to_process)})")
                for hour in range(24):
                    hour_start = datetime(year, month, current_day, hour, 0, 0)
                    print(f"⏱️ Elapsed: {elapsed(start_time)} | Category {cat_idx}/{total_categories}: Saving to '{csv_file}'")

                    # ensure we use the latest discovered reports base for the endpoint
                    activity_endpoint_url = f"{REPORTS_BASE}/v2/activity/{event_type_filter}"

                    events_hour, token = fetch_hour_with_minute_fallback(
                        token=token,
                        client_id=client_id,
                        client_secret=client_secret,
                        hour_start_dt=hour_start,
                        limit=1000,
                        offset_ceiling=10000,
                        verbose=True,
                        rate_limiter=rate_limiter,
                        filters=current_api_filters,
                        activity_endpoint_url=activity_endpoint_url
                    )

                    # Apply exclusion filters
                    if excluded_identity_names:
                        events_hour = [
                            ev for ev in events_hour
                            if not any(
                                (isinstance(id_data, dict) and id_data.get('label') in excluded_identity_names)
                                for id_data in ev.get('identities', [])
                            )
                        ]

                    # Apply action filter
                    if action_filter:
                        events_hour = [ev for ev in events_hour if ev.get('verdict', '').lower() == action_filter]

                    print(f"   ✅ Hour OK: {len(events_hour)} events")
                    save_events_function(events_hour, csv_writer)
                    total_events_for_this_category += len(events_hour)

            print(f"\n🏁 Completed category {cat_idx}/{total_categories}: {total_events_for_this_category} events saved in '{csv_file}'")
            print(f"⏱️ Total time for this category: {elapsed(start_time)}")

    print(f"\n✅ All requested data collection completed.")
    print(f"⏱️ Total script execution time: {elapsed(start_time)}")


if __name__ == "__main__":
    main()
