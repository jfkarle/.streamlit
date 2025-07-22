import csv
import os
import datetime
import pandas as pd
import calendar
import requests
import random
import json
import streamlit as st
from st_supabase_connection import SupabaseConnection, execute_query
from datetime import timedelta, time
from collections import Counter


DEBUG_MESSAGES = []


# --- DATA MODELS (CLASSES) ---
class Truck:
    def __init__(self, t_id, name, max_len):
        self.truck_id, self.truck_name, self.max_boat_length, self.is_crane = t_id, name, max_len, "Crane" in name

class Ramp:
    def __init__(self, r_id, name, station, tide_method="AnyTide", offset=None, boats=None):
        self.ramp_id, self.ramp_name, self.noaa_station_id, self.tide_calculation_method = r_id, name, station, tide_method
        self.tide_offset_hours1, self.allowed_boat_types = offset, boats or ["Powerboat", "Sailboat DT", "Sailboat MT"]

class Customer:
    def __init__(self, c_id, name):
        self.customer_id = int(c_id)
        self.customer_name = name

class Boat:
    def __init__(self, b_id, c_id, b_type, b_len, draft, storage_addr, pref_ramp, pref_truck, is_ecm):
        self.boat_id = int(b_id)
        self.customer_id = int(c_id)
        self.boat_type = b_type
        self.boat_length = b_len
        self.draft_ft = draft
        self.storage_address = storage_addr
        self.preferred_ramp_id = pref_ramp
        self.preferred_truck_id = pref_truck
        self.is_ecm_boat = is_ecm

class Job:
    def __init__(self, **kwargs):
        def _parse_datetime(dt_string):
            if not dt_string or not isinstance(dt_string, str): return None
            try: return datetime.datetime.fromisoformat(dt_string.replace(" ", "T"))
            except (ValueError, TypeError): return None
        def _parse_int(int_string):
            if not int_string: return None
            try: return int(int_string)
            except (ValueError, TypeError): return None
        self.job_id = _parse_int(kwargs.get("job_id"))
        self.customer_id = _parse_int(kwargs.get("customer_id"))
        self.boat_id = _parse_int(kwargs.get("boat_id"))
        self.service_type = kwargs.get("service_type")
        self.scheduled_start_datetime = _parse_datetime(kwargs.get("scheduled_start_datetime"))
        self.scheduled_end_datetime = _parse_datetime(kwargs.get("scheduled_end_datetime"))
        self.assigned_hauling_truck_id = kwargs.get("assigned_hauling_truck_id")
        self.assigned_crane_truck_id = kwargs.get("assigned_crane_truck_id")
        self.j17_busy_end_datetime = _parse_datetime(kwargs.get("j17_busy_end_datetime"))
        self.pickup_ramp_id = kwargs.get("pickup_ramp_id")
        self.dropoff_ramp_id = kwargs.get("dropoff_ramp_id")
        self.pickup_street_address = kwargs.get("pickup_street_address", "")
        self.dropoff_street_address = kwargs.get("dropoff_street_address", "")
        self.job_status = kwargs.get("job_status", "Scheduled")
        self.notes = kwargs.get("notes", "")

# --- CONFIGURATION AND GLOBAL CONSTANTS ---
HOME_BASE_TOWN = "Pem"
SOUTH_ROUTE = ["Han", "Nor", "Sci", "Mar", "Dux", "Kin", "Ply", "Bou", "San"]
NORTH_ROUTE = ["Wey", "Hin", "Coh", "Hul", "Qui", "Bos"]
YARD_ADDRESS = "43 Mattakeesett St, Pembroke, MA 02359"
DEFAULT_TRUCK_OPERATING_HOURS = {
    "S20/33": { 0: (time(7, 0), time(15, 0)), 1: (time(7, 0), time(15, 0)), 2: (time(7, 0), time(15, 0)), 3: (time(7, 0), time(15, 0)), 4: (time(7, 0), time(15, 0)), 5: (time(8, 0), time(12, 0)), 6: None },
    "S21/77": { 0: (time(8, 0), time(16, 0)), 1: (time(8, 0), time(16, 0)), 2: (time(8, 0), time(16, 0)), 3: (time(8, 0), time(16, 0)), 4: (time(8, 0), time(16, 0)), 5: None, 6: None },
    "S23/55": { 0: (time(8, 0), time(17, 0)), 1: (time(8, 0), time(17, 0)), 2: (time(8, 0), time(17, 0)), 3: (time(8, 0), time(17, 0)), 4: (time(8, 0), time(17, 0)), 5: (time(7, 30), time(17, 30)), 6: None },
    "J17":    { 0: (time(8, 0), time(16, 0)), 1: (time(8, 0), time(16, 0)), 2: (time(8, 0), time(16, 0)), 3: (time(8, 0), time(16, 0)), 4: (time(8, 0), time(16, 0)), 5: None, 6: None }
}
BOOKING_RULES = {'Powerboat': {'truck_mins': 90, 'crane_mins': 0},'Sailboat DT': {'truck_mins': 180, 'crane_mins': 60},'Sailboat MT': {'truck_mins': 180, 'crane_mins': 90}}

# --- IN-MEMORY DATA CACHES ---
CANDIDATE_CRANE_DAYS = { 'ScituateHarborJericho': [], 'PlymouthHarbor': [], 'WeymouthWessagusset': [], 'CohassetParkerAve': [] }
crane_daily_status = {}
ECM_TRUCKS, LOADED_CUSTOMERS, LOADED_BOATS, ECM_RAMPS, TRUCK_OPERATING_HOURS = {}, {}, {}, {}, {}
SCHEDULED_JOBS, PARKED_JOBS = [], {}

# --- DATABASE PERSISTENCE FUNCTIONS ---
@st.cache_resource
def get_db_connection():
    return st.connection(
        "supabase",
        type=SupabaseConnection,
        url="https://knexrzljvagiwqstapnk.supabase.co",
        key="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtuZXhyemxqdmFnaXdxc3RhcG5rIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTIwODY0ODIsImV4cCI6MjA2NzY2MjQ4Mn0.hgWhtefyiEmGj5CERladOe3hMBM-rVnwMGNwrt8FT6Y"
    )

def load_all_data_from_sheets():
    """Loads all data from Supabase, now including truck schedules."""
    global SCHEDULED_JOBS, PARKED_JOBS, LOADED_CUSTOMERS, LOADED_BOATS, ECM_TRUCKS, ECM_RAMPS, TRUCK_OPERATING_HOURS
    try:
        conn = get_db_connection()

        # --- Jobs ---
        jobs_resp = execute_query(conn.table("jobs").select("*"), ttl=0)
        all_jobs = [Job(**row) for row in jobs_resp.data]
        SCHEDULED_JOBS[:] = [job for job in all_jobs if job.job_status == "Scheduled"]
        PARKED_JOBS.clear()
        PARKED_JOBS.update({job.job_id: job for job in all_jobs if job.job_status == "Parked"})

        # --- Trucks ---
        trucks_resp = execute_query(conn.table("trucks").select("*"), ttl=0)
        ECM_TRUCKS.clear()
        for row in trucks_resp.data:
            t = Truck(
                t_id    = row["truck_id"],
                name    = row.get("truck_name"),
                max_len = row.get("max_boat_length")
            )
            ECM_TRUCKS[t.truck_id] = t
        
        # DEBUG: Print ECM_TRUCKS after loading
        DEBUG_MESSAGES.append("DEBUG: ECM_TRUCKS after loading:")
        DEBUG_MESSAGES.append(json.dumps({k: t.truck_name for k, t in ECM_TRUCKS.items()}, indent=2))

        # Build a name → ID map so schedules can be keyed by numeric truck_id
        name_to_id = {t.truck_name: t.truck_id for t in ECM_TRUCKS.values()}

        # --- Ramps ---
        ramps_resp = execute_query(conn.table("ramps").select("*"), ttl=0)
        ECM_RAMPS.clear()
        ECM_RAMPS.update({
            row["ramp_id"]: Ramp(
                r_id       = row["ramp_id"],
                name       = row.get("ramp_name"),
                station    = row.get("noaa_station_id"),
                tide_method= row.get("tide_calculation_method"),
                offset     = row.get("tide_offset_hours"),
                boats      = row.get("allowed_boat_types")
            )
            for row in ramps_resp.data
        })

        # --- Customers ---
        cust_resp = execute_query(conn.table("customers").select("*"), ttl=0)
        LOADED_CUSTOMERS.clear()
        LOADED_CUSTOMERS.update({
            int(row["customer_id"]): Customer(
                c_id = row["customer_id"],
                name = row.get("Customer", "")
            )
            for row in cust_resp.data
            if row.get("customer_id")
        })

        # --- Boats ---
        boat_resp = execute_query(conn.table("boats").select("*"), ttl=0)
        LOADED_BOATS.clear()
        LOADED_BOATS.update({
            int(row["boat_id"]): Boat(
                b_id       = row["boat_id"],
                c_id       = row["customer_id"],
                b_type     = row.get("boat_type"),
                b_len      = row.get("boat_length"),
                draft      = row.get("draft_ft"),
                storage_addr = row.get("storage_address", ""),
                pref_ramp  = row.get("preferred_ramp", ""),
                pref_truck = row.get("preferred_truck", ""),
                is_ecm     = str(row.get("is_ecm_boat", "no")).lower() == 'yes'
            )
            for row in boat_resp.data
            if row.get("boat_id")
        })

        # --- Truck Schedules (corrected) ---
        schedules_resp = execute_query(conn.table("truck_schedules").select("*"), ttl=0)
        processed_schedules = {}
        for row in schedules_resp.data:
            truck_name = row["truck_name"]
            truck_id   = name_to_id.get(truck_name)
            if truck_id is None:
                continue   # skip unknown names
            day        = row["day_of_week"]
            start_time = datetime.datetime.strptime(row["start_time"], '%H:%M:%S').time()
            end_time   = datetime.datetime.strptime(row["end_time"],   '%H:%M:%S').time()
            processed_schedules.setdefault(truck_id, {})[day] = (start_time, end_time)

        # DEBUG: Print processed_schedules before update
        DEBUG_MESSAGES.append("DEBUG: processed_schedules before update:")
        # Convert times to string for JSON serialization
        json_friendly_processed_schedules = {
            k: {d: f"{s.strftime('%H:%M')}-{e.strftime('%H:%M')}" for d, (s, e) in v.items()}
            for k, v in processed_schedules.items()
        }
        DEBUG_MESSAGES.append(json.dumps(json_friendly_processed_schedules, indent=2))

        TRUCK_OPERATING_HOURS.clear()
        TRUCK_OPERATING_HOURS.update(processed_schedules)

        # DEBUG: Print TRUCK_OPERATING_HOURS after update
        DEBUG_MESSAGES.append("DEBUG: TRUCK_OPERATING_HOURS after update:")
        # Convert times to string for JSON serialization
        json_friendly_truck_operating_hours = {
            k: {d: f"{s.strftime('%H:%M')}-{e.strftime('%H:%M')}" for d, (s, e) in v.items()}
            for k, v in TRUCK_OPERATING_HOURS.items()
        }
        DEBUG_MESSAGES.append(json.dumps(json_friendly_truck_operating_hours, indent=2))

        st.toast(
            f"Loaded data for {len(ECM_TRUCKS)} trucks, "
            f"{len(ECM_RAMPS)} ramps, {len(LOADED_CUSTOMERS)} customers.",
            icon="✅"
        )

    except Exception as e:
        st.error(f"Error loading data: {e}")
        raise

def save_job(job_to_save):
    conn = get_db_connection()
    job_dict = job_to_save.__dict__
    job_id = job_dict.get('job_id')
    try:
        if job_id and any(j.job_id == job_id for j in SCHEDULED_JOBS + list(PARKED_JOBS.values())):
            update_data = {k: v for k, v in job_dict.items() if k != 'job_id'}
            conn.table("jobs").update(update_data).eq("job_id", job_id).execute()
        else:
            insert_data = {k: v for k, v in job_dict.items() if k != 'job_id'}
            response = conn.table("jobs").insert(insert_data).execute()
            new_id = response.data[0]['job_id']
            job_to_save.job_id = new_id
    except Exception as e:
        st.error(f"Database save error for job {job_id or '(new)'}")
        st.exception(e)

def update_truck_schedule(truck_name, new_hours_dict):
    """Deletes all existing schedule entries for a truck and inserts the new ones."""
    try:
        conn = get_db_connection()
        
        # First, delete all old schedule entries for this truck
        conn.table("truck_schedules").delete().eq("truck_name", truck_name).execute()

        # Then, insert the new schedule entries
        rows_to_insert = []
        for day_of_week, hours in new_hours_dict.items():
            if hours:  # Only insert if the truck is working
                start_time, end_time = hours
                rows_to_insert.append({
                    "truck_name": truck_name,
                    "day_of_week": day_of_week,
                    "start_time": start_time.strftime('%H:%M:%S'),
                    "end_time": end_time.strftime('%H:%M:%S')
                })
        
        if rows_to_insert:
            conn.table("truck_schedules").insert(rows_to_insert).execute()
            
        return True, f"Schedule for {truck_name} updated successfully."
    except Exception as e:
        return False, f"Error updating schedule for {truck_name}: {e}"

def delete_job_from_db(job_id):
    try:
        conn = get_db_connection()
        conn.table("jobs").delete().eq("job_id", job_id).execute()
    except Exception as e:
        st.error(f"Failed to delete job {job_id}")
        st.exception(e)

# --- CORE HELPER FUNCTIONS ---
def get_customer_details(customer_id):
    return LOADED_CUSTOMERS.get(customer_id)
def get_boat_details(boat_id):
    return LOADED_BOATS.get(boat_id)
def get_ramp_details(ramp_id):
    return ECM_RAMPS.get(ramp_id)

def _abbreviate_town(address):
    if not address: return ""
    if address.isdigit(): return "Pem"
    abbr_map = { "pembroke": "Pem", "scituate": "Sci", "green harbor": "GrH", "marshfield": "Mar", "cohasset": "Coh", "weymouth": "Wey", "plymouth": "Ply", "sandwich": "San", "duxbury": "Dux", "humarock": "Hum", "hingham": "Hin", "hull": "Hul", "norwell": "Nor", "boston": "Bos", "quincy": "Qui", "kingston": "Kin", "hanover": "Han", "rockland": "Roc" }
    if 'HOME' in address.upper(): return "Pem"
    address_lower = address.lower()
    for town, abbr in abbr_map.items():
        if town in address_lower: return abbr
    return address.title().split(',')[0][:3]

def format_time_for_display(time_obj):
    return time_obj.strftime('%I:%M %p').lstrip('0') if isinstance(time_obj, datetime.time) else "InvalidTime"

def get_job_details(job_id):
    for job in SCHEDULED_JOBS:
        if job.job_id == job_id: return job
    return None

def get_parked_job_details(job_id):
    return PARKED_JOBS.get(job_id)

def cancel_job(job_id):
    job_to_cancel = get_job_details(job_id)
    if job_to_cancel:
        SCHEDULED_JOBS.remove(job_to_cancel)
        delete_job_from_db(job_id)
        return True
    return False

def park_job(job_id):
    job_to_park = get_job_details(job_id)
    if job_to_park:
        SCHEDULED_JOBS.remove(job_to_park)
        job_to_park.job_status = "Parked"
        PARKED_JOBS[job_id] = job_to_park
        save_job(job_to_park)
        return True
    return False

def get_monthly_tides_for_scituate(year, month):
    scituate_station_id = "8445138"
    try:
        start_date = datetime.date(year, month, 1)
        _, num_days = calendar.monthrange(year, month)
        end_date = datetime.date(year, month, num_days)
        return fetch_noaa_tides_for_range(scituate_station_id, start_date, end_date)
    except Exception as e:
        print(f"Error fetching monthly tides: {e}")
        return None

def _parse_annual_tide_file(filepath, begin_date, end_date):
    """
    Parses an annual NOAA tide prediction text file for a specific date range.
    Returns data in the same format as fetch_noaa_tides_for_range's grouped_tides.
    """
    grouped_tides = {}
    try:
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                # More robustly skip header lines by only processing lines that start with the year.
                if not line or not line[0].isdigit():
                    continue

                # Expected parts: ['2025/01/01', 'Wed', '12:01', 'AM', '8.81', '269', 'H']
                parts = line.split()

                if len(parts) < 6:
                    DEBUG_MESSAGES.append(f"WARNING: Skipping malformed tide data line in {filepath}: {line}")
                    continue

                try:
                    # Correctly parse the data row based on the actual file format
                    date_str = parts[0]
                    time_str = parts[2]
                    am_pm_str = parts[3]
                    height_str = parts[4]
                    type_str = parts[-1] # The 'H' or 'L' is the last element

                    datetime_to_parse = f"{date_str} {time_str} {am_pm_str}"
                    tide_dt_obj = datetime.datetime.strptime(datetime_to_parse, "%Y/%m/%d %I:%M %p")
                    current_date = tide_dt_obj.date()

                    # Only process data within the requested date range
                    if begin_date <= current_date <= end_date:
                        tide_info = {
                            'type': type_str.upper(),
                            'time': tide_dt_obj.time(),
                            'height': float(height_str)
                        }
                        grouped_tides.setdefault(current_date, []).append(tide_info)

                except (ValueError, IndexError) as e:
                    DEBUG_MESSAGES.append(f"WARNING: Error processing tide data line in {filepath}: '{line}' - {e}")
                    continue
    except FileNotFoundError:
        DEBUG_MESSAGES.append(f"ERROR: Local tide file not found: {filepath}")
    except Exception as e:
        DEBUG_MESSAGES.append(f"ERROR: General error reading local tide file {filepath}: {e}")

    return grouped_tides

def fetch_noaa_tides_for_range(station_id, start_date, end_date):
    # Construct local file path
    local_filepath = f"tide_data/{station_id}_annual.txt" # Adjust folder name if different

    # --- MODIFIED LOGIC ---
    if os.path.exists(local_filepath):
        DEBUG_MESSAGES.append(f"DEBUG: Reading tides from local file: {local_filepath}")
        local_tides = _parse_annual_tide_file(local_filepath, start_date, end_date)
        
        # This is the crucial change: only return if the local file actually had data for the range.
        if local_tides:
            DEBUG_MESSAGES.append(f"DEBUG: Successfully loaded {len(local_tides)} days of tide data from local file.")
            return local_tides # Return the valid local data.
        else:
            # If the file exists but has no data for the date, log it and fall through to the API call.
            DEBUG_MESSAGES.append(f"WARNING: Local file {local_filepath} found but yielded no data for range. Falling back to API.")
    else:
        DEBUG_MESSAGES.append(f"DEBUG: Local tide file not found: {local_filepath}. Proceeding with NOAA API call.")
    # --- END MODIFIED LOGIC ---


    # --- Existing NOAA API call logic (modified slightly for consistency) ---
    start_str, end_str = start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")
    params = {
        "product": "predictions",
        "application": "ecm-boat-scheduler",
        "begin_date": start_str,
        "end_date": end_str,
        "datum": "MLLW",
        "station": station_id,
        "time_zone": "lst_ldt",
        "units": "english",
        "interval": "hilo", # Keep hilo for now, as that's what the local files represent
        "format": "json"
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
    }

    DEBUG_MESSAGES.append(f"DEBUG: Attempting NOAA API call for station {station_id}...")
    DEBUG_MESSAGES.append(f"DEBUG: NOAA API URL params: {params}")
    DEBUG_MESSAGES.append(f"DEBUG: Request Headers sent: {headers}")

    try:
        resp = requests.get(
            "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter",
            params=params,
            headers=headers,
            timeout=15
        )

        DEBUG_MESSAGES.append(f"DEBUG: NOAA API Response Status Code: {resp.status_code}")
        DEBUG_MESSAGES.append(f"DEBUG: NOAA API Response Headers: {dict(resp.headers)}")
        DEBUG_MESSAGES.append(f"DEBUG: NOAA API Raw Response Text (full): {resp.text}") # Log full text

        resp.raise_for_status()

        if not resp.text.strip():
            DEBUG_MESSAGES.append("WARNING: NOAA API returned 200 OK, but response body is empty. Cannot parse JSON.")
            return {}

        raw_json_response = resp.json()
        DEBUG_MESSAGES.append("DEBUG: Raw NOAA API JSON response (from .json()):")
        DEBUG_MESSAGES.append(json.dumps(raw_json_response, indent=2))

        predictions = raw_json_response.get("predictions", [])
        DEBUG_MESSAGES.append("DEBUG: 'predictions' extracted from NOAA response:")
        DEBUG_MESSAGES.append(json.dumps(predictions, indent=2))

        DEBUG_MESSAGES.append("🔍 NOAA raw predictions:")
        DEBUG_MESSAGES.append(json.dumps(predictions, indent=2))

        grouped_tides = {}
        for tide in predictions:
            if 't' in tide:
                tide_dt = datetime.datetime.strptime(tide["t"], "%Y-%m-%d %H:%M"); date_key = tide_dt.date()
                grouped_tides.setdefault(date_key, []).append({'type': tide["type"].upper(), 'time': tide_dt.time(), 'height': float(tide["v"])})
            else:
                DEBUG_MESSAGES.append(f"WARNING: Skipping tide entry due to missing 't' key: {tide}")

        return grouped_tides
    except requests.exceptions.Timeout:
        DEBUG_MESSAGES.append(f"ERROR: NOAA API request timed out for station {station_id}")
        return {}
    except requests.exceptions.RequestException as e:
        DEBUG_MESSAGES.append(f"ERROR: Failed to connect to NOAA API for station {station_id}: {e}")
        if 'resp' in locals():
            DEBUG_MESSAGES.append(f"DEBUG: Response text in RequestException: {resp.text[:500]}")
        return {}
    except json.JSONDecodeError as e:
        DEBUG_MESSAGES.append(f"ERROR: Failed to decode JSON from NOAA API for station {station_id}: {e}. Raw response text: {resp.text[:500]}")
        return {}
    except Exception as e:
        DEBUG_MESSAGES.append(f"ERROR: General error fetching tides for station {station_id}: {e}")
        return {}
        
def get_concise_tide_rule(ramp, boat):
    if ramp.tide_calculation_method == "AnyTide": return "Any Tide"
    if ramp.tide_calculation_method == "AnyTideWithDraftRule": return "Any Tide (<5' Draft)" if boat.draft_ft and boat.draft_ft < 5.0 else "3 hrs +/- High Tide (≥5' Draft)"
    return f"{float(ramp.tide_offset_hours1):g} hrs +/- HT" if ramp.tide_offset_hours1 else "Tide Rule N/A"

def calculate_ramp_windows(ramp, boat, tide_data, date):
    if ramp.tide_calculation_method == "AnyTide":
        return [{'start_time': datetime.time.min, 'end_time': datetime.time.max}]
    if ramp.tide_calculation_method == "AnyTideWithDraftRule" and boat.draft_ft and boat.draft_ft < 5.0:
        return [{'start_time': datetime.time.min, 'end_time': datetime.time.max}]

    # Determine offset_hours based on method and draft
    if ramp.tide_calculation_method == "HoursAroundHighTide_WithDraftRule":
        offset_hours = 3.5 if boat.draft_ft and boat.draft_ft < 5.0 else 3.0
    else:
        # IMPORTANT: Do not treat 0 offset as 'no rule'. It means 0 hours around HT.
        # Ensure it's a float; default to 0.0 if None
        offset_hours = float(ramp.tide_offset_hours1) if ramp.tide_offset_hours1 is not None else 0.0

    # If there's no tide data, we can't calculate windows around tides.
    if not tide_data:
        DEBUG_MESSAGES.append(f"DEBUG: No tide data available for {date} at ramp {ramp.ramp_name}.")
        return []

    # Calculate windows if there's tide data
    offset = datetime.timedelta(hours=offset_hours)
    
    # Filter for High Tides ('H' type) and create windows
    high_tide_windows = []
    for t in tide_data:
        if t['type'] == 'H':
            tide_dt_combined = datetime.datetime.combine(date, t['time'])
            window_start_time = (tide_dt_combined - offset).time()
            window_end_time = (tide_dt_combined + offset).time()
            high_tide_windows.append({'start_time': window_start_time, 'end_time': window_end_time})
            DEBUG_MESSAGES.append(f"DEBUG: Calculated tide window for {t['time']}: {window_start_time} - {window_end_time} (Offset: {offset_hours}h)")

    if not high_tide_windows:
        DEBUG_MESSAGES.append(f"DEBUG: No high tides found in tide_data for {date} or no windows generated.")

    return high_tide_windows

def get_final_schedulable_ramp_times(
    ramp_obj,
    boat_obj,
    date_to_check,
    all_tides,
    truck_id,
    truck_hours_schedule
):
    day_of_week = date_to_check.weekday()
    truck_hours = truck_hours_schedule.get(truck_id, {}).get(day_of_week)
    if not truck_hours:
        return []

    truck_open_dt  = datetime.datetime.combine(date_to_check, truck_hours[0])
    truck_close_dt = datetime.datetime.combine(date_to_check, truck_hours[1])

    if not ramp_obj:
        return [{'start_time': truck_hours[0], 'end_time': truck_hours[1], 'high_tide_times': [], 'tide_rule_concise': 'N/A'}]

    tide_data_for_day = all_tides.get(date_to_check, [])
    tidal_windows = calculate_ramp_windows(ramp_obj, boat_obj, tide_data_for_day, date_to_check)
    
    all_schedulable_slots = []

    for t_win in tidal_windows:
        tidal_start_dt = datetime.datetime.combine(date_to_check, t_win['start_time'])
        tidal_end_dt   = datetime.datetime.combine(date_to_check, t_win['end_time'])

        if tidal_start_dt > tidal_end_dt:
            tidal_end_dt += datetime.timedelta(days=1)

        overlap_start = max(tidal_start_dt, truck_open_dt)
        overlap_end   = min(tidal_end_dt,   truck_close_dt)

        if overlap_start < overlap_end:
            # Create a dictionary to hold the debug trace for this specific window
            debug_trace = {
                "Truck Shift": f"{truck_open_dt.strftime('%I:%M %p')} - {truck_close_dt.strftime('%I:%M %p')}",
                "Tide Window": f"{tidal_start_dt.strftime('%I:%M %p')} - {tidal_end_dt.strftime('%I:%M %p')}",
                "Overlap Found": f"{overlap_start.strftime('%I:%M %p')} - {overlap_end.strftime('%I:%M %p')}",
                "Comparison": f"{overlap_start} < {overlap_end} = {overlap_start < overlap_end}"
            }
            
            all_schedulable_slots.append({
                'start_time'       : overlap_start.time(),
                'end_time'         : overlap_end.time(),
                'high_tide_times'  : [t['time'] for t in tide_data_for_day if t['type'] == 'H'],
                'tide_rule_concise': get_concise_tide_rule(ramp_obj, boat_obj),
                'debug_trace'      : debug_trace # Attach the debug info
            })

    return all_schedulable_slots

def get_suitable_trucks(boat_len, pref_truck_id=None, force_preferred=False):
    all_suitable = [t for t in ECM_TRUCKS.values() if not t.is_crane and t.max_boat_length is not None and boat_len <= t.max_boat_length]
    if force_preferred and pref_truck_id and any(t.truck_name == pref_truck_id for t in all_suitable):
        return [t for t in all_suitable if t.truck_name == pref_truck_id]
    return all_suitable

def _diagnose_failure_reasons(req_date, customer, boat, ramp_obj, service_type, truck_hours, manager_override, force_preferred_truck):
    """A modified version of the function with step-by-step debugging output."""
    # Use a local list to store messages for this specific run.
    diag_messages = []
    
    diag_messages.append("--- Failure Analysis ---")
    diag_messages.append(f"Debugging for: {req_date.strftime('%A, %Y-%m-%d')}")

    # Step 1: Find all trucks suitable for the boat's size.
    suitable_trucks = get_suitable_trucks(boat.boat_length, boat.preferred_truck_id, force_preferred_truck)
    diag_messages.append("**Step 1: Suitable Trucks**")
    diag_messages.append(json.dumps([t.truck_name for t in suitable_trucks], indent=2))

    if not suitable_trucks:
        diag_messages.append(f"**Boat Too Large:** No trucks in the fleet are rated for a boat of {boat.boat_length}ft.")
        return diag_messages

    # Step 2: Check which of those trucks are on duty.
    trucks_on_duty = {t.truck_name: truck_hours.get(t.truck_id, {}).get(req_date.weekday()) for t in suitable_trucks}
    diag_messages.append("**Step 2: Duty Status** (Should have time values)")
    diag_messages.append(json.dumps({k: str(v) if v else "Off Duty" for k, v in trucks_on_duty.items()}, indent=2))
    
    if not any(trucks_on_duty.values()):
        diag_messages.append(f"**No Trucks on Duty:** No suitable trucks are scheduled to work on {req_date.strftime('%A, %B %d')}.")
        return diag_messages

    # Step 3 & 4 are only relevant if a ramp is selected.
    if ramp_obj:
        # Step 3: Fetch tide predictions from NOAA.
        all_tides = fetch_noaa_tides_for_range(ramp_obj.noaa_station_id, req_date, req_date)
        tides_for_day = all_tides.get(req_date, [])
        diag_messages.append("**Step 3: Fetched Tides** (High Tides for today)")
        json_friendly_high_tides = [
            {'type': t['type'], 'time': str(t['time']), 'height': t['height']}
            for t in tides_for_day if t['type'] == 'H'
        ]
        diag_messages.append(json.dumps(json_friendly_high_tides, indent=2))

        # Step 5: Check for overlap between tide windows and each truck's shift.
        final_windows_found = False
        diag_messages.append("**Step 4 & 5: Overlap Calculation**")
        for truck in suitable_trucks:
            if trucks_on_duty.get(truck.truck_name):
                final_windows = get_final_schedulable_ramp_times(ramp_obj, boat, req_date, all_tides, truck.truck_id, truck_hours)
                diag_messages.append(f" - Overlap for **{truck.truck_name}**: `{len(final_windows)}` valid window(s) found.")
                if final_windows:
                    final_windows_found = True
        
        if not final_windows_found:
            diag_messages.append("**Tide Conditions Not Met:** No valid tide windows overlap with available truck working hours on this date.")
            return diag_messages

    # If we get here, it means trucks are on duty and tide windows are fine.
    diag_messages.append("**All Slots Booked:** All available time slots for suitable trucks are already taken on this date.")
    return diag_messages


def _compile_truck_schedules(jobs):
    schedule = {}
    for job in jobs:
        if job.job_status != "Scheduled": continue
        if hauler_id := getattr(job, 'assigned_hauling_truck_id', None):
            schedule.setdefault(hauler_id, []).append((job.scheduled_start_datetime, job.scheduled_end_datetime))
        if (crane_id := getattr(job, 'assigned_crane_truck_id', None)) and hasattr(job, 'j17_busy_end_datetime') and job.j17_busy_end_datetime:
            schedule.setdefault(crane_id, []).append((job.scheduled_start_datetime, job.j17_busy_end_datetime))
    return schedule

def check_truck_availability_optimized(truck_id, start_dt, end_dt, compiled_schedule):
    for busy_start, busy_end in compiled_schedule.get(truck_id, []):
        if start_dt < busy_end and end_dt > busy_start: return False
    return True

def find_available_job_slots(customer_id, boat_id, service_type, requested_date_str, selected_ramp_id=None, force_preferred_truck=True, num_suggestions_to_find=5, manager_override=False, crane_look_back_days=7, crane_look_forward_days=60, truck_operating_hours=None, prioritize_sailboats=True, **kwargs):
    truck_operating_hours = truck_operating_hours or TRUCK_OPERATING_HOURS
    try:
        requested_date = datetime.datetime.strptime(requested_date_str, '%Y-%m-%d').date()
    except ValueError:
        return [], "Error: Invalid date format.", [], False
    
    customer = get_customer_details(customer_id)
    boat = get_boat_details(boat_id)
    
    if not customer: return [], "Invalid Customer ID.", ["Customer could not be found in the system."], False
    if not boat: return [], "Sorry, no boat found for this customer.", ["A valid customer was found, but they do not have a boat linked to their account."], False
    
    ramp_obj = get_ramp_details(selected_ramp_id)
    rules = BOOKING_RULES.get(boat.boat_type, {})
    hauler_duration = timedelta(minutes=rules.get('truck_mins', 90))
    j17_duration = timedelta(minutes=rules.get('crane_mins', 0))
    needs_j17 = j17_duration.total_seconds() > 0
    suitable_trucks = get_suitable_trucks(boat.boat_length, boat.preferred_truck_id, force_preferred_truck)
    
    all_found_slots = []
    search_start_date = kwargs.get('strict_start_date') or (requested_date - timedelta(days=crane_look_back_days))
    search_end_date = kwargs.get('strict_end_date') or (requested_date + timedelta(days=crane_look_forward_days))
    
    all_tides = fetch_noaa_tides_for_range(ramp_obj.noaa_station_id, search_start_date, search_end_date) if ramp_obj else {}
    compiled_schedule = _compile_truck_schedules(SCHEDULED_JOBS)

    for i in range((search_end_date - search_start_date).days + 1):
        check_date = search_start_date + timedelta(days=i)
        for truck in suitable_trucks:
            windows = get_final_schedulable_ramp_times(ramp_obj, boat, check_date, all_tides, truck.truck_id, truck_operating_hours)
            
            # --- START of REPLACED BLOCK ---
            # This entire 'for window in windows:' loop is replaced with a more reliable structure.
            for window in windows:
                # Initialize slot_start_dt with the window's start time
                slot_start_dt = datetime.datetime.combine(check_date, window['start_time'])

                while slot_start_dt + hauler_duration <= datetime.datetime.combine(check_date, window['end_time']):
                    # Check truck availability for the current slot
                    if not check_truck_availability_optimized(truck.truck_name, slot_start_dt, slot_start_dt + hauler_duration, compiled_schedule):
                        # If unavailable, advance by 15 mins and check again
                        slot_start_dt += timedelta(minutes=15)
                        continue

                    # Check crane availability if needed
                    if needs_j17 and not check_truck_availability_optimized("J17", slot_start_dt, slot_start_dt + j17_duration, compiled_schedule):
                        # If unavailable, advance by 15 mins and check again
                        slot_start_dt += timedelta(minutes=15)
                        continue
                    
                    # If the slot is valid, append it to the list
                    all_found_slots.append({
                        'date': check_date, 
                        'time': slot_start_dt.time(), # Use the time from the datetime object
                        'truck_id': truck.truck_name, 
                        'j17_needed': needs_j17, 
                        'ramp_id': selected_ramp_id,
                        'tide_rule_concise': window.get('tide_rule_concise', 'N/A'), 
                        'high_tide_times': window.get('high_tide_times', [])
                    })

                    # Advance to the next 30-minute interval for the next potential slot
                    slot_start_dt += timedelta(minutes=30)
            # --- END of REPLACED BLOCK ---

    if not all_found_slots:
        return [], "No suitable slots could be found.", _diagnose_failure_reasons(requested_date, customer, boat, ramp_obj, service_type, truck_operating_hours, manager_override, force_preferred_truck), False
    
    # This block now correctly sorts the slots before returning
    all_found_slots.sort(key=lambda s: (abs(s['date'] - requested_date), s['time']))
    
    final_slots = []
    seen_dates = set()
    for slot in all_found_slots:
        if len(final_slots) >= num_suggestions_to_find: break
        if slot['date'] not in seen_dates:
            final_slots.append(slot)
            seen_dates.add(slot['date'])
            
    return final_slots, f"Found {len(final_slots)} best available slots.", [], False

def confirm_and_schedule_job(original_request, selected_slot, parked_job_to_remove=None):
    try:
        customer = get_customer_details(original_request['customer_id'])
        boat = get_boat_details(original_request['boat_id'])
        selected_ramp_id = selected_slot.get('ramp_id')
        pickup_addr, dropoff_addr, pickup_rid, dropoff_rid = "", "", None, None
        service_type = original_request['service_type']
        
        if service_type == "Launch":
            pickup_addr = boat.storage_address
            dropoff_rid = selected_ramp_id or boat.preferred_ramp_id
            dropoff_addr = get_ramp_details(dropoff_rid).ramp_name if dropoff_rid else ""
        elif service_type == "Haul":
            pickup_rid = selected_ramp_id or boat.preferred_ramp_id
            pickup_addr = get_ramp_details(pickup_rid).ramp_name if pickup_rid else ""
            dropoff_addr = boat.storage_address
        
        start_dt = datetime.datetime.combine(selected_slot['date'], selected_slot['time'])
        rules = BOOKING_RULES.get(boat.boat_type, {})
        crane_is_required = rules.get('crane_mins', 0) > 0
        hauler_end_dt = start_dt + timedelta(minutes=rules.get('truck_mins', 90))
        j17_end_dt = start_dt + timedelta(minutes=rules.get('crane_mins', 0)) if crane_is_required else None

        new_job = Job(
            customer_id=customer.customer_id, boat_id=boat.boat_id, service_type=service_type,
            scheduled_start_datetime=start_dt, scheduled_end_datetime=hauler_end_dt,
            assigned_hauling_truck_id=selected_slot['truck_id'],
            assigned_crane_truck_id="J17" if crane_is_required else None,
            j17_busy_end_datetime=j17_end_dt, pickup_ramp_id=pickup_rid, dropoff_ramp_id=dropoff_rid,
            job_status="Scheduled", pickup_street_address=pickup_addr, dropoff_street_address=dropoff_addr
        )
        
        SCHEDULED_JOBS.append(new_job)
        save_job(new_job)
        
        if parked_job_to_remove and parked_job_to_remove in PARKED_JOBS:
            del PARKED_JOBS[parked_job_to_remove]
            delete_job_from_db(parked_job_to_remove)
            
        message = f"SUCCESS: Job #{new_job.job_id} for {customer.customer_name} scheduled for {start_dt.strftime('%A, %b %d at %I:%M %p')}."
        return new_job.job_id, message
        
    except Exception as e:
        return None, f"An error occurred: {e}"

def calculate_scheduling_stats(all_customers, all_boats, scheduled_jobs):
    today = datetime.date.today()
    total_all_boats = len(all_boats)
    scheduled_customer_ids = {j.customer_id for j in scheduled_jobs if j.job_status == "Scheduled"}
    launched_customer_ids = {j.customer_id for j in scheduled_jobs if j.job_status == "Scheduled" and j.service_type == "Launch" and j.scheduled_start_datetime.date() < today}
    ecm_customer_ids = {boat.customer_id for boat in all_boats.values() if boat.is_ecm_boat}
    return {
        'all_boats': {'total': total_all_boats, 'scheduled': len(scheduled_customer_ids), 'launched': len(launched_customer_ids)},
        'ecm_boats': {'total': len(ecm_customer_ids), 'scheduled': len(scheduled_customer_ids.intersection(ecm_customer_ids)), 'launched': len(launched_customer_ids.intersection(ecm_customer_ids))}
    }
