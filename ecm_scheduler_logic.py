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
from datetime import timedelta, time, timezone
from collections import Counter
import streamlit as st # Ensure this is imported to access st.secrets
from geopy.geocoders import GoogleV3 # Change Nominatim to GoogleV3

Maps_API_KEY = st.secrets.get("Maps_API_KEY")
if not Maps_API_KEY:
    DEBUG_MESSAGES.append("ERROR: Google Maps API Key not found in Streamlit Secrets. Geocoding and Travel Time will likely fail.")
    # Optionally, raise an error or use a fallback here if the key is mandatory for app function

_geolocator = GoogleV3(api_key=Maps_API_KEY, user_agent="ecm_boat_scheduler_app")

_location_coords_cache = {} # Ensure this line is present here

DEBUG_MESSAGES = []


# --- DATA MODELS (CLASSES) ---
class Truck:
    def __init__(self, t_id, name, max_len):
        self.truck_id, self.truck_name, self.max_boat_length, self.is_crane = t_id, name, max_len, "Crane" in name

class Ramp:
    def __init__(self, r_id, name, station, tide_method="AnyTide", offset=None, boats=None, latitude=None, longitude=None): # <--- ADD latitude, longitude
        self.ramp_id, self.ramp_name, self.noaa_station_id, self.tide_calculation_method = r_id, name, station, tide_method
        self.tide_offset_hours1, self.allowed_boat_types = offset, boats or ["Powerboat", "Sailboat DT", "Sailboat MT"]
        self.latitude = float(latitude) if latitude is not None else None # Convert to float
        self.longitude = float(longitude) if longitude is not None else None # Convert to float

class Customer:
    def __init__(self, c_id, name):
        self.customer_id = int(c_id)
        self.customer_name = name

class Boat:
    def __init__(self, b_id, c_id, b_type, b_len, draft, storage_addr, pref_ramp, pref_truck, is_ecm, storage_latitude=None, storage_longitude=None): # <--- ADD storage_latitude, storage_longitude
        self.boat_id = int(b_id)
        self.customer_id = int(c_id)
        self.boat_type = b_type
        self.boat_length = b_len
        self.draft_ft = draft
        self.storage_address = storage_addr
        self.preferred_ramp_id = pref_ramp
        self.preferred_truck_id = pref_truck
        self.is_ecm_boat = is_ecm
        self.storage_latitude = float(storage_latitude) if storage_latitude is not None else None # Convert to float
        self.storage_longitude = float(storage_longitude) if storage_longitude is not None else None # Convert to float

class Job:
    def __init__(self, **kwargs):
        # This internal helper accepts existing datetime objects OR parses them if they are strings.
        def _parse_or_get_datetime(dt_value):
            if isinstance(dt_value, datetime.datetime):
                return dt_value
            if isinstance(dt_value, str):
                try:
                    return datetime.datetime.fromisoformat(dt_value.replace(" ", "T"))
                except (ValueError, TypeError):
                    return None
            return None # Return None for other types like None, int, etc.

        def _parse_int(int_string):
            if not int_string: return None
            try: return int(int_string)
            except (ValueError, TypeError): return None

        self.job_id = _parse_int(kwargs.get("job_id"))
        self.customer_id = _parse_int(kwargs.get("customer_id"))
        self.boat_id = _parse_int(kwargs.get("boat_id"))
        self.service_type = kwargs.get("service_type")
        self.scheduled_start_datetime = _parse_or_get_datetime(kwargs.get("scheduled_start_datetime"))
        self.scheduled_end_datetime = _parse_or_get_datetime(kwargs.get("scheduled_end_datetime"))
        self.assigned_hauling_truck_id = kwargs.get("assigned_hauling_truck_id")
        self.assigned_crane_truck_id = kwargs.get("assigned_crane_truck_id")
        self.j17_busy_end_datetime = _parse_or_get_datetime(kwargs.get("j17_busy_end_datetime"))
        self.pickup_ramp_id = kwargs.get("pickup_ramp_id")
        self.dropoff_ramp_id = kwargs.get("dropoff_ramp_id")
        self.pickup_street_address = kwargs.get("pickup_street_address", "")
        self.dropoff_street_address = kwargs.get("dropoff_street_address", "")
        self.job_status = kwargs.get("job_status", "Scheduled")
        self.notes = kwargs.get("notes", "")
        # <--- ADD THESE NEW LINES ---
        self.pickup_latitude = float(kwargs.get("pickup_latitude")) if kwargs.get("pickup_latitude") is not None else None
        self.pickup_longitude = float(kwargs.get("pickup_longitude")) if kwargs.get("pickup_longitude") is not None else None
        self.dropoff_latitude = float(kwargs.get("dropoff_latitude")) if kwargs.get("dropoff_longitude") is not None else None
        self.dropoff_longitude = float(kwargs.get("dropoff_longitude")) if kwargs.get("dropoff_longitude") is not None else None
        # <--- END NEW LINES ---

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
        for job in all_jobs:
            if not isinstance(job.scheduled_start_datetime, (datetime.datetime, type(None))):
                print(f"ERROR: Job {job.job_id} has non-datetime scheduled_start_datetime: {type(job.scheduled_start_datetime)} - {job.scheduled_start_datetime}")
            if not isinstance(job.scheduled_end_datetime, (datetime.datetime, type(None))):
                print(f"ERROR: Job {job.job_id} has non-datetime scheduled_end_datetime: {type(job.scheduled_end_datetime)} - {job.scheduled_end_datetime}")
            if job.j17_busy_end_datetime is not None and not isinstance(job.j17_busy_end_datetime, datetime.datetime):
                print(f"ERROR: Job {job.job_id} has non-datetime j17_busy_end_datetime: {type(job.j17_busy_end_datetime)} - {job.j17_busy_end_datetime}")
        
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

        TRUCK_OPERATING_HOURS.clear()
        TRUCK_OPERATING_HOURS.update(processed_schedules)

        # --- NEW: PROACTIVELY GEOCDE COMMON LOCATIONS ---
        DEBUG_MESSAGES.append("DEBUG: Pre-geocoding common locations...")
        # Geocode Yard Address
        _ = get_location_coords(address=YARD_ADDRESS)

        # Geocode all ramps
        for ramp_id, ramp_obj in ECM_RAMPS.items():
            _ = get_location_coords(ramp_id=ramp_id)
        
        # Geocode all boat storage addresses
        for boat_id, boat_obj in LOADED_BOATS.items():
            if boat_obj.storage_address:
                _ = get_location_coords(address=boat_obj.storage_address)
        
        # Geocode addresses from all jobs (pickup/dropoff streets)
        for job in all_jobs: # Use the 'all_jobs' list that was just loaded
            if job.pickup_street_address:
                _ = get_location_coords(address=job.pickup_street_address)
            if job.dropoff_street_address:
                _ = get_location_coords(address=job.dropoff_street_address)
        DEBUG_MESSAGES.append("DEBUG: Pre-geocoding complete.")
        # --- END NEW BLOCK ---
        
        # Convert times to string for JSON serialization (this part is informational/for logging, not functional)
        json_friendly_processed_schedules = {
            k: {d: f"{s.strftime('%H:%M')}-{e.strftime('%H:%M')}" for d, (s, e) in v.items()}
            for k, v in processed_schedules.items()
        }
        json_friendly_truck_operating_hours = {
            k: {d: f"{s.strftime('%H:%M')}-{e.strftime('%H:%M')}" for d, (s, e) in v.items()}
            for k, v in TRUCK_OPERATING_HOURS.items()
        }

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

    # Create a new dictionary for the database payload
    payload = {}
    for key, value in job_dict.items():
        # Check if the value is a datetime object
        if isinstance(value, datetime.datetime):
            # Convert it to an ISO 8601 formatted string
            payload[key] = value.isoformat()
        else:
            # Otherwise, keep the value as is
            payload[key] = value

    job_id = payload.get('job_id')
    try:
        if job_id and any(j.job_id == job_id for j in SCHEDULED_JOBS + list(PARKED_JOBS.values())):
            # Use the corrected payload for updates
            update_data = {k: v for k, v in payload.items() if k != 'job_id'}
            conn.table("jobs").update(update_data).eq("job_id", job_id).execute()
        else:
            # Use the corrected payload for inserts
            insert_data = {k: v for k, v in payload.items() if k != 'job_id'}
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

# Initialize the geolocator globally to avoid re-initializing on every call
# Use a specific user_agent string
_geolocator = Nominatim(user_agent="ecm_boat_scheduler_app")
_location_coords_cache = {} 

def get_location_coords(address=None, ramp_id=None):
    """
    Returns (latitude, longitude) for a given address or ramp.
    Uses a cache to avoid repeated geocoding requests.
    """
    cache_key = f"address:{address}" if address else f"ramp_id:{ramp_id}"
    if cache_key in _location_coords_cache:
        return _location_coords_cache[cache_key]

    coords = None
    if address:
        try:
            location = _geolocator.geocode(address + ", Pembroke, MA 02359") # Assume Pembroke if incomplete, or full address
            if location:
                coords = (location.latitude, location.longitude)
        except Exception as e:
            DEBUG_MESSAGES.append(f"ERROR: Geocoding address '{address}' failed: {e}")
    elif ramp_id:
        ramp_obj = get_ramp_details(ramp_id)
        if ramp_obj and ramp_obj.ramp_name:
            # You might need to refine ramp_name to a full address for better geocoding
            full_ramp_address = f"{ramp_obj.ramp_name}, {HOME_BASE_TOWN}, MA" # Adjust as needed for ramp accuracy
            try:
                location = _geolocator.geocode(full_ramp_address)
                if location:
                    coords = (location.latitude, location.longitude)
            except Exception as e:
                DEBUG_MESSAGES.append(f"ERROR: Geocoding ramp '{ramp_obj.ramp_name}' failed: {e}")
    
    if coords:
        _location_coords_cache[cache_key] = coords
    else:
        # Fallback for un-geocodable locations (e.g., return a default yard location)
        DEBUG_MESSAGES.append(f"WARNING: Could not geocode {address or ramp_id}. Returning default yard coords.")
        # Make sure YARD_ADDRESS is geocoded once and its coords stored as a constant
        if 'YARD_COORDS' not in globals(): # Ensure YARD_COORDS is defined and geocoded once
            try:
                yard_location = _geolocator.geocode(YARD_ADDRESS)
                globals()['YARD_COORDS'] = (yard_location.latitude, yard_location.longitude) if yard_location else (42.0833, -70.7681) # Fallback to Pembroke lat/lon
            except Exception as e:
                DEBUG_MESSAGES.append(f"ERROR: Initial geocoding of YARD_ADDRESS failed: {e}. Using hardcoded default.")
                globals()['YARD_COORDS'] = (42.0833, -70.7681) # Hardcoded Pembroke Lat/Lon

        coords = globals().get('YARD_COORDS', (42.0833, -70.7681)) # Fallback if YARD_COORDS somehow not set
        _location_coords_cache[cache_key] = coords # Cache the fallback too

    return coords

def calculate_travel_time(coords1, coords2):
    """
    Estimates travel time in minutes based on "as the crow flies" distance.
    This is a simplification; real travel time varies by roads, traffic, etc.
    Args:
        coords1 (tuple): (latitude, longitude) of start point.
        coords2 (tuple): (latitude, longitude) of end point.
    Returns:
        int: Estimated travel time in minutes.
    """
    if not coords1 or not coords2:
        return 0 # No travel time if coordinates are missing

    distance_miles = geodesic(coords1, coords2).miles
    
    # --- IMPORTANT: Calibrate this speed factor ---
    # This is a crucial assumption. A common average driving speed for estimation.
    # You'll need to adjust this based on typical speeds in your service area,
    # considering urban vs. rural driving, average truck speed, etc.
    # For example, if avg speed is 30 mph, then 1 mile takes 2 minutes.
    AVERAGE_SPEED_MPH = 25 # Example: 25 miles per hour
    
    if AVERAGE_SPEED_MPH <= 0: return 0

    travel_time_hours = distance_miles / AVERAGE_SPEED_MPH
    travel_time_minutes = int(travel_time_hours * 60)
    
    # Add a minimum travel time to account for setup/teardown or very short distances
    MIN_TRAVEL_TIME_MINUTES = 10 
    
    return max(travel_time_minutes, MIN_TRAVEL_TIME_MINUTES)

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
            # Changed this line to make the combined datetime timezone-aware (UTC)
            tide_dt_combined = datetime.datetime.combine(date, t['time'], tzinfo=timezone.utc)
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
    # New structure to track daily end locations: {truck_id: {date: (end_datetime, (lat, lon))}}
    daily_truck_last_location = {} 

    # Sort jobs by start time to accurately determine the *last* job on a given day
    # Ensure all scheduled_start_datetime are datetime objects with timezone for sorting
    sorted_jobs = sorted([j for j in jobs if j.scheduled_start_datetime], 
                         key=lambda j: j.scheduled_start_datetime)

    for job in sorted_jobs:
        if job.job_status != "Scheduled":
            continue

        job_date = job.scheduled_start_datetime.date()

        # Determine dropoff coordinates for the current job
        job_dropoff_coords = None
        if job.dropoff_street_address:
            job_dropoff_coords = get_location_coords(address=job.dropoff_street_address)
        elif job.dropoff_ramp_id:
            job_dropoff_coords = get_location_coords(ramp_id=job.dropoff_ramp_id)
        # Fallback to pickup if no dropoff is specified (e.g., if Transport job and dropoff is null)
        elif job.pickup_street_address:
             job_dropoff_coords = get_location_coords(address=job.pickup_street_address)
        elif job.pickup_ramp_id:
            job_dropoff_coords = get_location_coords(ramp_id=job.pickup_ramp_id)
        
        # If still no coords, default to yard (should log this as a data issue)
        if not job_dropoff_coords:
            DEBUG_MESSAGES.append(f"WARNING: Job {job.job_id} has no valid dropoff/pickup location for geocoding. Defaulting to yard coords for location tracking.")
            job_dropoff_coords = get_location_coords(address=YARD_ADDRESS) # Fallback to yard

        # Process hauling truck schedule and last location
        hauler_id = getattr(job, 'assigned_hauling_truck_id', None)
        if hauler_id and job.scheduled_start_datetime and job.scheduled_end_datetime:
            schedule.setdefault(hauler_id, []).append((job.scheduled_start_datetime, job.scheduled_end_datetime))
            
            current_last_for_hauler = daily_truck_last_location.get(hauler_id, {}).get(job_date)
            # Update last known location if this job ends later than the current last job for this truck on this day
            if not current_last_for_hauler or job.scheduled_end_datetime > current_last_for_hauler[0]:
                daily_truck_last_location.setdefault(hauler_id, {})[job_date] = \
                    (job.scheduled_end_datetime, job_dropoff_coords) # (datetime, (lat, lon))

        # Process crane truck schedule and last location (if different end time or location logic)
        crane_id = getattr(job, 'assigned_crane_truck_id', None)
        crane_end_time = getattr(job, 'j17_busy_end_datetime', None)
        if crane_id and job.scheduled_start_datetime and crane_end_time:
            schedule.setdefault(crane_id, []).append((job.scheduled_start_datetime, crane_end_time))
            
            current_last_for_crane = daily_truck_last_location.get(crane_id, {}).get(job_date)
            # Update last known location if this crane job ends later
            if not current_last_for_crane or crane_end_time > current_last_for_crane[0]:
                daily_truck_last_location.setdefault(crane_id, {})[job_date] = \
                    (crane_end_time, job_dropoff_coords) # (datetime, (lat, lon))
    
    # Return both the time schedule and the daily last known locations
    return schedule, daily_truck_last_location

def check_truck_availability_optimized(truck_id, start_dt, end_dt, compiled_schedule):
    for busy_start, busy_end in compiled_schedule.get(truck_id, []):
        # ADD THIS PRINT STATEMENT
        print(f"DEBUG: Comparing start_dt (type: {type(start_dt)}, value: {start_dt}) "
              f"with busy_end (type: {type(busy_end)}, value: {busy_end})")
        print(f"DEBUG: Comparing end_dt (type: {type(end_dt)}, value: {end_dt}) "
              f"with busy_start (type: {type(busy_start)}, value: {busy_start})")

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
    
    # Capture both return values from _compile_truck_schedules
    compiled_schedule, daily_truck_last_location = _compile_truck_schedules(SCHEDULED_JOBS)
    # print(f"DEBUG: Sample of compiled_schedule: {compiled_schedule}") # Commenting this out to avoid excessive log output

    # Define YARD_COORDS globally or ensure it's initialized via get_location_coords
    # Best practice: geocode YARD_ADDRESS once at app startup or first use.
    # For now, let's ensure it's accessed correctly if get_location_coords sets it globally.
    _ = get_location_coords(address=YARD_ADDRESS) # Ensure YARD_COORDS is populated in global scope via its cache

    # Get all scheduled jobs for the current boat (for 30-day check)
    existing_jobs_for_boat = [
        j for j in SCHEDULED_JOBS
        if j.boat_id == boat_id and j.job_status == "Scheduled"
    ]

    for i in range((search_end_date - search_start_date).days + 1):
        check_date = search_start_date + timedelta(days=i)
        
        # Determine the pickup location for the new job
        new_job_pickup_coords = None
        if service_type == "Launch":
            new_job_pickup_coords = get_location_coords(address=boat.storage_address)
        elif service_type == "Haul":
            new_job_pickup_coords = get_location_coords(ramp_id=selected_ramp_id)
        
        # If pickup location can't be determined, skip or fallback
        if not new_job_pickup_coords:
            DEBUG_MESSAGES.append(f"WARNING: Could not determine pickup location for potential job on {check_date}. Skipping this date.")
            continue

        for truck in suitable_trucks:
            # Determine the truck's starting location and time for this check_date
            # This is where the truck is coming from *before* the potential new job
            last_job_info_for_day = daily_truck_last_location.get(truck.truck_id, {}).get(check_date)
            last_job_info_for_prev_day = daily_truck_last_location.get(truck.truck_id, {}).get(check_date - timedelta(days=1))

            truck_start_time_for_day = truck_operating_hours.get(truck.truck_id, {}).get(check_date.weekday())
            if not truck_start_time_for_day: continue # Truck not working today

            truck_effective_available_from_dt = datetime.datetime.combine(check_date, truck_start_time_for_day[0], tzinfo=timezone.utc)
            truck_current_coords = globals().get('YARD_COORDS', (42.0833, -70.7681)) # Default to yard

            # If there was a job yesterday, that's where the truck finished
            if last_job_info_for_prev_day:
                truck_current_coords = last_job_info_for_prev_day[1] # Lat/Lon of last dropoff from previous day
            
            # If there was a job earlier TODAY, that's where the truck is currently
            if last_job_info_for_day:
                # The truck is available from when its last job finished today
                truck_effective_available_from_dt = max(truck_effective_available_from_dt, last_job_info_for_day[0])
                truck_current_coords = last_job_info_for_day[1] # Lat/Lon of last dropoff today

            # Calculate deadhead travel time from truck's current location to new job's pickup location
            deadhead_travel_minutes = calculate_travel_time(truck_current_coords, new_job_pickup_coords)
            deadhead_timedelta = timedelta(minutes=deadhead_travel_minutes)
            
            windows = get_final_schedulable_ramp_times(ramp_obj, boat, check_date, all_tides, truck.truck_id, truck_operating_hours)
            
            for window in windows:
                proposed_slot_start_dt = datetime.datetime.combine(check_date, window['start_time'], tzinfo=timezone.utc)
                
                # Adjust the proposed start time by the deadhead travel time
                # The truck can't start the job until it has finished its previous task (if any) and arrived at pickup
                slot_start_dt_adjusted_by_travel = max(proposed_slot_start_dt, truck_effective_available_from_dt + deadhead_timedelta)

                # Now, use this adjusted_slot_start_dt for all checks
                slot_start_dt = slot_start_dt_adjusted_by_travel

                # Ensure the job duration (hauler_duration) fits within the window after deadhead
                if slot_start_dt + hauler_duration > datetime.datetime.combine(check_date, window['end_time'], tzinfo=timezone.utc):
                    continue # This window doesn't work with deadhead

                # --- 30-day check for the boat ---
                is_too_soon = False
                for existing_job in existing_jobs_for_boat:
                    current_job_id_being_rebooked = kwargs.get('original_job_id_being_rebooked')
                    
                    if existing_job.scheduled_start_datetime and \
                       existing_job.job_id != current_job_id_being_rebooked:
                        time_difference = abs(slot_start_dt - existing_job.scheduled_start_datetime)
                        if time_difference <= timedelta(days=30):
                            is_too_soon = True
                            DEBUG_MESSAGES.append(f"DEBUG: Slot for boat {boat_id} at {slot_start_dt} is within 30 days of existing job {existing_job.job_id} at {existing_job.scheduled_start_datetime}.")
                            break

                if is_too_soon:
                    slot_start_dt += timedelta(minutes=15) # Try the next 15-min increment within the same window
                    continue

                # --- Truck availability check (using the adjusted slot_start_dt) ---
                if not check_truck_availability_optimized(truck.truck_name, slot_start_dt, slot_start_dt + hauler_duration, compiled_schedule):
                    slot_start_dt += timedelta(minutes=15)
                    continue

                # --- Crane availability check (using the adjusted slot_start_dt) ---
                if needs_j17 and not check_truck_availability_optimized("J17", slot_start_dt, slot_start_dt + j17_duration, compiled_schedule):
                    slot_start_dt += timedelta(minutes=15)
                    continue
                
                all_found_slots.append({
                    'date': check_date, 
                    'time': slot_start_dt.time(),
                    'truck_id': truck.truck_name, 
                    'j17_needed': needs_j17, 
                    'ramp_id': selected_ramp_id,
                    'tide_rule_concise': window.get('tide_rule_concise', 'N/A'), 
                    'high_tide_times': window.get('high_tide_times', []),
                    'debug_trace': {
                        **window.get('debug_trace', {}), # Keep existing window debug
                        'deadhead_travel_minutes': deadhead_travel_minutes,
                        'truck_start_coords': truck_current_coords,
                        'new_job_pickup_coords': new_job_pickup_coords
                    }
                })

                slot_start_dt += timedelta(minutes=30) # Move to the next potential start time

    # ... (rest of the find_available_job_slots function, including scoring and sorting) ...
    # --- New Unified Scoring System (add deadhead penalty here if desired) ---
    for slot in all_found_slots:
        score = 0
        score_trace = {}

        # 1. Crane Efficiency Bonus (Highest Priority)
        if needs_j17:
            is_crane_at_ramp = any(
                j.scheduled_start_datetime and j.scheduled_start_datetime.date() == slot['date'] and
                (j.pickup_ramp_id == slot.get('ramp_id') or j.dropoff_ramp_id == slot.get('ramp_id')) and
                j.assigned_crane_truck_id == 'J17'
                for j in SCHEDULED_JOBS
            )
            if is_crane_at_ramp:
                crane_bonus = 1000
                score += crane_bonus
                score_trace['Crane Efficiency Bonus'] = f"+{crane_bonus} (Crane already at ramp)"

        # 2. Sailboat Tide Prioritization Bonus
        if boat.boat_type in ['Sailboat DT', 'Sailboat MT'] and prioritize_sailboats:
            high_tides = slot.get('high_tide_times', [])
            if high_tides:
                # Ensure noon_dt is timezone-aware
                noon_dt = datetime.datetime.combine(slot['date'], datetime.time(12, 0), tzinfo=timezone.utc)
                min_diff_seconds = min(abs((datetime.datetime.combine(slot['date'], t, tzinfo=timezone.utc) - noon_dt).total_seconds()) for t in high_tides)
                diff_hours = round(min_diff_seconds / 3600, 1)
                tide_bonus = max(0, 100 - (diff_hours * 10))
                score += tide_bonus
                score_trace['Sailboat Tide Bonus'] = f"+{tide_bonus:.0f} (High tide {diff_hours}h from noon)"

        # New: Deadhead penalty (lower score for more travel time)
        slot_deadhead_minutes = slot.get('debug_trace', {}).get('deadhead_travel_minutes', 0)
        deadhead_penalty = slot_deadhead_minutes * 2 # Adjust factor (e.g., 2 points per minute of deadhead)
        score -= deadhead_penalty
        score_trace['Deadhead Penalty'] = f"-{deadhead_penalty} ({slot_deadhead_minutes} min deadhead)"

        # 3. Date Proximity Penalty (Tie-Breaker)
        days_away = abs(slot['date'] - requested_date).days
        if days_away > 0:
            proximity_penalty = days_away * 5 # This is now less impactful than deadhead or crane/tide
            score -= proximity_penalty
            score_trace['Date Proximity Penalty'] = f"-{proximity_penalty} ({days_away} days away)"

        slot['score'] = score
        if 'debug_trace' in slot:
            slot['debug_trace']['score_calculation'] = score_trace
            slot['debug_trace']['FINAL_SCORE'] = score

    all_found_slots.sort(key=lambda s: s.get('score', 0), reverse=True)

    final_slots = all_found_slots[:num_suggestions_to_find]

    if not final_slots:
        # Pass the full compiled schedule info for better diagnosis if needed
        return [], "No suitable slots could be found.", _diagnose_failure_reasons(requested_date, customer, boat, ramp_obj, service_type, truck_operating_hours, manager_override, force_preferred_truck), False

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
        
        start_dt = datetime.datetime.combine(selected_slot['date'], selected_slot['time'], tzinfo=timezone.utc)
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
    
    # This line is corrected to handle cases where the start time might be None
    launched_customer_ids = {j.customer_id for j in scheduled_jobs if j.job_status == "Scheduled" and j.service_type == "Launch" and j.scheduled_start_datetime and j.scheduled_start_datetime.date() < today}
    
    ecm_customer_ids = {boat.customer_id for boat in all_boats.values() if boat.is_ecm_boat}
    return {
        'all_boats': {'total': total_all_boats, 'scheduled': len(scheduled_customer_ids), 'launched': len(launched_customer_ids)},
        'ecm_boats': {'total': len(ecm_customer_ids), 'scheduled': len(scheduled_customer_ids.intersection(ecm_customer_ids)), 'launched': len(launched_customer_ids.intersection(ecm_customer_ids))}
    }


def generate_random_jobs(num_to_gen, start_date, end_date, service_type_filter, truck_hours):
    """
    Generates a specified number of random, valid jobs within a date range.
    This is a developer tool for populating the schedule with test data.
    """
    if not LOADED_CUSTOMERS or not LOADED_BOATS:
        return "Cannot generate jobs: Customer or Boat data is not loaded."

    services_to_use = ["Launch", "Haul"] if service_type_filter == "All" else [service_type_filter]
    customer_list = list(LOADED_CUSTOMERS.values())
    date_range_days = (end_date - start_date).days
    success_count = 0
    failure_count = 0

    for _ in range(num_to_gen):
        try:
            # 1. Get a random customer and their boat
            random_customer = random.choice(customer_list)
            customer_boats = [b for b in LOADED_BOATS.values() if b.customer_id == random_customer.customer_id]
            if not customer_boats:
                continue
            random_boat = random.choice(customer_boats)

            # 2. Get a random service and date
            random_service = random.choice(services_to_use)
            random_date = start_date + timedelta(days=random.randint(0, date_range_days))
            
            # 3. Get a random ramp if needed
            random_ramp_id = None
            if random_service in ["Launch", "Haul"]:
                random_ramp_id = random.choice(list(ECM_RAMPS.keys()))

            # 4. Find an available slot
            slots, _, _, _ = find_available_job_slots(
                customer_id=random_customer.customer_id,
                boat_id=random_boat.boat_id,
                service_type=random_service,
                requested_date_str=random_date.strftime('%Y-%m-%d'),
                selected_ramp_id=random_ramp_id,
                force_preferred_truck=False, # Use any capable truck for random generation
                num_suggestions_to_find=1,
                truck_operating_hours=truck_hours
            )

            # 5. Confirm the job if a slot was found
            if slots:
                confirm_and_schedule_job(
                    original_request={
                        'customer_id': random_customer.customer_id,
                        'boat_id': random_boat.boat_id,
                        'service_type': random_service
                    },
                    selected_slot=slots[0]
                )
                success_count += 1
            else:
                failure_count += 1
        except Exception:
            failure_count += 1
    
    return f"Job generation complete. Successfully created: {success_count}. Failed to find slots for: {failure_count}."


