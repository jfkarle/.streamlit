import csv
import os
import datetime                 # your existing “import datetime”
import pandas as pd
import calendar
import requests
import random
import json
import streamlit as st
from st_supabase_connection import SupabaseConnection, execute_query
from datetime import timedelta, time, timezone
from collections import Counter, defaultdict   # pull in defaultdict here
from geopy.geocoders import Nominatim

import os
from supabase import create_client
from st_supabase_connection import SupabaseConnection, execute_query

st.sidebar.write("🔑 Available secrets: " + ", ".join(st.secrets.keys()))

def _log_debug(msg):
    """Adds a timestamped message to the global debug log."""
    # Ensure DEBUG_MESSAGES is treated as a global variable
    global DEBUG_MESSAGES
    DEBUG_MESSAGES.insert(0, f"{datetime.datetime.now().strftime('%H:%M:%S')}: {msg}")

# 1) Read whatever the UI handed you
raw_url = st.secrets["SUPA_URL"]
raw_key = st.secrets["SUPA_KEY"]

# 2) Clean them up into single lines
SUPA_URL = raw_url.strip()
SUPA_KEY = raw_key.strip().replace("\n", "")

# 3) Now initialize Supabase
supabase = create_client(SUPA_URL, SUPA_KEY)

# 4) Sanity check
resp = supabase.table("boats").select("*").execute()
st.sidebar.write(f"🔍 Loaded {len(resp.data or [])} boats from Supabase")
_geolocator = Nominatim(user_agent="ecm_boat_scheduler_app")
_location_coords_cache = {} # Ensure this line is present here

DEBUG_MESSAGES = []

def _log_debug(msg):
    """Adds a timestamped message to the global debug log."""
    DEBUG_MESSAGES.insert(0, f"{datetime.datetime.now().strftime('%H:%M:%S')}: {msg}")

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
        # This internal helper now ensures all datetimes are timezone-aware (UTC).
        def _parse_or_get_datetime(dt_value):
            dt = None # Start with a null datetime
            if isinstance(dt_value, datetime.datetime):
                dt = dt_value
            elif isinstance(dt_value, str):
                try:
                    # Parse the string, replacing space with T for ISO compatibility
                    dt = datetime.datetime.fromisoformat(dt_value.replace(" ", "T"))
                except (ValueError, TypeError):
                    return None # Return None if parsing fails

            # If we successfully parsed or received a datetime object...
            if dt:
                # Check if it's naive (no timezone info)
                if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
                    # If naive, assume it's UTC and make it aware.
                    return dt.replace(tzinfo=timezone.utc)
                # If it's already aware, return it as is.
                return dt
            
            # Return None if input was invalid
            return None

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
        self.S17_busy_end_datetime = _parse_or_get_datetime(kwargs.get("S17_busy_end_datetime"))
        self.pickup_ramp_id = kwargs.get("pickup_ramp_id")
        self.dropoff_ramp_id = kwargs.get("dropoff_ramp_id")
        self.pickup_street_address = kwargs.get("pickup_street_address", "")
        self.dropoff_street_address = kwargs.get("dropoff_street_address", "")
        self.job_status = kwargs.get("job_status", "Scheduled")
        self.notes = kwargs.get("notes", "")
        self.pickup_latitude = float(kwargs.get("pickup_latitude")) if kwargs.get("pickup_latitude") is not None else None
        self.pickup_longitude = float(kwargs.get("pickup_longitude")) if kwargs.get("pickup_longitude") is not None else None
        self.dropoff_latitude = float(kwargs.get("dropoff_latitude")) if kwargs.get("dropoff_longitude") is not None else None
        self.dropoff_longitude = float(kwargs.get("dropoff_longitude")) if kwargs.get("dropoff_longitude") is not None else None

# --- CONFIGURATION AND GLOBAL CONSTANTS ---
HOME_BASE_TOWN = "Pem"
SOUTH_ROUTE = ["Han", "Nor", "Sci", "Mar", "Dux", "Kin", "Ply", "Bou", "San"]
NORTH_ROUTE = ["Wey", "Hin", "Coh", "Hul", "Qui", "Bos"]
YARD_ADDRESS = "43 Mattakeesett St, Pembroke, MA 02359"
DEFAULT_TRUCK_OPERATING_HOURS = {
    "S20/33": { 0: (time(7, 0), time(15, 0)), 1: (time(7, 0), time(15, 0)), 2: (time(7, 0), time(15, 0)), 3: (time(7, 0), time(15, 0)), 4: (time(7, 0), time(15, 0)), 5: (time(8, 0), time(12, 0)), 6: None },
    "S21/77": { 0: (time(8, 0), time(16, 0)), 1: (time(8, 0), time(16, 0)), 2: (time(8, 0), time(16, 0)), 3: (time(8, 0), time(16, 0)), 4: (time(8, 0), time(16, 0)), 5: None, 6: None },
    "S23/55": { 0: (time(8, 0), time(17, 0)), 1: (time(8, 0), time(17, 0)), 2: (time(8, 0), time(17, 0)), 3: (time(8, 0), time(17, 0)), 4: (time(8, 0), time(17, 0)), 5: (time(7, 30), time(17, 30)), 6: None },
    "S17":    { 0: (time(8, 0), time(16, 0)), 1: (time(8, 0), time(16, 0)), 2: (time(8, 0), time(16, 0)), 3: (time(8, 0), time(16, 0)), 4: (time(8, 0), time(16, 0)), 5: None, 6: None }
}
BOOKING_RULES = {'Powerboat': {'truck_mins': 90, 'crane_mins': 0},'Sailboat DT': {'truck_mins': 180, 'crane_mins': 60},'Sailboat MT': {'truck_mins': 180, 'crane_mins': 90}}

# --- IN-MEMORY DATA CACHES ---
IDEAL_CRANE_DAYS = set() # ADD THIS LINE
CANDIDATE_CRANE_DAYS = { 'ScituateHarborJericho': [], 'PlymouthHarbor': [], 'WeymouthWessagusset': [], 'CohassetParkerAve': [] }
crane_daily_status = {}
ECM_TRUCKS, LOADED_CUSTOMERS, LOADED_BOATS, ECM_RAMPS, TRUCK_OPERATING_HOURS = {}, {}, {}, {}, {}
SCHEDULED_JOBS, PARKED_JOBS = [], {}

### This helper function will create a new crane day near the requested date if a grouped slot is not found.

def get_s17_truck_id():
    """Finds the numeric truck_id for the truck named 'S17'."""
    for truck_id, truck_obj in ECM_TRUCKS.items():
        if truck_obj.truck_name == "S17":
            return truck_id
    return None # Return None if S17 is not found

def find_new_crane_day_slot(
    boat,
    customer,
    ramp_obj,
    requested_date,
    trucks,
    duration,
    S17_duration,
    service_type,
    search_window_days=30
):
    """
    Finds the nearest available date to create a new S17 crane day
    and a new slot.
    """
    import datetime
    from .ecm_scheduler_shared import _check_and_create_slot_detail, get_tide_data_for_date_window
    from .ecm_scheduler_data import SCHEDULED_JOBS, ECM_TRUCKS
    from .ecm_scheduler_logic import get_ramp_details

    # Create a search order, starting from the requested date and moving outwards.
    search_order = _generate_day_search_order(requested_date, search_window_days, search_window_days)

    for check_date in search_order:
        # Check if a crane is already scheduled at this ramp on this date
        ramp_is_busy = any(
            (job.pickup_ramp_id == ramp_obj.ramp_id or job.dropoff_ramp_id == ramp_obj.ramp_id) and 
            job.scheduled_start_datetime.date() == check_date
            for job in SCHEDULED_JOBS if job.assigned_crane_truck_id == 'S17'
        )

        # Check if the date is a valid crane day for this ramp based on tides
        if not ramp_is_busy:
            tide_data = get_tide_data_for_date_window(ramp_obj, check_date, check_date)
            candidate_high_tides = [t for t in tide_data.get(check_date, []) if t.get('type') == 'H' and 10 <= t['time'].hour < 14]
            
            if candidate_high_tides:
                # We found a suitable date and time! Create the new slot.
                primary_high_tide = sorted(candidate_high_tides, key=lambda t: abs(t['time'].hour - 12))[0]
                
                new_slot_params = {
                    'date': check_date,
                    'time': primary_high_tide['time'],
                    'crane_day_id': 'NEW_CRANE_DAY', # Use a unique ID to signify a new crane day
                    'truck_id': 'S17',
                    'ramp_id': ramp_obj.ramp_id,
                    'service_type': service_type,
                    'is_new_crane_day': True,
                    'high_tide_times': [t['time'] for t in candidate_high_tides]
                }
                
                # Check if this new slot is actually viable
                new_slot = _check_and_create_slot_detail(
                    check_date,
                    'S17',
                    ramp_obj,
                    customer,
                    boat,
                    trucks,
                    duration,
                    S17_duration,
                    service_type,
                    start_time=primary_high_tide['time']
                )

                if new_slot:
                    return new_slot

    return None # If no suitable new slot is found within the search window.
    
def _generate_day_search_order(start_date, look_back, look_forward):
    """Generates a list of dates to check, starting from the center and expanding outwards."""
    search_order = [start_date]
    max_range = max(look_back, look_forward)
    for i in range(1, max_range + 1):
        if i <= look_forward:
            search_order.append(start_date + timedelta(days=i))
        if i <= look_back:
            search_order.append(start_date - timedelta(days=i))
    return search_order

# --- DATABASE PERSISTENCE FUNCTIONS ---
@st.cache_resource
def get_db_connection():
    return st.connection(
        "supabase",
        type=SupabaseConnection,
        url="https://knexrzljvagiwqstapnk.supabase.co",
        key="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtuZXhyemxqdmFnaXdxc3RhcG5rIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTIwODY0ODIsImV4cCI6MjA2NzY2MjQ4Mn0.hgWhtefyiEmGj5CERladOe3hMBM-rVnwMGNwrt8FT6Y"
    )

from datetime import date

def generate_crane_day_candidates(
    look_ahead_days: int = 60,
    tide_start_hour: int = 10,
    tide_end_hour: int = 14,
    start_date: date = None
) -> dict:
    """
    For each ramp, scan the next `look_ahead_days` for a high tide
    between `tide_start_hour` (inclusive) and `tide_end_hour` (exclusive).
    Returns a dict: { ramp_id: [ {date, time, height}, … ], … }.
    """
    if start_date is None:
        start_date = date.today()
    end_date = start_date + timedelta(days=look_ahead_days)

    candidates = {}
    for ramp_id, ramp in ECM_RAMPS.items():
        candidates[ramp_id] = []
        # fetch all tides for the window
        tides_by_date = fetch_noaa_tides_for_range(ramp.noaa_station_id, start_date, end_date)
        for d, events in sorted(tides_by_date.items()):
            # look for at least one high tide in your preferred window
            for ev in events:
                if ev["type"] == "H" and tide_start_hour <= ev["time"].hour < tide_end_hour:
                    candidates[ramp_id].append({
                        "date": d,
                        "time": ev["time"],
                        "height": ev.get("height", None)
                    })
                    break  # only one candidate per day
    return candidates

def job_is_within_date_range(job_row, current_date, days_to_consider=21):
    # CORRECTED to look for 'scheduled_start_datetime' instead of 'scheduled_date'
    job_date_str = job_row.get('scheduled_start_datetime')
    if not job_date_str:
        return False
    
    try:
        # Parse the ISO format string from the database
        job_date = datetime.datetime.fromisoformat(job_date_str.replace(" ", "T"))
    except (ValueError, TypeError):
        return False

    # Ensure the current date is timezone-aware for a correct comparison
    if current_date.tzinfo is None:
        current_date = current_date.replace(tzinfo=timezone.utc)

    return (current_date - timedelta(days=days_to_consider)) <= job_date <= (current_date + timedelta(days=days_to_consider))
    
def load_all_data_from_sheets():
    """Loads all data from Supabase, now including truck schedules."""
    global SCHEDULED_JOBS, PARKED_JOBS, LOADED_CUSTOMERS, LOADED_BOATS, ECM_TRUCKS, ECM_RAMPS, TRUCK_OPERATING_HOURS, CANDIDATE_CRANE_DAYS
    try:
        conn = get_db_connection()

        # --- Jobs ---
        # MODIFIED: Explicitly list all columns in the select query to bypass any potential schema caching issues.
        query_columns = (
            "job_id, customer_id, boat_id, service_type, "
            "scheduled_start_datetime, scheduled_end_datetime, "
            "assigned_hauling_truck_id, assigned_crane_truck_id, "
            "S17_busy_end_datetime, pickup_ramp_id, dropoff_ramp_id, "
            "pickup_street_address, dropoff_street_address, job_status, notes, "
            "pickup_latitude, pickup_longitude, dropoff_latitude, dropoff_longitude"
        )
        jobs_resp = execute_query(conn.table("jobs").select(query_columns), ttl=0)

        if isinstance(jobs_resp.data, list):
            # Load all jobs that have a start date
            all_jobs = [Job(**row) for row in jobs_resp.data if row.get('scheduled_start_datetime')]
        else:
            print(f"WARNING: jobs_resp.data was not a list: {jobs_resp.data}")
            all_jobs = []

        SCHEDULED_JOBS.clear() 
        SCHEDULED_JOBS.extend([job for job in all_jobs if job.job_status == "Scheduled"])
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

        name_to_id = {t.truck_name: t.truck_id for t in ECM_TRUCKS.values()}

        # --- Ramps ---
        ramps_resp = execute_query(conn.table("ramps").select("*"), ttl=0)
        ECM_RAMPS.clear()
        ECM_RAMPS.update({
            # V-- THIS IS THE FIX --V
            str(row["ramp_id"]): Ramp(
                r_id        = row["ramp_id"],
                name        = row.get("ramp_name"),
                station     = row.get("noaa_station_id"),
                tide_method = row.get("tide_calculation_method"),
                offset      = row.get("tide_offset_hours"),
                boats       = row.get("allowed_boat_types"),
                latitude    = row.get("latitude"),
                longitude   = row.get("longitude")
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
                b_id               = row["boat_id"],
                c_id               = row["customer_id"],
                b_type             = row.get("boat_type"),
                b_len              = row.get("boat_length"),
                draft              = row.get("draft_ft"),
                storage_addr       = row.get("storage_address", ""),
                pref_ramp          = row.get("preferred_ramp", ""),
                pref_truck         = row.get("preferred_truck", ""),
                is_ecm             = str(row.get("is_ecm_boat", "no")).lower() == 'yes',
                storage_latitude   = row.get("storage_latitude"),
                storage_longitude  = row.get("storage_longitude")
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
                continue
            day        = row["day_of_week"]
            start_time = datetime.datetime.strptime(row["start_time"], '%H:%M:%S').time()
            end_time   = datetime.datetime.strptime(row["end_time"],   '%H:%M:%S').time()
            processed_schedules.setdefault(truck_id, {})[day] = (start_time, end_time)

        TRUCK_OPERATING_HOURS.clear()
        TRUCK_OPERATING_HOURS.update(processed_schedules)
        precalculate_ideal_crane_days()
        
    except Exception as e:
        st.error(f"Error loading data: {e}")
        raise
        
def save_job(job_to_save):
    conn = get_db_connection()
    job_dict = job_to_save.__dict__

    # Build the payload, serializing datetimes to ISO strings
    payload = {}
    for key, value in job_dict.items():
        if isinstance(value, datetime.datetime):
            payload[key] = value.isoformat()
        else:
            payload[key] = value

    job_id = payload.get('job_id')
    try:
        if job_id and any(j.job_id == job_id for j in SCHEDULED_JOBS + list(PARKED_JOBS.values())):
            # UPDATE existing record
            update_data = {k: v for k, v in payload.items() if k != 'job_id'}
            conn.table("jobs").update(update_data).eq("job_id", job_id).execute()
        else:
            # INSERT new record
            insert_data = {k: v for k, v in payload.items() if k != 'job_id'}

            # tell PostgREST to return the newly created row(s)
            response = (
                conn
                .table("jobs")
                .insert(insert_data, returning="representation")
                .execute()
            )

            # grab the new job_id back out
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

def get_location_coords(address=None, ramp_id=None, job_id=None, job_type=None, boat_id=None, initial_latitude=None, initial_longitude=None):
    """
    Returns (latitude, longitude) for a given address/ramp/job location.
    Prioritizes database lookup, then caches in-memory, then geocodes using GoogleV3 and saves to DB.
    job_type can be 'pickup' or 'dropoff' for job-specific coordinates.
    boat_id is used for context when geocoding boat.storage_address.
    initial_latitude/initial_longitude: Pass existing lat/lon from loaded objects to prioritize.
    """
    conn = get_db_connection()

    # Determine the entity type and its primary ID for DB lookups and caching
    entity_type = None
    entity_id = None
    db_table = None
    lat_col, lon_col = None, None
    pk_col_name_in_db = None # The actual primary key column name in the database table

    if ramp_id:
        entity_type = "ramp"
        entity_id = ramp_id
        db_table = "ramps"
        lat_col, lon_col = "latitude", "longitude"
        pk_col_name_in_db = "ramp_id"
    elif job_id and job_type in ['pickup', 'dropoff']:
        entity_type = "job"
        entity_id = job_id
        db_table = "jobs"
        lat_col, lon_col = f"{job_type}_latitude", f"{job_type}_longitude"
        pk_col_name_in_db = "job_id"
    elif boat_id: # For boat storage addresses, we need boat_id to save to the boats table
        entity_type = "boat_storage"
        entity_id = boat_id
        db_table = "boats"
        lat_col, lon_col = "storage_latitude", "storage_longitude"
        pk_col_name_in_db = "boat_id"
    elif address == YARD_ADDRESS:
        entity_type = "yard" # Special case, usually not in a main data table
        pass # No entity_id or db_table lookup for yard here, handled by global YARD_COORDS

    cache_key = f"{entity_type}:{entity_id}:{job_type}" if entity_type else f"address:{address}"
    if cache_key in _location_coords_cache:
        DEBUG_MESSAGES.append(f"DEBUG: Found {cache_key} in in-memory cache.")
        return _location_coords_cache[cache_key]

    coords = None

    # 0. Check initial_latitude/longitude parameters first (from loaded objects)
    if initial_latitude is not None and initial_longitude is not None:
        coords = (float(initial_latitude), float(initial_longitude))
        DEBUG_MESSAGES.append(f"DEBUG: Using initial coords for {cache_key}: {coords}")
        _location_coords_cache[cache_key] = coords
        return coords

    # 1. Try to load from database (if applicable entity type)
    if entity_type and entity_id and db_table and lat_col and lon_col and pk_col_name_in_db:
        try:
            db_response = execute_query(conn.table(db_table).select(f"{lat_col}, {lon_col}").eq(pk_col_name_in_db, entity_id), ttl=60).data
            if db_response and len(db_response) > 0 and db_response[0].get(lat_col) is not None and db_response[0].get(lon_col) is not None:
                coords = (float(db_response[0][lat_col]), float(db_response[0][lon_col]))
                DEBUG_MESSAGES.append(f"DEBUG: Loaded coords for {cache_key} from DB: {coords}")
                _location_coords_cache[cache_key] = coords
                return coords
        except Exception as e:
            DEBUG_MESSAGES.append(f"ERROR: Failed to load coords from DB for {cache_key}: {e}")

    # 2. If not found in DB or cache, perform live geocoding using GoogleV3
    address_to_geocode = None
    if address: # This covers YARD_ADDRESS, and generic addresses
        address_to_geocode = address
    elif ramp_id:
        ramp_obj = get_ramp_details(ramp_id)
        if ramp_obj and ramp_obj.ramp_name:
            address_to_geocode = f"{ramp_obj.ramp_name}, MA, USA" # Google V3 prefers more complete addresses
    elif boat_id: 
        boat_obj = get_boat_details(boat_id)
        if boat_obj and boat_obj.storage_address:
            address_to_geocode = boat_obj.storage_address
    elif job_id: # For jobs, need to get the specific pickup/dropoff string address or ramp name
        job_obj = get_job_details(job_id) # Need to load job details here
        if job_obj:
            if job_type == 'pickup':
                address_to_geocode = job_obj.pickup_street_address
                if not address_to_geocode and job_obj.pickup_ramp_id: # Fallback to ramp name if no street address
                    ramp_details = get_ramp_details(job_obj.pickup_ramp_id)
                    address_to_geocode = ramp_details.ramp_name + ", MA, USA" if ramp_details else None
            elif job_type == 'dropoff':
                address_to_geocode = job_obj.dropoff_street_address
                if not address_to_geocode and job_obj.dropoff_ramp_id: # Fallback to ramp name if no street address
                    ramp_details = get_ramp_details(job_obj.dropoff_ramp_id)
                    address_to_geocode = ramp_details.ramp_name + ", MA, USA" if ramp_details else None

    if address_to_geocode: # Only attempt if there's an address string and API key
        try:
            location = _geolocator.geocode(address_to_geocode, timeout=10) # Set timeout for external call
            if location:
                coords = (location.latitude, location.longitude)
                DEBUG_MESSAGES.append(f"DEBUG: Geocoded '{address_to_geocode}' (Google) successfully: {coords}")
                
                # 3. Save to database (if applicable entity type)
                if coords and entity_type and entity_id and db_table and lat_col and lon_col and pk_col_name_in_db:
                    try:
                        update_data = {lat_col: coords[0], lon_col: coords[1]}
                        conn.table(db_table).update(update_data).eq(pk_col_name_in_db, entity_id).execute()
                        DEBUG_MESSAGES.append(f"DEBUG: Saved geocoded coords to DB for {cache_key}.")
                    except Exception as db_e:
                        DEBUG_MESSAGES.append(f"ERROR: Failed to save geocoded coords to DB for {cache_key}: {db_e}")
            else:
                DEBUG_MESSAGES.append(f"WARNING: Google Geocoding for '{address_to_geocode}' returned no results. Status: {location.raw.get('status') if location and location.raw else 'UNKNOWN'}")
        except Exception as e:
            DEBUG_MESSAGES.append(f"ERROR: Google Geocoding '{address_to_geocode}' failed: {type(e).__name__}: {e}")
    else:
        DEBUG_MESSAGES.append(f"WARNING: No address to geocode or missing API key for {cache_key}.")

    # 4. Fallback if geocoding failed (using previously geocoded YARD_COORDS or hardcoded)
    if not coords:
        DEBUG_MESSAGES.append(f"WARNING: Could not determine valid coords for {cache_key}. Returning default yard coords.")
        # Ensure YARD_COORDS is geocoded once on startup or first use.
        if 'YARD_COORDS' not in globals() or globals()['YARD_COORDS'] is None:
            DEBUG_MESSAGES.append(f"DEBUG: Attempting to geocode YARD_ADDRESS for fallback (using GoogleV3).")
            try:
                yard_location = _geolocator.geocode(YARD_ADDRESS, timeout=10)
                globals()['YARD_COORDS'] = (yard_location.latitude, yard_location.longitude) if yard_location else (42.0833, -70.7681) # Pembroke default
                DEBUG_MESSAGES.append(f"DEBUG: YARD_COORDS set to: {globals()['YARD_COORDS']}")
            except Exception as e:
                DEBUG_MESSAGES.append(f"ERROR: Initial geocoding of YARD_ADDRESS for fallback failed: {e}. Using hardcoded default.")
                globals()['YARD_COORDS'] = (42.0833, -70.7681)

        coords = globals().get('YARD_COORDS', (42.0833, -70.7681)) # Fallback if YARD_COORDS somehow not set
    
    # Cache in-memory regardless of source (DB, live geocode, or fallback)
    _location_coords_cache[cache_key] = coords
    return coords

def calculate_travel_time(origin_coords, destination_coords):
    """
    Estimates travel time in minutes based on straight-line distance.
    This function makes NO external API calls.
    """
    # If coordinates are missing, return a default travel time.
    if not origin_coords or not destination_coords:
        return 15 # Default travel time in minutes

    # Calculate straight-line distance in miles
    straight_line_distance = _calculate_distance_miles(origin_coords, destination_coords)

    # Estimate actual travel distance by applying a circuitry factor
    # (e.g., 1.3 means we estimate roads are 30% longer than a straight line)
    travel_distance_miles = straight_line_distance * 1.3

    # Estimate time based on an average speed (e.g., 35 mph)
    # (travel_distance / mph) gives hours, * 60 gives minutes
    average_speed_mph = 35
    travel_time_minutes = (travel_distance_miles / average_speed_mph) * 60

    # Ensure a minimum travel time for very short distances
    return max(10, int(travel_time_minutes))
    
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

def _calculate_distance_miles(coords1, coords2):
    """Calculates the Haversine distance between two lat/lon points in miles."""
    import math
    if not coords1 or not coords2:
        return float('inf') # Return a large number if coords are missing

    R = 3958.8 # Earth radius in miles
    lat1, lon1 = math.radians(coords1[0]), math.radians(coords1[1])
    lat2, lon2 = math.radians(coords2[0]), math.radians(coords2[1])

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    distance = R * c
    return distance

def _round_time_to_nearest_quarter_hour(dt):
    """Rounds a datetime object UP to the nearest 15-minute interval."""
    if not isinstance(dt, datetime.datetime):
        return dt # Return as is if not a datetime object

    # If the time is already on a perfect 15-minute mark, do nothing
    if dt.minute % 15 == 0 and dt.second == 0 and dt.microsecond == 0:
        return dt

    # Calculate the number of minutes to add to round up
    minutes_to_add = (15 - dt.minute % 15)
    rounded_dt = dt + datetime.timedelta(minutes=minutes_to_add)
    
    # Set seconds and microseconds to zero for a clean time
    return rounded_dt.replace(second=0, microsecond=0)

def _calculate_target_date_score(slot_date, target_date):
    """
    Calculates a score based on how close the slot_date is to the target_date.
    A score of 100 is given for the exact date, with the score decreasing for
    each day further away.
    """
    if target_date is None:
        return 0 # No score if no target date is provided

    days_difference = abs((slot_date - target_date).days)
    
    # Give a high score for the target date and a decreasing score for surrounding days
    # This formula gives 100 for the target date, 90 for +/- 1 day, 80 for +/- 2 days, etc.
    score = max(0, 100 - (days_difference * 10))
    return score
    

def format_time_for_display(time_obj):
    return time_obj.strftime('%I:%M %p').lstrip('0') if isinstance(time_obj, datetime.time) else "InvalidTime"

def get_job_details(job_id):
    for job in SCHEDULED_JOBS:
        if job.job_id == job_id: return job
    return None

def find_same_service_conflict(boat_id, new_service_type, requested_date, all_scheduled_jobs):
    """
    Checks if the same service is already scheduled for a boat within a 30-day window.
    Args:
        boat_id (int): The ID of the boat to check.
        new_service_type (str): The service being requested (e.g., "Launch", "Haul").
        requested_date (datetime.date): The new date being requested.
        all_scheduled_jobs (list): A list of all currently scheduled Job objects.
    Returns:
        Job: The conflicting Job object if found, otherwise None.
    """
    thirty_days = datetime.timedelta(days=30)
    for job in all_scheduled_jobs:
        if job.boat_id == boat_id and job.service_type == new_service_type:
            if job.scheduled_start_datetime:
                # Check if the existing job date is within 30 days of the new requested date
                if abs(job.scheduled_start_datetime.date() - requested_date) <= thirty_days:
                    return job # Return the specific job that is causing the conflict
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
    """
    _log_debug(f"Attempting to parse file: {filepath} for dates {begin_date} to {end_date}")
    grouped_tides = {}
    
    try:
        with open(filepath, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or not line[0].isdigit():
                    continue

                parts = line.split()
                if len(parts) < 6:
                    continue

                try:
                    # Capture all necessary parts from the line
                    date_str = parts[0]
                    day_of_week_str = parts[1]
                    time_str = parts[2]
                    am_pm_str = parts[3]
                    height_str = parts[4]
                    type_str = parts[-1]

                    # --- FIX 1 of 2: Include the day of the week in the string to be parsed ---
                    datetime_to_parse = f"{date_str} {day_of_week_str} {time_str} {am_pm_str}"
                    
                    # --- FIX 2 of 2: Add '%a' to the format to handle "Mon", "Tue", etc. ---
                    tide_dt_obj = datetime.datetime.strptime(datetime_to_parse, "%Y/%m/%d %a %I:%M %p")
                    
                    # This part remains the same
                    tide_dt_obj = tide_dt_obj.replace(year=begin_date.year)
                    current_date = tide_dt_obj.date()

                    if begin_date <= current_date <= end_date:
                        tide_info = {
                            'type': type_str.upper(),
                            'time': tide_dt_obj.time(),
                            'height': float(height_str)
                        }
                        grouped_tides.setdefault(current_date, []).append(tide_info)

                except (ValueError, IndexError) as e:
                    _log_debug(f"--> PARSE ERROR on line: '{line}'. Details: {e}")
                    continue
    except FileNotFoundError:
        _log_debug(f"ERROR: Local tide file not found: {filepath}")
    except Exception as e:
        _log_debug(f"ERROR: General error reading local tide file {filepath}: {e}")

    _log_debug(f"Finished parsing. Found {len(grouped_tides)} days with valid tides.")
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
    truck_hours_schedule,
    ramp_tide_blackout_enabled=True
):
    """
    Calculates the final, schedulable time slots for a specific truck at a ramp on a given day.
    -- CORRECTED to handle timezone-aware datetime objects consistently. --
    """
    day_of_week = date_to_check.weekday()
    
    # --- Check 1: Collective Truck Hours ---
    earliest_truck_start = datetime.time(23, 59)
    latest_truck_end = datetime.time(0, 0)
    any_truck_working_today = False

    for t_id, t_hours_daily in truck_hours_schedule.items():
        if t_hours := t_hours_daily.get(day_of_week):
            any_truck_working_today = True
            earliest_truck_start = min(earliest_truck_start, t_hours[0])
            latest_truck_end = max(latest_truck_end, t_hours[1])

    if not any_truck_working_today:
        DEBUG_MESSAGES.append(f"DEBUG: No trucks working on {date_to_check.strftime('%Y-%m-%d')}. Returning empty slots.")
        return []

    specific_truck_hours = truck_hours_schedule.get(truck_id, {}).get(day_of_week)
    if not specific_truck_hours:
        return []

    # FIX: Make collective truck hours timezone-aware (UTC)
    collective_truck_open_dt = datetime.datetime.combine(date_to_check, earliest_truck_start, tzinfo=timezone.utc)
    collective_truck_close_dt = datetime.datetime.combine(date_to_check, latest_truck_end, tzinfo=timezone.utc)

    if not ramp_obj:
        return [{'start_time': specific_truck_hours[0], 'end_time': specific_truck_hours[1], 'high_tide_times': [], 'tide_rule_concise': 'N/A'}]

    # --- Check 2: Tidal Windows ---
    tide_data_for_day = all_tides.get(date_to_check, [])
    tidal_windows = calculate_ramp_windows(ramp_obj, boat_obj, tide_data_for_day, date_to_check)
    
    filtered_tidal_windows = []
    
    # --- THIS LINE IS CORRECTED ---
    # It now checks that draft_ft is not None before comparing it.
    if ramp_obj.tide_calculation_method == "AnyTide" or (ramp_obj.tide_calculation_method == "AnyTideWithDraftRule" and boat_obj.draft_ft is not None and boat_obj.draft_ft < 5.0):
        # For AnyTide, the tidal window is effectively the entire day.
        filtered_tidal_windows = tidal_windows
    else:
        # For tide-dependent ramps, filter windows by collective truck hours
        for t_win in tidal_windows:
            # FIX: Make tidal windows timezone-aware (UTC)
            tidal_start_dt = datetime.datetime.combine(date_to_check, t_win['start_time'], tzinfo=timezone.utc)
            tidal_end_dt = datetime.datetime.combine(date_to_check, t_win['end_time'], tzinfo=timezone.utc)

            if tidal_start_dt > tidal_end_dt:
                tidal_end_dt += datetime.timedelta(days=1)
            
            # Compare two aware datetimes
            overlap_start = max(tidal_start_dt, collective_truck_open_dt)
            overlap_end = min(tidal_end_dt, collective_truck_close_dt)

            if overlap_start < overlap_end:
                filtered_tidal_windows.append(t_win)
            else:
                DEBUG_MESSAGES.append(f"DEBUG: Tide window {t_win['start_time']}-{t_win['end_time']} at {ramp_obj.ramp_name} does NOT overlap with collective truck hours {earliest_truck_start}-{latest_truck_end} on {date_to_check}. Skipping.")

    # --- Blackout Logic ---
    if ramp_tide_blackout_enabled and not filtered_tidal_windows and ramp_obj.tide_calculation_method not in ["AnyTide", "AnyTideWithDraftRule"]:
        DEBUG_MESSAGES.append(f"DEBUG: Ramp {ramp_obj.ramp_name} 'blacked out' on {date_to_check} due to no viable tide windows overlapping truck hours.")
        return []

    # --- Final Slot Generation (for the specific truck) ---
    # FIX: Make specific truck hours timezone-aware (UTC)
    specific_truck_open_dt = datetime.datetime.combine(date_to_check, specific_truck_hours[0], tzinfo=timezone.utc)
    specific_truck_close_dt = datetime.datetime.combine(date_to_check, specific_truck_hours[1], tzinfo=timezone.utc)

    all_schedulable_slots = []
    for t_win in filtered_tidal_windows:
        # FIX: Make tidal window timezone-aware (UTC) again for specific truck comparison
        tidal_start_dt = datetime.datetime.combine(date_to_check, t_win['start_time'], tzinfo=timezone.utc)
        tidal_end_dt = datetime.datetime.combine(date_to_check, t_win['end_time'], tzinfo=timezone.utc)

        if tidal_start_dt > tidal_end_dt:
            tidal_end_dt += datetime.timedelta(days=1)

        # Compare two aware datetimes
        overlap_start = max(tidal_start_dt, specific_truck_open_dt)
        overlap_end = min(tidal_end_dt, specific_truck_close_dt)

        if overlap_start < overlap_end:
            all_schedulable_slots.append({
                'start_time': overlap_start.time(), 'end_time': overlap_end.time(),
                'high_tide_times': [t['time'] for t in tide_data_for_day if t['type'] == 'H'],
                'low_tide_times': [t['time'] for t in tide_data_for_day if t['type'] == 'L'],
                'tide_rule_concise': get_concise_tide_rule(ramp_obj, boat_obj)
            })

    return all_schedulable_slots

def get_suitable_trucks(boat_len, pref_truck_id=None, force_preferred=False):
    all_suitable = [t for t in ECM_TRUCKS.values() if not t.is_crane and t.max_boat_length is not None and boat_len <= t.max_boat_length]
    if force_preferred and pref_truck_id and any(t.truck_name == pref_truck_id for t in all_suitable):
        return [t for t in all_suitable if t.truck_name == pref_truck_id]
    return all_suitable

def _diagnose_failure_reasons(req_date, boat, ramp_obj, truck_hours, force_preferred_truck):
    """Provides a step-by-step diagnostic for scheduling failures."""
    reasons = [f"--- Failure Analysis ---", f"Debugging for: {req_date.strftime('%A, %Y-%m-%d')}"]
    
    # Step 1: Find suitable trucks
    suitable_trucks = get_suitable_trucks(boat.boat_length, boat.preferred_truck_id, force_preferred_truck)
    reasons.append(f"**Step 1: Suitable Trucks** (Force Preferred: {force_preferred_truck})")
    reasons.append(json.dumps([t.truck_name for t in suitable_trucks]))
    if not suitable_trucks:
        reasons.append(f"**Failure Reason:** No trucks found for a {boat.boat_length}ft boat.")
        return reasons

    # Step 2: Check which trucks are on duty
    working_trucks = [t for t in suitable_trucks if truck_hours.get(t.truck_id, {}).get(req_date.weekday())]
    reasons.append("**Step 2: Duty Status**")
    reasons.append(json.dumps({t.truck_name: str(truck_hours.get(t.truck_id, {}).get(req_date.weekday())) for t in suitable_trucks}))
    if not working_trucks:
        reasons.append("**Failure Reason:** No suitable trucks are working on this day.")
        return reasons

    if not ramp_obj:
        reasons.append("**No Ramp Selected:** Cannot perform tide analysis.")
        return reasons
    
    # Step 3: Fetch Tides
    all_tides = fetch_noaa_tides_for_range(ramp_obj.noaa_station_id, req_date, req_date)
    tides_for_day = all_tides.get(req_date, [])
    reasons.append(f"**Step 3: Fetched Tides for {ramp_obj.ramp_name}**")
    reasons.append(json.dumps([{'type': t['type'], 'time': str(t['time'])} for t in tides_for_day]))

    # Step 4: Calculate Overlap and check if job duration fits
    reasons.append("**Step 4: Overlap Calculation**")
    longest_window_found = timedelta(0)
    for truck in working_trucks:
        windows = get_final_schedulable_ramp_times(ramp_obj, boat, req_date, all_tides, truck.truck_id, truck_hours)
        reasons.append(f" - Overlap for {truck.truck_name}: {len(windows)} valid window(s) found.")
        if windows:
            for window in windows:
                start_dt = datetime.datetime.combine(req_date, window['start_time'])
                end_dt = datetime.datetime.combine(req_date, window['end_time'])
                if end_dt > start_dt:
                    longest_window_found = max(longest_window_found, end_dt - start_dt)

    # --- NEW, MORE ACCURATE DURATION CHECK ---
    rules = BOOKING_RULES.get(boat.boat_type, {})
    # Note: This check uses base duration and does not account for travel time, but is a good indicator.
    job_duration = timedelta(minutes=rules.get('truck_mins', 90))
    
    if longest_window_found.total_seconds() > 0 and longest_window_found < job_duration:
        reasons.append(f"**Failure Reason: Job Too Long for Window:** The base job duration ({job_duration.total_seconds()/60:.0f} mins) is longer than the longest available time window ({longest_window_found.total_seconds()/60:.0f} mins) on this day.")
        return reasons
    
    # This is now the true fallback reason.
    reasons.append("**Failure Reason: All Slots Booked:** All available time slots for suitable trucks are already taken on this date.")
    return reasons


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
        crane_end_time = getattr(job, 'S17_busy_end_datetime', None)
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

# PASTE THIS ENTIRE BLOCK INTO YOUR ECM_scheduler_logic.py FILE

# --- NEW HELPER: Pre-calculates ideal crane days based on tides ---
def precalculate_ideal_crane_days(year=2025):
    """
    Analyzes tides for the entire season and stores optimal crane days.
    An "ideal" day has a high tide between 10 AM and 2 PM.
    This should be called once after all ramps are loaded.
    """
    global IDEAL_CRANE_DAYS
    IDEAL_CRANE_DAYS.clear()
    
    # Filter for ramps that allow sailboats
    sailboat_ramps = [r for r in ECM_RAMPS.values() if "Sailboat" in str(r.allowed_boat_types)]
    
    # Define the season (e.g., April to October)
    for month in range(4, 11):
        for ramp in sailboat_ramps:
            # In a real app, this would use your fetch_noaa_tides_for_range.
            # For this example, we simulate finding ideal tides.
            # NOTE: You may need to adapt this to use your actual fetch_noaa_tides_for_range
            start_date = datetime.date(year, month, 1)
            end_date = datetime.date(year, month, calendar.monthrange(year, month)[1])
            tides_for_month = fetch_noaa_tides_for_range(ramp.noaa_station_id, start_date, end_date)
            
            for day, events in tides_for_month.items():
                for tide in events:
                    if tide['type'] == 'H' and 10 <= tide['time'].hour < 14:
                        IDEAL_CRANE_DAYS.add((ramp.ramp_id, day))
                        # Found a good tide for this day, no need to check other tides on the same day
                        break
    _log_debug(f"Pre-calculated {len(IDEAL_CRANE_DAYS)} ideal crane days for the season.")

# --- NEW HELPER: Finds a slot on a specific day using the new efficiency rules ---
# Replace your old function with this CORRECTED version
def _find_slot_on_day(search_date, boat, service_type, ramp_id, crane_needed, compiled_schedule, customer_id):
    """
    Finds the first available slot on a specific day, respecting all efficiency rules.
    """
    is_ecm_boat = boat.is_ecm_boat
    ramp = get_ramp_details(str(ramp_id)) # Ensure ramp_id is a string for lookup
    if not ramp: return None

    # Determine season for ECM boat logic
    is_launch_season = 4 <= search_date.month <= 7
    
    # Determine search direction based on season and boat type ("ECM Boat Priority")
    if is_launch_season:
        start_hour = 9 if not is_ecm_boat else 7
        time_iterator = range(start_hour, 16) # Search forward 7/9am to 4pm
    else: # Haul season
        start_hour = 14 if not is_ecm_boat else 15
        time_iterator = range(start_hour, 7, -1) # Search backward 3/4pm to 7am

    # Get job duration
    rules = BOOKING_RULES.get(boat.boat_type, {})
    hauler_duration = timedelta(minutes=rules.get('truck_mins', 90))
    crane_duration = timedelta(minutes=rules.get('crane_mins', 0))

    # REMOVED THIS LINE - This is the fix!
    # compiled_schedule, _ = _compile_truck_schedules(SCHEDULED_JOBS)
    
    # Get valid working windows for this ramp/boat/day
    all_tides = fetch_noaa_tides_for_range(ramp.noaa_station_id, search_date, search_date)
    
    # Iterate through each suitable truck
    for truck in get_suitable_trucks(boat.boat_length):
        schedulable_windows = get_final_schedulable_ramp_times(ramp, boat, search_date, all_tides, truck.truck_id, TRUCK_OPERATING_HOURS)
        
        for window in schedulable_windows:
            # Iterate through the time slots in the determined direction
            for hour in time_iterator:
                for minute in [0, 15, 30, 45]:
                    slot_start_time = time(hour, minute)
                    
                    # Check if the proposed time is within a valid window
                    if not (window['start_time'] <= slot_start_time <= window['end_time']):
                        continue

                    slot_start_dt = datetime.datetime.combine(search_date, slot_start_time, tzinfo=timezone.utc)
                    slot_end_dt = slot_start_dt + hauler_duration

                    # Check if the job fits within the window
                    if slot_end_dt.time() > window['end_time'] and slot_end_dt.date() == search_date:
                        continue
                        
                    # Check truck availability
                    if not check_truck_availability_optimized(truck.truck_id, slot_start_dt, slot_end_dt, compiled_schedule):
                        continue

                    # Check crane availability if needed
                    s17_id = get_s17_truck_id()
                    if crane_needed:
                        crane_end_dt = slot_start_dt + crane_duration
                        if not check_truck_availability_optimized(s17_id, slot_start_dt, crane_end_dt, compiled_schedule):
                            continue
                    
                    # If we get here, we found a valid slot
                    return {
                        'customer_id': customer_id, # <-- ADD THIS LINE
                        "date": search_date, "time": slot_start_time, "truck_id": truck.truck_id,
                        "ramp_id": ramp_id, "service_type": service_type, "S17_needed": crane_needed,
                        "scheduled_end_datetime": slot_end_dt, 
                        "S17_busy_end_datetime": crane_end_dt if crane_needed else None,
                        'tide_rule_concise': window.get('tide_rule_concise', 'N/A'),
                        'high_tide_times': window.get('high_tide_times', []),
                        'boat_draft': boat.draft_ft
                    }
    return None

# --- REPLACEMENT: The new efficiency-driven slot finding engine ---
def find_available_job_slots(customer_id, boat_id, service_type, requested_date_str, selected_ramp_id, num_suggestions_to_find=3, **kwargs):
    """
    The new efficiency-driven slot finding engine.
    """
    global DEBUG_MESSAGES; DEBUG_MESSAGES.clear()

    # --- THIS BLOCK IS NEW ---
    # 1. Compile the master schedule ONCE before any loops start.
    _log_debug("Compiling master truck schedule once...")
    compiled_schedule, _ = _compile_truck_schedules(SCHEDULED_JOBS)
    _log_debug("Master schedule compiled.")
    # --- END OF NEW BLOCK ---

    boat = get_boat_details(boat_id)
    requested_date = datetime.datetime.strptime(requested_date_str, "%Y-%m-%d").date()
    crane_needed = "Sailboat" in boat.boat_type
    
    found_slots = []
    
    # --- PHASE 1: OPPORTUNISTIC EFFICIENCY SEARCH ---
    _log_debug("PHASE 1: Starting opportunistic search...")
    search_window = [requested_date + timedelta(days=i) for i in range(-7, 8)]
    
    active_days = {job.scheduled_start_datetime.date() for job in SCHEDULED_JOBS if job.scheduled_start_datetime.date() in search_window}
    sorted_active_days = sorted(list(active_days), key=lambda d: abs(d - requested_date))
    
    for day in sorted_active_days:
        if crane_needed and (selected_ramp_id, day) not in IDEAL_CRANE_DAYS:
            _log_debug(f"Skipping active day {day}: Not an ideal crane day.")
            continue
        
        _log_debug(f"Searching for piggyback slot on active day: {day}")
        # 2. Pass the pre-calculated schedule into the helper function.
        #    NOTE: You must also modify _find_slot_on_day to accept this new argument.
        slot = _find_slot_on_day(day, boat, service_type, selected_ramp_id, crane_needed, compiled_schedule)
        if slot:
            found_slots.append(slot)
        if len(found_slots) >= num_suggestions_to_find:
            break
            
    if found_slots:
        _log_debug(f"Found {len(found_slots)} highly efficient slots in Phase 1.")
        return found_slots, "Found highly efficient slots on already active days.", [], False

    # --- PHASE 2: FALLBACK SEARCH ---
    _log_debug("PHASE 2: No efficient slots found, starting fallback search.")
    search_dates = []
    if crane_needed:
        potential_dates = [d for r, d in IDEAL_CRANE_DAYS if r == selected_ramp_id and d >= requested_date]
        search_dates = sorted(potential_dates)[:14] # Check the next 14 ideal days
        _log_debug(f"Crane needed. Searching ideal days: {search_dates}")
    else:
        search_dates = [requested_date + timedelta(days=i) for i in range(14)]
        _log_debug(f"No crane. Searching forward from {requested_date}")
        
    for day in search_dates:
        # 3. Also pass the pre-calculated schedule here.
        slot = _find_slot_on_day(day, boat, service_type, selected_ramp_id, crane_needed, compiled_schedule)
        if slot:
            found_slots.append(slot)
        if len(found_slots) >= num_suggestions_to_find:
            break
            
    if found_slots:
        return found_slots, "Found available slots on the next best days.", [], False
    else:
        return [], "Could not find any available slots within the next 14 days.",

# --- REPLACEMENT: The enhanced testing utility ---
def simulate_job_requests(total_jobs_to_gen=50):
    """
    A smarter simulation utility to test the new efficiency logic.
    """
    global SCHEDULED_JOBS
    SCHEDULED_JOBS = [] # Reset schedule for simulation
    
    # 1. Generate a Diverse Job Pool
    job_requests = []
    all_boats = list(LOADED_BOATS.values())
    random.shuffle(all_boats)
    
    if not all_boats: return "Cannot simulate, no boats loaded."

    busy_week_start = datetime.date(2025, 5, 19) # Simulate a busy spring week

    for boat in all_boats[:total_jobs_to_gen]:
        # Cluster 50% of requests into one busy week
        if random.random() < 0.5:
            req_date = busy_week_start + timedelta(days=random.randint(0, 4))
        else:
            req_date = datetime.date(2025, random.randint(4, 8), random.randint(1, 28))
            
        job_requests.append({
            "customer_id": boat.customer_id, "boat_id": boat.boat_id,
            "service_type": "Launch", "requested_date_str": req_date.strftime("%Y-%m-%d"),
            "selected_ramp_id": boat.preferred_ramp_id
        })
        
    # 2. Run the Simulation
    successful_bookings = 0
    print(f"Starting simulation for {len(job_requests)} jobs...")
    
    for i, request in enumerate(job_requests):
        print(f"Simulating request {i+1}/{len(job_requests)} for boat {request['boat_id']}...")
        slots, _, _, _ = find_available_job_slots(**request)
        
        if slots:
            # Always pick the first (best) suggestion
            confirm_and_schedule_job(request, slots[0])
            successful_bookings += 1
        else:
            print(f"--> Could not find a slot for request: {request}")
            
    # 3. Provide a Better Summary
    total_truck_days = len({(j.scheduled_start_datetime.date(), j.assigned_hauling_truck_id) for j in SCHEDULED_JOBS})
    total_crane_days = len({(j.scheduled_start_datetime.date(), j.assigned_crane_truck_id) for j in SCHEDULED_JOBS if j.assigned_crane_truck_id})
    avg_jobs_per_truck_day = len(SCHEDULED_JOBS) / total_truck_days if total_truck_days > 0 else 0
    
    summary = (
        f"SIMULATION COMPLETE:\n"
        f"- Successfully scheduled {successful_bookings} of {total_jobs_to_gen} jobs.\n"
        f"- Utilized {total_truck_days} unique truck-days.\n"
        f"- Created {total_crane_days} unique crane-days.\n"
        f"- Achieved an average of {avg_jobs_per_truck_day:.2f} jobs per truck-day."
    )
    _log_debug(summary)
    return summary

def analyze_job_distribution(scheduled_jobs, all_boats, all_ramps):
    """
    Analyzes the distribution of scheduled jobs by day of the week and by ramp
    to provide data for PDF report charts.
    """
    # --- Analysis by Day of Week ---
    day_counts = Counter()
    day_map = {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
    for job in scheduled_jobs:
        if job.scheduled_start_datetime:
            day_of_week = job.scheduled_start_datetime.weekday()
            day_counts[day_map[day_of_week]] += 1
    
    # --- Analysis by Ramp ---
    ramp_counts = Counter()
    for job in scheduled_jobs:
        # To avoid double-counting a transport job, we prioritize the dropoff ramp
        ramp_to_count = job.dropoff_ramp_id or job.pickup_ramp_id
        if ramp_to_count and ramp_to_count in all_ramps:
            ramp_name = all_ramps[ramp_to_count].ramp_name
            # Abbreviate long ramp names for the chart
            if len(ramp_name) > 15:
                ramp_name = ramp_name.split()[0]
            ramp_counts[ramp_name] += 1
    
    # Create the analysis dictionary to return
    analysis = {
        'by_day': dict(day_counts),
        'by_ramp': dict(ramp_counts)
    }
    
    return analysis

def perform_efficiency_analysis(scheduled_jobs):
    """
    Performs an in-depth analysis of fleet efficiency based on scheduled jobs.
    """
    if not scheduled_jobs:
        return {}

    # 1. Group jobs by date and then by truck
    daily_truck_schedules = {}
    for job in scheduled_jobs:
        if not job.scheduled_start_datetime or not job.assigned_hauling_truck_id:
            continue
        job_date = job.scheduled_start_datetime.date()
        truck_id = job.assigned_hauling_truck_id
        
        daily_truck_schedules.setdefault(job_date, {}).setdefault(truck_id, []).append(job)

    # 2. Initialize metrics
    total_truck_days = 0
    low_utilization_days = 0
    excellent_timing_days = 0
    poor_timing_days = 0
    total_deadhead_minutes = 0
    total_on_clock_minutes = 0
    total_productive_minutes = 0
    
    # 3. Analyze each truck's daily performance
    for date, trucks in daily_truck_schedules.items():
        for truck_id, jobs in trucks.items():
            total_truck_days += 1
            
            # Sort jobs chronologically for the day
            jobs.sort(key=lambda j: j.scheduled_start_datetime)
            num_jobs = len(jobs)
            
            # Metric 1: Truck Day Utilization
            if num_jobs <= 2:
                low_utilization_days += 1
            
            first_job_start = jobs[0].scheduled_start_datetime
            last_job_end = jobs[-1].scheduled_end_datetime
            
            # Metric 2: Job Timing Efficiency
            if first_job_start.time() < datetime.time(9, 0) and num_jobs >= 3 and last_job_end.time() <= datetime.time(15, 0):
                excellent_timing_days += 1
            if first_job_start.time() >= datetime.time(13, 0):
                poor_timing_days += 1

            # Metric 3 & 4: Proximity and Other Efficiency Metrics
            day_on_clock_minutes = (last_job_end - first_job_start).total_seconds() / 60
            total_on_clock_minutes += day_on_clock_minutes

            day_productive_minutes = sum((j.scheduled_end_datetime - j.scheduled_start_datetime).total_seconds() / 60 for j in jobs)
            total_productive_minutes += day_productive_minutes

            # Deadhead Calculation
            # Leg 1: From yard to the first job
            yard_coords = get_location_coords(address=YARD_ADDRESS)
            first_pickup_coords = get_location_coords(address=jobs[0].pickup_street_address, ramp_id=jobs[0].pickup_ramp_id)
            total_deadhead_minutes += calculate_travel_time(yard_coords, first_pickup_coords)

            # Intermediate Legs: From dropoff of job N to pickup of job N+1
            for i in range(num_jobs - 1):
                prev_job_dropoff_coords = get_location_coords(address=jobs[i].dropoff_street_address, ramp_id=jobs[i].dropoff_ramp_id)
                next_job_pickup_coords = get_location_coords(address=jobs[i+1].pickup_street_address, ramp_id=jobs[i+1].pickup_ramp_id)
                total_deadhead_minutes += calculate_travel_time(prev_job_dropoff_coords, next_job_pickup_coords)

    # 4. Compile final analysis
    analysis = {
        "total_truck_days": total_truck_days,
        "low_utilization_days": low_utilization_days,
        "excellent_timing_days": excellent_timing_days,
        "poor_timing_days": poor_timing_days,
        "avg_jobs_per_day": len(scheduled_jobs) / total_truck_days if total_truck_days > 0 else 0,
        "avg_deadhead_per_day": total_deadhead_minutes / total_truck_days if total_truck_days > 0 else 0,
        "efficiency_percent": (total_productive_minutes / total_on_clock_minutes * 100) if total_on_clock_minutes > 0 else 0,
    }
    return analysis

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
def confirm_and_schedule_job(original_request, selected_slot, parked_job_to_remove=None):
    try:
        customer = get_customer_details(original_request['customer_id'])
        boat = get_boat_details(original_request['boat_id'])
        service_type = original_request['service_type']
        selected_ramp_id = selected_slot.get('ramp_id')
        
        pickup_addr, dropoff_addr, pickup_rid, dropoff_rid = "", "", None, None
        
        if service_type == "Launch":
            pickup_addr = boat.storage_address
            dropoff_rid = selected_ramp_id or boat.preferred_ramp_id
            dropoff_ramp_obj = get_ramp_details(dropoff_rid)
            dropoff_addr = dropoff_ramp_obj.ramp_name if dropoff_ramp_obj else ""
            
        elif service_type == "Haul":
            dropoff_addr = boat.storage_address
            pickup_rid = selected_ramp_id or boat.preferred_ramp_id
            pickup_ramp_obj = get_ramp_details(pickup_rid)
            pickup_addr = pickup_ramp_obj.ramp_name if pickup_ramp_obj else ""
        
        start_dt = datetime.datetime.combine(selected_slot['date'], selected_slot['time'], tzinfo=timezone.utc)
        hauler_end_dt = selected_slot['scheduled_end_datetime']
        S17_end_dt      = selected_slot.get('S17_busy_end_datetime')
        
        # --- START: Corrected Logic ---
        # 1. Find the actual numeric ID for the crane truck named "S17".
        s17_truck_id = None
        for truck in ECM_TRUCKS.values():
            if truck.truck_name == "S17":
                s17_truck_id = truck.truck_id
                break
        # --- END: Corrected Logic ---

        new_job = Job(
            customer_id=customer.customer_id, boat_id=boat.boat_id, service_type=service_type,
            scheduled_start_datetime=start_dt,
            scheduled_end_datetime=hauler_end_dt,
            assigned_hauling_truck_id=selected_slot['truck_id'],
            # 2. Use the numeric ID variable here instead of the hardcoded text "S17".
            assigned_crane_truck_id=s17_truck_id if selected_slot.get('S17_needed') else None,
            S17_busy_end_datetime=S17_end_dt,
            pickup_ramp_id=pickup_rid, dropoff_ramp_id=dropoff_rid,
            job_status="Scheduled", pickup_street_address=pickup_addr, dropoff_street_address=dropoff_addr
        )
        
        save_job(new_job)
        SCHEDULED_JOBS.append(new_job)
        
        if parked_job_to_remove and parked_job_to_remove in PARKED_JOBS:
            del PARKED_JOBS[parked_job_to_remove]
            delete_job_from_db(parked_job_to_remove)
            
        message = f"SUCCESS: Job #{new_job.job_id} for {customer.customer_name} scheduled for {start_dt.strftime('%A, %b %d at %I:%M %p')}."
        return new_job.job_id, message
        
    except Exception as e:
        return None, f"An error occurred: {e}"

def find_available_ramps_for_boat(boat, all_ramps):
    """
    Finds ramps suitable for a given boat by checking the boat's type against a ramp's allowed boat types.
    """
    matching_ramps = {
        ramp_id: ramp for ramp_id, ramp in all_ramps.items()
        if boat.boat_type in ramp.allowed_boat_types
    }
    
    # Fallback: if no specific ramps match, return all of them to allow a manual override.
    if not matching_ramps:
        return all_ramps.keys()

    return matching_ramps.keys()


def get_S17_crane_grouping_slot(boat, customer, ramp_obj, requested_date, trucks, duration, S17_duration, service_type):
    """
    Attempts to group a sailboat crane job with an existing crane job at the same ramp within ±7 days.
    """
    import datetime
    from .ecm_scheduler_shared import _check_and_create_slot_detail
    from .ecm_scheduler_data import SCHEDULED_JOBS

    date_range = [requested_date + datetime.timedelta(days=delta) for delta in range(-7, 8)]

    for scheduled_job in SCHEDULED_JOBS:
        if scheduled_job.job_status != "Scheduled" or scheduled_job.crane_truck_id != "S17":
            continue
        if scheduled_job.ramp_id != ramp_obj.ramp_id:
            continue
        if scheduled_job.scheduled_date not in date_range:
            continue
        
        return None

        # Found match, attempt grouping
        check_date = scheduled_job.scheduled_date
        slot = _check_and_create_slot_detail(
            check_date, trucks, ramp_obj, True,
            duration, S17_duration, boat, customer, service_type
        )
        if slot:
            return slot
    return None
