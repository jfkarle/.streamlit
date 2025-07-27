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
# In ecm_scheduler_logic.py

from geopy.geocoders import Nominatim # Change GoogleV3 to Nominatim

# The API key is no longer needed for geocoding
Maps_API_KEY = st.secrets.get("Maps_API_KEY") # This can remain for the Distance Matrix API if you still use it

# Optionally, you can remove the API key check if you are no longer using any paid Google services
if not Maps_API_KEY:
    DEBUG_MESSAGES.append("WARNING: Google Maps API Key not found. Travel time estimates may be affected.")

# Update the geolocator to use the free Nominatim service
_geolocator = Nominatim(user_agent="ecm_boat_scheduler_app")

_location_coords_cache = {} # Ensure this line is present here

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
        self.j17_busy_end_datetime = _parse_or_get_datetime(kwargs.get("j17_busy_end_datetime"))
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
    "J17":    { 0: (time(8, 0), time(16, 0)), 1: (time(8, 0), time(16, 0)), 2: (time(8, 0), time(16, 0)), 3: (time(8, 0), time(16, 0)), 4: (time(8, 0), time(16, 0)), 5: None, 6: None }
}
BOOKING_RULES = {'Powerboat': {'truck_mins': 90, 'crane_mins': 0},'Sailboat DT': {'truck_mins': 180, 'crane_mins': 60},'Sailboat MT': {'truck_mins': 180, 'crane_mins': 90}}

# --- IN-MEMORY DATA CACHES ---
CANDIDATE_CRANE_DAYS = { 'ScituateHarborJericho': [], 'PlymouthHarbor': [], 'WeymouthWessagusset': [], 'CohassetParkerAve': [] }
crane_daily_status = {}
ECM_TRUCKS, LOADED_CUSTOMERS, LOADED_BOATS, ECM_RAMPS, TRUCK_OPERATING_HOURS = {}, {}, {}, {}, {}
SCHEDULED_JOBS, PARKED_JOBS = [], {}

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

# In ecm_scheduler_logic.py

def load_all_data_from_sheets():
    """Loads all data from Supabase, now including truck schedules."""
    global SCHEDULED_JOBS, PARKED_JOBS, LOADED_CUSTOMERS, LOADED_BOATS, ECM_TRUCKS, ECM_RAMPS, TRUCK_OPERATING_HOURS
    try:
        conn = get_db_connection()

        # --- Jobs ---
        # Select all columns, including new lat/lon columns, which will be passed to Job(**row)
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
        # Select all columns, including new lat/lon columns, which will be passed to Ramp(**row)
        ramps_resp = execute_query(conn.table("ramps").select("*"), ttl=0)
        ECM_RAMPS.clear()
        ECM_RAMPS.update({
            row["ramp_id"]: Ramp(
                r_id       = row["ramp_id"],
                name       = row.get("ramp_name"),
                station    = row.get("noaa_station_id"),
                tide_method= row.get("tide_calculation_method"),
                offset     = row.get("tide_offset_hours"),
                boats      = row.get("allowed_boat_types"),
                latitude   = row.get("latitude"), # Pass new lat/lon from DB row
                longitude  = row.get("longitude")  # Pass new lat/lon from DB row
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
        # Ensure 'select("*")' fetches the new lat/lon columns from Supabase
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
                is_ecm     = str(row.get("is_ecm_boat", "no")).lower() == 'yes',
                storage_latitude = row.get("storage_latitude"), # Pass new lat/lon from DB row
                storage_longitude = row.get("storage_longitude") # Pass new lat/lon from DB row
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
        
        # Geocode Yard Address (this call ensures YARD_COORDS is set and potentially cached)
        # It relies on get_location_coords's internal logic to check if YARD_COORDS already exists
        _ = get_location_coords(address=YARD_ADDRESS) 

        # Geocode all ramps:
        # Pass ramp_id and existing lat/lon so get_location_coords can read from/write to DB if coords are missing.
        for ramp_id, ramp_obj in ECM_RAMPS.items():
            _ = get_location_coords(ramp_id=ramp_id, initial_latitude=ramp_obj.latitude, initial_longitude=ramp_obj.longitude) 
        
        # Geocode all boat storage addresses:
        # Pass boat_id and existing lat/lon so get_location_coords can read from/write to DB if coords are missing.
        for boat_id, boat_obj in LOADED_BOATS.items():
            if boat_obj.storage_address: # Only process if there's an address string
                _ = get_location_coords(address=boat_obj.storage_address, boat_id=boat_obj.boat_id,
                                        initial_latitude=boat_obj.storage_latitude, initial_longitude=boat_obj.storage_longitude)
        
        # Geocode addresses from all jobs (pickup/dropoff streets)
        # Pass job_id, job_type and existing lat/lon so get_location_coords can read from/write to DB if coords are missing.
        for job in all_jobs: 
            # Process pickup address/ramp (prioritize street address over ramp if both exist for geocoding)
            if job.pickup_street_address: 
                _ = get_location_coords(address=job.pickup_street_address, job_id=job.job_id, job_type='pickup',
                                        initial_latitude=job.pickup_latitude, initial_longitude=job.pickup_longitude)
            # If pickup is a ramp_id and no street address, it should have been covered by the 'ramps' loop.
            
            # Process dropoff address/ramp
            if job.dropoff_street_address:
                _ = get_location_coords(address=job.dropoff_street_address, job_id=job.job_id, job_type='dropoff',
                                        initial_latitude=job.dropoff_latitude, initial_longitude=job.dropoff_longitude)
            # If dropoff is a ramp_id and no street address, it should have been covered by the 'ramps' loop.
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
_location_coords_cache = {} 

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

    if address_to_geocode and Maps_API_KEY: # Only attempt if there's an address string and API key
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

def _diagnose_failure_reasons(req_date, customer, boat, ramp_obj, service_type, truck_hours, manager_override, force_preferred_truck):
    """Provides a step-by-step diagnostic for scheduling failures."""
    diag_messages = []
    diag_messages.append("--- Failure Analysis ---")
    diag_messages.append(f"Debugging for: {req_date.strftime('%A, %Y-%m-%d')}")

    # Step 1: Find suitable trucks
    suitable_trucks = get_suitable_trucks(boat.boat_length, boat.preferred_truck_id, force_preferred_truck)
    diag_messages.append(f"**Step 1: Suitable Trucks** (Force Preferred: {force_preferred_truck})")
    diag_messages.append(json.dumps([t.truck_name for t in suitable_trucks]))
    if not suitable_trucks:
        diag_messages.append(f"**Failure Reason:** No trucks were found that can handle a {boat.boat_length}ft boat with the current preference rules.")
        return diag_messages

    # Step 2: Check which trucks are on duty
    trucks_on_duty = {t.truck_id: truck_hours.get(t.truck_id, {}).get(req_date.weekday()) for t in suitable_trucks}
    diag_messages.append("**Step 2: Duty Status**")
    diag_messages.append(json.dumps({ECM_TRUCKS[tid].truck_name: str(hrs) if hrs else "Off Duty" for tid, hrs in trucks_on_duty.items()}))
    working_truck_ids = [tid for tid, hrs in trucks_on_duty.items() if hrs]
    if not working_truck_ids:
        diag_messages.append(f"**Failure Reason:** No suitable trucks are scheduled to work on this day.")
        return diag_messages

    # Step 3 & 4 are only relevant if a ramp is selected
    if not ramp_obj:
        diag_messages.append("**No Ramp Selected:** Cannot perform tide analysis.")
    else:
        all_tides = fetch_noaa_tides_for_range(ramp_obj.noaa_station_id, req_date, req_date)
        tides_for_day = all_tides.get(req_date, [])
        diag_messages.append(f"**Step 3: Fetched Tides for {ramp_obj.ramp_name}**")
        diag_messages.append(json.dumps([{'type': t['type'], 'time': str(t['time'])} for t in tides_for_day if t['type'] == 'H']))

        final_windows_found_for_any_truck = False
        longest_window_found = timedelta(0)
        
        diag_messages.append("**Step 4: Overlap Calculation**")
        for truck_id in working_truck_ids:
            final_windows = get_final_schedulable_ramp_times(ramp_obj, boat, req_date, all_tides, truck_id, truck_hours)
            diag_messages.append(f" - Overlap for **{ECM_TRUCKS[truck_id].truck_name}**: `{len(final_windows)}` valid window(s) found.")
            if final_windows:
                final_windows_found_for_any_truck = True
                for window in final_windows:
                    start_dt = datetime.datetime.combine(req_date, window['start_time'])
                    end_dt = datetime.datetime.combine(req_date, window['end_time'])
                    if end_dt > start_dt:
                        longest_window_found = max(longest_window_found, end_dt - start_dt)

        if not final_windows_found_for_any_truck:
            diag_messages.append("**Failure Reason: Tide/Work Hour Conflict:** No valid tide windows overlap with available truck working hours.")
            return diag_messages
        
        # --- NEW, MORE ACCURATE FINAL CHECK ---
        rules = BOOKING_RULES.get(boat.boat_type, {})
        job_duration = timedelta(minutes=rules.get('truck_mins', 90))
        
        if longest_window_found < job_duration:
            diag_messages.append(f"**Failure Reason: Job Too Long for Window:** The required job duration ({job_duration.total_seconds()/60:.0f} mins) is longer than the longest available time window ({longest_window_found.total_seconds()/60:.0f} mins) on this day.")
            return diag_messages

    # This is now the true final reason if all other checks pass.
    diag_messages.append("**Failure Reason: All Slots Booked:** All available time slots for suitable trucks are already taken on this date.")
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

def find_available_job_slots(customer_id, boat_id, service_type, requested_date_str,
                             selected_ramp_id=None, force_preferred_truck=True, num_suggestions_to_find=5,
                             manager_override=False, crane_look_back_days=7, crane_look_forward_days=60,
                             truck_operating_hours=None, prioritize_sailboats=True,
                             ramp_tide_blackout_enabled=True,
                             scituate_powerboat_priority_enabled=True,
                             is_bulk_job=False,
                             dynamic_duration_enabled=True,
                             search_start_date=None,
                             search_end_date=None,
                             max_job_distance=10,
                             **kwargs):
    """
    Finds best slots using "closest day first" search, ECM boat priority, and full scoring logic.
    """
    truck_operating_hours = truck_operating_hours or TRUCK_OPERATING_HOURS
    try:
        requested_date = datetime.datetime.strptime(requested_date_str, '%Y-%m-%d').date()
    except ValueError:
        return [], "Error: Invalid date format.", [], False

    customer = get_customer_details(customer_id)
    boat = get_boat_details(boat_id)
    if not customer or not boat: return [], "Invalid Customer or Boat ID.", [], False
    
    is_priority_sailboat = "Sailboat" in boat.boat_type and boat.draft_ft is not None and boat.draft_ft > 3.0

    ramp_obj = get_ramp_details(selected_ramp_id)
    rules = BOOKING_RULES.get(boat.boat_type, {})
    hauler_duration = timedelta(minutes=rules.get('truck_mins', 90))
    j17_duration = timedelta(minutes=rules.get('crane_mins', 0))
    needs_j17 = j17_duration.total_seconds() > 0
    suitable_trucks = get_suitable_trucks(boat.boat_length, boat.preferred_truck_id, force_preferred_truck)

    min_search_date = requested_date - timedelta(days=crane_look_back_days if not is_bulk_job else 0)
    max_search_date = requested_date + timedelta(days=crane_look_forward_days)
    day_search_order = _generate_day_search_order(requested_date, crane_look_back_days, crane_look_forward_days)

    all_tides = fetch_noaa_tides_for_range(ramp_obj.noaa_station_id, min_search_date, max_search_date) if ramp_obj else {}
    compiled_schedule, daily_truck_last_location = _compile_truck_schedules(SCHEDULED_JOBS)
    yard_coords = get_location_coords(address=YARD_ADDRESS)
    
    prime_tide_days = set()
    if is_priority_sailboat and prioritize_sailboats:
        duxbury_ramp_id = "3000002"
        prime_day_tides = fetch_noaa_tides_for_range(ECM_RAMPS[duxbury_ramp_id].noaa_station_id, min_search_date, max_search_date)
        prime_window_start, prime_window_end = time(10, 0), time(14, 0)
        for day, tides in prime_day_tides.items():
            if any(t['type'] == 'H' and prime_window_start <= t['time'] <= prime_window_end for t in tides):
                prime_tide_days.add(day)

    all_found_slots = []
    SEARCH_LIMIT = num_suggestions_to_find if not is_bulk_job else 50

    for check_date in day_search_order:
        if not (min_search_date <= check_date <= max_search_date): continue
        if is_priority_sailboat and prioritize_sailboats and check_date not in prime_tide_days:
            continue

        if service_type == "Launch": new_job_coords = get_location_coords(address=boat.storage_address, boat_id=boat.boat_id)
        elif service_type in ["Haul", "Transport"]: new_job_coords = get_location_coords(ramp_id=selected_ramp_id)
        else: continue

        for truck in suitable_trucks:
            hauler_last_job = daily_truck_last_location.get(truck.truck_id, {}).get(check_date)
            
            if hauler_last_job:
                truck_current_coords = hauler_last_job[1]
                distance_from_last_job = _calculate_distance_miles(truck_current_coords, new_job_coords)
                if distance_from_last_job > max_job_distance:
                    continue
            else:
                truck_current_coords = yard_coords
            
            truck_hours = truck_operating_hours.get(truck.truck_id, {}).get(check_date.weekday())
            if not truck_hours: continue
            
            windows = get_final_schedulable_ramp_times(ramp_obj, boat, check_date, all_tides, truck.truck_id, truck_operating_hours, ramp_tide_blackout_enabled)
            
            for window in windows:
                proposed_start_dt = datetime.datetime.combine(check_date, window['start_time'], tzinfo=timezone.utc)
                window_end_dt = datetime.datetime.combine(check_date, window['end_time'], tzinfo=timezone.utc)
                hauler_available_from = _round_time_to_nearest_quarter_hour(datetime.datetime.combine(check_date, truck_hours[0], tzinfo=timezone.utc))
                if hauler_last_job: hauler_available_from = max(hauler_available_from, _round_time_to_nearest_quarter_hour(hauler_last_job[0]))
                
                hauler_travel = timedelta(minutes=calculate_travel_time(truck_current_coords, new_job_coords))
                actual_start_dt = max(proposed_start_dt, hauler_available_from)

                if dynamic_duration_enabled:
                    hauler_total_duration = hauler_duration + hauler_travel
                else:
                    hauler_total_duration = hauler_duration

                latest_possible_start_dt = datetime.datetime.combine(check_date, truck_hours[1], tzinfo=timezone.utc) - hauler_total_duration
                is_first_slot = actual_start_dt.time() == truck_hours[0]
                is_last_slot = actual_start_dt >= latest_possible_start_dt
                is_reserved_slot = (service_type == "Launch" and is_first_slot) or (service_type == "Haul" and is_last_slot)

                if is_reserved_slot and not boat.is_ecm_boat:
                    continue
                
                hauler_end_dt = _round_time_to_nearest_quarter_hour(actual_start_dt + hauler_total_duration)
                
                if hauler_end_dt <= window_end_dt:
                    if check_truck_availability_optimized(truck.truck_id, actual_start_dt, hauler_end_dt, compiled_schedule):
                        # Crane logic needs full implementation for sailboat jobs
                        if not needs_j17:
                            # --- THIS BLOCK IS CORRECTED ---
                            # It now includes the tide information from the 'window' object.
                            all_found_slots.append({
                                'date': check_date, 'time': actual_start_dt.time(),
                                'hauler_end_dt': hauler_end_dt, 'j17_end_dt': None,
                                'truck_id': truck.truck_id, 'j17_needed': needs_j17, 'ramp_id': selected_ramp_id,
                                'is_priority_slot': is_reserved_slot and boat.is_ecm_boat,
                                'tide_rule_concise': window.get('tide_rule_concise', 'N/A'),
                                'high_tide_times': window.get('high_tide_times', []),
                                'debug_trace': {'deadhead_travel_minutes': hauler_travel.total_seconds() / 60}
                            })

        if len(all_found_slots) >= SEARCH_LIMIT: break
    
    # Scoring logic
    ideal_start_time = time(8, 0)
    for slot in all_found_slots:
        score = 0
        score_trace = {}
        
        days_away = abs((slot['date'] - requested_date).days)
        score -= days_away * 10
        score_trace['Date Proximity Penalty'] = f"-{days_away * 10}"

        if slot.get('is_priority_slot'):
            score += 500
            score_trace['ECM Priority Bonus'] = "+500"
        
        slot_start_dt = datetime.datetime.combine(slot['date'], slot['time'])
        minutes_from_ideal = abs((slot_start_dt - datetime.datetime.combine(slot['date'], ideal_start_time)).total_seconds() / 60)
        start_time_bonus = max(0, 200 - (minutes_from_ideal * 0.5))
        score += start_time_bonus
        score_trace['Start Time Bonus'] = f"+{start_time_bonus:.0f}"

        deadhead = slot.get('debug_trace', {}).get('deadhead_travel_minutes', 0)
        score -= deadhead * 2
        score_trace['Deadhead Penalty'] = f"-{deadhead * 2:.0f}"

        slot['score'] = score
        slot['debug_trace']['score_calculation'] = score_trace
        slot['debug_trace']['FINAL_SCORE'] = score
            
    all_found_slots.sort(key=lambda s: s.get('score', 0), reverse=True)

    if not all_found_slots:
        return [], "No suitable slots could be found.", _diagnose_failure_reasons(requested_date, customer, boat, ramp_obj, service_type, truck_operating_hours, manager_override, force_preferred_truck), False

    return all_found_slots[:num_suggestions_to_find], f"Found {len(all_found_slots)} potential slots.", [], False

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
            # Safety check to prevent crash if ramp is not found
            dropoff_ramp_obj = get_ramp_details(dropoff_rid)
            dropoff_addr = dropoff_ramp_obj.ramp_name if dropoff_ramp_obj else ""
            
        elif service_type == "Haul":
            dropoff_addr = boat.storage_address
            pickup_rid = selected_ramp_id or boat.preferred_ramp_id
            # Safety check to prevent crash if ramp is not found
            pickup_ramp_obj = get_ramp_details(pickup_rid)
            pickup_addr = pickup_ramp_obj.ramp_name if pickup_ramp_obj else ""
        
        # Use the precise, pre-calculated start and end times from the selected slot
        start_dt = datetime.datetime.combine(selected_slot['date'], selected_slot['time'], tzinfo=timezone.utc)
        hauler_end_dt = selected_slot['hauler_end_dt']
        j17_end_dt = selected_slot.get('j17_end_dt')
        
        new_job = Job(
            customer_id=customer.customer_id, boat_id=boat.boat_id, service_type=service_type,
            scheduled_start_datetime=start_dt,
            scheduled_end_datetime=hauler_end_dt,
            assigned_hauling_truck_id=selected_slot['truck_id'],
            assigned_crane_truck_id="J17" if selected_slot.get('j17_needed') else None,
            j17_busy_end_datetime=j17_end_dt,
            pickup_ramp_id=pickup_rid, dropoff_ramp_id=dropoff_rid,
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


def generate_random_jobs(num_to_gen, target_date, service_type_filter, truck_hours, dynamic_duration_enabled=False, max_job_distance=10):
    """
    Generates jobs, now enforcing the preferred_truck constraint to better simulate real-world entry.
    """
    if not LOADED_BOATS:
        return "Cannot generate jobs: Boat data is not loaded."

    services_to_use = ["Launch", "Haul"] if service_type_filter == "All" else [service_type_filter]
    
    any_tide_ramps = [r.ramp_id for r in ECM_RAMPS.values() if "AnyTide" in r.tide_calculation_method and r.ramp_id]
    tide_dependent_ramps = [r.ramp_id for r in ECM_RAMPS.values() if "AnyTide" not in r.tide_calculation_method and r.ramp_id]

    all_boats = list(LOADED_BOATS.values())
    random.shuffle(all_boats)
    
    priority_sailboats = [b for b in all_boats if "Sailboat" in b.boat_type and b.draft_ft is not None and b.draft_ft > 3.0]
    powerboats = [b for b in all_boats if "Powerboat" in b.boat_type]
    other_boats = [b for b in all_boats if b not in priority_sailboats and b not in powerboats]
    
    boats_to_schedule = (priority_sailboats + powerboats + other_boats)[:num_to_gen]
    
    success_count = 0
    failure_count = 0
    first_failure_details = None

    for boat in boats_to_schedule:
        customer = get_customer_details(boat.customer_id)
        if not customer: continue

        selected_ramp_id = None
        is_sailboat = "Sailboat" in boat.boat_type
        if is_sailboat and tide_dependent_ramps:
            selected_ramp_id = random.choice(tide_dependent_ramps)
        elif not is_sailboat and any_tide_ramps:
            selected_ramp_id = random.choice(any_tide_ramps)
        else:
            selected_ramp_id = random.choice(list(ECM_RAMPS.keys()))

        slots, msg, reasons, _ = find_available_job_slots(
            customer_id=customer.customer_id,
            boat_id=boat.boat_id,
            service_type="Haul",
            requested_date_str=target_date.strftime('%Y-%m-%d'),
            selected_ramp_id=selected_ramp_id,
            force_preferred_truck=True,
            num_suggestions_to_find=1,
            truck_operating_hours=truck_hours,
            is_bulk_job=True,
            dynamic_duration_enabled=dynamic_duration_enabled,
            max_job_distance=max_job_distance
        )

        if slots:
            confirm_and_schedule_job(
                original_request={'customer_id': customer.customer_id, 'boat_id': boat.boat_id, 'service_type': "Haul"},
                selected_slot=slots[0]
            )
            success_count += 1
        else:
            failure_count += 1
            if first_failure_details is None:
                job_details = (
                    f"Attempted to schedule:\n"
                    f"  - Customer: {customer.customer_name}\n"
                    f"  - Boat: {boat.boat_length}' {boat.boat_type}\n"
                    f"  - Service: Haul starting from {target_date.strftime('%Y-%m-%d')}"
                )
                failure_analysis = "\n".join(reasons)
                first_failure_details = f"\n\n--- Analysis of First Failure ---\n{job_details}\n\n{failure_analysis}"
    
    summary = f"Job generation complete. Successfully created: {success_count}. Failed to find slots for: {failure_count}."
    if first_failure_details:
        summary += first_failure_details
        
    return summary
