from dataclasses import dataclass, field
from typing import Optional, List, Union

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
from requests.adapters import HTTPAdapter, Retry

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
_geolocator = Nominatim(user_agent="ecm_boat_scheduler_app")
_location_coords_cache = {} # Ensure this line is present here

DEBUG_MESSAGES = []

def _log_debug(msg):
    """Adds a timestamped message to the global debug log."""
    DEBUG_MESSAGES.insert(0, f"{datetime.datetime.now().strftime('%H:%M:%S')}: {msg}")

def fetch_scheduled_jobs():
    """
    Fetches and updates the global SCHEDULED_JOBS list from the database
    with all required columns.
    """
    global SCHEDULED_JOBS
    try:
        conn = get_db_connection()
        # CORRECTED: This now includes all columns to create complete Job objects.
        query_columns = (
            "job_id, customer_id, boat_id, service_type, "
            "scheduled_start_datetime, scheduled_end_datetime, "
            "assigned_hauling_truck_id, assigned_crane_truck_id, "
            "S17_busy_end_datetime, pickup_ramp_id, dropoff_ramp_id, "
            "pickup_street_address, dropoff_street_address, job_status, notes, "
            "pickup_latitude, pickup_longitude, dropoff_latitude, dropoff_longitude"
        )
        jobs_resp = execute_query(conn.table("jobs").select(query_columns).eq("job_status", "Scheduled"), ttl=0)
        
        SCHEDULED_JOBS.clear()
        if isinstance(jobs_resp.data, list):
            SCHEDULED_JOBS.extend([Job(**row) for row in jobs_resp.data if row.get('scheduled_start_datetime')])
        
        _log_debug(f"Refreshed schedule: Found {len(SCHEDULED_JOBS)} jobs.")
    except Exception as e:
        st.error(f"Error refreshing jobs from database: {e}")



# --- DATA MODELS (CLASSES) ---
@dataclass
class Truck:
    truck_id: int
    truck_name: Optional[str] = None
    max_boat_length: Optional[Union[int, float]] = None
    is_crane: bool = field(init=False)

    def __post_init__(self):
        self.is_crane = "Crane" in (self.truck_name or "")

    # Backward-compatible constructor
    def __init__(self, t_id, name, max_len):
        object.__setattr__(self, "truck_id", int(t_id) if t_id is not None else None)
        object.__setattr__(self, "truck_name", name)
        object.__setattr__(self, "max_boat_length", max_len)
        # __post_init__ emulation (since we bypass dataclass init)
        object.__setattr__(self, "is_crane", "Crane" in (name or ""))

@dataclass
class Ramp:
    ramp_id: Union[int, str]
    ramp_name: Optional[str]
    noaa_station_id: Optional[str]
    tide_calculation_method: str = "AnyTide"
    tide_offset_hours1: Optional[Union[int, float]] = None
    allowed_boat_types: Optional[List[str]] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    def __init__(self, r_id, name, station, tide_method="AnyTide", offset=None, boats=None, latitude=None, longitude=None):
        object.__setattr__(self, "ramp_id", r_id)
        object.__setattr__(self, "ramp_name", name)
        object.__setattr__(self, "noaa_station_id", station)
        object.__setattr__(self, "tide_calculation_method", tide_method or "AnyTide")
        object.__setattr__(self, "tide_offset_hours1", offset)
        object.__setattr__(self, "allowed_boat_types", boats or ["Powerboat", "Sailboat DT", "Sailboat MT"])
        object.__setattr__(self, "latitude", float(latitude) if latitude is not None else None)
        object.__setattr__(self, "longitude", float(longitude) if longitude is not None else None)

@dataclass
class Customer:
    customer_id: int
    customer_name: str

    def __init__(self, c_id, name):
        object.__setattr__(self, "customer_id", int(c_id))
        object.__setattr__(self, "customer_name", name)

@dataclass
class Boat:
    boat_id: int
    customer_id: int
    boat_type: Optional[str]
    boat_length: Optional[Union[int, float]]
    draft_ft: Optional[Union[int, float]]
    storage_address: Optional[str]
    preferred_ramp_id: Optional[Union[int, str]]
    preferred_truck_id: Optional[Union[int, str]]
    is_ecm_boat: bool = False
    storage_latitude: Optional[float] = None
    storage_longitude: Optional[float] = None

    def __init__(self, b_id, c_id, b_type, b_len, draft, storage_addr, pref_ramp, pref_truck, is_ecm, storage_latitude=None, storage_longitude=None):
        object.__setattr__(self, "boat_id", int(b_id))
        object.__setattr__(self, "customer_id", int(c_id))
        object.__setattr__(self, "boat_type", b_type)
        object.__setattr__(self, "boat_length", b_len)
        object.__setattr__(self, "draft_ft", draft)
        object.__setattr__(self, "storage_address", storage_addr)
        object.__setattr__(self, "preferred_ramp_id", pref_ramp)
        object.__setattr__(self, "preferred_truck_id", pref_truck)
        object.__setattr__(self, "is_ecm_boat", bool(is_ecm))
        object.__setattr__(self, "storage_latitude", float(storage_latitude) if storage_latitude is not None else None)
        object.__setattr__(self, "storage_longitude", float(storage_longitude) if storage_longitude is not None else None)

@dataclass
class Job:
    job_id: Optional[int] = None
    customer_id: Optional[int] = None
    boat_id: Optional[int] = None
    service_type: Optional[str] = None
    scheduled_start_datetime: Optional[datetime.datetime] = None
    scheduled_end_datetime: Optional[datetime.datetime] = None
    assigned_hauling_truck_id: Optional[Union[int, str]] = None
    assigned_crane_truck_id: Optional[Union[int, str]] = None
    S17_busy_end_datetime: Optional[datetime.datetime] = None
    pickup_ramp_id: Optional[Union[int, str]] = None
    dropoff_ramp_id: Optional[Union[int, str]] = None
    pickup_street_address: Optional[str] = None
    dropoff_street_address: Optional[str] = None
    job_status: Optional[str] = None
    notes: Optional[str] = None
    pickup_latitude: Optional[float] = None
    pickup_longitude: Optional[float] = None
    dropoff_latitude: Optional[float] = None
    dropoff_longitude: Optional[float] = None

    def __init__(self, **kwargs):
        # Preserve backward-compatible kwargs and types
        for field in ("job_id","customer_id","boat_id","service_type",
                      "assigned_hauling_truck_id","assigned_crane_truck_id",
                      "pickup_ramp_id","dropoff_ramp_id",
                      "pickup_street_address","dropoff_street_address",
                      "job_status","notes",
                      "pickup_latitude","pickup_longitude",
                      "dropoff_latitude","dropoff_longitude"):
            object.__setattr__(self, field, kwargs.get(field))

        # Parse datetimes with tz awareness if possible
        def _parse_dt(val):
            if val is None:
                return None
            if isinstance(val, datetime.datetime):
                return val
            if isinstance(val, str):
                try:
                    return datetime.datetime.fromisoformat(val.replace(" ", "T"))
                except Exception:
                    return None
            return None

        start_dt = _parse_dt(kwargs.get("scheduled_start_datetime"))
        end_dt = _parse_dt(kwargs.get("scheduled_end_datetime"))
        s17_dt = _parse_dt(kwargs.get("S17_busy_end_datetime"))

        object.__setattr__(self, "scheduled_start_datetime", start_dt)
        object.__setattr__(self, "scheduled_end_datetime", end_dt)
        object.__setattr__(self, "S17_busy_end_datetime", s17_dt)

        # Coerce numeric ids
        for fid in ("job_id","customer_id","boat_id"):
            v = getattr(self, fid)
            try:
                object.__setattr__(self, fid, int(v) if v is not None else None)
            except Exception:
                pass

