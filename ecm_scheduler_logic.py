from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, List, Union, Tuple, Set

import csv
import os
from typing import Optional, List, Union, Tuple, Set, Dict
import datetime as dt
from datetime import time, date, timedelta, timezone
from datetime import datetime as _dt, time as _time, date as _date, timedelta as _td
import pandas as pd
import calendar
import requests
import random
import json
import streamlit as st
from st_supabase_connection import SupabaseConnection, execute_query
from collections import Counter, defaultdict   # pull in defaultdict here
from geopy.geocoders import Nominatim
from supabase import create_client
from requests.adapters import HTTPAdapter, Retry

# --- Tide policy knobs (you can tweak these) ---
LAUNCH_PREP_MIN_POWER = 30        # powerboat time before arriving to ramp
LAUNCH_PREP_MIN_SAIL  = 120       # sailboat time before arriving to ramp
LAUNCH_RAMP_MIN       = 60        # minutes at ramp during a launch (both types)

HAUL_RAMP_MIN         = 30        # minutes at ramp at the start of a haul

# how much earlier than the window we’ll allow ramp work to begin (prep can happen before)
LAUNCH_EARLY_ALLOW_POWER_MIN = 30
LAUNCH_EARLY_ALLOW_SAIL_MIN  = 120


# --- IN-MEMORY DATA CACHES & GLOBALS (must be defined before any function uses them) ---
DEBUG_MESSAGES: list[str] = []
TRAVEL_TIME_MATRIX: dict = {}

IDEAL_CRANE_DAYS: set[tuple[str, dt.date]] = set()
CANDIDATE_CRANE_DAYS: dict[str, list[dict]] = {
    'ScituateHarborJericho': [],
    'PlymouthHarbor': [],
    'WeymouthWessagusset': [],
    'CohassetParkerAve': []
}

# Master entities
ECM_TRUCKS: dict[str, "Truck"] = {}
LOADED_CUSTOMERS: dict[int, "Customer"] = {}
LOADED_BOATS: dict[int, "Boat"] = {}
ECM_RAMPS: dict[str, "Ramp"] = {}
TRUCK_OPERATING_HOURS: dict[str, dict[int, tuple[dt.time, dt.time]]] = {}

# Jobs
SCHEDULED_JOBS: list["Job"] = []
PARKED_JOBS: dict[int, "Job"] = {}

# Tide protection windows
CRANE_WINDOWS: dict[tuple[str, dt.date], list[tuple[dt.time, dt.time]]] = {}
ANYTIDE_LOW_TIDE_WINDOWS: dict[tuple[str, dt.date], list[tuple[dt.time, dt.time]]] = {}

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

# --- PROTECTED WINDOWS & PREFERENCES (precomputed) ---
CRANE_WINDOWS = {}  # {(ramp_id:str, date:date): [(start_time, end_time), ...]}
ANYTIDE_LOW_TIDE_WINDOWS = {}  # {(ramp_id:str, date:date): [(start_time, end_time), ...]}

# ================================
# GLOBAL DEFAULTS / SAFE BOOTSTRAP
# (must exist before any functions use them)
# ================================
YARD_ADDRESS = "43 Mattakeesett St, Pembroke, MA 02359"
DEFAULT_NOAA_STATION = "8445138"  # Scituate Harbor, MA


# Pre-initialize global caches and registries
DEBUG_MESSAGES: list[str] = []

CRANE_WINDOWS: dict[tuple[str, dt.date], list[tuple[dt.time, dt.time]]] = {}
ANYTIDE_LOW_TIDE_WINDOWS: dict[tuple[str, dt.date], list[tuple[dt.time, dt.time]]] = {}

IDEAL_CRANE_DAYS: set[tuple[str, dt.date]] = set()
CANDIDATE_CRANE_DAYS = {
    'ScituateHarborJericho': [],
    'PlymouthHarbor': [],
    'WeymouthWessagusset': [],
    'CohassetParkerAve': [],
}
crane_daily_status: dict = {}

ECM_TRUCKS: dict = {}
LOADED_CUSTOMERS: dict = {}
LOADED_BOATS: dict = {}
ECM_RAMPS: dict = {}
TRUCK_OPERATING_HOURS: dict = {}

SCHEDULED_JOBS: list = []
PARKED_JOBS: dict = {}

DEBUG_MESSAGES = []

BOOKING_RULES = {
    'Powerboat':  {'truck_mins': 90,  'crane_mins': 0},
    'Sailboat DT':{'truck_mins': 180, 'crane_mins': 60},
    'Sailboat MT':{'truck_mins': 180, 'crane_mins': 90},
}

# ADD THIS NEW CODE BLOCK

RAMP_ABBREVIATIONS = {
    "Cohasset Harbor (Parker Ave)": "Coh Ramp",
    "Cordage Park (Ply)": "Cordage",
    "Duxbury Harbor": "Dux Ramp",
    "Ferry Street  (Marshfield Yacht  Club)": "Ferry Ramp",
    "Green Harbor  (Taylors)": "Green H Taylors",
    "Hingham Harbor": "Hing Ramp",
    "Hull (A St, Sunset, Steamboat -": "Hull A St",
    "Hull (X Y Z v st)": "Hull XYZ",
    "Plymouth Harbor": "Plym Ramp",
    "Roht (A to Z/ Mary's)": "Roht",
    "Green harbor (Safe Harbor)": "GH Safe",
    "Scituate Harbor  (Jericho Road)": "Sci Ramp",
    "South River Yacht Yard": "SRYY",
    "Weymouth Harbor": "Wey Ramp",
    "Scituate Jericho": "Sci Jericho",
    "Scituate Boat Works": "Sci BW",
    "Steamboat": "Steamboat",
    "Port Norfolk Yacht Club": "PNYC",
    "Bullman Marine": "Bullman",
    "Savin HIll Yacht Club": "Savin H",
}

def get_ramp_display_name(full_ramp_name):
    """Returns the abbreviated ramp name if one exists, otherwise returns the full name."""
    if full_ramp_name is None:
        return ""
    return RAMP_ABBREVIATIONS.get(full_ramp_name, full_ramp_name)

def _log_debug(msg):
    """Adds a timestamped message to the global debug log."""
    # Ensure DEBUG_MESSAGES is treated as a global variable
    global DEBUG_MESSAGES
    DEBUG_MESSAGES.insert(0, f"{dt.datetime.now().strftime('%H:%M:%S')}: {msg}")

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
class Truck:
    def __init__(self, t_id, name, max_len):
        self.truck_id, self.truck_name, self.max_boat_length, self.is_crane = t_id, name, max_len, "Crane" in name

class Ramp:
    def __init__(self, r_id, name, station, tide_method="AnyTide", offset=None, boats=None, latitude=None, longitude=None):
        self.ramp_id = r_id
        self.ramp_name = name
        self.noaa_station_id = station
        self.tide_calculation_method = tide_method
        self.tide_offset_hours1 = offset
        self.allowed_boat_types = boats or ["Powerboat", "Sailboat DT", "Sailboat MT"]
        self.latitude = float(latitude) if latitude is not None else None
        self.longitude = float(longitude) if longitude is not None else None

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
        # ----- small helpers -----
        def _parse_int(v):
            try:
                return int(v) if v is not None and str(v).strip() != "" else None
            except (ValueError, TypeError):
                return None

        def _parse_float(v):
            try:
                return float(v) if v is not None and str(v).strip() != "" else None
            except (ValueError, TypeError):
                return None

        # ----- scalar ids / strings -----
        self.job_id                     = _parse_int(kwargs.get("job_id"))
        self.customer_id                = _parse_int(kwargs.get("customer_id"))
        self.boat_id                    = _parse_int(kwargs.get("boat_id"))
        self.service_type               = kwargs.get("service_type")

        # ----- datetimes (always timezone-aware UTC when present) -----
        self.scheduled_start_datetime   = self._parse_or_get_datetime(kwargs.get("scheduled_start_datetime"))
        self.scheduled_end_datetime     = self._parse_or_get_datetime(kwargs.get("scheduled_end_datetime"))
        self.assigned_hauling_truck_id  = kwargs.get("assigned_hauling_truck_id")
        self.assigned_crane_truck_id    = kwargs.get("assigned_crane_truck_id")
        self.S17_busy_end_datetime      = self._parse_or_get_datetime(kwargs.get("S17_busy_end_datetime"))

        # ----- ramps / addresses -----
        self.pickup_ramp_id             = kwargs.get("pickup_ramp_id")
        self.dropoff_ramp_id            = kwargs.get("dropoff_ramp_id")
        self.pickup_street_address      = kwargs.get("pickup_street_address", "") or ""
        self.dropoff_street_address     = kwargs.get("dropoff_street_address", "") or ""

        # ----- status / notes -----
        self.job_status                 = kwargs.get("job_status", "Scheduled")
        self.notes                      = kwargs.get("notes", "")

        # ----- coords (floats or None) -----
        self.pickup_latitude            = _parse_float(kwargs.get("pickup_latitude"))
        self.pickup_longitude           = _parse_float(kwargs.get("pickup_longitude"))
        self.dropoff_latitude           = _parse_float(kwargs.get("dropoff_latitude"))
        self.dropoff_longitude          = _parse_float(kwargs.get("dropoff_longitude"))

    # ---- helper lives at class level (NOT nested inside __init__) ----
    def _parse_or_get_datetime(self, dt_value):
        """Return a timezone-aware (UTC) datetime or None."""
        parsed = None

        if isinstance(dt_value, dt.datetime):
            parsed = dt_value
        elif isinstance(dt_value, str):
            try:
                # tolerate "YYYY-MM-DD HH:MM:SS" by replacing the space with "T"
                parsed = dt.datetime.fromisoformat(dt_value.replace(" ", "T"))
            except (ValueError, TypeError):
                return None

        if parsed is None:
            return None

        # make UTC if naive
        if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
            return parsed.replace(tzinfo=dt.timezone.utc)
        return parsed

    # --- Back-compat alias properties (keep existing code working) ---
    @property
    def scheduled_start_dt(self):
        return self.scheduled_start_datetime

    @property
    def scheduled_end_dt(self):
        return self.scheduled_end_datetime

### Helpers

# ---- TIDE POLICY (default, adjustable later from UI) ----
DEFAULT_TIDE_POLICY = {
    # Launch: allowed lead *before* the ramp window OPENS
    "launch_open_lead_power_mins": 30,     # powerboats
    "launch_open_lead_sail_mins": 120,     # sailboats

    # Haul: job may START this much time *before* the ramp window CLOSES
    "haul_close_lead_all_mins": 30,

    # Step size when scanning start times
    "scan_step_mins": 15,
}

# A process-wide policy you can change at runtime if you want
_GLOBAL_TIDE_POLICY = DEFAULT_TIDE_POLICY.copy()

def set_global_tide_policy(policy: dict | None):
    """Optional: let the UI push a policy once. Safe no-op if None."""
    global _GLOBAL_TIDE_POLICY
    if isinstance(policy, dict):
        merged = DEFAULT_TIDE_POLICY.copy()
        merged.update({k: v for k, v in policy.items() if v is not None})
        _GLOBAL_TIDE_POLICY = merged
    return _GLOBAL_TIDE_POLICY


### New rules around truck job sart before and after ramp tide windows
import datetime as dt

def _minutes(n: int) -> dt.timedelta:
    return dt.timedelta(minutes=int(n or 0))

def job_duration_minutes(boat_type: str, service_type: str) -> int:
    # Universal truth from your spec:
    #   - Launch: 90 min (power) / 180 min (sail)
    #   - Haul:   90 min (power) / 180 min (sail)
    is_sail = ("sail" in (boat_type or "").lower())
    if service_type in ("Launch", "Haul"):
        return 180 if is_sail else 90
    # Other services (Paint, Sandblast) – treat like 90 unless you want to be stricter
    return 90

def _windows_dt_for_day(windows_time_list, day: dt.date):
    """
    Convert [(time_start, time_end), ...] → [(datetime_start, datetime_end), ...]
    """
    out = []
    for ws, we in (windows_time_list or []):
        if isinstance(ws, dt.time) and isinstance(we, dt.time):
            out.append((dt.datetime.combine(day, ws), dt.datetime.combine(day, we)))
    return out

def tide_policy_ok(
    service_type: str,
    boat,
    start_dt: dt.datetime,
    end_dt:   dt.datetime,
    windows: list[tuple[dt.time, dt.time]],
    policy: dict | None = None,
) -> bool:
    """
    Implements your universal truths:

    Powerboats: 90 min job. First 30 min is load/drive. Can START 30 min BEFORE window OPENS.
    Sailboats:  180 min job. First 120 min is truck+crane prep. Can START 120 min BEFORE window OPENS.
    Hauls (both): The first 30 min of on-ramp work must be completed BEFORE window CLOSES.
    """
    policy = (policy or _GLOBAL_TIDE_POLICY or DEFAULT_TIDE_POLICY)

    is_sail = "Sail" in (getattr(boat, "boat_type", "") or "")
    if service_type == "Launch":
        lead = policy.get("launch_prep_sail_min", 120) if is_sail else policy.get("launch_prep_power_min", 30)
        critical = start_dt + dt.timedelta(minutes=lead)

        for a, b in windows:
            ws = dt.datetime.combine(start_dt.date(), a, tzinfo=start_dt.tzinfo)
            we = dt.datetime.combine(start_dt.date(), b, tzinfo=start_dt.tzinfo)
            if ws <= critical <= we:
                return True
        return False

    # --- THIS SECTION IS NOW CORRECTED ---
    if service_type == "Haul":
        on_ramp_duration = dt.timedelta(minutes=policy.get("haul_close_lead_all_mins", 30))
        for a, b in windows:
            ws = dt.datetime.combine(start_dt.date(), a, tzinfo=start_dt.tzinfo)
            we = dt.datetime.combine(start_dt.date(), b, tzinfo=start_dt.tzinfo)
            
            # The job must start within the window (ws <= start_dt)
            # AND it must start early enough that the on-ramp part finishes by the window's end.
            if ws <= start_dt and (start_dt + on_ramp_duration) <= we:
                return True
        return False

    # Other services: require the start to actually be in a window
    for a, b in windows:
        ws = dt.datetime.combine(start_dt.date(), a, tzinfo=start_dt.tzinfo)
        we = dt.datetime.combine(start_dt.date(), b, tzinfo=start_dt.tzinfo)
        if ws <= start_dt <= we:
            return True
    return False


# --- Public helper: probe the exact requested day once ---
def probe_requested_date_slot(
    customer_id: str,
    boat_id: str,
    service_type: str,
    requested_date_str: str,
    selected_ramp_id: str | None,
    relax_truck_preference: bool = False,
    tide_policy: dict | None = None,
):
    """
    Returns a single CAN-DO slot for the exact requested date if one exists, otherwise None.
    Does NOT search other days.
    """
    if not requested_date_str:
        return None

    try:
        day = dt.datetime.strptime(requested_date_str, "%Y-%m-%d").date()
    except Exception:
        return None

    fetch_scheduled_jobs()
    compiled_schedule, _ = _compile_truck_schedules(SCHEDULED_JOBS)

    boat = get_boat_details(boat_id)
    if not boat:
        return None

    ramp_id = selected_ramp_id or getattr(boat, "preferred_ramp_id", None)
    if not ramp_id:
        return None

    all_suitable = get_suitable_trucks(boat.boat_length)
    preferred, others = [], []
    if boat.preferred_truck_id:
        for t in all_suitable:
            (preferred if t.truck_name == boat.preferred_truck_id else others).append(t)
    else:
        others = all_suitable

    trucks = preferred or (all_suitable if relax_truck_preference else preferred)
    trucks = trucks or others

    crane_needed = "Sail" in (boat.boat_type or "")
    return _find_slot_on_day(
        day,
        boat,
        service_type,
        ramp_id,
        crane_needed,
        compiled_schedule,
        customer_id,
        trucks,
        is_opportunistic_search=False,
        tide_policy=tide_policy,
    )

def _norm_id(x):
    return None if x is None else str(x)

def _get_ramp_rule(ramp_id):
    """Return (method, base_offset_hours) for a ramp_id, or ('AnyTide', 0) fallback."""
    ramp = ECM_RAMPS.get(_norm_id(ramp_id))
    if not ramp:
        return ("AnyTide", 0.0)
    # Expect .tide_calculation_method and .tide_offset_hours on ramp object
    method = getattr(ramp, "tide_calculation_method", "AnyTide") or "AnyTide"
    offset = float(getattr(ramp, "tide_offset_hours", 0.0) or 0.0)
    return (method, offset)

def _is_anytide_for_boat(method, boat_draft_ft):
    """
    AnyTide => always True.
    AnyTideWithDraftRule => True only if shallow (<5.0 ft).
    Otherwise => False.
    """
    if method == "AnyTide": 
        return True
    if method == "AnyTideWithDraftRule":
        try:
            return float(boat_draft_ft or 0.0) < 5.0
        except:
            return False
    return False

def _window_offset_for_boat(method, base_offset, boat_draft_ft):
    """
    HoursAroundHighTide => base_offset
    HoursAroundHighTide_WithDraftRule => base_offset (+0.5h if shallow draft)
    AnyTide / AnyTideWithDraftRule(deep boats) => base_offset
    """
    try:
        draft = float(boat_draft_ft or 0.0)
    except:
        draft = 0.0

    if method == "HoursAroundHighTide_WithDraftRule":
        return (base_offset + 0.5) if draft < 5.0 else base_offset
    return base_offset

def _day_high_low_tides(ramp, day_date):
    """
    Returns (highs, lows) where each is a list of dt.time objects for the given date.
    Uses the ramp's NOAA station via your existing tide fetcher.
    """
    if not ramp or not getattr(ramp, "noaa_station_id", None):
        return ([], [])
    data = fetch_noaa_tides_for_range(ramp.noaa_station_id, day_date, day_date) or {}
    events = data.get(day_date, [])
    highs = [e["time"] for e in events if e.get("type") == "H"]
    lows  = [e["time"] for e in events if e.get("type") == "L"]
    return (highs, lows)

def _within_any_high_tide_window(dt_local, ramp, method, base_offset, boat_draft_ft):
    """
    True if dt_local falls within ±offset around any day's high tide for this ramp.
    """
    if not isinstance(dt_local, datetime):
        return False
    offset = _window_offset_for_boat(method, base_offset, boat_draft_ft)

    highs, _ = _day_high_low_tides(ramp, dt_local.date())
    for ht in highs:
        ht_dt = datetime.combine(dt_local.date(), ht, tzinfo=timezone.utc)  # keep everything UTC in your app
        if abs((dt_local - ht_dt).total_seconds()) <= offset * 3600:
            return True
    return False

def passes_tide_rules(slot_dict, when_pickup_dt, when_dropoff_dt, boat_obj):
    """
    HARD gate: both ends must be legal at their actual ramp-times.
    - Launch: pickup ramp at start, dropoff ramp at end
    - Haul:   pickup ramp at start, dropoff ramp at end   (same pattern)
    - Other:  if only one ramp defined, validate that one at start time
    """
    # Normalize & pull data
    pickup_ramp_id  = _norm_id(slot_dict.get("pickup_ramp_id")) or _norm_id(slot_dict.get("ramp_id"))
    dropoff_ramp_id = _norm_id(slot_dict.get("dropoff_ramp_id"))
    service_type    = slot_dict.get("service_type", "Launch")
    draft_ft        = getattr(boat_obj, "draft_ft", None)

    # Helper to check one ramp/time
    def _check_one(ramp_id, at_dt):
        if ramp_id is None or at_dt is None:
            return True  # if missing, do not fail here
        ramp = ECM_RAMPS.get(ramp_id)
        method, base = _get_ramp_rule(ramp_id)
        # Any-tide cases
        if _is_anytide_for_boat(method, draft_ft):
            return True
        # Otherwise must be within a high-tide window:
        return _within_any_high_tide_window(at_dt, ramp, method, base, draft_ft)

    # Compute which timestamps matter (simple, effective model)
    ok_pickup  = _check_one(pickup_ramp_id, when_pickup_dt)
    ok_dropoff = True
    if dropoff_ramp_id:
        ok_dropoff = _check_one(dropoff_ramp_id, when_dropoff_dt)

    return (ok_pickup and ok_dropoff)

# --------------- SCORING ----------------

def _route_distance_minutes(a_latlon, b_latlon):
    # ~straight-line minutes proxy; replace with your travel matrix if available
    if not a_latlon or not b_latlon:
        return 25.0
    (ax, ay), (bx, by) = a_latlon, b_latlon
    km = math.hypot(ax - bx, ay - by) * 111.0  # deg->km rough
    return (km / 50.0) * 60.0  # 50 km/h -> minutes

def _score_candidate(slot, compiled_schedule, daily_last_locations, after_threshold=False, prime_days=None):
    """
    Larger is better. Packs days, rewards piggybacks & proximity.
    """
    if prime_days is None:
        prime_days = set()

    score = 0.0
    truck_id = str(slot.get("truck_id"))
    date = slot.get("date")
    ramp_id = str(slot.get("ramp_id"))
    boat = get_boat_details(slot.get("boat_id"))

    # Get number of jobs already on this truck for this day
    todays = [iv for iv in compiled_schedule.get(truck_id, []) if iv and hasattr(iv[0], "date") and iv[0].date() == date]
    n = len(todays)

    # --- Scoring for packing days (existing logic) ---
    if n == 0: score += 2.0
    if n == 1: score += 6.0
    if n == 2: score += 10.0
    if n >= 3: score += 8.0

    if slot.get("is_piggyback"):
        score += 8.0

    # --- NEW: Scoring for geographic proximity ---
    try:
        ramp_details = get_ramp_details(ramp_id)
        if boat and ramp_details and hasattr(boat, 'storage_address'):
            storage_town = _abbreviate_town(boat.storage_address)
            ramp_name = ramp_details.ramp_name

            # Look up the pre-calculated travel time
            travel_minutes = TRAVEL_TIME_MATRIX.get(storage_town, {}).get(ramp_name)

            if travel_minutes is not None:
                # Reward shorter travel times. A 60-min trip gets 0 bonus.
                # A 10-min trip gets a bonus of 5.
                proximity_bonus = max(0.0, (60 - travel_minutes) / 10.0)
                score += proximity_bonus
    except Exception as e:
        _log_debug(f"Could not calculate proximity score: {e}")

    # --- Scoring for prime days (existing logic) ---
    if after_threshold and date in prime_days:
        tide_method = getattr(ramp_details, "tide_calculation_method", "AnyTide") if ramp_details else "AnyTide"
        boat_type = getattr(boat, "boat_type", "") if boat else ""

        if "Powerboat" in boat_type and tide_method == "AnyTide":
            score += 6.0
        elif "Sailboat" in boat_type:
            score -= 5.0

    return score
def tide_window_for_day(ramp, day):
    """
    Return list of (start_time, end_time) LOCAL time windows when a job may START.
    - If ramp has NO tide rule (hours <= 0): return [] (no explicit restriction).
    - If ramp HAS a tide rule (>0): compute ±hours around EACH high tide.
      If we cannot fetch a usable high tide time for that day, return [] (no legal start).
    """
    from datetime import time as dtime

    hours = getattr(ramp, "tide_offset_hours1", None)
    if not hours or hours <= 0:
        return []

    station_id = getattr(ramp, "noaa_station_id", None) or DEFAULT_NOAA_STATION
    events_by_day = fetch_noaa_tides_for_range(station_id, day, day) or {}
    events = events_by_day.get(day, [])

    def _as_time(x):
        if isinstance(x, dtime):
            return x
        if isinstance(x, str):
            for fmt in ("%H:%M", "%I:%M %p"):
                try:
                    return dt.datetime.strptime(x.strip(), fmt).time()
                except Exception:
                    pass
        return None

    highs = []
    for e in events:
        if not isinstance(e, dict) or e.get("type") != "H":
            continue
        tval = _as_time(e.get("time"))
        if tval:
            highs.append(tval)

    if not highs:
        return []

    windows = []
    for ht in highs:
        start_dt = (dt.datetime.combine(day, ht) - dt.timedelta(hours=hours)).time()
        end_dt   = (dt.datetime.combine(day, ht) + dt.timedelta(hours=hours)).time()
        if start_dt <= end_dt:
            windows.append((start_dt, end_dt))
        else:
            windows.append((dtime(0, 0), end_dt))
            windows.append((start_dt, dtime(23, 59)))
    return windows

def time_within_any_window(check_time: dt.time, windows: List[Tuple[dt.time, dt.time]]) -> bool:
    for a,b in windows:
        if a <= check_time <= b:
            return True
    return False

### This helper function will create a new crane day near the requested date if a grouped slot is not found.

# --- Low-tide prime day helpers (11:00–13:00 local), with Scituate fallback ---
_SCITUATE_STATION = "8445138"  # permanent fallback per your rules

def _station_for_ramp_or_scituate(ramp_id: str | None) -> str:
    if ramp_id:
        r = get_ramp_details(ramp_id)
        if r and getattr(r, "noaa_station_id", None):
            return str(r.noaa_station_id)
    return _SCITUATE_STATION

def get_low_tide_prime_days(station_id: str, start_day: _date, end_day: _date) -> set[_date]:
    """
    Returns a set of dates in [start_day, end_day] where there is at least one LOW tide
    between 11:00 and 13:00 local (rounded times in your data are fine).
    """
    prime = set()
    tides_by_day = fetch_noaa_tides_for_range(station_id, start_day, end_day)  # existing function you already use
    if not tides_by_day:
        return prime
    for d, readings in tides_by_day.items():
        for t in readings:
            if t.get("type") == "L":
                tt = t.get("time")
                if isinstance(tt, _time):
                    if _time(11, 0) <= tt <= _time(13, 0):
                        prime.add(d)
                        break
    return prime

def get_prime_tide_days(tides_by_day, tide_type="L", start_hour=11, end_hour=13):
    from datetime import time as dtime
    prime_days = set()
    for d, events in tides_by_day.items():
        for e in events:
            if e.get("type") == tide_type:
                t = e.get("time")
                if isinstance(t, dtime) and dtime(start_hour, 0) <= t <= dtime(end_hour, 0):
                    prime_days.add(d)
                    break
    return prime_days

def order_dates_with_low_tide_bias(requested_date: _date, candidate_dates: list[_date], prime_days: set[_date]) -> list[_date]:
    """
    Reorders candidate_dates:
      1) First: all candidate dates that are prime AND within ±3 days of requested_date,
         ordered by absolute proximity to requested_date (tie-break: earlier first).
      2) Then: the remaining candidate dates, ordered by proximity to requested_date.
    """
    def _dist(d: _date) -> int:
        return abs((d - requested_date).days)
    near_prime = [d for d in candidate_dates if d in prime_days and _dist(d) <= 3]
    rest = [d for d in candidate_dates if d not in near_prime]
    near_prime.sort(key=lambda d: (_dist(d), d))
    rest.sort(key=lambda d: (_dist(d), d))
    return near_prime + rest


def get_s17_truck_id():
    """Finds the numeric truck_id for the truck named 'S17'."""
    for truck_id, truck_obj in ECM_TRUCKS.items():
        if truck_obj.truck_name == "S17":
            return truck_id
    return None # Return None if S17 is not found



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
@st.cache_resource
def get_db_connection():
    url = (
        st.secrets.get("SUPABASE_URL")
        or st.secrets.get("SUPA_URL")
        or os.environ.get("SUPABASE_URL")
    )
    key = (
        st.secrets.get("SUPABASE_KEY")
        or st.secrets.get("SUPA_KEY")
        or os.environ.get("SUPABASE_KEY")
    )
    if url and key:
        return st.connection("supabase", type=SupabaseConnection, url=url.strip(), key=key.strip())
    return st.connection("supabase", type=SupabaseConnection)


# --- Added robust HTTP session + cached geocoder helpers ---
def _get_retry_session(_total=3, _backoff=0.5):
    s = requests.Session()
    retries = Retry(total=_total, backoff_factor=_backoff, status_forcelist=[500, 502, 503, 504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    return s

@st.cache_data(show_spinner=False, ttl=86400)
def _geocode_with_backoff(geolocator, address: str, timeout=10):
    import time, random
    delay = 0.5
    for _ in range(4):
        try:
            return geolocator.geocode(address, timeout=timeout)
        except Exception:
            time.sleep(delay)
            delay *= 2 * (1 + random.random() / 4)
    return None
    
# --- End helpers ---

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
        job_date = dt.datetime.fromisoformat(job_date_str.replace(" ", "T"))
    except (ValueError, TypeError):
        return False

    # Ensure the current date is timezone-aware for a correct comparison
    if current_date.tzinfo is None:
        current_date = current_date.replace(tzinfo=timezone.utc)

    return (current_date - timedelta(days=days_to_consider)) <= job_date <= (current_date + timedelta(days=days_to_consider))

def load_travel_time_matrix(filepath: str = "Town_to_Ramp_Matrix.csv"):
    """Loads the pre-calculated travel time CSV into a nested dictionary for fast lookups."""
    global TRAVEL_TIME_MATRIX
    if TRAVEL_TIME_MATRIX: # Avoid reloading if already populated
        return

    try:
        # This logic finds the file whether run locally or on Streamlit Cloud
        base_path = os.path.dirname(os.path.abspath(__file__))
        full_path = os.path.join(base_path, filepath)

        if not os.path.exists(full_path):
             # Fallback for Streamlit Cloud if the file is in the root
             full_path = filepath
             if not os.path.exists(full_path):
                 _log_debug(f"ERROR: Travel time matrix file not found at {filepath}.")
                 return

        with open(full_path, mode='r', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            header = next(reader) # Skip header row
            for row in reader:
                from_town, to_ramp, minutes = row
                if from_town not in TRAVEL_TIME_MATRIX:
                    TRAVEL_TIME_MATRIX[from_town] = {}
                TRAVEL_TIME_MATRIX[from_town][to_ramp] = int(minutes)
        _log_debug(f"Successfully loaded travel times for {len(TRAVEL_TIME_MATRIX)} towns.")
    except Exception as e:
        _log_debug(f"ERROR: Failed to load or parse travel time matrix: {e}")
        

def load_all_data_from_sheets():
    """Loads all data from Supabase, now including truck schedules."""
    global SCHEDULED_JOBS, PARKED_JOBS, LOADED_CUSTOMERS, LOADED_BOATS, ECM_TRUCKS, ECM_RAMPS, TRUCK_OPERATING_HOURS, CANDIDATE_CRANE_DAYS
    try:
        conn = get_db_connection()

        # Safety guards
        if SCHEDULED_JOBS is None: SCHEDULED_JOBS = []
        if PARKED_JOBS is None: PARKED_JOBS = {}
    
        # --- Jobs ---
        query_columns = (
            "job_id, customer_id, boat_id, service_type, "
            "scheduled_start_datetime, scheduled_end_datetime, "
            "assigned_hauling_truck_id, assigned_crane_truck_id, "
            "S17_busy_end_datetime, pickup_ramp_id, dropoff_ramp_id, "
            "pickup_street_address, dropoff_street_address, job_status, notes, "
            "pickup_latitude, pickup_longitude, dropoff_latitude, dropoff_longitude"
        )
        jobs_resp = execute_query(conn.table("jobs").select(query_columns), ttl=0)
        all_jobs = [Job(**row) for row in jobs_resp.data if row.get('scheduled_start_datetime')] if isinstance(jobs_resp.data, list) else []
        
        SCHEDULED_JOBS.clear() 
        SCHEDULED_JOBS.extend([job for job in all_jobs if job.job_status == "Scheduled"])
        PARKED_JOBS.clear()
        PARKED_JOBS.update({job.job_id: job for job in all_jobs if job.job_status == "Parked"})

        # --- Trucks ---
        trucks_resp = execute_query(conn.table("trucks").select("*"), ttl=0)
        ECM_TRUCKS.clear()
        ECM_TRUCKS.update({row["truck_id"]: Truck(t_id=row["truck_id"], name=row.get("truck_name"), max_len=row.get("max_boat_length")) for row in trucks_resp.data})
        name_to_id = {t.truck_name: t.truck_id for t in ECM_TRUCKS.values()}

        # --- Ramps (Corrected Type Handling) ---
        ramps_resp = execute_query(conn.table("ramps").select("*"), ttl=0)
        ECM_RAMPS.clear()
        for row in ramps_resp.data:
            try:
                ramp_id_str = str(int(row["ramp_id"]))
                
                # --- THIS IS THE FIX: Robustly parse the allowed_boat_types column ---
                allowed_boats_raw = row.get("allowed_boat_types")
                allowed_boats_list = []
                if isinstance(allowed_boats_raw, str):
                    # Handle string format like '{"Powerboat","Sailboat"}'
                    allowed_boats_list = allowed_boats_raw.strip('{}').split(',')
                elif isinstance(allowed_boats_raw, list):
                    # Already a list, just use it
                    allowed_boats_list = allowed_boats_raw
                # --- END FIX ---

                ECM_RAMPS[ramp_id_str] = Ramp(
                    r_id=row["ramp_id"], name=row.get("ramp_name"), station=row.get("noaa_station_id"),
                    tide_method=row.get("tide_calculation_method"), offset=row.get("tide_offset_hours"),
                    boats=allowed_boats_list, # Pass the correctly parsed list
                    latitude=row.get("latitude"), longitude=row.get("longitude")
                )
            except (ValueError, TypeError):
                _log_debug(f"Skipping ramp with invalid ID: {row.get('ramp_id')}")

        # --- Customers ---
        cust_resp = execute_query(conn.table("customers").select("*"), ttl=0)
        LOADED_CUSTOMERS.clear()
        LOADED_CUSTOMERS.update({int(r["customer_id"]): Customer(c_id=r["customer_id"], name=r.get("Customer", "")) for r in cust_resp.data if r.get("customer_id")})

        # --- Boats (Corrected Type Handling) ---
        boat_resp = execute_query(conn.table("boats").select("*"), ttl=0)
        LOADED_BOATS.clear()
        for row in boat_resp.data:
            if not row.get("boat_id"): continue
            try:
                pref_ramp_val = row.get("preferred_ramp")
                pref_ramp_str = str(int(pref_ramp_val)) if pref_ramp_val is not None else ""
                
                LOADED_BOATS[int(row["boat_id"])] = Boat(
                    b_id=row["boat_id"], c_id=row["customer_id"], b_type=row.get("boat_type"),
                    b_len=row.get("boat_length"), draft=row.get("boat_draft") or row.get("draft_ft"),
                    storage_addr=row.get("storage_address", ""), pref_ramp=pref_ramp_str,
                    pref_truck=row.get("preferred_truck", ""), is_ecm=str(row.get("is_ecm_boat", "no")).lower() == 'yes',
                    storage_latitude=row.get("storage_latitude"), storage_longitude=row.get("storage_longitude")
                )
            except (ValueError, TypeError):
                _log_debug(f"Skipping boat with invalid data: {row.get('boat_id')}")

        # --- Truck Schedules ---
        schedules_resp = execute_query(conn.table("truck_schedules").select("*"), ttl=0)
        processed_schedules = {}
        for row in schedules_resp.data:
            truck_id = name_to_id.get(row["truck_name"])
            if truck_id is None: continue
            day = row["day_of_week"]
            start_time = dt.datetime.strptime(row["start_time"], '%H:%M:%S').time()
            end_time = dt.datetime.strptime(row["end_time"],   '%H:%M:%S').time()
            processed_schedules.setdefault(truck_id, {})[day] = (start_time, end_time)
        
        TRUCK_OPERATING_HOURS.clear()
        TRUCK_OPERATING_HOURS.update(processed_schedules)
        
        # Populate candidate crane days and ideal days
        CANDIDATE_CRANE_DAYS.clear()
        CANDIDATE_CRANE_DAYS.update(generate_crane_day_candidates())
        precalculate_ideal_crane_days()

    # ... inside load_all_data_from_sheets() ...
    
        # Populate candidate crane days and ideal days
        CANDIDATE_CRANE_DAYS.clear()
        CANDIDATE_CRANE_DAYS.update(generate_crane_day_candidates())
        precalculate_ideal_crane_days()
    
        # ADD THIS LINE HERE
        load_travel_time_matrix()
    
    except Exception as e:
            st.error(f"Error loading data: {e}")
            raise

    # Build protected tide windows
    try:
        start = dt.date.today()
        _build_protected_windows(start, start + dt.timedelta(days=90))
        _log_debug("Built protected tide windows for next 90 days.")
    except Exception as e:
        _log_debug(f"WARNING: could not build protected windows: {e}")


def _build_protected_windows(start_date: dt.date, end_date: dt.date):
    """Populate CRANE_WINDOWS and ANYTIDE_LOW_TIDE_WINDOWS for the given date range."""
    CRANE_WINDOWS.clear()
    ANYTIDE_LOW_TIDE_WINDOWS.clear()

    for ramp_id, ramp in ECM_RAMPS.items():
        tides = fetch_noaa_tides_for_range(ramp.noaa_station_id, start_date, end_date)
        for d, events in tides.items():
            # Crane windows: HT ± offset for tide-dependent ramps (used to RESERVE for sailboats)
            if ramp.tide_calculation_method not in ("AnyTide", "AnyTideWithDraftRule"):
                offset_hours = float(ramp.tide_offset_hours1) if ramp.tide_offset_hours1 is not None else 0.0
                if offset_hours >= 0:
                    w = []
                    delta = dt.timedelta(hours=offset_hours)
                    for ev in events:
                        if ev['type'] == 'H':
                            lt = (dt.datetime.combine(d, ev['time'], tzinfo=timezone.utc) - delta).time()
                            rt = (dt.datetime.combine(d, ev['time'], tzinfo=timezone.utc) + delta).time()
                            w.append((lt, rt))
                    if w:
                        CRANE_WINDOWS[(str(ramp_id), d)] = w

            # AnyTide low‑tide preference windows: low tide falling between 10–14 local
            if ramp.tide_calculation_method in ("AnyTide", "AnyTideWithDraftRule"):
                w = []
                for ev in events:
                    if ev['type'] == 'L' and 10 <= ev['time'].hour < 14:
                        # a small 60‑min window centered on LT; adjust as you like
                        lt = (dt.datetime.combine(d, ev['time'], tzinfo=timezone.utc) - dt.timedelta(minutes=30)).time()
                        rt = (dt.datetime.combine(d, ev['time'], tzinfo=timezone.utc) + dt.timedelta(minutes=30)).time()
                        w.append((lt, rt))
                if w:
                    ANYTIDE_LOW_TIDE_WINDOWS[(str(ramp_id), d)] = w


def delete_all_jobs():
    """
    Deletes ALL records from the 'jobs' table in the database.
    Returns a tuple of (success_boolean, message_string).
    """
    try:
        conn = get_db_connection()
        
        # To delete all rows, we perform a delete with a filter that matches everything.
        # This deletes all rows where the job_id is not -1 (which is all of them).
        conn.table("jobs").delete().neq("job_id", -1).execute()

        _log_debug("Successfully deleted all jobs from the database.")
        
        # Also clear the in-memory list to reflect the change immediately
        global SCHEDULED_JOBS
        SCHEDULED_JOBS.clear()
        
        return True, "Success! All jobs have been permanently deleted from the database."
        
    except Exception as e:
        _log_debug(f"ERROR: Failed to delete all jobs. Details: {e}")
        st.error(f"An error occurred while trying to delete jobs: {e}")
        return False, f"Error: Could not delete jobs. Details: {e}"


def save_job(job_to_save):
    conn = get_db_connection()
    job_dict = job_to_save.__dict__

    # Build the payload, serializing datetimes to ISO strings
    payload = {}
    for key, value in job_dict.items():
        if isinstance(value, dt.datetime):
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
    Prioritizes database lookup, then caches in-memory, then geocodes using xxxxx and saves to DB.
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
            location = _geocode_with_backoff(_geolocator, address_to_geocode)
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
                yard_location = _geocode_with_backoff(_geolocator,YARD_ADDRESS, timeout=10)
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
    abbr_map = { "pembroke": "Pemb", "Brockton": "Brock", "east bridgewater": "E Bridge", "west bridgewater": "W Bridge", "scituate": "Sci", "green harbor": "Grn Harb", "marshfield": "Mfield", "cohasset": "Coh", "weymouth": "Wey", "plymouth": "Ply", "sandwich": "Sand", "duxbury": "Dux", "humarock": "Hum", "hingham": "Hing", "hull": "Hull", "norwell": "Norw", "boston": "Bos", "quincy": "Qui", "kingston": "King", "hanover": "Hnvr", "rockland": "Rock" }
    if 'HOME' in address.upper(): return "Pem"
    address_lower = address.lower()
    for town, abbr in abbr_map.items():
        if town in address_lower: return abbr
    return address.title().split(',')[0][:3]

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

def _round_time_to_nearest_quarter_hour(ts):
    """Rounds a datetime object UP to the nearest 15-minute interval."""
    if not isinstance(ts, dt.datetime):
        return ts  # Return as-is if not a datetime

    # already on a 15-minute mark?
    if ts.minute % 15 == 0 and ts.second == 0 and ts.microsecond == 0:
        return ts

    minutes_to_add = (15 - ts.minute % 15)
    rounded = ts + dt.timedelta(minutes=minutes_to_add)
    return rounded.replace(second=0, microsecond=0)

def format_time_for_display(time_obj):
    return time_obj.strftime('%I:%M %p').lstrip('0') if isinstance(time_obj, dt.time) else "InvalidTime"

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
        requested_date (dt.date): The new date being requested.
        all_scheduled_jobs (list): A list of all currently scheduled Job objects.
    Returns:
        Job: The conflicting Job object if found, otherwise None.
    """
    thirty_days = dt.timedelta(days=30)
    for job in all_scheduled_jobs:
        if job.boat_id == boat_id and job.service_type == new_service_type:
            if job.scheduled_start_datetime:
                # Check if the existing job date is within 30 days of the new requested date
                if abs(job.scheduled_start_dt.date() - requested_date) <= thirty_days:
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
        start_date = dt.date(year, month, 1)
        _, num_days = calendar.monthrange(year, month)
        end_date = dt.date(year, month, num_days)
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
                    tide_dt_obj = dt.datetime.strptime(datetime_to_parse, "%Y/%m/%d %a %I:%M %p")
                    
                    # This part remains the same
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

@st.cache_data(show_spinner=False, ttl=3600)

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
        resp = _get_retry_session().get(
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
                tide_dt = dt.datetime.strptime(tide["t"], "%Y-%m-%d %H:%M"); date_key = tide_dt.date()
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

def _is_anytide(ramp_id: str) -> bool:
    r = ECM_RAMPS.get(str(ramp_id))
    if not r: return False
    return r.tide_calculation_method in ("AnyTide", "AnyTideWithDraftRule")

def _in_any_window(start_dt: dt.datetime, windows, date: dt.date) -> bool:
    """
    Generic helper: does start_dt fall inside ANY of the (start_time, end_time) windows for 'date'?
    All comparisons are done with timezone-aware (UTC) datetimes.
    """
    try:
        from datetime import timezone as _tz
        for s, e in (windows or []):
            sdt = dt.datetime.combine(date, s, tzinfo=_tz.utc)
            edt = dt.datetime.combine(date, e, tzinfo=_tz.utc)
            if sdt <= start_dt <= edt:
                return True
    except Exception:
        pass
    return False
def get_final_schedulable_ramp_times(ramp: Ramp, boat: Boat, tides_for_day: dict, draft: float):
    from datetime import time

    if ramp.ramp_id == 'ScituateHarborJericho' and draft <= 5:
        # Override: full open window for boats with ≤5' draft at Scituate
        return [(time(8, 0), time(14, 30))]

    method = ramp.tide_method or "AnyTide"
    high_tides = tides_for_day.get("high", [])
    low_tides = tides_for_day.get("low", [])

    if method == "AnyTide":
        return [(time(8, 0), time(14, 30))]
    elif method == "HighTideOnly":
        return expand_tide_window(high_tides, hours=3)
    elif method == "LowTideOnly":
        return expand_tide_window(low_tides, hours=3)
    elif method == "HighTideWithDraftRule":
        if draft <= 5:
            return [(time(8, 0), time(14, 30))]
        else:
            return expand_tide_window(high_tides, hours=3)
    elif method == "LowTideWithDraftRule":
        if draft <= 5:
            return [(time(8, 0), time(14, 30))]
        else:
            return expand_tide_window(low_tides, hours=3)
    else:
        return expand_tide_window(high_tides, hours=3)
        
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
                start_dt = dt.datetime.combine(req_date, window['start_time'])
                end_dt = dt.datetime.combine(req_date, window['end_time'])
                if end_dt > start_dt:
                    longest_window_found = max(longest_window_found, end_dt - start_dt)

    # --- NEW, MORE ACCURATE DURATION CHECK ---
    rules_map = globals().get("BOOKING_RULES", {})
    rules = rules_map.get(getattr(boat, "boat_type", None), {})
    hauler_duration = timedelta(minutes=rules.get("truck_mins", 90))
    crane_duration  = timedelta(minutes=rules.get("crane_mins", 0))
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

        job_date = job.scheduled_start_dt.date()

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

def _count_jobs_on_truck_day(truck_id, date_obj, compiled_schedule):
    """Counts jobs already on a truck's given day using compiled_schedule."""
    cnt = 0
    for busy_start, _ in compiled_schedule.get(str(truck_id), []):
        if busy_start.date() == date_obj:
            cnt += 1
    return cnt

def _geo_cluster_bonus(slot, daily_last_locations):
    """+2 if pickup near last job location, +1 if near yard for first job, else 0."""
    try:
        truck_id = slot['truck_id']
        date_obj = slot['date']
        last = daily_last_locations.get(truck_id, {}).get(date_obj)  # (end_dt, (lat,lon))
        if last and last[1]:
            # Compare to pickup location (storage or ramp)
            pick_coords = None
            if slot.get('pickup_street_address'):
                pick_coords = get_location_coords(address=slot['pickup_street_address'])
            elif slot.get('pickup_ramp_id'):
                pick_coords = get_location_coords(ramp_id=slot['pickup_ramp_id'])
            elif slot.get('boat_id'):
                pick_coords = get_location_coords(boat_id=slot['boat_id'])
            if pick_coords:
                dist = _calculate_distance_miles(last[1], pick_coords)
                return 2 if dist is not None and dist <= 5 else 0
        else:
            yard = get_location_coords(address=YARD_ADDRESS)
            if yard:
                # first job of day: closer to yard is better
                if slot.get('pickup_ramp_id'):
                    pick_coords = get_location_coords(ramp_id=slot['pickup_ramp_id'])
                elif slot.get('boat_id'):
                    pick_coords = get_location_coords(boat_id=slot['boat_id'])
                else:
                    pick_coords = None
                if pick_coords:
                    dist = _calculate_distance_miles(yard, pick_coords)
                    return 1 if dist is not None and dist <= 10 else 0
    except Exception:
        pass
    return 0

def _total_jobs_from_compiled_schedule(compiled_schedule):
    """compiled_schedule is {truck_id: [(start_dt, end_dt), ...]}"""
    try:
        return sum(len(v) for v in compiled_schedule.values())
    except Exception:
        return 0

def _select_best_slots(all_found_slots, compiled_schedule, daily_last_locations, requested_date, prime_days, k=3):
    """
    Rank slots using the _score_candidate(...) and return top-k.
    """
    total_now = _total_jobs_from_compiled_schedule(compiled_schedule)
    after_threshold = total_now >= 25

    scored = []
    for s in (all_found_slots or []):
        try:
            # Pass prime_days to the scoring function
            sc = _score_candidate(s, compiled_schedule, daily_last_locations, after_threshold=after_threshold, prime_days=prime_days)
            sc += _calculate_target_date_score(s.get("date"), requested_date)
        except Exception:
            sc = float("-inf")

        if sc == float("-inf"):
            continue

        scored.append((sc, s))

    if not scored:
        return []

    scored.sort(key=lambda x: x[0], reverse=True)
    k = max(1, int(k or 1))
    return [s for _, s in scored[:k]]


def check_truck_availability_optimized(truck_id, start_dt, end_dt, compiled_schedule):
    for busy_start, busy_end in compiled_schedule.get(str(truck_id), []):
        
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
            start_date = dt.date(year, month, 1)
            end_date = dt.date(year, month, calendar.monthrange(year, month)[1])
            tides_for_month = fetch_noaa_tides_for_range(ramp.noaa_station_id, start_date, end_date)
            
            for day, events in tides_for_month.items():
                for tide in events:
                    if tide['type'] == 'H' and 10 <= tide['time'].hour < 14:
                        IDEAL_CRANE_DAYS.add((ramp.ramp_id, day))
                        # Found a good tide for this day, no need to check other tides on the same day
                        break
    _log_debug(f"Pre-calculated {len(IDEAL_CRANE_DAYS)} ideal crane days for the season.")

# Precompute protected windows for ~90 days (tweak as needed)
# today = dt.date.today()
# _build_protected_windows(today, today + dt.timedelta(days=90))


# --- NEW HELPER: Finds a slot on a specific day using the new efficiency rules ---
# Replace your old function with this CORRECTED version

def find_available_job_slots(customer_id, boat_id, service_type, requested_date_str, selected_ramp_id, num_suggestions_to_find=3, tide_policy=None, max_distance_miles=None, **kwargs):
    """
    Finds available slots, now with an added filter for max_distance_miles between jobs.
    """
    global DEBUG_MESSAGES; DEBUG_MESSAGES.clear()

    relax_truck_preference = kwargs.get('relax_truck_preference', False)
    fetch_scheduled_jobs()

    # ... (The first part of the function remains the same) ...
    if not requested_date_str:
        return [], "Please select a target date before searching.", [], False
    try:
        requested_date = dt.datetime.strptime(requested_date_str, "%Y-%m-%d").date()
    except ValueError:
        return [], f"Date '{requested_date_str}' is not valid.", [], True

    compiled_schedule, daily_last_locations = _compile_truck_schedules(SCHEDULED_JOBS)
    boat = get_boat_details(boat_id)
    if not boat:
        return [], f"Could not find boat ID: {boat_id}", [], True
    crane_needed = "Sailboat" in boat.boat_type

    # ... (Truck separation and date window logic remains the same) ...
    all_suitable_trucks = get_suitable_trucks(boat.boat_length)
    preferred_trucks, other_trucks = [], []
    if boat.preferred_truck_id:
        for t in all_suitable_trucks:
            (preferred_trucks if t.truck_name == boat.preferred_truck_id else other_trucks).append(t)
    else:
        other_trucks = all_suitable_trucks

    # (Date search logic remains the same...)

    def _run_search(trucks_to_search, search_message_type, requested_date, prime_days):
        # ... (The _run_search function itself remains the same) ...
        # It calls _find_slot_on_day and returns a list of found slots
        found = []
        # ...
        return best, msg

    # ... (The main search calls to _run_search remain the same) ...

    # --- THIS IS THE NEW FILTERING LOGIC ---
    if max_distance_miles is not None and found_slots:
        _log_debug(f"Applying max distance filter: {max_distance_miles} miles")
        filtered_slots = []
        for slot in found_slots:
            truck_id = slot.get("truck_id")
            slot_date = slot.get("date")
            ramp_id = slot.get("ramp_id")

            # Check if there are other jobs for this truck on this day
            last_loc_info = daily_last_locations.get(truck_id, {}).get(slot_date)
            if last_loc_info:
                # This is not the first job of the day, so check distance
                last_coords = last_loc_info[1]
                new_coords = get_location_coords(ramp_id=ramp_id)

                if last_coords and new_coords:
                    distance = _calculate_distance_miles(last_coords, new_coords)
                    if distance <= max_distance_miles:
                        filtered_slots.append(slot) # Distance is OK
                    else:
                        _log_debug(f"Filtering out slot on {slot_date} at {ramp_id}. Distance {distance:.1f} mi > {max_distance_miles} mi.")
                else:
                    filtered_slots.append(slot) # Can't calculate distance, so allow it
            else:
                # This is the first job of the day, always allow it
                filtered_slots.append(slot)

        found_slots = filtered_slots # Replace the original list with the filtered one
    # --- END NEW FILTERING LOGIC ---


    if found_slots:
        return (found_slots, message, [], False)

    # (The rest of the fallback logic remains the same)
    # ...


# --- Seasonal batch generator (Spring/Fall), sequential dates, safe if fewer boats remain ---

def simulate_job_requests(
    total_jobs_to_gen: int = 50,
    service_type: str = "Haul", # Now a direct parameter
    year: int = 2025,
    seed: int | None = None,
    start_date_str: str | None = None,
    end_date_str: str | None = None,
    season: str | None = None,
    **kwargs,
):
    """
    Generates jobs and attempts to schedule them, returning a summary and a list of failures.
    """
    import datetime as dt
    import random as _rnd

    if seed is not None:
        _rnd.seed(seed)

    # Date Generation Logic
    valid_dates = []
    if start_date_str and end_date_str:
        try:
            start_date = dt.datetime.strptime(start_date_str, "%Y-%m-%d").date()
            end_date = dt.datetime.strptime(end_date_str, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return "Error: Invalid start or end date format.", []
    else:
        season_norm = (season or "fall").strip().lower()
        months = (5, 6) if season_norm == "spring" else (9, 10)
        start_date = dt.date(year, months[0], 1)
        end_date = dt.date(year, months[1], calendar.monthrange(year, months[1])[1])

    current_date = start_date
    while current_date <= end_date:
        wd = current_date.weekday()
        is_sun = (wd == 6)
        is_sat = (wd == 5)
        if not is_sun and (not is_sat or current_date.month in (5, 9)):
            valid_dates.append(current_date)
        current_date += dt.timedelta(days=1)

    if not valid_dates:
        return "No valid working dates in the selected range.", []

    # Get available boats
    scheduled_boat_ids = {j.boat_id for j in SCHEDULED_JOBS}
    remaining_boats = [b for b in LOADED_BOATS.values() if b.boat_id not in scheduled_boat_ids]
    if not remaining_boats:
        return "No remaining boats to schedule.", []

    # --- Generate Requests and Track Failures ---
    num_to_schedule = min(total_jobs_to_gen, len(remaining_boats))
    boats_to_schedule = _rnd.sample(remaining_boats, k=num_to_schedule)
    
    successful = 0
    failed_requests = [] # New list to track failures
    
    for boat in boats_to_schedule:
        ramp_id_to_use = boat.preferred_ramp_id
        if not ramp_id_to_use or not get_ramp_details(ramp_id_to_use):
            suitable_ramps = list(find_available_ramps_for_boat(boat, ECM_RAMPS))
            ramp_id_to_use = _rnd.choice(suitable_ramps) if suitable_ramps else None
        
        if not ramp_id_to_use:
            failed_requests.append({'boat_id': boat.boat_id, 'requested_date': 'N/A', 'reason': 'No suitable ramp found'})
            continue

        random_date = _rnd.choice(valid_dates)
        request = {
            "customer_id": boat.customer_id, "boat_id": boat.boat_id, "service_type": service_type,
            "requested_date_str": random_date.strftime("%Y-%m-%d"),
            "selected_ramp_id": ramp_id_to_use, "relax_truck_preference": True,
        }

        slots, _, _, _ = find_available_job_slots(**request)
        if slots:
            confirm_and_schedule_job(slots[0])
            successful += 1
        else:
            # Add the failed request to our list
            failed_requests.append({
                'boat_id': boat.boat_id,
                'requested_date': random_date.strftime("%b %d, %Y"),
                'ramp_name': get_ramp_details(ramp_id_to_use).ramp_name,
                'reason': 'No slot found in search window'
            })

    summary = (f"Batch requested: {num_to_schedule}. Remaining boats: {len(remaining_boats) - successful}. Scheduled now: {successful}.")
    return summary, failed_requests


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
        job_date = job.scheduled_start_dt.date()
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
            if first_job_start.time() < dt.time(9, 0) and num_jobs >= 3 and last_job_end.time() <= dt.time(15, 0):
                excellent_timing_days += 1
            if first_job_start.time() >= dt.time(13, 0):
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
    today = dt.date.today()
    total_all_boats = len(all_boats)
    scheduled_customer_ids = {j.customer_id for j in scheduled_jobs if j.job_status == "Scheduled"}

    launched_customer_ids = {
        j.customer_id
        for j in scheduled_jobs
        if (
            j.job_status == "Scheduled"
            and j.service_type == "Launch"
            and j.scheduled_start_datetime
            and j.scheduled_start_datetime.date() < today
        )
    }

    ecm_customer_ids = {boat.customer_id for boat in all_boats.values() if boat.is_ecm_boat}
    return {
        'all_boats': {
            'total': total_all_boats,
            'scheduled': len(scheduled_customer_ids),
            'launched': len(launched_customer_ids),
        },
        'ecm_boats': {
            'total': len(ecm_customer_ids),
            'scheduled': len(scheduled_customer_ids.intersection(ecm_customer_ids)),
            'launched': len(launched_customer_ids.intersection(ecm_customer_ids)),
        }
    }
    
def confirm_and_schedule_job(final_slot: dict, parked_job_to_remove: int = None):
    """
    Creates a new Job object from a finalized slot, saves it to the database,
    removes an old parked job if rebooking, and refreshes the in-memory schedule.
    """
    try:
        # 1. Look up the boat object
        boat = get_boat_details(final_slot.get('boat_id'))
        if not boat:
            return None, "Error: Could not find boat details for the selected job."

        # 2. Construct datetime objects
        #    FIX: Use the 'dt' alias for the datetime module
        start_dt = dt.datetime.combine(final_slot['date'], final_slot['time'], tzinfo=timezone.utc)
        
        # Use the full end datetime from the slot if available, otherwise calculate it
        end_dt = final_slot.get('scheduled_end_datetime')
        if not end_dt:
            rules = BOOKING_RULES.get(boat.boat_type, {'truck_mins': 90})
            duration = timedelta(minutes=rules['truck_mins'])
            end_dt = start_dt + duration
        
        # Ensure end_dt is timezone-aware
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)

        # 3. Determine pickup and dropoff locations based on service type
        service_type = final_slot.get('service_type')
        pickup_addr, dropoff_addr = "", ""
        pickup_ramp, dropoff_ramp = None, None

        if service_type == "Launch":
            pickup_addr = boat.storage_address
            dropoff_ramp = final_slot.get('ramp_id')
        elif service_type == "Haul":
            pickup_ramp = final_slot.get('ramp_id')
            dropoff_addr = boat.storage_address
        
        # 4. Create a complete Job object
        new_job = Job(
            customer_id=final_slot.get('customer_id'),
            boat_id=final_slot.get('boat_id'),
            service_type=service_type,
            scheduled_start_datetime=start_dt,
            scheduled_end_datetime=end_dt,
            assigned_hauling_truck_id=final_slot.get('truck_id'),
            assigned_crane_truck_id=get_s17_truck_id() if final_slot.get('S17_needed') else None,
            S17_busy_end_datetime=final_slot.get('S17_busy_end_datetime'),
            pickup_ramp_id=pickup_ramp,
            dropoff_ramp_id=dropoff_ramp,
            pickup_street_address=pickup_addr,
            dropoff_street_address=dropoff_addr,
            job_status="Scheduled"
        )

        # 5. Save the new job to the database
        save_job(new_job) # This will also assign the new job_id back to the object

        # 6. If this was a rebooking, delete the old parked job
        if parked_job_to_remove:
            delete_job_from_db(parked_job_to_remove)
            _log_debug(f"Removed old parked job ID: {parked_job_to_remove}")

        # 7. Refresh the global jobs list to reflect the changes immediately
        fetch_scheduled_jobs()

        # 8. Return the new Job ID and a success message
        customer = get_customer_details(new_job.customer_id)
        message = (
            f"Successfully scheduled {service_type} for {customer.customer_name} on "
            f"{start_dt.strftime('%A, %b %d @ %-I:%M %p')}."
        )
        return new_job.job_id, message

    except Exception as e:
        _log_debug(f"ERROR in confirm_and_schedule_job: {e}")
        return None, f"An unexpected error occurred during confirmation: {e}"


def find_available_job_slots(customer_id, boat_id, service_type, requested_date_str, selected_ramp_id, num_suggestions_to_find=3, tide_policy=None, **kwargs):
    """
    Finds available slots by first searching ONLY for the preferred truck, then
    falling back to other trucks ONLY if relax_truck_preference is True.
    Uses scoring to rank all found candidates and returns the top-K.
    """
    global DEBUG_MESSAGES; DEBUG_MESSAGES.clear()

    relax_truck_preference = kwargs.get('relax_truck_preference', False)
    fetch_scheduled_jobs()

    # --- Validation & Initial Setup ---
    if not requested_date_str:
        return [], "Please select a target date before searching.", [], False
    try:
        requested_date = dt.datetime.strptime(requested_date_str, "%Y-%m-%d").date()
    except ValueError:
        return [], f"Date '{requested_date_str}' is not valid.", [], True

    compiled_schedule, daily_last_locations = _compile_truck_schedules(SCHEDULED_JOBS)

    boat = get_boat_details(boat_id)
    if not boat:
        return [], f"Could not find boat ID: {boat_id}", [], True

    crane_needed = "Sailboat" in boat.boat_type
    
    # --- Separate Trucks into Preferred and Others ---
    all_suitable_trucks = get_suitable_trucks(boat.boat_length)
    preferred_trucks, other_trucks = [], []
    if boat.preferred_truck_id:
        for t in all_suitable_trucks:
            if t.truck_name == boat.preferred_truck_id:
                preferred_trucks.append(t)
            else:
                other_trucks.append(t)
    else:
        other_trucks = all_suitable_trucks

    # --- Build candidate day windows ---
    opp_window = [requested_date + dt.timedelta(days=i) for i in range(-7, 8)]
    if crane_needed:
        potential = [d for r_id, d in IDEAL_CRANE_DAYS if str(r_id) == str(selected_ramp_id) and d >= requested_date]
        early = [d for d in potential if d <= requested_date + dt.timedelta(days=21)]
        fb_days = sorted(early)[:30]
        if not fb_days:
            wider = [d for d in potential if d <= requested_date + dt.timedelta(days=45)]
            fb_days = sorted(wider)[:30]
    else:
        season_end_date = dt.date(requested_date.year, 10, 31)
        days_to_search = (season_end_date - requested_date).days + 1
        if days_to_search < 14:
            days_to_search = 14
        fb_days = [requested_date + dt.timedelta(days=i) for i in range(days_to_search)]

    span_start = min(fb_days) if fb_days else requested_date
    span_end = max(fb_days) if fb_days else requested_date

    station_id = _station_for_ramp_or_scituate(selected_ramp_id)
    prime_days = get_low_tide_prime_days(station_id, span_start, span_end)

    s17_id = get_s17_truck_id()
    active_crane_days = { j.scheduled_start_dt.date() for j in SCHEDULED_JOBS if j.scheduled_start_datetime and j.scheduled_start_dt.date() in opp_window and j.assigned_crane_truck_id == str(s17_id) and (str(j.pickup_ramp_id) == str(selected_ramp_id) or str(j.dropoff_ramp_id) == str(selected_ramp_id)) }
    
    opp_days = sorted(list(active_crane_days), key=lambda d: abs((d - requested_date).days))
    opp_days = order_dates_with_low_tide_bias(requested_date, opp_days, prime_days)
    fb_days = order_dates_with_low_tide_bias(requested_date, fb_days, prime_days)

    def _run_search(trucks_to_search, search_message_type, requested_date, prime_days):
        found = []
        POOL_CAP = max(20, num_suggestions_to_find * 20)
        
        for day in opp_days:
            slot = _find_slot_on_day( day, boat, service_type, selected_ramp_id, crane_needed, compiled_schedule, customer_id, trucks_to_search, is_opportunistic_search=True )
            if slot: found.append(slot)
        
        if len(found) < POOL_CAP:
            for day in fb_days:
                slot = _find_slot_on_day( day, boat, service_type, selected_ramp_id, crane_needed, compiled_schedule, customer_id, trucks_to_search, is_opportunistic_search=(day in active_crane_days) )
                if slot: found.append(slot)

        if found:
            best = _select_best_slots(found, compiled_schedule, daily_last_locations, requested_date, prime_days, k=num_suggestions_to_find)
            return (best, f"Found {len(best)} slot(s) using {search_message_type} truck.")
        return ([], None)

    found_slots, message = [], None
    trucks_to_try = preferred_trucks if boat.preferred_truck_id else other_trucks

    if trucks_to_try:
        search_type = "preferred" if boat.preferred_truck_id else "any suitable"
        found_slots, message = _run_search(trucks_to_try, search_type, requested_date, prime_days)

    if (not found_slots) and relax_truck_preference and other_trucks:
        found_slots, message = _run_search(other_trucks, "other", requested_date, prime_days)

    if found_slots:
        return (found_slots, message, [], False)

    # --- GUARANTEED FALLBACK ---
    k = max(1, int(num_suggestions_to_find or 3))
    # Fallback search logic... (rest of the function is the same)
    
    # ... (rest of the fallback logic as it was)
    return ([], f"No slots found after extensive search.", DEBUG_MESSAGES, True)

def find_available_ramps_for_boat(boat, all_ramps):
    """
    Finds a list of ramp IDs suitable for a given boat by checking the boat's type
    against a ramp's allowed boat types.
    """
    matching_ramp_ids = []
    for ramp_id, ramp in all_ramps.items():
        # This check ensures both objects have the attributes we need, preventing crashes.
        if hasattr(boat, 'boat_type') and hasattr(ramp, 'allowed_boat_types'):
            # The 'in' operator checks if the boat's type is in the ramp's list of allowed types
            if boat.boat_type in ramp.allowed_boat_types:
                matching_ramp_ids.append(ramp_id)
    
    # If no specific ramps match (e.g., for a rare boat type),
    # return all ramps to allow for a manual override in the UI.
    if not matching_ramp_ids:
        return list(all_ramps.keys())

    return matching_ramp_ids

def get_S17_crane_grouping_slot(boat, customer, ramp_obj, requested_date, trucks, duration, S17_duration, service_type):
    """
    Attempts to group a sailboat crane job with an existing crane job at the same ramp within ±7 days.
    """
    import datetime
    from .ecm_scheduler_shared import _check_and_create_slot_detail
    from .ecm_scheduler_data import SCHEDULED_JOBS

    date_range = [requested_date + dt.timedelta(days=delta) for delta in range(-7, 8)]

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

def _is_crane_window(slot) -> bool:
    """True if slot datetime sits inside a precomputed crane window for this ramp/day."""
    key = (str(slot['ramp_id']), slot['date'])
    wins = CRANE_WINDOWS.get(key, [])
    if not wins:
        return False
    start_dt = dt.datetime.combine(slot['date'], slot['time'], tzinfo=timezone.utc)
    return _in_any_window(start_dt, wins, slot['date'])

def _is_low_tide_window(slot) -> bool:
    """True if slot datetime sits inside (10–14) low‑tide window on AnyTide ramps."""
    key = (str(slot['ramp_id']), slot['date'])
    wins = ANYTIDE_LOW_TIDE_WINDOWS.get(key, [])
    if not wins:
        return False
    start_dt = dt.datetime.combine(slot['date'], slot['time'], tzinfo=timezone.utc)
    return _in_any_window(start_dt, wins, slot['date'])

def calculate_ramp_windows(ramp, boat, tide_data, date):
    """
    Returns a list of dictionaries with 'start_time' and 'end_time' keys representing
    tidal windows at the given ramp on the given date for the specified boat.
    Uses HIGH tide windows with an offset determined by ramp method and draft.
    """
    # Any‑tide ramps are effectively open all day (unless draft rule restricts deep boats)
    if ramp.tide_calculation_method == "AnyTide":
        return [{'start_time': dt.time.min, 'end_time': dt.time.max}]
    if ramp.tide_calculation_method == "AnyTideWithDraftRule" and boat.draft_ft and boat.draft_ft < 5.0:
        return [{'start_time': dt.time.min, 'end_time': dt.time.max}]

    # Determine offset_hours based on method and draft
    if ramp.tide_calculation_method == "HoursAroundHighTide_WithDraftRule":
        offset_hours = 3.5 if boat.draft_ft and boat.draft_ft < 5.0 else 3.0
    else:
        # IMPORTANT: 0 offset is a real rule (exact HT). Default to 0.0 if None.
        offset_hours = float(ramp.tide_offset_hours1) if ramp.tide_offset_hours1 is not None else 0.0

    # If there's no tide data, we can't calculate windows around tides.
    if not tide_data:
        DEBUG_MESSAGES.append(f"DEBUG: No tide data available for {date} at ramp {getattr(ramp, 'ramp_name', ramp)}.")
        return []

    offset = dt.timedelta(hours=offset_hours)
    windows = []
    for t in tide_data:
        if t.get('type') == 'H':
            # Use timezone‑aware math (UTC)
            center = dt.datetime.combine(date, t['time'], tzinfo=timezone.utc)
            start_t = (center - offset).time()
            end_t   = (center + offset).time()
            windows.append({'start_time': start_t, 'end_time': end_t})
            DEBUG_MESSAGES.append(f"DEBUG: Tide window @ {t['time']}: {start_t}–{end_t} (±{offset_hours}h)")
    return windows


def _find_slot_on_day(
    day: date,
    boat,
    service_type: str,
    ramp_id: str,
    crane_needed: bool,
    compiled_schedule,
    customer_id,
    trucks_to_check: list,
    is_opportunistic_search: bool = False,
    tide_policy: dict | None = None,
):
    """Single-day scanner that only iterates inside allowed tide windows."""
    ramp = get_ramp_details(str(ramp_id))
    if not ramp:
        return None

    boat_type = (getattr(boat, "boat_type", "") or "").lower()
    is_sail = "sail" in boat_type
    duration_mins = 180 if service_type in ("Launch", "Haul") and is_sail else 90
    job_duration = timedelta(minutes=duration_mins)

    # Build tide windows for this ramp/day
    windows = tide_window_for_day(ramp, day)

    # Special all-day exception: Powerboat <5' at Scituate
    is_power = boat_type.startswith("power")
    try:
        draft_ft = float(getattr(boat, "draft_ft", getattr(boat, "draft", 0)) or 0.0)
    except Exception:
        draft_ft = 0.0
    sid_raw = getattr(ramp, "noaa_station_id", None)
    sid = str(sid_raw).strip() if sid_raw is not None else ""
    is_scituate = (sid == str(DEFAULT_NOAA_STATION)) or ("scituate" in str(getattr(ramp, "name", "")).lower())
    if is_power and draft_ft < 5.0 and is_scituate:
        from datetime import time as _dtime
        windows = [(_dtime(0, 0), _dtime(23, 59))]

    # If the ramp has a tide rule but we failed to build a window, bail early.
    if getattr(ramp, "tide_rule_hours", 0) > 0 and not windows:
        return None

    policy = (tide_policy or globals().get("_GLOBAL_TIDE_POLICY") or globals().get("DEFAULT_TIDE_POLICY") or {})
    step = timedelta(minutes=int(policy.get("scan_step_mins", 15)))

    rules_map = globals().get("BOOKING_RULES", {}) or {}
    rules = rules_map.get(getattr(boat, "boat_type", None), {}) or {}
    crane_minutes = int(rules.get("crane_mins", 0))

    for truck in (trucks_to_check or []):
        hours = (TRUCK_OPERATING_HOURS.get(truck.truck_id, {}) or {}).get(day.weekday())
        if not hours:
            continue
        truck_open  = dt.datetime.combine(day, hours[0], tzinfo=timezone.utc)
        truck_close = dt.datetime.combine(day, hours[1], tzinfo=timezone.utc)

        # Reserve first 90 minutes for ECM boats only
        reserve_first_slot = timedelta(minutes=90)
        earliest = truck_open if getattr(boat, "is_ecm_boat", False) else (truck_open + reserve_first_slot)
        latest_start = truck_close - job_duration
        if earliest > latest_start:
            continue

        # Build candidate ranges: only scan INSIDE windows (or whole day if none)
        candidate_ranges = []
        if windows:
            for (w0, w1) in windows:
                w_start = max(earliest, dt.datetime.combine(day, w0, tzinfo=timezone.utc))
                w_end   = min(latest_start, dt.datetime.combine(day, w1, tzinfo=timezone.utc))
                if w_start <= w_end:
                    candidate_ranges.append((w_start, w_end))
        else:
            candidate_ranges.append((earliest, latest_start))

        for (range_start, range_end) in candidate_ranges:
            start_dt = range_start
            while start_dt <= range_end:
                end_dt = start_dt + job_duration

                if not check_truck_availability_optimized(truck.truck_id, start_dt, end_dt, compiled_schedule):
                    start_dt += step
                    continue

                if not tide_policy_ok(service_type, boat, start_dt, end_dt, windows, policy):
                    start_dt += step
                    continue

                crane_end_dt = None
                if crane_needed and crane_minutes > 0:
                    s17_id = get_s17_truck_id()
                    crane_end_dt = start_dt + timedelta(minutes=crane_minutes)
                    if not check_truck_availability_optimized(s17_id, start_dt, crane_end_dt, compiled_schedule):
                        start_dt += step
                        continue

                try:
                    tides_today = fetch_noaa_tides_for_range(sid or DEFAULT_NOAA_STATION, day, day) or {}
                    highs = [t["time"] for t in tides_today.get(day, []) if t.get("type") == "H" and isinstance(t.get("time"), dt.time)]
                except Exception:
                    highs = []

                return {
                    "is_piggyback": is_opportunistic_search,
                    "boat_id": boat.boat_id,
                    "customer_id": customer_id,
                    "date": day,
                    "time": start_dt.time(),
                    "truck_id": truck.truck_id,
                    "ramp_id": ramp_id,
                    "service_type": service_type,
                    "S17_needed": bool(crane_needed),
                    "scheduled_end_datetime": end_dt,
                    "S17_busy_end_datetime": crane_end_dt,
                    "tide_rule_concise": get_concise_tide_rule(ramp, boat),
                    "high_tide_times": highs,
                    "boat_draft": getattr(boat, "draft_ft", None),
                }

                start_dt += step

    return None
