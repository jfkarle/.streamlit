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

# --- IN-MEMORY DATA CACHES & GLOBALS (must be defined before any function uses them) ---
DEBUG_MESSAGES: list[str] = []

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
    *,
    service_type: str,
    boat_type: str,
    day: dt.date,
    start_time: dt.time,
    duration_min: int,
    tide_windows_time,      # list[(time_start, time_end)]
    tide_policy: dict | None,
    ramp_method: str | None = None
) -> bool:
    """
    Implements your rules:

    Launch:
      - The LAST N minutes of the job must be inside the tide window ("water phase").
      - Job may START up to PREP minutes BEFORE the window opens.
        (PREP minutes = launch_prep_power_min | launch_prep_sail_min)

    Haul:
      - The FIRST N minutes of the job must be inside the tide window ("water phase" at the beginning).

    AnyTide ramps bypass all checks.
    """
    # AnyTide → always OK
    if str(ramp_method) == "AnyTide":
        return True

    tide_policy = tide_policy or {}
    start = dt.datetime.combine(day, start_time)
    end = start + _minutes(duration_min)
    windows = _windows_dt_for_day(tide_windows_time, day)

    if service_type == "Launch":
        water_phase_min = int(tide_policy.get("launch_water_phase_min", 60))
        prep_power = int(tide_policy.get("launch_prep_power_min", 30))
        prep_sail  = int(tide_policy.get("launch_prep_sail_min", 120))
        is_sail    = ("sail" in (boat_type or "").lower())
        prep_td    = _minutes(prep_sail if is_sail else prep_power)
        water_td   = _minutes(water_phase_min)

        water_phase_start = end - water_td  # the final minutes must be within the window
        for ws, we in windows:
            # start must not be earlier than (window start - prep)
            # and the final water phase must be fully inside the window
            if start >= (ws - prep_td) and water_phase_start >= ws and end <= we:
                return True
        return False

    elif service_type == "Haul":
        first_phase_min = int(tide_policy.get("haul_water_phase_min", 30))
        first_td = _minutes(first_phase_min)

        for ws, we in windows:
            # first water-phase must be fully inside the window
            if start >= ws and (start + first_td) <= we:
                return True
        return False

    # Other service types – no tide restriction
    return True


# --- Public helper: probe the exact requested day once ---
def probe_requested_date_slot(
    customer_id,
    boat_id,
    service_type,
    requested_date_str,
    selected_ramp_id,
    relax_truck_preference=False,
    tide_policy=None,                # ← NEW
    **kwargs
):
    """
    Returns a single slot dict for the exact requested date if feasible, else None.
    Tries preferred trucks first, then (optionally) other suitable trucks.
    """
    if not requested_date_str:
        return None
    try:
        requested_date = dt.datetime.strptime(requested_date_str, "%Y-%m-%d").date()
    except Exception:
        return None

    fetch_scheduled_jobs()
    compiled_schedule, _ = _compile_truck_schedules(SCHEDULED_JOBS)

    boat = get_boat_details(boat_id)
    if not boat:
        return None

    crane_needed = "Sailboat" in boat.boat_type
    all_suitable = get_suitable_trucks(boat.boat_length)
    preferred, others = [], []
    if boat.preferred_truck_id:
        for t in all_suitable:
            (preferred if t.truck_name == boat.preferred_truck_id else others).append(t)
    else:
        others = all_suitable

    def _try(trucks):
        if not trucks:
            return None
        return _find_slot_on_day(
            requested_date, boat, service_type, selected_ramp_id, crane_needed,
            compiled_schedule, customer_id, trucks, is_opportunistic_search=False
        )

    return _try(preferred) or (_try(others) if relax_truck_preference else None)

# --- HARD CONSTRAINTS + SCORING UPGRADE PACK ---

import math

# --- Booking Rules Defaults ---
BOOKING_RULES = {
    "Powerboat": {"truck_mins": 90, "crane_mins": 0},
    "Sailboat":  {"truck_mins": 180, "crane_mins": 90},
    # Add other types as needed...
}

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

def _score_candidate(slot, compiled_schedule, daily_last_locations, after_threshold=False):
    """
    Larger is better. Packs days, rewards piggybacks & proximity, gentle Any‑tide low‑tide nudge.
    """
    score = 0.0
    truck_id = str(slot.get("truck_id"))
    date     = slot.get("date")
    start_t  = slot.get("time")
    ramp_id  = str(slot.get("ramp_id"))
    pickup_id= str(slot.get("pickup_ramp_id") or ramp_id)
    drop_id  = str(slot.get("dropoff_ramp_id") or "")

    # Robust: support {truck_id: {date: [..]}} OR {truck_id: [(start_dt, end_dt), ...]}
    raw_sched = compiled_schedule.get(truck_id, [])
    if isinstance(raw_sched, dict):
        todays = raw_sched.get(date, [])
    else:
        # filter this truck's intervals to the current date
        todays = [iv for iv in raw_sched if iv and hasattr(iv[0], "date") and iv[0].date() == date]
    n = len(todays)
    if n == 0: score += 2.0
    if n == 1: score += 6.0
    if n == 2: score += 10.0
    if n >= 3: score += 8.0

    if slot.get("is_piggyback"):
        score += 8.0
    if todays:
        last = todays[-1]
        last_ramp = str(getattr(last, "dropoff_ramp_id", None) or getattr(last, "pickup_ramp_id", None) or "")
        if last_ramp and last_ramp == (drop_id or pickup_id):
            score += 4.0

    # proximity to last location
    last_loc = (daily_last_locations.get(truck_id, {}) or {}).get(date)
    def _ramp_latlon(_rid):
        r = ECM_RAMPS.get(_rid) if _rid else None
        lat = getattr(r, "latitude", None)
        lon = getattr(r, "longitude", None)
        return (lat, lon) if (lat is not None and lon is not None) else None
    next_loc = _ramp_latlon(pickup_id) or _ramp_latlon(ramp_id)
    if last_loc and last_loc[1] and next_loc and None not in next_loc:
        import math
        (ax, ay), (bx, by) = last_loc[1], next_loc
        km = math.hypot(ax - bx, ay - by) * 111.0
        mins = (km / 50.0) * 60.0
        score -= min(8.0, mins / 6.0)

    # early start preference
    try:
        if isinstance(start_t, dt.time):
            score += max(0.0, 2.0 - (start_t.hour - 7) * 0.25)
    except Exception:
        pass

    # gentle bonus after volume threshold
    if after_threshold:
        score += 1.5 * max(0, min(n, 3))

        slot_date = slot.get("date")
        boat_type = slot.get("boat_type", "")
        ramp_id = str(slot.get("ramp_id") or slot.get("pickup_ramp_id"))
        ramp = ECM_RAMPS.get(ramp_id)
        tide_method = getattr(ramp, "tide_calculation_method", "AnyTide")
        
        # Prime day scoring
        if slot_date in low_prime_days:
            if boat_type == "Powerboat" and tide_method == "AnyTide":
                score += 6.0  # Powerboat gets bonus
            elif "Sailboat" in boat_type:
                score -= 5.0  # Sailboat penalized
        
        if slot_date in high_prime_days:
            if "Sailboat" in boat_type:
                score += 6.0
            if boat_type == "Powerboat" and tide_method != "AnyTide":
                score -= 4.0
       
    return score


def tide_window_for_day(ramp_id: str, day: dt.date):
    """
    Returns a list of (start_time, end_time) tuples in local time when this ramp is usable.
    Uses the ramp's NOAA station (no fallback unless ramp has no station).
    """
    ramp = get_ramp_details(ramp_id)
    if not ramp:
        return []  # unknown ramp -> no tide gating
    station = getattr(ramp, "noaa_station_id", None)
    if not station:
        return []  # ramps without station are treated AnyTide

    tides = fetch_noaa_tides_for_range(str(station), day, day).get(day, [])
    method = (getattr(ramp, "tide_method", None) or getattr(ramp, "tide_rule", None) or "AnyTide")

    # Example rules: adjust to match your ramp rules
    # AnyTide: unrestricted; DT/MT: allow ±X around High (or Low), per ramp config
    if str(method) == "AnyTide":
        return [(_time(0,0), _time(23,59))]

    windows = []
    # If your ramps store window mins like ramp.window_minutes_each_side, use that here.
    pad = getattr(ramp, "window_minutes_each_side", 60)  # default 60m each side
    use_high = getattr(ramp, "uses_high_tide", True)     # some ramps might use low
    for t in tides:
        if (use_high and t.get("type") == "H") or ((not use_high) and t.get("type") == "L"):
            tt = t["time"]
            if not isinstance(tt, _time): 
                continue
            center = _dt.combine(day, tt)
            start = (center - dt.timedelta(minutes=pad)).time()
            end   = (center + dt.timedelta(minutes=pad)).time()
            windows.append((start, end))
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

def _find_slot_on_day(
    day: dt.date,
    boat,
    service_type,
    ramp_id,
    crane_needed,
    compiled_schedule,
    customer_id,
    trucks_to_search,
    is_opportunistic_search=False,
    tide_policy=None       # ← NEW
):
    """
    Finds the first available slot on a specific day, honoring tide rules for the ramp and job type.
    """
    ramp = get_ramp_details(str(ramp_id))
    if not ramp:
        return None

    # Season heuristics
    is_ecm = bool(getattr(boat, "is_ecm_boat", False))
    is_launch = 4 <= search_date.month <= 7
    if is_launch:
        start_hour = 7 if is_ecm else 9
        hours_iter = range(start_hour, 16)
    else:
        start_hour = 15 if is_ecm else 14
        hours_iter = range(start_hour, 7, -1)

    rules_map = globals().get("BOOKING_RULES", {}) or {}
    rules = rules_map.get(getattr(boat, "boat_type", None), {}) or {}
    
    haul_minutes  = int(rules.get("truck_mins", 90))
    crane_minutes = int(rules.get("crane_mins", 0))
    
    hauler_duration = timedelta(minutes=haul_minutes)
    crane_duration  = timedelta(minutes=crane_minutes)

    tides = fetch_noaa_tides_for_range(ramp.noaa_station_id, search_date, search_date)
    day_tides = tides.get(search_date, [])
    tide_windows = calculate_ramp_windows(ramp, boat, day_tides, search_date)

    tide_dependent = ramp.tide_calculation_method not in ("AnyTide", "AnyTideWithDraftRule")
    if tide_dependent and not tide_windows:
        return None

    for truck in trucks_to_check or []:
        hours = (TRUCK_OPERATING_HOURS.get(truck.truck_id, {}) or {}).get(search_date.weekday())
        if not hours:
            continue
        truck_open = dt.datetime.combine(search_date, hours[0], tzinfo=timezone.utc)
        truck_close= dt.datetime.combine(search_date, hours[1], tzinfo=timezone.utc)

        for h in hours_iter:
            for m in (0,15,30,45):
                start_dt = dt.datetime.combine(search_date, time(h, m), tzinfo=timezone.utc)
                end_dt   = start_dt + dt.timedelta(minutes=haul_minutes)
                if not (truck_open <= start_dt and end_dt <= truck_close):
                    continue
                if not check_truck_availability_optimized(truck.truck_id, start_dt, end_dt, compiled_schedule):
                    continue

                # Tide-critical checks vary by service type
                ok_tide = False
                if not tide_dependent:
                    ok_tide = True
                elif service_type == "Launch":
                    # The ramp needs to be usable roughly two hours after truck start
                    critical = start_dt + dt.timedelta(hours=2)
                    for w in tide_windows:
                        ws = dt.datetime.combine(search_date, w['start_time'], tzinfo=timezone.utc)
                        we = dt.datetime.combine(search_date, w['end_time'],   tzinfo=timezone.utc)
                        if ws <= critical <= we:
                            ok_tide = True
                            break
                elif service_type == "Haul":
                    # The first 30 minutes must intersect a tide window
                    c0 = start_dt
                    c1 = start_dt + dt.timedelta(minutes=30)
                    for w in tide_windows:
                        ws = dt.datetime.combine(search_date, w['start_time'], tzinfo=timezone.utc)
                        we = dt.datetime.combine(search_date, w['end_time'],   tzinfo=timezone.utc)
                        if c0 < we and c1 > ws:
                            ok_tide = True
                            break

                if not ok_tide:
                    continue

                crane_end = None
                if crane_needed:
                    s17_id = get_s17_truck_id()
                    crane_end = start_dt + dt.timedelta(minutes=crane_minutes)
                    if not check_truck_availability_optimized(s17_id, start_dt, crane_end, compiled_schedule):
                        continue

                return {
                    'is_piggyback': is_opportunistic_search,
                    'boat_id': boat.boat_id,
                    'customer_id': customer_id,
                    'date': search_date,
                    'time': start_dt.time(),
                    'truck_id': truck.truck_id,
                    'ramp_id': ramp_id,
                    'service_type': service_type,
                    'S17_needed': crane_needed,
                    'scheduled_end_datetime': end_dt,
                    'S17_busy_end_datetime': crane_end,
                    'tide_rule_concise': get_concise_tide_rule(ramp, boat),
                    'high_tide_times': [t['time'] for t in day_tides if t.get('type') == 'H'],
                    'boat_draft': getattr(boat, 'draft_ft', None),
                }
    return None



def find_slot_across_days(requested_date, ramp_id, boat, service_type, crane_needed,
                          compiled_schedule, customer_id, trucks_to_check,
                          num_suggestions_to_find):
    """
    Keep searching around the requested date until we have at least k slots.
    Searches forward AND backward, expanding the radius as needed.
    """
    k = max(1, int(num_suggestions_to_find or 3))
    found = []
    # Base span chosen by ramp tide method; this is just the first pass.
    def _base_span(ramp_id):
        ramp = ECM_RAMPS.get(ramp_id)
        method = getattr(ramp, "tide_calculation_method", "AnyTide")
        return 6 if method == "AnyTide" else (10 if method == "AnyTideWithDraftRule" else 15)

    # Expand outward until we have k or we’ve gone pretty far
    max_radius = 45  # safety ceiling
    radius = _base_span(ramp_id)
    while len(found) < k and radius <= max_radius:
        search_order = _generate_day_search_order(requested_date, radius, radius)
        for day in search_order:
            # Skip dates we already tried (avoid duplicates on expansion)
            if any(s["date"] == day for s in found):
                continue
            slot = _find_slot_on_day(day, boat, service_type, ramp_id, crane_needed,
                                     compiled_schedule, customer_id, trucks_to_check)
            if slot:
                found.append(slot)
                if len(found) >= k:
                    break
        radius += 5  # expand window in chunks
    return found[:k]



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
    
def load_all_data_from_sheets():
    """Loads all data from Supabase, now including truck schedules."""
    global SCHEDULED_JOBS, PARKED_JOBS, LOADED_CUSTOMERS, LOADED_BOATS, ECM_TRUCKS, ECM_RAMPS, TRUCK_OPERATING_HOURS, CANDIDATE_CRANE_DAYS
    try:
        conn = get_db_connection()

        # Safety guards (in case someone refactors import order later)
        if SCHEDULED_JOBS is None: SCHEDULED_JOBS = []
        if PARKED_JOBS is None: PARKED_JOBS = {}
    
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
                draft              = row.get("boat_draft") or row.get("draft_ft"),
                storage_addr       = row.get("storage_address", ""),
                pref_ramp          = str(row.get("preferred_ramp") or ""),
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
            start_time = dt.datetime.strptime(row["start_time"], '%H:%M:%S').time()
            end_time   = dt.datetime.strptime(row["end_time"],   '%H:%M:%S').time()
            processed_schedules.setdefault(truck_id, {})[day] = (start_time, end_time)

        TRUCK_OPERATING_HOURS.clear()
        TRUCK_OPERATING_HOURS.update(processed_schedules)
        precalculate_ideal_crane_days()
        
    except Exception as e:
        st.error(f"Error loading data: {e}")
        raise

    # Build tide-protected windows now that ramps are loaded
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

def _select_best_slots(all_found_slots, compiled_schedule, daily_last_locations, k=3):
    """
    Rank slots using the 4-arg _score_candidate(...) and return top-k.
    Activates the packing nudge after total scheduled jobs >= 25.

    Extra nudges:
      - +20 bonus if ramp is AnyTide AND slot date is a prime low-tide day (11:00–13:00 low tide).
      - -15 penalty if ramp is NOT AnyTide on a prime low-tide day (soft discourage, not a block).
    """
    # Tunable weights for the AnyTide prime-day preference
    ANYTIDE_PRIME_BONUS = 20.0
    NON_ANYTIDE_PRIME_PENALTY = 15.0 # <-- ADD THIS CONSTANT

    total_now = _total_jobs_from_compiled_schedule(compiled_schedule)
    after_threshold = total_now >= 25

    scored = []
    for s in (all_found_slots or []):
        try:
            sc = _score_candidate(s, compiled_schedule, daily_last_locations, after_threshold=after_threshold)
        except Exception:
            sc = float("-inf")

        if sc == float("-inf"):
            continue

        # -------- AnyTide prime-day nudge (soft) --------
        try:
            slot_date = s.get("date")
            if hasattr(slot_date, "date"):
                slot_date = slot_date.date()

            ramp_id = s.get("ramp_id")
            if slot_date and ramp_id:
                station_id = _station_for_ramp_or_scituate(ramp_id)
                prime_days = get_low_tide_prime_days(station_id, slot_date, slot_date)
                if slot_date in prime_days:
                    ramp_obj = get_ramp_details(ramp_id)
                    tide_method = getattr(ramp_obj, "tide_method", None) or getattr(ramp_obj, "tide_rule", None)
                    # V-- THIS IS THE MODIFIED LOGIC --V
                    if str(tide_method) == "AnyTide":
                        sc += ANYTIDE_PRIME_BONUS
                    else:
                        sc -= NON_ANYTIDE_PRIME_PENALTY
        except Exception:
            pass
        # -------- end AnyTide nudge --------

        scored.append((sc, s))

    if not scored:
        return []

    scored.sort(key=lambda x: x[0], reverse=True)
    k = max(1, int(k or 1))
    return [s for _, s in scored[:k]]


def check_truck_availability_optimized(truck_id, start_dt, end_dt, compiled_schedule):
    for busy_start, busy_end in compiled_schedule.get(str(truck_id), []):
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

def _find_slot_on_day(search_date, boat, service_type, ramp_id, crane_needed, compiled_schedule, customer_id, trucks_to_check, is_opportunistic_search=False):
    """
    Finds the first available slot on a specific day, but only for the trucks provided in trucks_to_check.
    """
    ramp = get_ramp_details(str(ramp_id))
    if not ramp: return None

    # --- Initial Setup ---
    is_ecm_boat = boat.is_ecm_boat
    is_launch_season = 4 <= search_date.month <= 7
    if is_launch_season:
        start_hour = 9 if not is_ecm_boat else 7
        time_iterator = range(start_hour, 16)
    else: # Haul season
        start_hour = 14 if not is_ecm_boat else 15
        time_iterator = range(start_hour, 7, -1)

    rules_map = globals().get("BOOKING_RULES", {})
    rules = rules_map.get(getattr(boat, "boat_type", None), {})
    hauler_duration = timedelta(minutes=rules.get("truck_mins", 90))
    crane_duration  = timedelta(minutes=rules.get("crane_mins", 0))

    all_tides = fetch_noaa_tides_for_range(ramp.noaa_station_id, search_date, search_date)
    tide_windows_for_day = calculate_ramp_windows(ramp, boat, all_tides.get(search_date, []), search_date)

    is_tide_dependent = ramp.tide_calculation_method not in ["AnyTide", "AnyTideWithDraftRule"]
    if is_tide_dependent and not tide_windows_for_day:
        return None

    # --- Main Loop ---
    for truck in trucks_to_check:
        truck_operating_hours = TRUCK_OPERATING_HOURS.get(truck.truck_id, {}).get(search_date.weekday())
        if not truck_operating_hours:
            continue 

        truck_start_dt = dt.datetime.combine(search_date, truck_operating_hours[0], tzinfo=timezone.utc)
        truck_end_dt = dt.datetime.combine(search_date, truck_operating_hours[1], tzinfo=timezone.utc)

        for hour in time_iterator:
            for minute in [0, 15, 30, 45]:
                slot_start_dt = dt.datetime.combine(search_date, time(hour, minute), tzinfo=timezone.utc)
                slot_end_dt = slot_start_dt + hauler_duration

                if not (slot_start_dt >= truck_start_dt and slot_end_dt <= truck_end_dt): continue
                if not check_truck_availability_optimized(truck.truck_id, slot_start_dt, slot_end_dt, compiled_schedule): continue

                tide_check_passed = False
                if not is_tide_dependent: tide_check_passed = True
                elif service_type == "Launch":
                    tide_critical_moment = slot_start_dt + timedelta(hours=2)
                    for tide_win in tide_windows_for_day:
                        tide_win_start = dt.datetime.combine(search_date, tide_win['start_time'], tzinfo=timezone.utc)
                        tide_win_end = dt.datetime.combine(search_date, tide_win['end_time'], tzinfo=timezone.utc)
                        if tide_win_start <= tide_critical_moment <= tide_win_end: tide_check_passed = True; break
                elif service_type == "Haul":
                    tide_critical_start = slot_start_dt
                    tide_critical_end = slot_start_dt + timedelta(minutes=30)
                    for tide_win in tide_windows_for_day:
                        tide_win_start = dt.datetime.combine(search_date, tide_win['start_time'], tzinfo=timezone.utc)
                        tide_win_end = dt.datetime.combine(search_date, tide_win['end_time'], tzinfo=timezone.utc)
                        if tide_critical_start < tide_win_end and tide_critical_end > tide_win_start: tide_check_passed = True; break

                if not tide_check_passed: continue

                crane_end_dt = None
                if crane_needed:
                    s17_id = get_s17_truck_id()
                    crane_end_dt = slot_start_dt + crane_duration
                    if not check_truck_availability_optimized(s17_id, slot_start_dt, crane_end_dt, compiled_schedule): continue

                return {
                    'is_piggyback': is_opportunistic_search,
                    'boat_id': boat.boat_id, 'customer_id': customer_id, "date": search_date,
                    "time": slot_start_dt.time(), "truck_id": truck.truck_id, "ramp_id": ramp_id,
                    "service_type": service_type, "S17_needed": crane_needed, "scheduled_end_datetime": slot_end_dt, 
                    "S17_busy_end_datetime": crane_end_dt,
                    'tide_rule_concise': get_concise_tide_rule(ramp, boat),
                    'high_tide_times': [t['time'] for t in all_tides.get(search_date, []) if t['type'] == 'H'],
                    'boat_draft': boat.draft_ft
                }
    return None

# --- REPLACEMENT: The new efficiency-driven slot finding engine ---

def score_slot(slot, boat, ramp, tide_window, crane_truck_needed, high_tide_times, low_tide_times):
    score = 10  # Base score
    start_time = slot['start_time']

    # BONUS: Crane efficiency — center on high tide for sailboats
    if crane_truck_needed and high_tide_times:
        for ht in high_tide_times:
            delta = abs((datetime.combine(datetime.today(), ht) - datetime.combine(datetime.today(), start_time)).total_seconds()) / 60
            if delta <= 90:
                score += 2

    # PENALTY: Sailboat on low tide prime day (11AM–1PM) and >5′ draft
    low_prime = any(11 <= lt.hour <= 13 for lt in low_tide_times)
    if crane_truck_needed and low_prime:
        if boat.draft > 5:
            return -1  # REJECT this slot
        elif ramp.ramp_id != 'ScituateHarborJericho':
            score -= 4  # apply minor penalty to sailboats elsewhere

    return score


def find_available_job_slots(customer_id, boat_id, service_type, requested_date_str, selected_ramp_id, num_suggestions_to_find=3, tide_policy=None, **kwargs):
    """
    Finds available slots by first searching ONLY for the preferred truck, then
    falling back to other trucks ONLY if relax_truck_preference is True.
    Uses scoring to rank all found candidates and returns the top-K.

    Enhancements:
      - Reserve "prime" low-tide days (any LOW between 11:00–13:00) for AnyTide ramps only.
      - When the requested date is within ±3 days of a prime day, search those prime days first.
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
    ramp = get_ramp_details(selected_ramp_id) if selected_ramp_id else None

    def _is_anytide_ramp(r):
        if not r:
            return False
        tide_method = getattr(r, "tide_method", None) or getattr(r, "tide_rule", None)
        return str(tide_method) == "AnyTide"

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

    # --- Build candidate day windows (for tide-bias & reservation) ---
    opp_window = [requested_date + dt.timedelta(days=i) for i in range(-7, 8)]
    if crane_needed:
        potential = [d for r_id, d in IDEAL_CRANE_DAYS if str(r_id) == str(selected_ramp_id) and d >= requested_date]
        early = [d for d in potential if d <= requested_date + dt.timedelta(days=21)]
        fb_days = sorted(early)[:30]
        if not fb_days:
            wider = [d for d in potential if d <= requested_date + dt.timedelta(days=45)]
            fb_days = sorted(wider)[:30]
    else:
        fb_days = [requested_date + dt.timedelta(days=i) for i in range(14)]

    if opp_window or fb_days:
        span_start = min([*opp_window, *fb_days]) if (opp_window or fb_days) else requested_date
        span_end = max([*opp_window, *fb_days]) if (opp_window or fb_days) else requested_date
    else:
        span_start = span_end = requested_date

    station_id = _station_for_ramp_or_scituate(selected_ramp_id)
    prime_days = get_low_tide_prime_days(station_id, span_start, span_end)

    s17_id = get_s17_truck_id()
    active_crane_days = {
        j.scheduled_start_dt.date()
        for j in SCHEDULED_JOBS
        if j.scheduled_start_datetime
        and j.scheduled_start_dt.date() in opp_window
        and j.assigned_crane_truck_id == str(s17_id)
        and (str(j.pickup_ramp_id) == str(selected_ramp_id) or str(j.dropoff_ramp_id) == str(selected_ramp_id))
    }
    opp_days = sorted(list(active_crane_days), key=lambda d: abs((d - requested_date).days))
    opp_days = order_dates_with_low_tide_bias(requested_date, opp_days, prime_days)
    fb_days = order_dates_with_low_tide_bias(requested_date, fb_days, prime_days)

    def _run_search(trucks_to_search, search_message_type):
        found = []
        POOL_CAP = max(20, num_suggestions_to_find * 20)

        # Phase 1: Opportunistic Search
        for day in opp_days:
            slot = _find_slot_on_day(
                day, boat, service_type, selected_ramp_id, crane_needed,
                compiled_schedule, customer_id, trucks_to_search,
                is_opportunistic_search=True
            )
            if slot:
                found.append(slot)

        # Phase 2: Fallback Search
        if len(found) < POOL_CAP:
            for day in fb_days:
                is_also_opportunistic = day in active_crane_days
                slot = _find_slot_on_day(
                    day, boat, service_type, selected_ramp_id, crane_needed,
                    compiled_schedule, customer_id, trucks_to_search,
                    is_opportunistic_search=is_also_opportunistic
                )
                if slot:
                    found.append(slot)

        if found:
            best = _select_best_slots(found, compiled_schedule, daily_last_locations, k=num_suggestions_to_find)
            msg = f"Found {len(best)} slot(s) using {search_message_type} truck."
            return (best, msg)
        return ([], None)

    found_slots, message = [], None
    trucks_to_try = preferred_trucks if boat.preferred_truck_id else other_trucks

    if trucks_to_try:
        search_type = "preferred" if boat.preferred_truck_id else "any suitable"
        found_slots, message = _run_search(trucks_to_try, search_type)

    if (not found_slots) and relax_truck_preference and other_trucks:
        found_slots, message = _run_search(other_trucks, "other")

    if found_slots:
        return (found_slots, message, [], False)

    if prime_days:
        # Fallback retry logic if reservation blocked all possible slots
        if ramp and _is_anytide_ramp(ramp):
            _log_debug("Retrying search with prime-day restriction disabled...")
            retry_fb_days = sorted(set(fb_days + opp_days))
            retry_fb_days = order_dates_with_low_tide_bias(requested_date, retry_fb_days, set())

            def retry_search():
                retry_found = []
                for day in retry_fb_days:
                    slot = _find_slot_on_day(
                        day, boat, service_type, selected_ramp_id, crane_needed,
                        compiled_schedule, customer_id, trucks_to_try,
                        is_opportunistic_search=day in active_crane_days
                    )
                    if slot:
                        retry_found.append(slot)
                        if len(retry_found) >= num_suggestions_to_find:
                            break
                return retry_found
        
            retry_slots = retry_search()
            if retry_slots:
                best = _select_best_slots(retry_slots, compiled_schedule, daily_last_locations, k=num_suggestions_to_find)
                return (best, f"Prime-day reservation relaxed — {len(best)} slot(s) available.", [], False)

    # --- GUARANTEED FALLBACK: expand search radius until we have k slots ---
    k = max(1, int(num_suggestions_to_find or 3))
    FALLBACK_MAX_RADIUS = 60            # days
    FALLBACK_STEP = 5                   # grow radius in 5-day chunks
    checked_dates = set()
    pool = []

    def _search_dates(dates, trucks, is_opportunistic=False):
        for day in dates:
            if day in checked_dates:
                continue
            checked_dates.add(day)
            slot = _find_slot_on_day(
                day, boat, service_type, selected_ramp_id, crane_needed,
                compiled_schedule, customer_id, trucks,
                is_opportunistic_search=is_opportunistic
            )
            if slot:
                pool.append(slot)
                _log_debug(f"✓ Slot found on {day} (total now {len(pool)})")
                if len(pool) >= k:
                    return True
            else:
                _log_debug(f"– No valid slot on {day}")
        return False

    # Preferred first, then others (only if allowed)
    radius = 7
    while len(pool) < k and radius <= FALLBACK_MAX_RADIUS:
        days = [requested_date + dt.timedelta(days=offset)
                for offset in range(-radius, radius+1)]
        _log_debug(f"Expanding search to ±{radius} days (currently {len(pool)} slot(s) found)")

        if preferred_trucks:
            if _search_dates(days, preferred_trucks, is_opportunistic=False):
                break
        if relax_truck_preference and other_trucks:
            if _search_dates(days, other_trucks, is_opportunistic=False):
                break
        radius += FALLBACK_STEP

    used_radius = min(radius, FALLBACK_MAX_RADIUS)
    if pool:
        best = _select_best_slots(pool, compiled_schedule, daily_last_locations, k=k)
        _log_debug(f"Guaranteed fallback finished after ±{used_radius} days with {len(best)} slot(s).")
        return (
            best,
            f"Expanded search ±{used_radius} days — {len(best)} slot(s) found.",
            DEBUG_MESSAGES,
            False
        )
    _log_debug(f"Guaranteed fallback failed: no slots after ±{FALLBACK_MAX_RADIUS} days.")
    return (
        [],
        f"No slots found after searching ±{FALLBACK_MAX_RADIUS} days.",
        DEBUG_MESSAGES,
        True
    )


# --- Seasonal batch generator (Spring/Fall), sequential dates, safe if fewer boats remain ---
def simulate_job_requests(
    total_jobs_to_gen: int = 50,
    season: str = "spring",      # "spring" -> May/June (Launch) ; "fall" -> Sep/Oct (Haul)
    year: int = 2025,
    seed: int | None = None,
    **kwargs,                    # tolerate extra args (e.g., truck_hours) from older UI calls
):
    """
    Generates up to 'total_jobs_to_gen' jobs for the chosen season and immediately tries to schedule them.
    - Spring  => Launches in May–June
    - Fall    => Hauls   in Sep–Oct
    - Dates are assigned SEQUENTIALLY across the 2-month window (not random).
    - No Sundays; Saturdays allowed only in May & September (per your rules).
    - If fewer unscheduled boats remain than requested, schedules only what's available.
    Returns a short summary string.
    """
    import datetime, random as _rnd

    if seed is not None:
        _rnd.seed(seed)

    season_norm = season.strip().lower()
    if season_norm not in ("spring", "fall"):
        raise ValueError("season must be 'spring' or 'fall'")

    # Month windows + job type
    if season_norm == "spring":
        months = (5, 6)   # May, June
        req_type = "Launch"
    else:
        months = (9, 10)  # September, October
        req_type = "Haul"

    # Build sequential valid dates across the 2-month window
    def _month_last_day(y, m):
        if m == 12:
            return dt.date(y, 12, 31)
        return dt.date(y, m + 1, 1) - dt.timedelta(days=1)

    valid_dates = []
    for m in months:
        d = dt.date(year, m, 1)
        last = _month_last_day(year, m)
        while d <= last:
            wd = d.weekday()  # Mon=0 ... Sun=6
            is_sun = (wd == 6)
            is_sat = (wd == 5)
            # No Sundays ever; Saturdays allowed only in May & September
            if not is_sun and (not is_sat or m in (5, 9)):
                valid_dates.append(d)
            d += dt.timedelta(days=1)

    if not valid_dates:
        return "No valid dates generated for the selected season."

    # Determine which boats are still unscheduled
    scheduled_boat_ids = {j.boat_id for j in SCHEDULED_JOBS} if 'SCHEDULED_JOBS' in globals() else set()
    all_boats_list = list(LOADED_BOATS.values())
    remaining_boats = [b for b in all_boats_list if getattr(b, "boat_id", None) not in scheduled_boat_ids]

    if not remaining_boats:
        return f"No remaining boats to schedule for {season.title()}."

    # Cap requested jobs to what's actually available
    take = min(total_jobs_to_gen, len(remaining_boats))

    # Assign dates sequentially across the window; if we run out of days, wrap around
    # (Slot finder will enforce capacity; this just seeds requests.)
    requests = []
    for i in range(take):
        boat = remaining_boats[i]
        d = valid_dates[i % len(valid_dates)]
        requests.append({
            "customer_id": getattr(boat, "customer_id", None),
            "boat_id": boat.boat_id,
            "service_type": req_type,
            "requested_date_str": d.strftime("%Y-%m-%d"),
            "selected_ramp_id": getattr(boat, "preferred_ramp_id", None),
            "relax_truck_preference": True,
        })

    # Try to schedule each request in order
    successful = 0
    for req in requests:
        slots, _, _, _ = find_available_job_slots(**req)
        if slots:
            confirm_and_schedule_job(slots[0])
            successful += 1

    summary = (
        f"{season.title()} batch requested: {total_jobs_to_gen}. "
        f"Remaining boats available: {len(remaining_boats)}. "
        f"Scheduled now: {successful}."
    )
    _log_debug(summary) if 'DEBUG_MESSAGES' in globals() else None
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

