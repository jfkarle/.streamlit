# ecm_scheduler_logic.py
# FINAL VERIFIED VERSION

import csv
import datetime
import requests
import random # <--- ADD THIS LINE
from datetime import timedelta, time

# --- NEW: Data Model for Individual Truck/Crane Operating Hours ---
# This structure replaces the old global operating_hours_rules list.
# The UI in app.py will modify this dictionary in the session state.
DEFAULT_TRUCK_OPERATING_HOURS = {
    # Using Monday=0, Sunday=6, and time(hour, minute) format
    "S20/33": {
        0: (time(7, 0), time(15, 0)), 1: (time(7, 0), time(15, 0)),
        2: (time(7, 0), time(15, 0)), 3: (time(7, 0), time(15, 0)),
        4: (time(7, 0), time(15, 0)), 5: (time(8, 0), time(12, 0)),
        6: None,
    },
    "S21/77": {
        0: (time(8, 0), time(16, 0)), 1: (time(8, 0), time(16, 0)),
        2: (time(8, 0), time(16, 0)), 3: (time(8, 0), time(16, 0)),
        4: (time(8, 0), time(16, 0)), 5: None,
        6: None,
    },
    "S23/55": {
        0: (time(8, 0), time(17, 0)), 1: (time(8, 0), time(17, 0)),
        2: (time(8, 0), time(17, 0)), 3: (time(8, 0), time(17, 0)),
        4: (time(8, 0), time(17, 0)), 5: (time(7, 30), time(17, 30)),
        6: None,
    },
    "J17": {
        0: (time(8, 0), time(16, 0)), 1: (time(8, 0), time(16, 0)),
        2: (time(8, 0), time(16, 0)), 3: (time(8, 0), time(16, 0)),
        4: (time(8, 0), time(16, 0)), 5: None,
        6: None,
    }
}

# --- Utility Functions ---

CANDIDATE_CRANE_DAYS = {
    'ScituateHarborJericho': [],
    'PlymouthHarbor': [],
    'WeymouthWessagusset': [],
    'CohassetParkerAve': []
}
ACTIVE_CRANE_DAYS = {
    'ScituateHarborJericho': [],
    'PlymouthHarbor': [],
    'WeymouthWessagusset': [],
    'CohassetParkerAve': []
}
# --- NEW: Configuration for Crane Day Logic ---
CRANE_DAY_LOGIC_ENABLED = True # Master toggle for the entire feature
CANDIDATE_DAY_TIDE_WINDOW = (time(10, 30), time(14, 30)) # 10:30 AM to 2:30 PM
MAX_SEARCH_DAYS_FUTURE = 120 # Search up to 120 days in the future
SEARCH_DAYS_PAST = 7         # Search up to 7 days in the past
CRANE_DAY_REVERSION_WINDOW_DAYS = 7 # Days before an empty Crane Day opens up

def _get_crane_job_count_for_day(check_date, ramp_id):
    """Counts active crane jobs for a given ramp on a specific date."""
    count = 0
    for job in SCHEDULED_JOBS:
        if (job.job_status == "Scheduled" and 
            getattr(job, 'assigned_crane_truck_id', None) == "J17" and
            job.scheduled_start_datetime.date() == check_date):
            job_ramp_id = getattr(job, 'pickup_ramp_id', None) or getattr(job, 'dropoff_ramp_id', None)
            if job_ramp_id == ramp_id:
                count += 1
    return count
def fetch_noaa_tides_for_range(station_id, start_date, end_date):
    """Fetches all tide predictions for a given station over a date range in a single API call."""
    start_str = start_date.strftime("%Y%m%d")
    end_str = end_date.strftime("%Y%m%d")
    
    base = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
    params = {
        "product": "predictions", "application": "ecm-boat-scheduler",
        "begin_date": start_str, "end_date": end_str, "datum": "MLLW",
        "station": station_id, "time_zone": "lst_ldt", "units": "english",
        "interval": "hilo", "format": "json"
    }
    
    try:
        resp = requests.get(base, params=params, timeout=15) # Increased timeout for larger request
        resp.raise_for_status()
        
        # Process the full range of predictions
        predictions = resp.json().get("predictions", [])
        
        # Group the results by date for easy lookup later
        grouped_tides = {}
        for tide in predictions:
            tide_dt = datetime.datetime.strptime(tide["t"], "%Y-%m-%d %H:%M")
            date_key = tide_dt.date()
            if date_key not in grouped_tides:
                grouped_tides[date_key] = []
            
            grouped_tides[date_key].append({
                'type': tide["type"].upper(),
                'time': tide_dt.time(),
                'height': tide["v"]
            })
        return grouped_tides
        
    except Exception as e:
        print(f"ERROR fetching tides for station {station_id} from {start_str} to {end_str}: {e}")
        return {}

def format_time_for_display(time_obj):
    """Formats a time object for display, e.g., 8:00 AM."""
    if not isinstance(time_obj, datetime.time):
        return "InvalidTime"
    # Use '%-I' on Linux/macOS or '%#I' on Windows to remove leading zero
    # A more portable way is to use lstrip
    return time_obj.strftime('%I:%M %p').lstrip('0')

def get_all_tide_times_for_ramp_and_date(ramp_obj, date_obj):
    """
    Fetches all high and low tide times for a given ramp and date
    using the new range-based NOAA API call.
    """
    if not ramp_obj or not ramp_obj.noaa_station_id:
        print(f"[ERROR] Ramp '{ramp_obj.ramp_name if ramp_obj else 'Unknown'}' missing NOAA station ID.")
        return {'H': [], 'L': []}

    # Call the new function for a single-day range
    tides_for_range = fetch_noaa_tides_for_range(ramp_obj.noaa_station_id, date_obj, date_obj)
    
    # Get the specific day's data from the returned dictionary
    tide_data_for_day = tides_for_range.get(date_obj, [])

    all_tides = {'H': [], 'L': []}
    for tide_entry in tide_data_for_day:
        tide_type = tide_entry.get('type')
        if tide_type in ['H', 'L']:
            all_tides[tide_type].append(tide_entry)
            
    return all_tides

def get_concise_tide_rule(ramp, boat):
    if ramp.tide_calculation_method == "AnyTide":
        return "Any Tide"
    if ramp.tide_calculation_method == "AnyTideWithDraftRule":
        if boat.draft_ft is not None and boat.draft_ft < 5.0:
            return "Any Tide (<5' Draft)"
        return "3 hrs +/- High Tide (≥5' Draft)"
    if ramp.tide_offset_hours1:
        return f"{float(ramp.tide_offset_hours1):g} hrs +/- HT"
    return "Tide Rule N/A"

def calculate_ramp_windows(ramp, boat, tide_data, date):
    """
    Calculates the valid time windows for a given ramp and boat based on tide rules.
    This corrected version uses an if/elif/else structure to prevent logical fall-through.
    """
    # Case 1: Ramps with no tide restrictions.
    if ramp.tide_calculation_method == "AnyTide":
        return [{'start_time': datetime.time.min, 'end_time': datetime.time.max}]

    # Case 2: Ramps that have a tide rule based on boat draft.
    elif ramp.tide_calculation_method == "AnyTideWithDraftRule":
        # Shallow draft (< 5ft) boats have no restrictions.
        if boat.draft_ft is not None and boat.draft_ft < 5.0:
            return [{'start_time': datetime.time.min, 'end_time': datetime.time.max}]
        # DEEP DRAFT (>= 5ft) boats get the restricted window.
        else:
            if not tide_data:
                return []
            offset = datetime.timedelta(hours=3) # Hardcoded 3-hour window
            return [{'start_time': (datetime.datetime.combine(date, t['time']) - offset).time(),
                     'end_time': (datetime.datetime.combine(date, t['time']) + offset).time()}
                    for t in tide_data if t['type'] == 'H']

    # Case 3: All other tide rules that use a specific offset (e.g., "HoursAroundHighTide").
    else:
        if not tide_data:
            return []
        # Use the ramp's specific offset value.
        offset = datetime.timedelta(hours=float(ramp.tide_offset_hours1 or 0))
        return [{'start_time': (datetime.datetime.combine(date,t['time'])-offset).time(),
                 'end_time': (datetime.datetime.combine(date,t['time'])+offset).time()}
                for t in tide_data if t['type']=='H']

def is_j17_at_ramp(check_date, ramp_id):
    if not ramp_id: return False
    return ramp_id in crane_daily_status.get(check_date.strftime('%Y-%m-%d'), {}).get('ramps_visited', set())

def load_crane_day_candidates(tide_data):
    for ramp_name in CANDIDATE_CRANE_DAYS.keys():
        candidate_days = []
        for date_obj, high_tide_time in tide_data.get(ramp_name, []):
            if time(10,30) <= high_tide_time <= time(14,30):
                candidate_days.append(date_obj)
        CANDIDATE_CRANE_DAYS[ramp_name] = candidate_days
        

def is_powerboat_blocked_for_crane_day(ramp_name, check_date, job_start_time):
    if ramp_name not in CANDIDATE_CRANE_DAYS:
        return False
    if check_date not in CANDIDATE_CRANE_DAYS[ramp_name]:
        return False

    # Get high tide time for that ramp/date
    high_tide_time = ecm.get_high_tide_time_for_ramp_and_date(ramp_name, check_date)
    if not high_tide_time:
        return False

    window_start = (datetime.combine(check_date, high_tide_time) - timedelta(hours=3)).time()
    window_end = (datetime.combine(check_date, high_tide_time) + timedelta(hours=3)).time()

    return window_start <= job_start_time <= window_end

def load_candidate_days_from_file(filename="candidate_days.csv"):
    """
    Reads the pre-calculated candidate days from a local CSV file
    at startup. This is much faster than scanning live.
    """
    global CANDIDATE_CRANE_DAYS
    print("Loading Candidate Crane Days from local file...")
    try:
        with open(filename, mode='r') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                ramp_id = row['ramp_id']
                if ramp_id in CANDIDATE_CRANE_DAYS:
                    CANDIDATE_CRANE_DAYS[ramp_id].append({
                        "date": datetime.datetime.strptime(row['date'], "%Y-%m-%d").date(),
                        "high_tide_time": datetime.datetime.strptime(row['high_tide_time'], "%H:%M:%S").time()
                    })
        print("Successfully loaded Candidate Crane Days.")
    except FileNotFoundError:
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!!! CRITICAL ERROR: `candidate_days.csv` not found.  !!!")
        print("!!! The app cannot run without this file. Please      !!!")
        print("!!! run `one_time_scanner.py` and upload the CSV.     !!!")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

def _get_crane_job_count_for_day(check_date, ramp_id):
    """Counts active crane jobs for a given ramp on a specific date."""
    count = 0
    for job in SCHEDULED_JOBS:
        if (job.job_status == "Scheduled" and 
            getattr(job, 'assigned_crane_truck_id', None) == "J17" and
            job.scheduled_start_datetime.date() == check_date):
            job_ramp_id = getattr(job, 'pickup_ramp_id', None) or getattr(job, 'dropoff_ramp_id', None)
            if job_ramp_id == ramp_id:
                count += 1
    return count

# --- Configuration & Data Models ---

TODAY_FOR_SIMULATION = datetime.date.today()
JOB_ID_COUNTER = 3000
SCHEDULED_JOBS = []
BOOKING_RULES = {'Powerboat': {'truck_mins': 90, 'crane_mins': 0},'Sailboat DT': {'truck_mins': 180, 'crane_mins': 60},'Sailboat MT': {'truck_mins': 180, 'crane_mins': 90}}
crane_daily_status = {}
class Truck:
    def __init__(self, t_id, name, max_len): self.truck_id=t_id; self.truck_name=name; self.max_boat_length=max_len; self.is_crane="Crane" in name
class Ramp:
    def __init__(self, r_id, name, station, tide_method="AnyTide", offset=None, boats=None):
        self.ramp_id=r_id; self.ramp_name=name; self.noaa_station_id=station; self.tide_calculation_method=tide_method
        self.tide_offset_hours1=offset; self.allowed_boat_types=boats or ["Powerboat", "Sailboat DT", "Sailboat MT"]
class Customer:
    def __init__(self, c_id, name, street_address, truck_id=None, is_ecm=False, home_line2="", home_citystatezip=""):
        self.customer_id = c_id
        self.customer_name = name
        self.street_address = street_address
        self.preferred_truck_id = truck_id
        self.is_ecm_customer = is_ecm
        self.home_line2 = home_line2
        self.home_citystatezip = home_citystatezip
class Boat:
    def __init__(self, b_id, c_id, b_type, b_len, draft=None): self.boat_id=b_id; self.customer_id=c_id; self.boat_type=b_type; self.boat_length=b_len; self.draft_ft=draft
class Job:
    def __init__(self, **kwargs): self.job_status = "Scheduled"; self.__dict__.update(kwargs)
class OperatingHoursEntry:
    def __init__(self, season, day, open_t, close_t): self.season=season; self.day_of_week=day; self.open_time=open_t; self.close_time=close_t

# --- Data Initialization ---
ECM_TRUCKS = { "S20/33": Truck("S20/33", "S20", 60), "S21/77": Truck("S21/77", "S21", 45), "S23/55": Truck("S23/55", "S23", 30), "J17": Truck("J17", "J17 (Crane)", 999)}
ECM_RAMPS = {
    # Arguments mapped to: Ramp(r_id, name, station, tide_method, offset, boats)
    "SandwichBasin": Ramp("SandwichBasin", "Sandwich Basin", "8447180", "AnyTide", None, ["Powerboat"]), # CORRECTED  
    "PlymouthHarbor": Ramp("PlymouthHarbor", "Plymouth Harbor", "8446493", "HoursAroundHighTide", 3.0), # VERIFIED
    "CordagePark": Ramp("CordagePark", "Cordage Park (Plymouth)", "8446493", "HoursAroundHighTide", 1.5, ["Powerboat"]), # VERIFIED
    "DuxburyHarbor": Ramp("DuxburyHarbor", "Duxbury Harbor (Town Pier)", "8446166", "HoursAroundHighTide", 1.0, ["Powerboat"]), # CORRECTED
    "GreenHarborTaylors": Ramp("GreenHarborTaylors", "Green Harbor (Taylors)", "8446009", "HoursAroundHighTide", 3.0, ["Powerboat"]), # VERIFIED
    "GreenHarborSafeHarbor": Ramp("GreenHarborSafeHarbor", "Safe Harbor (Green Harbor)", "8446009", "HoursAroundHighTide", 1.0, ["Powerboat"]), # VERIFIED
    "FerryStreet": Ramp("FerryStreet", "Ferry Street MYC", "8446009", "HoursAroundHighTide", 3.0, ["Powerboat"]), 
    "SouthRiverYachtYard": Ramp("SouthRiverYachtYard", "SRYY", "8446009", "HoursAroundHighTide", 2.0, ["Powerboat"]),
    "ScituateHarborJericho": Ramp("ScituateHarborJericho", "Scituate Harbor (Jericho Road)", "8445138", "AnyTideWithDraftRule"), # CORRECTED
    "CohassetParkerAve": Ramp("CohassetParkerAve", "Cohasset Harbor (Parker Ave)", "8444762", "HoursAroundHighTide", 3.0), # CORRECTED
    "HullASt": Ramp("HullASt", "Hull (A St, Sunset, Steamboat)", "8444351", "HoursAroundHighTide_WithDraftRule", 3.0), # CORRECTED
    "HinghamHarbor": Ramp("HinghamHarbor", "Hingham Harbor", "8444662", "HoursAroundHighTide", 3.0), # CORRECTED
    "WeymouthWessagusset": Ramp("WeymouthWessagusset", "Weymouth Harbor (Wessagusset)", "8444788", "HoursAroundHighTide", 3.0), # CORRECTED
}

LOADED_CUSTOMERS = {}; LOADED_BOATS = {}
def load_customers_and_boats_from_csv(filename="ECM Sample Cust.csv"):
    global LOADED_CUSTOMERS, LOADED_BOATS
    try:
        with open(filename, mode='r', encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            for i, row in enumerate(reader):
                cust_id = f"C{1001+i}"
                boat_id = f"B{5001+i}"
                home_line2 = row.get('Bill to 2', '').strip()
                home_citystatezip = row.get('Bill to 3', '').strip()

                LOADED_CUSTOMERS[cust_id] = Customer(
                    cust_id,
                    row['customer_name'],
                    row.get('street_address', ''),
                    row.get('preferred_truck'),
                    row.get('is_ecm_boat', '').lower() in ['true', 'yes'],
                    home_line2,
                    home_citystatezip
                )

                try:
                    boat_length = float(row.get('boat_length', '').strip())
                    boat_draft = float(row.get('boat_draft', '').strip() or 0)
                except ValueError:
                    print(f"Skipping row {i} due to invalid boat_length or boat_draft")
                    continue
                
                LOADED_BOATS[boat_id] = Boat(
                    boat_id,
                    cust_id,
                    row['boat_type'],
                    boat_length,
                    boat_draft
                )
        
        # --- NEW ---
        # After loading all other data, scan for candidate days
        load_candidate_days_from_file()
        return True
    except FileNotFoundError:
        return False

# --- Core Logic Functions ---
get_customer_details = LOADED_CUSTOMERS.get; get_boat_details = LOADED_BOATS.get; get_ramp_details = ECM_RAMPS.get

def calculate_scheduling_stats(all_customers, all_boats, scheduled_jobs):
    """
    Calculates scheduling statistics for all boats and ECM boats specifically.
    A boat is only considered "launched" if its launch date is in the past.
    """
    today = datetime.date.today()

    # --- Calculate stats for ALL boats ---
    total_all_boats = len(all_boats)
    
    scheduled_customer_ids = {j.customer_id for j in scheduled_jobs if j.job_status == "Scheduled"}
    scheduled_all_boats = len(scheduled_customer_ids)

    # NEW: Only count launches where the scheduled date is before today
    launched_customer_ids = {
        j.customer_id for j in scheduled_jobs 
        if j.job_status == "Scheduled" 
        and j.service_type == "Launch" 
        and j.scheduled_start_datetime.date() < today
    }
    launched_all_boats = len(launched_customer_ids)

    # --- Calculate stats for ECM boats ONLY ---
    ecm_customer_ids = {c_id for c_id, cust in all_customers.items() if cust.is_ecm_customer}
    total_ecm_boats = len(ecm_customer_ids)

    scheduled_ecm_boats = len(scheduled_customer_ids.intersection(ecm_customer_ids))
    launched_ecm_boats = len(launched_customer_ids.intersection(ecm_customer_ids))

    return {
        'all_boats': {'total': total_all_boats, 'scheduled': scheduled_all_boats, 'launched': launched_all_boats},
        'ecm_boats': {'total': total_ecm_boats, 'scheduled': scheduled_ecm_boats, 'launched': launched_ecm_boats}
    }
    
def get_final_schedulable_ramp_times(ramp_obj, boat_obj, date_to_check, all_tides_in_range, truck_id, truck_hours_schedule):
    """
    Calculates the final, schedulable time windows by combining a specific
    truck's working hours with the ramp's tidal windows for a given day.
    """
    # 1. Get the specific truck's working hours for the given day
    day_of_week = date_to_check.weekday()
    truck_hours = truck_hours_schedule.get(truck_id, {}).get(day_of_week)
    if not truck_hours:
        return [] # This truck is not working on this day

    truck_open_dt = datetime.datetime.combine(date_to_check, truck_hours[0])
    truck_close_dt = datetime.datetime.combine(date_to_check, truck_hours[1])

    # 2. Handle "Transport" jobs that have no ramp (window is just truck hours)
    if not ramp_obj:
        return [{
            'start_time': truck_hours[0],
            'end_time': truck_hours[1],
            'high_tide_times': [],
            'tide_rule_concise': 'N/A'
        }]

    # 3. Get the ramp's tidal windows based on its rules
    tide_data_for_day = all_tides_in_range.get(date_to_check, [])
    tidal_windows = calculate_ramp_windows(ramp_obj, boat_obj, tide_data_for_day, date_to_check)

    # 4. Find the overlap between the truck's working hours and the ramp's tidal windows
    final_windows = []
    for t_win in tidal_windows:
        tidal_start_dt = datetime.datetime.combine(date_to_check, t_win['start_time'])
        tidal_end_dt = datetime.datetime.combine(date_to_check, t_win['end_time'])

        # Find the intersection of the two windows
        overlap_start = max(tidal_start_dt, truck_open_dt)
        overlap_end = min(tidal_end_dt, truck_close_dt)

        if overlap_start < overlap_end:
            final_windows.append({
                'start_time': overlap_start.time(),
                'end_time': overlap_end.time(),
                'high_tide_times': [t['time'] for t in tide_data_for_day if t['type'] == 'H'],
                'tide_rule_concise': get_concise_tide_rule(ramp_obj, boat_obj)
            })

    return final_windows

def get_suitable_trucks(boat_len, pref_truck_id=None, force_preferred=False):
    all_suitable = [t for t in ECM_TRUCKS.values() if not t.is_crane and boat_len <= t.max_boat_length]
    if force_preferred and pref_truck_id and any(t.truck_id == pref_truck_id for t in all_suitable):
        return [t for t in all_suitable if t.truck_id == pref_truck_id]
    return all_suitable

def check_truck_availability(truck_id, start_dt, end_dt):
    for job in SCHEDULED_JOBS:
        if job.job_status == "Scheduled" and (
            getattr(job, 'assigned_hauling_truck_id', None) == truck_id or
            (getattr(job, 'assigned_crane_truck_id', None) == truck_id and truck_id == "J17")
        ):
            job_start = job.scheduled_start_datetime
            job_end = getattr(job, 'j17_busy_end_datetime', job.scheduled_end_datetime) if truck_id == "J17" else job.scheduled_end_datetime
            
            # ⬇️ Log if there's a conflict
            if start_dt < job_end and end_dt > job_start:
                print(f"[DEBUG] Conflict for truck {truck_id}:")
                print(f"    Requested window: {start_dt.strftime('%Y-%m-%d %I:%M %p')} to {end_dt.strftime('%I:%M %p')}")
                customer = get_customer_details(job.customer_id)
                customer_name = customer.customer_name if customer else "Unknown"
                print(f"    Conflicts with: {customer_name}, from {job_start.strftime('%I:%M %p')} to {job_end.strftime('%I:%M %p')} on {job_start.date()}")
                return False

    return True

def _check_and_create_slot_detail(s_date, p_time, truck, cust, boat, service, ramp, ecm_hours, duration, j17_duration, window_details, is_active_crane_day=False, is_candidate_crane_day=False):
    start_dt = datetime.datetime.combine(s_date, p_time); hauler_end_dt = start_dt + datetime.timedelta(hours=duration)
    if hauler_end_dt.time() > ecm_hours['close'] and not (hauler_end_dt.time() == ecm_hours['close'] and hauler_end_dt.date() == s_date): return None
    if not check_truck_availability(truck.truck_id, start_dt, hauler_end_dt): return None
    needs_j17 = BOOKING_RULES.get(boat.boat_type, {}).get('crane_mins', 0) > 0
    if needs_j17 and not check_truck_availability("J17", start_dt, start_dt + datetime.timedelta(hours=j17_duration)): return None
    return {'date': s_date, 'time': p_time, 'truck_id': truck.truck_id, 'j17_needed': needs_j17, 'type': "Open", 
            'ramp_id': ramp.ramp_id if ramp else None, 
            'priority_score': 1 if needs_j17 and ramp and is_j17_at_ramp(s_date, ramp.ramp_id) else 0, 
            'is_active_crane_day': is_active_crane_day, # NEW
            'is_candidate_crane_day': is_candidate_crane_day, # NEW
            **window_details}
def get_j17_crane_grouping_slot(boat, customer, ramp_obj, requested_date_obj, trucks, duration, j17_duration, service_type):
    """Searches ±14 forward and -7 back to find dates where J17 is already assigned at same ramp"""
    grouping_slot = None
    for offset in list(range(-7, 15)):  # -7 to +14 days
        check_date = requested_date_obj + timedelta(days=offset)
        date_str = check_date.strftime('%Y-%m-%d')
        if date_str in crane_daily_status and ramp_obj.ramp_id in crane_daily_status[date_str].get('ramps_visited', set()):
            ecm_hours = get_ecm_operating_hours(check_date)
            if not ecm_hours:
                continue
            tide_windows = get_final_schedulable_ramp_times(ramp_obj, boat, check_date)
            for truck in trucks:
                for window in tide_windows:
                    p_start_time, p_end_time = window['start_time'], window['end_time']
                    p_time = p_start_time
                    while p_time < p_end_time:
                        temp_dt = datetime.datetime.combine(check_date, p_time)
                        if temp_dt.minute % 30 != 0:
                            p_time = (temp_dt + timedelta(minutes=30 - (temp_dt.minute % 30))).time()
                        if p_time >= p_end_time:
                            break
                        slot = _check_and_create_slot_detail(
                            check_date, p_time, truck, customer, boat, service_type,
                            ramp_obj, ecm_hours, duration, j17_duration, window
                        )
                        if slot:
                            slot['priority_score'] = 999  # Force it to top of list
                            return slot
                        p_time = (datetime.datetime.combine(datetime.date.min, p_time) + timedelta(minutes=30)).time()
    return grouping_slot
    
def find_available_job_slots(customer_id, boat_id, service_type, requested_date_str,
                             selected_ramp_id=None,
                             force_preferred_truck=True,
                             num_suggestions_to_find=5,
                             manager_override=False,
                             crane_look_back_days=7,
                             crane_look_forward_days=60,
                             truck_operating_hours=None,
                             **kwargs):
    """
    Finds available job slots using a multi-stage search that dynamically
    calculates availability based on individual truck hours and tide windows.
    """
    try:
        requested_date = datetime.datetime.strptime(requested_date_str, '%Y-%m-%d').date()
    except ValueError:
        return [], "Error: Invalid date format.", [], False

    customer, boat = get_customer_details(customer_id), get_boat_details(boat_id)
    if not customer or not boat: return [], "Invalid Customer/Boat ID.", [], False

    ramp_obj = get_ramp_details(selected_ramp_id)
    if service_type in ["Launch", "Haul"] and ramp_obj:
        if boat.boat_type not in ramp_obj.allowed_boat_types:
            message = (f"Validation Error: The selected boat type ('{boat.boat_type}') is not "
                       f"permitted at the selected ramp ('{ramp_obj.ramp_name}').")
            return [], message, [], False

    # This function now requires the truck operating hours to be passed in
    if truck_operating_hours is None:
        return [], "System Error: Truck operating hours not provided.", [], False


    rules = BOOKING_RULES.get(boat.boat_type, {})
    hauler_duration = datetime.timedelta(minutes=rules.get('truck_mins', 90))
    j17_duration = datetime.timedelta(minutes=rules.get('crane_mins', 0))
    needs_j17 = j17_duration.total_seconds() > 0
    suitable_trucks = get_suitable_trucks(boat.boat_length, customer.preferred_truck_id, force_preferred_truck)

    # --- THIS IS THE NEW DYNAMIC SLOT FINDER ---
    # --- THIS IS THE NEW DYNAMIC SLOT FINDER ---
    def _find_slots_for_dates(date_list, slot_type_flag):
        found_slots = []
        # Pre-fetch all tides for the entire date range for performance
        all_tides = {}
        if ramp_obj and date_list:
            all_tides = fetch_noaa_tides_for_range(ramp_obj.noaa_station_id, min(date_list), max(date_list))

        for check_date in sorted(list(set(date_list))):
            slot_found_for_day = False
            if len(found_slots) >= num_suggestions_to_find: break

            for truck in suitable_trucks:
                # Get the final combined window of truck hours and tides
                windows = get_final_schedulable_ramp_times(ramp_obj, boat, check_date, all_tides, truck.truck_id, truck_operating_hours)

                for window in windows:
                    p_time = window['start_time']
                    end_time = window['end_time']

                    while p_time < end_time:
                        slot_start_dt = datetime.datetime.combine(check_date, p_time)

                        # Check availability of the hauling truck
                        if not check_truck_availability(truck.truck_id, slot_start_dt, slot_start_dt + hauler_duration):
                            p_time = (slot_start_dt + datetime.timedelta(minutes=30)).time()
                            continue

                        # Check availability of the crane if needed
                        if needs_j17 and not check_truck_availability("J17", slot_start_dt, slot_start_dt + j17_duration):
                            p_time = (slot_start_dt + datetime.timedelta(minutes=30)).time()
                            continue

                        # If we get here, the slot is valid
                        final_slot = {
                            'date': check_date, 'time': p_time, 'truck_id': truck.truck_id,
                            'j17_needed': needs_j17, 'type': slot_type_flag, 'ramp_id': selected_ramp_id,
                            'is_active_crane_day': (slot_type_flag == 'Active Day Grouping'),
                            'is_candidate_crane_day': (slot_type_flag == 'Candidate Day Activation'),
                            'tide_rule_concise': window.get('tide_rule_concise'),
                            'high_tide_times': window.get('high_tide_times', [])
                        }
                        found_slots.append(final_slot)
                        slot_found_for_day = True
                        break # Found a slot with this truck, move to next day

                    if slot_found_for_day:
                        break # Found a slot for this day, move to next day
                if slot_found_for_day:
                    break # Found a slot for this day, move to next day
        return found_slots

    # --- MAIN LOGIC ---
    if needs_j17 and not manager_override:
        search_start_date = requested_date - datetime.timedelta(days=crane_look_back_days)
        search_end_date = requested_date + datetime.timedelta(days=crane_look_forward_days)
        
        # STAGE 1: Search ACTIVE crane days
        active_days = {datetime.datetime.strptime(d_str, '%Y-%m-%d').date() for d_str, status in crane_daily_status.items() if selected_ramp_id in status.get('ramps_visited', set())}
        active_days_in_range = [d for d in active_days if search_start_date <= d <= search_end_date]
        if active_days_in_range:
            slots = _find_slots_for_dates(active_days_in_range, "Active Day Grouping")
            if slots:
                slots.sort(key=lambda s: abs(s['date'] - requested_date))
                return slots, "Found slots by grouping with an existing crane job.", [], True

        # STAGE 2: Search CANDIDATE crane days
        candidate_days_info = CANDIDATE_CRANE_DAYS.get(selected_ramp_id, [])
        candidate_dates = [day['date'] for day in candidate_days_info if search_start_date <= day['date'] <= search_end_date]
        if candidate_dates:
            slots = _find_slots_for_dates(candidate_dates, "Candidate Day Activation")
            if slots:
                slots.sort(key=lambda s: abs(s['date'] - requested_date))
                return slots, "Found open slots on ideal tide days, activating a new crane day.", [], True

    # STAGE 3: General search
    general_search_dates = [requested_date + datetime.timedelta(days=i) for i in range(crane_look_forward_days)]
    slots = _find_slots_for_dates(general_search_dates, "General Availability")
    if not slots:
        return [], "No suitable slots could be found in the specified window.", [], False
    
    slots.sort(key=lambda s: abs(s['date'] - requested_date))
    return slots, f"Found {len(slots)} available slots.", [], False

def confirm_and_schedule_job(original_request, selected_slot):
    # 1. Get all the required objects
    customer = get_customer_details(original_request['customer_id'])
    boat = get_boat_details(original_request['boat_id'])
    ramp = get_ramp_details(selected_slot.get('ramp_id'))
    
    # 2. Validate inputs
    if not customer or not boat:
        return None, "Error: Could not find Customer or Boat details."
    if original_request['service_type'] in ["Launch", "Haul"] and not ramp:
        return None, "Error: A ramp must be selected for this service type."

    # --- NEW: Fetch all tide data at the moment of booking ---
    tide_data = get_all_tide_times_for_ramp_and_date(ramp, selected_slot['date'])
    high_tides = tide_data.get('H', [])
    low_tides = tide_data.get('L', [])
    # --- END NEW ---

    # 3. Calculate job details
    job_id = globals()['JOB_ID_COUNTER'] + 1
    globals()['JOB_ID_COUNTER'] += 1
    
    start_dt = datetime.datetime.combine(selected_slot['date'], selected_slot['time'])
    rules = BOOKING_RULES.get(boat.boat_type, {})
    hauler_duration_hours = rules.get('truck_mins', 90) / 60.0
    hauler_end_dt = start_dt + datetime.timedelta(hours=hauler_duration_hours)
    
    j17_end_dt = None
    if selected_slot.get('j17_needed'):
        j17_duration_hours = rules.get('crane_mins', 0) / 60.0
        j17_end_dt = start_dt + datetime.timedelta(hours=j17_duration_hours)

    pickup_addr, dropoff_addr = "", ""
    pickup_rid, dropoff_rid = None, None
    service_type = original_request['service_type']

    if service_type == "Launch":
        pickup_addr = "HOME"
        dropoff_addr = ramp.ramp_name
        dropoff_rid = ramp.ramp_id
    elif service_type == "Haul":
        pickup_addr = ramp.ramp_name
        pickup_rid = ramp.ramp_id
        dropoff_addr = "HOME"
    
    # 4. Create the new Job object, now including the tide data
    new_job = Job(
        job_id=job_id,
        customer_id=customer.customer_id,
        boat_id=boat.boat_id,
        service_type=service_type,
        scheduled_start_datetime=start_dt,
        scheduled_end_datetime=hauler_end_dt,
        assigned_hauling_truck_id=selected_slot['truck_id'],
        assigned_crane_truck_id="J17" if selected_slot.get('j17_needed') else None,
        j17_busy_end_datetime=j17_end_dt,
        pickup_ramp_id=pickup_rid,
        dropoff_ramp_id=dropoff_rid,
        # --- NEW: Add tide data to the saved job record ---
        high_tides=high_tides,
        low_tides=low_tides,
        # --- Note: Other attributes from previous versions are consolidated by **kwargs in Job class ---
        job_status="Scheduled",
        notes=f"Booked via type: {selected_slot.get('type', 'N/A')}.",
        pickup_street_address=pickup_addr,
        dropoff_street_address=dropoff_addr,
    )

    # 5. Add the job to the schedule
    SCHEDULED_JOBS.append(new_job)

    # 6. Update crane status if needed
    if new_job.assigned_crane_truck_id:
        date_str = new_job.scheduled_start_datetime.strftime('%Y-%m-%d')
        if date_str not in crane_daily_status:
            crane_daily_status[date_str] = {'ramps_visited': set()}
        if new_job.pickup_ramp_id:
            crane_daily_status[date_str]['ramps_visited'].add(new_job.pickup_ramp_id)
        if new_job.dropoff_ramp_id:
            crane_daily_status[date_str]['ramps_visited'].add(new_job.dropoff_ramp_id)
            
    # 7. Return success message
    return new_job.job_id, f"SUCCESS: Job {new_job.job_id} for {customer.customer_name} scheduled."

def generate_random_jobs(num_to_generate, start_date, end_date, service_type_filter, truck_operating_hours):
    """
    Finds and schedules a specified number of random, valid jobs within a given
    date range and for a specific service type.
    """
    if not LOADED_CUSTOMERS or not LOADED_BOATS or not ECM_RAMPS:
        return "Error: Cannot generate jobs, master data not loaded."
    if start_date > end_date:
        return "Error: Start date cannot be after end date."

    print(f"--- Starting Bulk Job Generation for {num_to_generate} jobs ---")
    success_count = 0
    fail_count = 0

    customer_ids = list(LOADED_CUSTOMERS.keys())
    ramp_ids = list(ECM_RAMPS.keys())
    date_range_days = (end_date - start_date).days

    for i in range(num_to_generate):
        service_type = random.choice(["Launch", "Haul", "Transport"]) if service_type_filter.lower() == 'all' else service_type_filter

        random_customer_id = random.choice(customer_ids)
        customer = get_customer_details(random_customer_id)
        boat = next((b for b in LOADED_BOATS.values() if b.customer_id == random_customer_id), None)
        if not boat:
            fail_count += 1
            continue

        random_ramp_id = random.choice(ramp_ids) if service_type in ["Launch", "Haul"] else None
        random_date = start_date + datetime.timedelta(days=random.randint(0, date_range_days))

        slots, _, _, _ = find_available_job_slots(
            customer_id=random_customer_id,
            boat_id=boat.boat_id,
            service_type=service_type,
            requested_date_str=random_date.strftime('%Y-%m-%d'),
            selected_ramp_id=random_ramp_id,
            force_preferred_truck=False,
            truck_operating_hours=truck_operating_hours # Pass the schedule
        )

        if slots:
            selected_slot = random.choice(slots)
            job_request = {'customer_id': random_customer_id, 'boat_id': boat.boat_id, 'service_type': service_type}
            new_job_id, _ = confirm_and_schedule_job(job_request, selected_slot)

            if new_job_id:
                success_count += 1
            else:
                fail_count += 1
        else:
            fail_count += 1

    summary_message = f"Bulk generation complete. Successfully created {success_count} jobs. Failed to find slots for {fail_count} attempts."
    print(f"--- {summary_message} ---")
    return summary_message

def calculate_scheduling_stats(all_customers, all_boats, scheduled_jobs):
    """
    Calculates scheduling statistics for all boats and ECM boats specifically.
    """
    # --- Calculate stats for ALL boats ---
    total_all_boats = len(all_boats)
    
    # Get unique customer IDs from all scheduled jobs
    scheduled_customer_ids = {j.customer_id for j in scheduled_jobs if j.job_status == "Scheduled"}
    scheduled_all_boats = len(scheduled_customer_ids)

    # Get unique customer IDs from LAUNCH jobs only
    launched_customer_ids = {j.customer_id for j in scheduled_jobs if j.job_status == "Scheduled" and j.service_type == "Launch"}
    launched_all_boats = len(launched_customer_ids)

    # --- Calculate stats for ECM boats ONLY ---
    ecm_customer_ids = {c_id for c_id, cust in all_customers.items() if cust.is_ecm_customer}
    total_ecm_boats = len(ecm_customer_ids)

    # Find the intersection of scheduled/launched customers and ECM customers
    scheduled_ecm_boats = len(scheduled_customer_ids.intersection(ecm_customer_ids))
    launched_ecm_boats = len(launched_customer_ids.intersection(ecm_customer_ids))

    return {
        'all_boats': {'total': total_all_boats, 'scheduled': scheduled_all_boats, 'launched': launched_all_boats},
        'ecm_boats': {'total': total_ecm_boats, 'scheduled': scheduled_ecm_boats, 'launched': launched_ecm_boats}
    }
