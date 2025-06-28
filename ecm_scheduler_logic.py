# ecm_scheduler_logic.py
# FINAL VERIFIED VERSION

import csv
import datetime
import requests
from datetime import timedelta, time

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

def fetch_noaa_tides(station_id, date_to_check):
    date_str = date_to_check.strftime("%Y%m%d")
    base = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
    params = {"product":"predictions", "application":"ecm-boat-scheduler", "begin_date":date_str, "end_date":date_str, "datum":"MLLW", "station":station_id, "time_zone":"lst_ldt", "units":"english", "interval":"hilo", "format":"json"}
    try:
        resp = requests.get(base, params=params, timeout=10)
        resp.raise_for_status()
        # --- MODIFIED LINE ---
        # Now returns a dictionary including the tide height ('v')
        return [{'type': i["type"].upper(), 
                 'time': datetime.datetime.strptime(i["t"], "%Y-%m-%d %H:%M").time(),
                 'height': i["v"]} 
                for i in resp.json().get("predictions", [])]
    except Exception as e:
        print(f"ERROR fetching tides for station {station_id}: {e}")
        return []

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
    using the NOAA API and returns a dictionary.
    """
    if not ramp_obj or not ramp_obj.noaa_station_id:
        print(f"[ERROR] Ramp '{ramp_obj.ramp_name if ramp_obj else 'Unknown'}' missing NOAA station ID.")
        return {'H': [], 'L': []}

    tide_data = fetch_noaa_tides(ramp_obj.noaa_station_id, date_obj)

    all_tides = {'H': [], 'L': []}
    for tide_entry in tide_data:
        tide_type = tide_entry.get('type')
        if tide_type in ['H', 'L']:
            # --- THE FIX ---
            # This now appends the entire tide record (with time and height),
            # not just the time object.
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
    "ScituateHarborJericho": Ramp("ScituateHarborJericho", "Scituate Harbor (Jericho Road)", "8445138", "AnyTideWithDraftRule"), # CORRECTED
    "CohassetParkerAve": Ramp("CohassetParkerAve", "Cohasset Harbor (Parker Ave)", "8444762", "HoursAroundHighTide", 3.0), # CORRECTED
    "HullASt": Ramp("HullASt", "Hull (A St, Sunset, Steamboat)", "8444351", "HoursAroundHighTide_WithDraftRule", 3.0), # CORRECTED
    "HinghamHarbor": Ramp("HinghamHarbor", "Hingham Harbor", "8444662", "HoursAroundHighTide", 3.0), # CORRECTED
    "WeymouthWessagusset": Ramp("WeymouthWessagusset", "Weymouth Harbor (Wessagusset)", "8444788", "HoursAroundHighTide", 3.0), # CORRECTED
}
operating_hours_rules = [
    OperatingHoursEntry("Standard", 0, datetime.time(8,0), datetime.time(16,0)), OperatingHoursEntry("Standard", 1, datetime.time(8,0), datetime.time(16,0)),
    OperatingHoursEntry("Standard", 2, datetime.time(8,0), datetime.time(16,0)), OperatingHoursEntry("Standard", 3, datetime.time(8,0), datetime.time(16,0)),
    OperatingHoursEntry("Standard", 4, datetime.time(8,0), datetime.time(16,0)), OperatingHoursEntry("Busy", 0, datetime.time(7,30), datetime.time(17,30)),
    OperatingHoursEntry("Busy", 1, datetime.time(7,30), datetime.time(17,30)), OperatingHoursEntry("Busy", 2, datetime.time(7,30), datetime.time(17,30)),
    OperatingHoursEntry("Busy", 3, datetime.time(7,30), datetime.time(17,30)), OperatingHoursEntry("Busy", 4, datetime.time(7,30), datetime.time(17,30)),
    OperatingHoursEntry("BusySaturday", 5, datetime.time(7,30), datetime.time(17,30)),
]
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
                    row.get('is_ecm_boat', '').lower() == 'true',
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

def get_ecm_operating_hours(date):
    season = "Busy" if date.month in [4,5,6,9,10] else "Standard"
    if season == "Busy" and date.weekday() == 5 and date.month in [5,9]: season = "BusySaturday"
    for rule in operating_hours_rules:
        if rule.season == season and rule.day_of_week == date.weekday(): return {"open": rule.open_time, "close": rule.close_time}
    return None

def calculate_ramp_windows(ramp, boat, tide_data, date):
    if ramp.tide_calculation_method == "AnyTide":
        return [{'start_time': datetime.time.min, 'end_time': datetime.time.max}]
    if ramp.tide_calculation_method == "AnyTideWithDraftRule":
        if boat.draft_ft is not None and boat.draft_ft < 5.0:
            return [{'start_time': datetime.time.min, 'end_time': datetime.time.max}]
        offset = datetime.timedelta(hours=3)
        return [{'start_time': (datetime.datetime.combine(date, t['time']) - offset).time(),
                 'end_time': (datetime.datetime.combine(date, t['time']) + offset).time()}
                for t in tide_data if t['type'] == 'H']
    if not tide_data:
        return []
    offset = datetime.timedelta(hours=float(ramp.tide_offset_hours1 or 0))
    return [{'start_time': (datetime.datetime.combine(date,t['time'])-offset).time(), 
             'end_time': (datetime.datetime.combine(date,t['time'])+offset).time()} 
            for t in tide_data if t['type']=='H']

def get_final_schedulable_ramp_times(ramp_obj, boat_obj, date_to_check):
    ecm_hours = get_ecm_operating_hours(date_to_check)
    if not ecm_hours:
        return []

    ecm_open = datetime.datetime.combine(date_to_check, ecm_hours['open'])
    ecm_close = datetime.datetime.combine(date_to_check, ecm_hours['close'])
    tide_data = fetch_noaa_tides(ramp_obj.noaa_station_id, date_to_check)
    
    # --- THIS IS THE CORRECTED CALL ---
    tidal_windows = calculate_ramp_windows(ramp_obj, boat_obj, tide_data, date_to_check)
    
    final_windows = []
    for t_win in tidal_windows:
        tidal_start = datetime.datetime.combine(date_to_check, t_win['start_time'])
        tidal_end = datetime.datetime.combine(date_to_check, t_win['end_time'])
        overlap_start = max(tidal_start, ecm_open)
        overlap_end = min(tidal_end, ecm_close)
        
        if overlap_start < overlap_end:
            final_windows.append({
                'start_time': overlap_start.time(),
                'end_time': overlap_end.time(),
                'high_tide_times': [t['time'] for t in tide_data if t['type'] == 'H'],
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
    

def find_available_job_slots(customer_id, boat_id, service_type, requested_date_str, selected_ramp_id=None, 
                             force_preferred_truck=True, relax_ramp=False, manager_override=False, 
                             num_suggestions_to_find=3, 
                             crane_look_back_days=7, 
                             crane_look_forward_days=60, 
                             **kwargs):
    global CRANE_DAY_LOGIC_ENABLED
    try:
        requested_date_obj = datetime.datetime.strptime(requested_date_str, '%Y-%m-%d').date()
    except ValueError:
        return [], "Error: Invalid date format.", [], False

    customer, boat = get_customer_details(customer_id), get_boat_details(boat_id)
    if not customer or not boat: return [], "Invalid Customer/Boat ID.", [], False
    
    ramp_obj = get_ramp_details(selected_ramp_id)
    if not ramp_obj and service_type in ["Launch", "Haul"]:
        return [], "A ramp must be selected for this service.", [], False

    if ramp_obj and boat.boat_type not in ramp_obj.allowed_boat_types:
        message = f"Error: The selected boat type '{boat.boat_type}' is not allowed at {ramp_obj.ramp_name}."
        return [], message, [], False

    def _find_first_slot_on_day(check_date, ramp_obj, trucks_to_check, is_crane_job_flag):
    if check_date == datetime.date(2025, 10, 7):
    print(f"DEBUG: Checking Oct 7th for customer {cust.customer_name}")
    print(f"DEBUG: ECM Hours for Oct 7th: {ecm_hours}")
    print(f"DEBUG: Tidal Windows for Oct 7th: {windows}")
    print(f"DEBUG: Truck to check: {trucks_to_check[0].truck_id}")
    print(f"DEBUG: Required duration (hours): {duration_hours}")
    print(f"DEBUG: J17 duration (hours): {j17_duration}")
    # Add print statements inside the 'for w in windows:' loop and 'while p_time < w['end_time']:' loop
    # to see which slots are being tried and why they might be skipped/return None.
    # Example:
    # print(f"  Trying slot {p_time} in window {w['start_time']}-{w['end_time']}")
    # if not check_truck_availability(...): print(f"    Truck not available at {p_time}")
    # if needs_j17 and not check_truck_availability("J17",...): print(f"    J17 not available at {p_time}")

        
        
        ecm_hours = get_ecm_operating_hours(check_date)
        if not ecm_hours: return None
        
        windows = get_final_schedulable_ramp_times(ramp_obj, boat, check_date)
        rules = BOOKING_RULES.get(boat.boat_type, {})
        duration_hours = rules.get('truck_mins', 90) / 60.0
        j17_duration = rules.get('crane_mins', 0) / 60.0

        blocked_window = None
        if not is_crane_job_flag and not manager_override:
            candidate_days_at_ramp = CANDIDATE_CRANE_DAYS.get(ramp_obj.ramp_id, [])
            for day_info in candidate_days_at_ramp:
                if day_info['date'] == check_date:
                    ht = day_info['high_tide_time']
                    start_blocked = (datetime.datetime.combine(check_date, ht) - timedelta(hours=3)).time()
                    end_blocked = (datetime.datetime.combine(check_date, ht) + timedelta(hours=3)).time()
                    blocked_window = (start_blocked, end_blocked)
                    break
        
        for w in windows:
            p_time = w['start_time']
            while p_time < w['end_time']:
                if blocked_window and (blocked_window[0] <= p_time < blocked_window[1]):
                    p_time = (datetime.datetime.combine(check_date, p_time) + timedelta(minutes=30)).time()
                    continue
                
                is_first_slot = (p_time == ecm_hours['open'])
                end_of_slot_dt = datetime.datetime.combine(check_date, p_time) + timedelta(hours=duration_hours)
                is_last_slot = (end_of_slot_dt.time() >= ecm_hours['close'])
                is_ecm_launch = (service_type == "Launch" and customer.is_ecm_customer)
                is_ecm_haul = (service_type == "Haul" and customer.is_ecm_customer)

                if (is_first_slot and not is_ecm_launch) or (is_last_slot and not is_ecm_haul):
                    p_time = (datetime.datetime.combine(check_date, p_time) + timedelta(minutes=30)).time()
                    continue

                slot = _check_and_create_slot_detail(check_date, p_time, trucks_to_check[0], customer, boat, service_type, ramp_obj, ecm_hours, duration_hours, j17_duration, w, is_active_crane_day=False, is_candidate_crane_day=False)
                if slot:
                    return slot 
                p_time = (datetime.datetime.combine(datetime.date.min, p_time) + timedelta(minutes=30)).time()
        return None


    found_slots = []
    found_dates = set() 
    message = ""
    is_crane_job = boat.boat_type.startswith("Sailboat") and service_type in ["Launch", "Haul"]
    trucks = get_suitable_trucks(boat.boat_length, customer.preferred_truck_id, force_preferred_truck)
    if not trucks:
        return [], "No suitable trucks for this boat length.", [], False

    # --- NEW: Collect all potential search dates within the window first ---
    search_start_date = requested_date_obj - timedelta(days=crane_look_back_days)
    search_end_date = requested_date_obj + timedelta(days=crane_look_forward_days)
    
    potential_search_dates = []
    current_iter_date = search_start_date
    while current_iter_date <= search_end_date:
        potential_search_dates.append(current_iter_date)
        current_iter_date += timedelta(days=1)

    # Separate out active crane days and sort them by how far before the requested date they are
    active_crane_dates_in_window = sorted([
        job.scheduled_start_datetime.date() 
        for job in SCHEDULED_JOBS 
        if getattr(job, 'assigned_crane_truck_id') and 
           (getattr(job, 'pickup_ramp_id') == selected_ramp_id or getattr(job, 'dropoff_ramp_id') == selected_ramp_id) and
           (search_start_date <= job.scheduled_start_datetime.date() <= search_end_date)
    ], key=lambda d: requested_date_obj - d if d < requested_date_obj else timedelta.max) # Sort earlier dates first

    # Create a list of dates to search, prioritizing earlier active crane days
    dates_to_search_prioritized = []

    # 1. Add earlier active crane days (sorted by proximity to requested_date, or simply ascending if you prefer)
    for day in active_crane_dates_in_window:
        if day < requested_date_obj and day not in dates_to_search_prioritized:
            dates_to_search_prioritized.append(day)
    
    # 2. Add the requested date if it's within the window and not already covered by an earlier active day
    if search_start_date <= requested_date_obj <= search_end_date and requested_date_obj not in dates_to_search_prioritized:
        dates_to_search_prioritized.append(requested_date_obj)

    # 3. Add other dates (including future active/candidate and powerboat search)
    # Sort remaining potential dates by proximity to requested_date, ensuring requested_date itself is checked early
    remaining_dates = sorted([d for d in potential_search_dates if d not in dates_to_search_prioritized],
                             key=lambda d: abs(d - requested_date_obj))

    dates_to_search_prioritized.extend(remaining_dates)


    # --- Iterate through the prioritized list to find slots ---
    for day in dates_to_search_prioritized:
        if len(found_slots) >= num_suggestions_to_find and not ((day < requested_date_obj or day > requested_date_obj) and day in active_crane_dates_in_window):
            # If we have enough suggestions AND this isn't a high-priority earlier active crane day, break.
            # This allows earlier active crane days to always be checked even if num_suggestions is met.
            break 
    
        if day not in found_dates:
            slot = _find_first_slot_on_day(day, ramp_obj, trucks, is_crane_job)
            if slot:
                _is_active = day in active_crane_dates_in_window
                _is_candidate = any(cd['date'] == day for cd in CANDIDATE_CRANE_DAYS.get(selected_ramp_id, []))

                slot['is_active_crane_day'] = _is_active
                slot['is_candidate_crane_day'] = _is_candidate

                if day < requested_date_obj and _is_active: # Prioritize earlier active crane days
                    slot['priority_score'] = 1000 
                
                found_slots.append(slot)
                found_dates.add(day)
    
    # Final sort by priority_score (desc) and then by date (asc)
    found_slots.sort(key=lambda s: (s.get('priority_score', 0), s['date']), reverse=True)

    if not found_slots:
        message = "No suitable slots could be found within the search window."
        return [], message, [], False

    # Check if requested date was actually suggested
    requested_date_in_suggestions = any(slot['date'] == requested_date_obj for slot in found_slots)
    
    # Custom message logic for crane jobs
    if is_crane_job and not requested_date_in_suggestions and found_slots and found_slots[0]['date'] < requested_date_obj and found_slots[0]['is_active_crane_day']:
        days_prior = (requested_date_obj - found_slots[0]['date']).days
        message = (f"Your requested date of {requested_date_obj.strftime('%B %d')} was not offered. "
                   f"An earlier, active crane day on {found_slots[0]['date'].strftime('%B %d')} ({days_prior} days prior) "
                   f"was found at the ramp to optimize crane scheduling.")
    elif found_slots:
        message = f"Found {len(found_slots)} available Crane Day(s) with ideal tides." # Revert to generic for other cases
    else:
        message = "No suitable slots could be found within the search window." # Fallback

    return found_slots, message, [], False


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
