# ecm_scheduler_logic.py
# FINAL VERIFIED VERSION

import csv
import datetime
import requests
from datetime import timedelta, time


# --- Utility Functions ---


CANDIDATE_CRANE_DAYS = {
    'Scituate': [],
    'Plymouth': [],
    'Weymouth': [],
    'Cohasset': []
}

ACTIVE_CRANE_DAYS = {
    'Scituate': [],
    'Plymouth': [],
    'Weymouth': [],
    'Cohasset': []
}

def fetch_noaa_tides(station_id, date_to_check):
    date_str = date_to_check.strftime("%Y%m%d")
    base = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
    params = {"product":"predictions", "application":"ecm-boat-scheduler", "begin_date":date_str, "end_date":date_str, "datum":"MLLW", "station":station_id, "time_zone":"lst_ldt", "units":"english", "interval":"hilo", "format":"json"}
    try:
        resp = requests.get(base, params=params, timeout=10)
        resp.raise_for_status()
        return [{'type': i["type"].upper(), 'time': datetime.datetime.strptime(i["t"], "%Y-%m-%d %H:%M").time()} for i in resp.json().get("predictions", [])]
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

def get_high_tide_times_for_ramp_and_date(ramp_obj, date_obj):
    """Fetch high tide times for a given ramp and date directly from NOAA API (live, on-the-fly)."""

    if not ramp_obj or not ramp_obj.noaa_station_id:
        print(f"[ERROR] Ramp '{ramp_obj.ramp_name if ramp_obj else 'Unknown'}' missing NOAA station ID.")
        return []

    tide_data = fetch_noaa_tides(ramp_obj.noaa_station_id, date_obj)

    high_tide_times = []
    for tide_entry in tide_data:
        if tide_entry.get('type') == 'H':
            high_tide_times.append(tide_entry.get('time'))

    if not high_tide_times:
        print(f"[WARNING] No high tide times found from NOAA for {ramp_obj.ramp_name} on {date_obj}.")

    return high_tide_times

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
    "CohassetParkerAve": Ramp("CohassetHarbor", "Cohasset Harbor (Parker Ave)", "8444762", "HoursAroundHighTide", 3.0), # CORRECTED
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

def _check_and_create_slot_detail(s_date, p_time, truck, cust, boat, service, ramp, ecm_hours, duration, j17_duration, window_details):
    start_dt = datetime.datetime.combine(s_date, p_time); hauler_end_dt = start_dt + datetime.timedelta(hours=duration)
    if hauler_end_dt.time() > ecm_hours['close'] and not (hauler_end_dt.time() == ecm_hours['close'] and hauler_end_dt.date() == s_date): return None
    if not check_truck_availability(truck.truck_id, start_dt, hauler_end_dt): return None
    needs_j17 = BOOKING_RULES.get(boat.boat_type, {}).get('crane_mins', 0) > 0
    if needs_j17 and not check_truck_availability("J17", start_dt, start_dt + datetime.timedelta(hours=j17_duration)): return None
    return {'date': s_date, 'time': p_time, 'truck_id': truck.truck_id, 'j17_needed': needs_j17, 'type': "Open", 'ramp_id': ramp.ramp_id if ramp else None, 'priority_score': 1 if needs_j17 and ramp and is_j17_at_ramp(s_date, ramp.ramp_id) else 0, **window_details}

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
    

def find_available_job_slots(customer_id, boat_id, service_type, requested_date_str, selected_ramp_id=None, force_preferred_truck=True, relax_ramp=False, ignore_forced_search=False, **kwargs):
    try:
        requested_date_obj = datetime.datetime.strptime(requested_date_str, '%Y-%m-%d').date()
    except ValueError:
        return [], "Error: Invalid date format.", [], False

    customer, boat = get_customer_details(customer_id), get_boat_details(boat_id)

    for job in SCHEDULED_JOBS:
        if job.customer_id == customer_id and job.job_status == "Scheduled":
            return [], f"Error: Customer '{get_customer_details(customer_id).customer_name}' is already scheduled.", [], False
    if not customer or not boat:
        return [], "Error: Invalid Cust/Boat ID.", [], False

    ramps_to_search = []
    if relax_ramp:
        for ramp in ECM_RAMPS.values():
            if boat.boat_type in ramp.allowed_boat_types:
                ramps_to_search.append(ramp)
    else:
        ramp_obj_single = get_ramp_details(selected_ramp_id)
        if service_type in ["Launch", "Haul"]:
            if not ramp_obj_single:
                return [], "Error: A ramp must be selected for this service type.", [], False
            if boat.boat_type not in ramp_obj_single.allowed_boat_types:
                return [], f"Ramp '{ramp_obj_single.ramp_name}' doesn't allow {boat.boat_type}s.", [], False
            ramps_to_search.append(ramp_obj_single)

    forced_date = None
    ramp_obj = get_ramp_details(selected_ramp_id)
    if boat.boat_type.startswith("Sailboat") and ramp_obj and not ignore_forced_search:
        for job in SCHEDULED_JOBS:
            if getattr(job, 'assigned_crane_truck_id', None) and (getattr(job, 'pickup_ramp_id', None) == selected_ramp_id or getattr(job, 'dropoff_ramp_id', None) == selected_ramp_id) and abs((requested_date_obj - job.scheduled_start_datetime.date()).days) <= 7:
                forced_date = job.scheduled_start_datetime.date()
                break

    # ✅ At this point, normal slot search continues
    # (Insert your normal slot search loops here...)

    # ✅ Initialize safe return variables
    potential_slots = []
    was_forced = False
    expl = "No suitable slots found."

    # Example: After your main slot-finding logic
    if forced_date:
        was_forced = True
        # (Forced-date specific slot search logic...)
        # (Populate potential_slots)

    # If no forced date, continue with normal logic:
    # (Continue populating potential_slots...)

    # ✅ Deduplicate and sort
    seen = set()
    unique_slots = []
    for slot in potential_slots:
        key = (slot['date'], slot['time'], slot['truck_id'])
        if key not in seen:
            seen.add(key)
            unique_slots.append(slot)
    potential_slots = unique_slots
    potential_slots.sort(key=lambda s: (-s.get('priority_score', 0), s['date'], s['time']))

    top_slots = potential_slots[:6] if potential_slots else []

    # ✅ Finalize explanation text
    if top_slots:
        expl = f"Found {len(top_slots)} slot(s), starting {top_slots[0]['date'].strftime('%A, %b %d')}."

    # ✅ Return clean final tuple
    return top_slots, expl, [], was_forced

def build_crane_day_slot_list(ramp_obj, boat, customer, requested_date_obj, service_type, trucks, duration, j17_duration):
    candidate_ramp_name = ramp_obj.town if ramp_obj else None
    slot_list = []
    relaxed = False
    warning_msgs = []

    # ✅ Step 1: Search existing Active Crane Days for this ramp
    search_dates = ACTIVE_CRANE_DAYS.get(candidate_ramp_name, []).copy()

    # ✅ Step 2: Activate and search additional future candidate days at this ramp until >=3 slots found
    candidate_days_remaining = [d for d in CANDIDATE_CRANE_DAYS.get(candidate_ramp_name, []) if d not in search_dates and d >= requested_date_obj]

    while len(slot_list) < 3 and candidate_days_remaining:
        next_crane_day = candidate_days_remaining.pop(0)
        ACTIVE_CRANE_DAYS[candidate_ramp_name].append(next_crane_day)
        search_dates.append(next_crane_day)
        warning_msgs.append(f"⚠️ Activated new Crane Day: {next_crane_day.strftime('%b %d')} at {candidate_ramp_name} to meet slot minimum.")

        # Search for slots on this new day
        day_slots = search_single_day_for_crane(
            next_crane_day, ramp_obj, boat, customer,
            service_type, trucks, duration, j17_duration
        )
        slot_list.extend(day_slots)

    # ✅ Step 3: Keep adding more candidate days until 3 slots exist or all dates exhausted
    # Already handled above inside the while loop

    # ✅ Step 4: If still under 3 slots → Relax ramp and truck rules and search other ramps
    if len(slot_list) < 3:
        relaxed = True
        warning_msgs.append("⚠️ Relaxed ramp and truck rules to meet 3-slot minimum. Searching other ramps and all trucks...")

        for other_ramp_name in CANDIDATE_CRANE_DAYS.keys():
            if other_ramp_name == candidate_ramp_name:
                continue  # Already checked this ramp

            other_ramp_obj = None
            for ramp in ECM_RAMPS.values():
                if ramp.town == other_ramp_name:
                    other_ramp_obj = ramp
                    break
            if not other_ramp_obj:
                continue

            for date in CANDIDATE_CRANE_DAYS[other_ramp_name]:
                day_slots = search_single_day_for_crane(
                    date, other_ramp_obj, boat, customer,
                    service_type, ECM_TRUCKS.values(), duration, j17_duration
                )
                # ✅ Mark all relaxed slots for UI highlight
                for slot in day_slots:
                    slot['relaxed'] = True
                slot_list.extend(day_slots)

                if len(slot_list) >= 3:
                    break
            if len(slot_list) >= 3:
                break

    # ✅ Always cap at 6 slots for display, even though minimum is 3
    return slot_list[:6], warning_msgs, relaxed
    
    rules = BOOKING_RULES.get(boat.boat_type, {})
    duration = rules.get('truck_mins', 90) / 60.0
    j17_duration = rules.get('crane_mins', 0) / 60.0
    is_ecm_haul_priority = (service_type == "Haul" and customer.is_ecm_customer)
    trucks = get_suitable_trucks(boat.boat_length, customer.preferred_truck_id, force_preferred_truck)
    requested_slot = None
    ecm_hours_requested = get_ecm_operating_hours(requested_date_obj)
    if ecm_hours_requested:
        tide_windows_requested = get_final_schedulable_ramp_times(ramp_obj, boat, requested_date_obj)
        for truck in trucks:
            for window in tide_windows_requested:
                p_start_time, p_end_time = window['start_time'], window['end_time']
                p_time = p_start_time
                while p_time < p_end_time:
                    temp_dt = datetime.datetime.combine(requested_date_obj, p_time)
                    if temp_dt.minute % 30 != 0:
                        p_time = (temp_dt + timedelta(minutes=30 - (temp_dt.minute % 30))).time()
                    if p_time >= p_end_time:
                        break
                    slot = _check_and_create_slot_detail(
                        requested_date_obj, p_time, truck, customer, boat,
                        service_type, ramp_obj, ecm_hours_requested,
                        duration, j17_duration, window
                    )
                    if slot:
                        requested_slot = slot
                        break
                    p_time = (datetime.datetime.combine(datetime.date.min, p_time) + timedelta(minutes=30)).time()
            if requested_slot:
                break

        crane_grouping_slot = None
        if boat.boat_type.lower().startswith("sailboat") and ramp_obj:
            crane_grouping_slot = get_j17_crane_grouping_slot(
                boat, customer, ramp_obj, requested_date_obj,
                trucks, duration, j17_duration, service_type
            )

        # ✅ Your new explanatory message block (correctly outside both loops)
        if not requested_slot and not crane_grouping_slot:
            tide_times = get_high_tide_times_for_ramp_and_date(ramp_obj, requested_date_obj)
            explanation_parts = []
        
            if not tide_times:
                explanation_parts.append(f"No high tide on {requested_date_str} at {ramp_obj.ramp_name}.")
            else:
                ecm_open = ecm_hours_requested.get('open')
                ecm_close = ecm_hours_requested.get('close')
                too_early = all(t < datetime.time(8, 0) for t in tide_times)
                too_late = all(t > datetime.time(14, 30) for t in tide_times)
        
                if too_early:
                    explanation_parts.append(f"Tide times are before allowed truck start time (8:00 AM).")
                if too_late:
                    explanation_parts.append(f"Tide times are after allowed truck start cutoff (2:30 PM).")
        
            if not explanation_parts:
                explanation_parts.append(f"No trucks or time slots available on {requested_date_str}.")
        
            requested_date_reason_message = f"Requested date '{requested_date_str}' cannot be scheduled: {' '.join(explanation_parts)}" 
   
    if not trucks:
        return [], "No suitable trucks for this boat.", [], False

    potential_slots, was_forced = [], False
    ramps_to_iterate = ramps_to_search if service_type in ["Launch", "Haul"] else [None]

    if forced_date:
        was_forced = True
        for ramp_to_check in ramps_to_iterate:
            ecm_hours = get_ecm_operating_hours(forced_date)
            if not ecm_hours: continue
            windows = get_final_schedulable_ramp_times(ramp_to_check, boat, forced_date) if ramp_to_check else [{'start_time': ecm_hours['open'], 'end_time': ecm_hours['close']}]
            for truck in trucks:
                for w in windows:
                    p_time, p_end = w['start_time'], w['end_time']
                    while p_time < p_end:
                        if len(potential_slots) >= 6: break
                        temp_dt = datetime.datetime.combine(forced_date, p_time)
                        if temp_dt.minute % 30 != 0:
                            p_time = (temp_dt + datetime.timedelta(minutes=30 - (temp_dt.minute % 30))).time()
                        if p_time >= p_end: break
                        slot = _check_and_create_slot_detail(forced_date, p_time, truck, customer, boat, service_type, ramp_to_check, ecm_hours, duration, j17_duration, w)
                        if slot:
                            potential_slots.append(slot)
                            break
                        p_time = (datetime.datetime.combine(datetime.date.min, p_time) + datetime.timedelta(minutes=30)).time()

        if potential_slots:
            seen = set()
            unique_slots = []
            for slot in potential_slots:
                key = (slot['date'], slot['time'], slot['truck_id'])
                if key not in seen:
                    seen.add(key)
                    unique_slots.append(slot)
            potential_slots = unique_slots

            potential_slots.sort(key=lambda s: (-s.get('priority_score', 0), s['date'], s['time']))
            top_slots = potential_slots[:6]
            expl = f"Found slots on {forced_date.strftime('%A, %b %d')} to group with an existing crane job."
            return top_slots, expl, [], was_forced

    # Continue normal search...
    # Rest of your 'else' search logic stays as-is below.
    ## End "expl = f"Found slots on {forced_date.strftime('%A, %b %d')} to group with an existing crane job."" Correction 9:19 AM
        
        else:
            # Deduplicate forced date slots and return
            seen = set()
            unique_slots = []
            for slot in potential_slots:
                key = (slot['date'], slot['time'], slot['truck_id'])
                if key not in seen:
                    seen.add(key)
                    unique_slots.append(slot)
            potential_slots = unique_slots
        
            # Sort and limit
            potential_slots.sort(key=lambda s: (-s.get('priority_score', 0), s['date'], s['time']))
            top_slots = potential_slots[:6]
        
            expl = f"Found slots on {forced_date.strftime('%A, %b %d')} to group with an existing crane job."
            return top_slots, expl, [], was_forced
    else:
        slots_before, slots_after = [], []
        # Phase 1: Before
        d = max(TODAY_FOR_SIMULATION, requested_date_obj - datetime.timedelta(days=5))
        while d < requested_date_obj and len(slots_before) < 2:
            for ramp_to_check in ramps_to_iterate:
                ecm_hours = get_ecm_operating_hours(d)
                if not ecm_hours: continue
                windows = get_final_schedulable_ramp_times(ramp_to_check, boat, d) if ramp_to_check else [{'start_time': ecm_hours['open'], 'end_time': ecm_hours['close']}]
                for truck in trucks:
                    if len(slots_before) >= 2: break
                    for w in windows:
                        p_start_time, p_end_time = w['start_time'], w['end_time']
                        if is_ecm_haul_priority:
                            end_dt = datetime.datetime.combine(d, p_end_time)
                            p_time_dt = end_dt - datetime.timedelta(hours=duration)
                            if p_time_dt.minute % 30 != 0: p_time_dt -= datetime.timedelta(minutes=p_time_dt.minute % 30)
                            while p_time_dt.time() >= p_start_time:
                                if len(slots_before) >= 2: break
                                slot = _check_and_create_slot_detail(d, p_time_dt.time(), truck, customer, boat, service_type, ramp_to_check, ecm_hours, duration, j17_duration, w)
                                if slot:
                                    slots_before.append(slot)
                                    break
                                p_time_dt -= datetime.timedelta(minutes=30)
                        else:
                            p_time = p_start_time
                            while p_time < p_end_time:
                                if len(slots_before) >= 2: break
                                temp_dt = datetime.datetime.combine(d, p_time)
                                if temp_dt.minute % 30 != 0: p_time = (temp_dt + datetime.timedelta(minutes=30 - (temp_dt.minute % 30))).time()
                                if p_time >= p_end_time: break
                                slot = _check_and_create_slot_detail(d, p_time, truck, customer, boat, service_type, ramp_to_check, ecm_hours, duration, j17_duration, w)
                                if slot:
                                    slots_before.append(slot)
                                    break
                                p_time = (datetime.datetime.combine(datetime.date.min, p_time) + datetime.timedelta(minutes=30)).time()
                        if len(slots_before) >= 2: break
            d += datetime.timedelta(days=1)
            
        # Phase 2: After
        d, i = requested_date_obj, 0
        while len(slots_after) < 4 and i < 45:
            for ramp_to_check in ramps_to_iterate:
                ecm_hours = get_ecm_operating_hours(d)
                if not ecm_hours: continue
                windows = get_final_schedulable_ramp_times(ramp_to_check, boat, d) if ramp_to_check else [{'start_time': ecm_hours['open'], 'end_time': ecm_hours['close']}]
                for truck in trucks:
                    if len(slots_after) >= 4: break
                    for w in windows:
                        p_start_time, p_end_time = w['start_time'], w['end_time']
                        if is_ecm_haul_priority:
                            end_dt = datetime.datetime.combine(d, p_end_time)
                            p_time_dt = end_dt - datetime.timedelta(hours=duration)
                            if p_time_dt.minute % 30 != 0: p_time_dt -= datetime.timedelta(minutes=p_time_dt.minute % 30)
                            while p_time_dt.time() >= p_start_time:
                                if len(slots_after) >= 4: break
                                slot = _check_and_create_slot_detail(d, p_time_dt.time(), truck, customer, boat, service_type, ramp_to_check, ecm_hours, duration, j17_duration, w)
                                if slot:
                                    slots_after.append(slot)
                                    break
                                p_time_dt -= datetime.timedelta(minutes=30)
                        else:
                            p_time = p_start_time
                            while p_time < p_end_time:
                                if len(slots_after) >= 4: break
                                temp_dt = datetime.datetime.combine(d, p_time)
                                if temp_dt.minute % 30 != 0:
                                    p_time = (temp_dt + datetime.timedelta(minutes=30 - (temp_dt.minute % 30))).time()
                                if p_time >= p_end_time: break
                        
                                # ⬇️ INSERT DEBUG PRINT HERE
                                print(f"[DEBUG] Evaluating slot:")
                                print(f"    Date: {d}, Time: {p_time}")
                                print(f"    Truck: {truck.truck_id}")
                                print(f"    Duration: {duration} hr")
                                print(f"    Window: {p_start_time} to {p_end_time}")
                                print(f"    Ramp: {ramp_to_check.ramp_name if ramp_to_check else 'N/A'}")
                        
                                # ⬇️ Original line follows immediately
                                slot = _check_and_create_slot_detail(d, p_time, truck, customer, boat, service_type, ramp_to_check, ecm_hours, duration, j17_duration, w)
                                if slot:
                                    slots_after.append(slot)
                                    break
                                p_time = (datetime.datetime.combine(datetime.date.min, p_time) + datetime.timedelta(minutes=30)).time()
                        if len(slots_after) >= 4: break
            d += datetime.timedelta(days=1)
            i += 1
        potential_slots = []
        if crane_grouping_slot:
            potential_slots.append(crane_grouping_slot)
        if requested_slot:
            potential_slots.append(requested_slot)
        potential_slots += slots_before + slots_after
        
        if not potential_slots:
            return [], "No suitable slots found.", [], was_forced
        
        # Deduplicate by (date, time, truck_id)
        seen = set()
        unique_slots = []
        for slot in potential_slots:
            key = (slot['date'], slot['time'], slot['truck_id'])
            if key not in seen:
                seen.add(key)
                unique_slots.append(slot)
        potential_slots = unique_slots
        
        # Sort and limit
        potential_slots.sort(key=lambda s: (-s.get('priority_score', 0), s['date'], s['time']))
        top_slots = potential_slots[:6]
        
        # Explanation string (avoid NameError)
        if was_forced and forced_date:
            expl = f"Found slots on {forced_date.strftime('%A, %b %d')} to group with an existing crane job."
        elif top_slots:
            expl = f"Found {len(top_slots)} available slots starting from {top_slots[0]['date'].strftime('%A, %b %d')}."
        else:
            expl = "No suitable slots found."
        
        return top_slots, expl, [], was_forced



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
    
    # --- THIS IS THE CORRECTED PART ---
    # 4. Create the new Job object by passing arguments explicitly
    new_job = Job(
        job_id=job_id,
        customer_id=customer.customer_id,
        boat_id=boat.boat_id,
        service_type=service_type,
        requested_date=datetime.datetime.strptime(original_request['requested_date_str'], '%Y-%m-%d').date(),
        scheduled_start_datetime=start_dt,
        calculated_job_duration_hours=hauler_duration_hours,
        scheduled_end_datetime=hauler_end_dt,
        assigned_hauling_truck_id=selected_slot['truck_id'],
        assigned_crane_truck_id="J17" if selected_slot.get('j17_needed') else None,
        j17_busy_end_datetime=j17_end_dt,
        pickup_ramp_id=pickup_rid,
        pickup_street_address=pickup_addr,
        dropoff_ramp_id=dropoff_rid,
        dropoff_street_address=dropoff_addr,
        job_status="Scheduled",
        notes=f"Booked via type: {selected_slot.get('type', 'N/A')}.",
        # These are placeholders for attributes your Job class expects
        pickup_loc_coords=None,
        dropoff_loc_coords=None
    )

    # 5. Add the job to the schedule
    SCHEDULED_JOBS.append(new_job)

    # 6. Update crane status
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
