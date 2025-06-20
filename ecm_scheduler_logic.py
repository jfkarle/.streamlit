# ecm_scheduler_logic.py
# FINAL VERIFIED VERSION

import csv
import datetime
import requests

# --- Utility Functions ---

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

def round_time_to_nearest_15_minutes(t):
    """Rounds a datetime.time object to the nearest 15 minutes."""
    # Combine with a dummy date to create a datetime object for calculations
    dt = datetime.datetime.combine(datetime.date.min, t)
    
    # Calculate the number of seconds from the beginning of the hour
    seconds_past_hour = dt.minute * 60 + dt.second
    
    # Calculate the remainder when divided by 900 seconds (15 minutes)
    remainder = seconds_past_hour % 900
    
    # If the remainder is greater than 450 seconds (7.5 minutes), round up
    if remainder > 450:
        dt_rounded = dt + datetime.timedelta(seconds=900 - remainder)
    # Otherwise, round down
    else:
        dt_rounded = dt - datetime.timedelta(seconds=remainder)
        
    return dt_rounded.time()

def get_concise_tide_rule(ramp, boat):
    if ramp.tide_calculation_method == "AnyTide": return "Any Tide"
    if ramp.tide_offset_hours1: return f"{float(ramp.tide_offset_hours1):g}hrs +/- HT"
    return "Tide Rule N/A"

def is_j17_at_ramp(check_date, ramp_id):
    if not ramp_id: return False
    return ramp_id in crane_daily_status.get(check_date.strftime('%Y-%m-%d'), {}).get('ramps_visited', set())

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
    def __init__(self, c_id, name, truck_id=None, is_ecm=False): self.customer_id=c_id; self.customer_name=name; self.preferred_truck_id=truck_id; self.is_ecm_customer=is_ecm
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
    "SandwichBasin": Ramp("SandwichBasin", "Sandwich Basin", "8447180", "AnyTide", ["Powerboat"]),
    "PlymouthHarbor": Ramp("PlymouthHarbor", "Plymouth Harbor", "8446493", "HoursAroundHighTide", 3.0),
    "FerryStreet": Ramp("FerryStreet", "Ferry Street (Scituate)", "8443970", "HoursAroundHighTide", 3.0, ["Powerboat"]),
    "SouthRiverYachtYard": Ramp("SouthRiverYachtYard", "SRYY (Scituate)", "8443970", "HoursAroundHighTide", 2.0, ["Powerboat"]),    
    "CordagePark": Ramp("CordagePark", "Cordage Park (Plymouth)", "8446493", "HoursAroundHighTide", 1.5, ["Powerboat"]),
    "DuxburyHarbor": Ramp("DuxburyHarbor", "Duxbury Harbor (Town Pier)", "8446166", "HoursAroundHighTide", 1.0, ["Powerboat"]),
    "GreenHarborTaylors": Ramp("GreenHarborTaylors", "Green Harbor (Taylors)", "8446166", "HoursAroundHighTide", 3.0, ["Powerboat"]),
    "JonesRiver": Ramp("JonesRiver", "Ferry Street (Kingston)", "8443970", "HoursAroundHighTide", 1.5, ["Powerboat"]),
    "RohtMarine": Ramp("RohtMarine", "Roht Marine Marine (Scituate)", "8443970", "HoursAroundHighTide", 1.5, ["Powerboat"]),
    "GreenHarborSafeHarbor": Ramp("GreenHarborSafeHarbor", "Safe Harbor (Green Harbor)", "8446166", "HoursAroundHighTide", 1.0, ["Powerboat"]),
    "ScituateHarborJericho": Ramp("ScituateHarborJericho", "Scituate Harbor (Jericho Road)", "8443970", "AnyTideWithDraftRule"),
    "CohassetParkerAve": Ramp("CohassetHarbor", "Cohasset Harbor (Parker Ave)", "8443970", "HoursAroundHighTide", 3.0),
    "HullASt": Ramp("HullASt", "Hull (A St, Sunset, Steamboat)", "8444199", "HoursAroundHighTide_WithDraftRule", 3.0),
    "HinghamHarbor": Ramp("HinghamHarbor", "Hingham Harbor", "8444775", "HoursAroundHighTide", 3.0),
    "WeymouthWessagusset": Ramp("WeymouthWessagusset", "Weymouth Harbor (Wessagusset)", "8444788", "HoursAroundHighTide", 3.0),
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
                cust_id = f"C{1001+i}"; boat_id = f"B{5001+i}"
                LOADED_CUSTOMERS[cust_id] = Customer(cust_id, row['customer_name'], row.get('preferred_truck'), row.get('is_ecm_boat','').lower()=='true')
                LOADED_BOATS[boat_id] = Boat(boat_id, cust_id, row['boat_type'], float(row['boat_length']), float(row.get('boat_draft') or 0))
        return True
    except FileNotFoundError: return False

# --- Core Logic Functions ---
get_customer_details = LOADED_CUSTOMERS.get; get_boat_details = LOADED_BOATS.get; get_ramp_details = ECM_RAMPS.get

def get_high_tide_times_for_ramp(ramp_id, date):
    ramp = get_ramp_details(ramp_id)
    if not ramp:
        return []
    tide_data = fetch_noaa_tides(ramp.noaa_station_id, date)
    return [entry["time"] for entry in tide_data if entry["type"] == "H"]

def round_time_to_nearest_15_minutes(time_obj):
    """
    Rounds a datetime.time object to the nearest 15-minute interval.
    Example: 10:07:00 becomes 10:00:00, 10:23:00 becomes 10:15:00.
    """
    minutes = time_obj.minute
    rounded_minutes = (minutes // 15) * 15
    return time_obj.replace(minute=rounded_minutes, second=0, microsecond=0)

def get_ecm_operating_hours(date):
    season = "Busy" if date.month in [4,5,6,9,10] else "Standard"
    if season == "Busy" and date.weekday() == 5 and date.month in [5,9]: season = "BusySaturday"
    for rule in operating_hours_rules:
        if rule.season == season and rule.day_of_week == date.weekday(): return {"open": rule.open_time, "close": rule.close_time}
    return None

def calculate_ramp_windows(ramp, boat, tide_data, date):
    if ramp.tide_calculation_method in ["AnyTide", "AnyTideWithDraftRule"]: return [{'start_time': datetime.time.min, 'end_time': datetime.time.max}]
    if not tide_data: return []
    offset = datetime.timedelta(hours=float(ramp.tide_offset_hours1 or 0))
    return [{'start_time': (datetime.datetime.combine(date,t['time'])-offset).time(), 'end_time': (datetime.datetime.combine(date,t['time'])+offset).time()} for t in tide_data if t['type']=='H']

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
        if job.job_status == "Scheduled" and (getattr(job, 'assigned_hauling_truck_id', None) == truck_id or (getattr(job, 'assigned_crane_truck_id', None) == truck_id and truck_id == "J17")):
            job_end = getattr(job, 'j17_busy_end_datetime', job.scheduled_end_datetime) if truck_id == "J17" else job.scheduled_end_datetime
            if start_dt < job_end and end_dt > job.scheduled_start_datetime: return False
    return True

def _check_and_create_slot_detail(s_date, p_time, truck, cust, boat, service, ramp, ecm_hours, duration, j17_duration, window_details, debug_messages, reason_for_suggestion=None):
    start_dt = datetime.datetime.combine(s_date, p_time); hauler_end_dt = start_dt + datetime.timedelta(hours=duration)
    if hauler_end_dt.time() > ecm_hours['close'] and not (hauler_end_dt.time() == ecm_hours['close'] and hauler_end_dt.date() == s_date): return None
    if not check_truck_availability(truck.truck_id, start_dt, hauler_end_dt): return None
    needs_j17 = BOOKING_RULES.get(boat.boat_type, {}).get('crane_mins', 0) > 0
    if needs_j17 and not check_truck_availability("J17", start_dt, start_dt + datetime.timedelta(hours=j17_duration)): return None
    
    return {'date': s_date, 'time': p_time, 'truck_id': truck.truck_id, 'j17_needed': needs_j17, 
            'type': "Open", 'ramp_id': ramp.ramp_id if ramp else None, 
            'priority_score': 1 if needs_j17 and ramp and is_j17_at_ramp(s_date, ramp.ramp_id) else 0,
            'reason_for_suggestion': reason_for_suggestion, # This is now valid
            **window_details}

def find_available_job_slots(customer_id, boat_id, service_type, requested_date_str,
                             selected_ramp_id=None, transport_dropoff_details=None,
                             start_after_slot_details=None):
    
    global original_job_request_details, DEBUG_LOG_MESSAGES
    DEBUG_LOG_MESSAGES = [f"FindSlots Start: Cust({customer_id}) Boat({boat_id}) Svc({service_type}) ReqDate({requested_date_str}) Ramp({selected_ramp_id})"]
    original_job_request_details = {'transport_dropoff_details': transport_dropoff_details, 'customer_id': customer_id, 'boat_id': boat_id, 'service_type': service_type}
    
    try: 
        requested_date_obj = datetime.datetime.strptime(requested_date_str, '%Y-%m-%d').date()
    except ValueError: 
        return [], "Error: Invalid date format.", DEBUG_LOG_MESSAGES
    
    customer = get_customer_details(customer_id)
    boat = get_boat_details(boat_id)
    if not customer or not boat: 
        return [], "Error: Invalid Customer/Boat ID.", DEBUG_LOG_MESSAGES

    # --- This helper encapsulates a search for a single day ---
    def search_single_day(search_date):
        slots_found = []
        ecm_op_hours = get_ecm_operating_hours(search_date)
        if not ecm_op_hours or (boat.boat_type in ["Sailboat MD", "Sailboat MT"] and search_date.weekday() == 5 and search_date.month not in [5, 9]):
            return []
        
        job_duration_hours = 3.0 if boat.boat_type in ["Sailboat MD", "Sailboat MT"] else 1.5
        needs_j17 = boat.boat_type in ["Sailboat MD", "Sailboat MT"]
        j17_actual_busy_duration_hours = 1.0 if boat.boat_type == "Sailboat MD" else (1.5 if boat.boat_type == "Sailboat MT" else 0)
        suitable_truck_ids = get_suitable_trucks(boat.boat_length, customer.preferred_truck_id)
        
        ramp_obj = None; daily_windows = []
        if service_type in ["Launch", "Haul"]:
            ramp_obj = ECM_RAMPS.get(selected_ramp_id)
            if not ramp_obj: return []
            daily_windows = get_final_schedulable_ramp_times(ramp_obj, boat, search_date)
        elif service_type == "Transport":
            daily_windows = [{'start_time': ecm_op_hours['open'], 'end_time': ecm_op_hours['close']}]
        
        if not daily_windows: return []

        for truck_id in suitable_truck_ids:
            for window in daily_windows:
                potential_time = window['start_time']
                while potential_time < window['end_time']:
                    temp_dt = datetime.datetime.combine(search_date, potential_time)
                    if temp_dt.minute not in [0, 30]:
                        if temp_dt.minute < 30: temp_dt = temp_dt.replace(minute=30, second=0, microsecond=0)
                        else: temp_dt = (temp_dt + datetime.timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
                        potential_time = temp_dt.time()
                    if potential_time >= window['end_time']: break
                    
                    slot_detail = _check_and_create_slot_detail(search_date, potential_time, truck_id, customer, boat, service_type, ramp_obj, ecm_op_hours, job_duration_hours, needs_j17, j17_actual_busy_duration_hours, window, DEBUG_LOG_MESSAGES)
                    if slot_detail:
                        slots_found.append(slot_detail)
                    potential_time = (datetime.datetime.combine(datetime.date.min, potential_time) + datetime.timedelta(minutes=30)).time()
        return slots_found

    # --- Step 1: Strict Search on Requested Date ---
    if not start_after_slot_details: # Only do strict search for initial request
        DEBUG_LOG_MESSAGES.append(f"Phase 1: Strict search on requested date: {requested_date_obj}")
        slots_on_requested_date = search_single_day(requested_date_obj)
        if slots_on_requested_date:
            DEBUG_LOG_MESSAGES.append(f"Found {len(slots_on_requested_date)} slots on requested date.")
            slots_on_requested_date.sort(key=lambda x: x['time'])
            return slots_on_requested_date[:3], "Found available slots on your requested date.", DEBUG_LOG_MESSAGES
    
    # --- Step 2: Expanded Search if Strict Search Fails or for Roll Forward ---
    if start_after_slot_details:
        DEBUG_LOG_MESSAGES.append(f"Rolling forward...")
        effective_search_start_date = start_after_slot_details['date']
        min_start_time = (datetime.datetime.combine(effective_search_start_date, start_after_slot_details['time']) + datetime.timedelta(minutes=1)).time()
    else:
        DEBUG_LOG_MESSAGES.append(f"No slots on {requested_date_obj}. Expanding search to -7/+21 days.")
        effective_search_start_date = requested_date_obj - datetime.timedelta(days=7)
        if effective_search_start_date < TODAY_FOR_SIMULATION: effective_search_start_date = TODAY_FOR_SIMULATION
        min_start_time = None
    
    search_end_limit_date = requested_date_obj + datetime.timedelta(days=21)
    
    all_found_slots = []
    current_search_date = effective_search_start_date
    while current_search_date <= search_end_limit_date:
        slots_today = search_single_day(current_search_date)
        if min_start_time and current_search_date == effective_search_start_date:
            all_found_slots.extend([s for s in slots_today if s['time'] >= min_start_time])
            min_start_time = None # Only apply this filter to the very first day of roll-forward
        else:
            all_found_slots.extend(slots_today)
        
        if len(all_found_slots) >= 15: # Collect a decent pool then stop
            break
        current_search_date += datetime.timedelta(days=1)
        
    if not all_found_slots:
        return [], "No suitable slots found within the expanded search window.", DEBUG_LOG_MESSAGES

    # --- Step 3: Prioritize by Closest Date for Expanded Search Results ---
    def sort_key(slot):
        day_diff = abs((slot['date'] - requested_date_obj).days)
        is_preferred = 0 if customer.preferred_truck_id and slot['truck_id'] == customer.preferred_truck_id else 1
        return (day_diff, slot['time'], is_preferred)

    all_found_slots.sort(key=sort_key)
    
    final_slots = []
    used_date_times = set()
    for slot in all_found_slots:
        if len(final_slots) >= 3: break
        slot_dt = datetime.datetime.combine(slot['date'], slot['time'])
        if slot_dt not in used_date_times:
            final_slots.append(slot)
            used_date_times.add(slot_dt)
    
    message = f"Found best available slots, but your requested date of {requested_date_str} was not available."
    return final_slots, message, DEBUG_LOG_MESSAGES

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
        pickup_addr = f"Cust: {customer.customer_name} Home"
        dropoff_addr = ramp.ramp_name
        dropoff_rid = ramp.ramp_id
    elif service_type == "Haul":
        pickup_addr = ramp.ramp_name
        pickup_rid = ramp.ramp_id
        dropoff_addr = f"Cust: {customer.customer_name}"
    
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
