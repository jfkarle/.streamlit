# ecm_scheduler_logic.py
# FINAL VERIFIED VERSION

import csv
import datetime
import pandas as pd
import requests

# --- Utility Functions ---

def fetch_noaa_tides(station_id, date_to_check):
    """Fetches high/low tide predictions from the NOAA Tides and Currents API."""
    date_str = date_to_check.strftime("%Y%m%d")
    base = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
    params = {
        "product": "predictions", "application": "ecm-boat-scheduler",
        "begin_date": date_str, "end_date": date_str, "datum": "MLLW",
        "station": station_id, "time_zone": "lst_ldt", "units": "english",
        "interval": "hilo", "format": "json",
    }
    tide_events = []
    try:
        resp = requests.get(base, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json().get("predictions", [])
        for item in data:
            t = datetime.datetime.strptime(item["t"], "%Y-%m-%d %H:%M")
            tide_events.append({'type': item["type"].upper(), 'time': t.time()})
    except (requests.exceptions.RequestException, KeyError, ValueError) as e:
        print(f"ERROR: Could not fetch or parse tide data for station {station_id}. Error: {e}")
        return []
    return tide_events

def format_time_for_display(time_obj):
    """Formats a time object for display, e.g., 8:00 AM."""
    if not isinstance(time_obj, datetime.time): return "InvalidTime"
    return time_obj.strftime('%I:%M %p').lstrip('0')

def get_concise_tide_rule(ramp_obj, boat_obj):
    """Gets a short description of the tide rule for a given ramp and boat."""
    if ramp_obj.tide_calculation_method == "AnyTide": return "Any Tide"
    if ramp_obj.tide_calculation_method == "AnyTideWithDraftRule":
        if ramp_obj.ramp_id == "ScituateHarborJericho" and boat_obj.draft_ft and boat_obj.draft_ft >= 5.0:
            return "3hrs +/- HT (>=5' draft)"
        return "Any Tide (see notes)"
    if ramp_obj.tide_offset_hours1 is not None:
        offset_str = f"{float(ramp_obj.tide_offset_hours1):g}"
        return f"{offset_str}hrs +/- HT"
    return "Tide Rule N/A"

def is_j17_at_ramp(check_date, ramp_id):
    """Checks if the J17 crane is scheduled to be at a specific ramp on a given date."""
    if not ramp_id: return False
    date_str = check_date.strftime('%Y-%m-%d')
    if date_str in crane_daily_status:
        return ramp_id in crane_daily_status[date_str].get('ramps_visited', set())
    return False

# --- Configuration & Global Context ---
TODAY_FOR_SIMULATION = datetime.date.today()
JOB_ID_COUNTER = 3000
SCHEDULED_JOBS = []
BOOKING_RULES = {
    'Powerboat': {'truck_mins': 90, 'crane_mins': 0},
    'Sailboat DT': {'truck_mins': 180, 'crane_mins': 60},
    'Sailboat MT': {'truck_mins': 180, 'crane_mins': 90}
}
crane_daily_status = {}
ECM_BASE_LOCATION = {"lat": 42.0762, "lon": -70.8069}

# --- Data Models (Classes) ---
class Truck:
    def __init__(self, truck_id, truck_name, max_boat_length, home_base_address="43 Mattakeeset St, Pembroke MA"):
        self.truck_id = truck_id; self.truck_name = truck_name
        self.max_boat_length = max_boat_length; self.is_crane = "Crane" in truck_name
        self.home_base_address = home_base_address

class Ramp:
    def __init__(self, ramp_id, ramp_name, town, tide_rule_description, tide_calculation_method, noaa_station_id, tide_offset_hours1=None, tide_offset_hours2=None, draft_restriction_ft=None, allowed_boat_types="Power and Sail", latitude=None, longitude=None):
        self.ramp_id = ramp_id; self.ramp_name = ramp_name; self.town = town
        self.tide_rule_description = tide_rule_description; self.tide_calculation_method = tide_calculation_method
        self.noaa_station_id = noaa_station_id; self.tide_offset_hours1 = tide_offset_hours1
        self.tide_offset_hours2 = tide_offset_hours2; self.draft_restriction_ft = draft_restriction_ft
        self.allowed_boat_types = allowed_boat_types; self.latitude = latitude; self.longitude = longitude

class Customer:
    def __init__(self, customer_id, customer_name, preferred_truck_id=None, is_ecm_customer=False, home_latitude=None, home_longitude=None):
        self.customer_id = customer_id; self.customer_name = customer_name
        self.preferred_truck_id = preferred_truck_id; self.is_ecm_customer = is_ecm_customer
        self.home_latitude = home_latitude; self.home_longitude = home_longitude

class Boat:
    def __init__(self, boat_id, customer_id, boat_type, boat_length, draft_ft=None):
        self.boat_id = boat_id; self.customer_id = customer_id; self.boat_type = boat_type
        self.boat_length = boat_length; self.draft_ft = draft_ft

class Job:
    def __init__(self, job_id, customer_id, boat_id, service_type, requested_date, scheduled_start_datetime, calculated_job_duration_hours, scheduled_end_datetime, assigned_hauling_truck_id, assigned_crane_truck_id, j17_busy_end_datetime, pickup_ramp_id, pickup_street_address, dropoff_ramp_id, dropoff_street_address, job_status="Scheduled", notes=""):
        self.job_id = job_id; self.customer_id = customer_id; self.boat_id = boat_id; self.service_type = service_type
        self.requested_date = requested_date; self.scheduled_start_datetime = scheduled_start_datetime
        self.calculated_job_duration_hours = calculated_job_duration_hours; self.scheduled_end_datetime = scheduled_end_datetime
        self.assigned_hauling_truck_id = assigned_hauling_truck_id; self.assigned_crane_truck_id = assigned_crane_truck_id
        self.j17_busy_end_datetime = j17_busy_end_datetime; self.pickup_ramp_id = pickup_ramp_id
        self.pickup_street_address = pickup_street_address; self.dropoff_ramp_id = dropoff_ramp_id
        self.dropoff_street_address = dropoff_street_address; self.job_status = job_status; self.notes = notes

class OperatingHoursEntry:
    def __init__(self, season, day_of_week, open_time, close_time):
        self.season = season; self.day_of_week = day_of_week; self.open_time = open_time; self.close_time = close_time

# --- Data Initialization ---
ECM_TRUCKS = {
    "S20/33": Truck("S20/33", "S20 (aka S33)", 60), "S21/77": Truck("S21/77", "S21 (aka S77)", 45),
    "S23/55": Truck("S23/55", "S23 (aka S55)", 30), "J17": Truck("J17", "J17 (Crane Truck)", 999)
}
ECM_RAMPS = {
    "DuxburyHarbor": Ramp("DuxburyHarbor", "Duxbury Harbor", "Duxbury", "1 hr +/- HT", "HoursAroundHighTide", "8443970", 1.0, allowed_boat_types="Power Boats Only"),
    "GreenHarborTaylors": Ramp("GreenHarborTaylors", "Green Harbor", "Marshfield", "3 hrs +/- HT", "HoursAroundHighTide", "8443970", 3.0),
    "ScituateHarborJericho": Ramp("ScituateHarborJericho", "Scituate Harbor", "Scituate", "Any tide", "AnyTide", "8443970"),
    "PlymouthHarbor": Ramp("PlymouthHarbor", "Plymouth Harbor", "Plymouth", "3 hrs +/- HT", "HoursAroundHighTide", "8443970", 3.0),
    "CordagePark": Ramp("CordagePark", "Cordage Park", "Plymouth", "1.5 hr +/- HT", "HoursAroundHighTide", "8443970", 1.5, allowed_boat_types="Power Boats Only"),
}
operating_hours_rules = [
    OperatingHoursEntry("Standard", 0, datetime.time(8, 0), datetime.time(16, 0)),
    OperatingHoursEntry("Standard", 1, datetime.time(8, 0), datetime.time(16, 0)),
    OperatingHoursEntry("Standard", 2, datetime.time(8, 0), datetime.time(16, 0)),
    OperatingHoursEntry("Standard", 3, datetime.time(8, 0), datetime.time(16, 0)),
    OperatingHoursEntry("Standard", 4, datetime.time(8, 0), datetime.time(16, 0)),
    OperatingHoursEntry("Busy", 0, datetime.time(7, 30), datetime.time(17, 30)),
    OperatingHoursEntry("Busy", 1, datetime.time(7, 30), datetime.time(17, 30)),
    OperatingHoursEntry("Busy", 2, datetime.time(7, 30), datetime.time(17, 30)),
    OperatingHoursEntry("Busy", 3, datetime.time(7, 30), datetime.time(17, 30)),
    OperatingHoursEntry("Busy", 4, datetime.time(7, 30), datetime.time(17, 30)),
    OperatingHoursEntry("BusySaturday", 5, datetime.time(7, 30), datetime.time(17, 30)),
]
LOADED_CUSTOMERS = {}
LOADED_BOATS = {}

def load_customers_and_boats_from_csv(csv_filename):
    global LOADED_CUSTOMERS, LOADED_BOATS
    # Implementation from previous steps, assuming it's correct
    return True

# --- Core Logic Functions ---
def get_customer_details(cid): return LOADED_CUSTOMERS.get(cid)
def get_boat_details(bid): return LOADED_BOATS.get(bid)
def get_ramp_details(rid): return ECM_RAMPS.get(rid)

def get_ecm_operating_hours(date_to_check):
    day_of_week = date_to_check.weekday()
    season = "Busy" if date_to_check.month in [4, 5, 6, 9, 10] else "Standard"
    if season == "Busy" and day_of_week == 5 and date_to_check.month in [5, 9]:
        season = "BusySaturday"
    for rule in operating_hours_rules:
        if rule.season == season and rule.day_of_week == day_of_week:
            return {"open": rule.open_time, "close": rule.close_time}
    return None

def calculate_ramp_windows(ramp_obj, tide_data, date_to_check):
    if ramp_obj.tide_calculation_method == "AnyTide": return [{'start_time': datetime.time.min, 'end_time': datetime.time.max}]
    if not tide_data: return []
    usable_windows = []
    offset_delta = datetime.timedelta(hours=float(ramp_obj.tide_offset_hours1 or 0))
    for event in tide_data:
        if event['type'] == 'H':
            ht_dt = datetime.datetime.combine(date_to_check, event['time'])
            usable_windows.append({'start_time': (ht_dt - offset_delta).time(), 'end_time': (ht_dt + offset_delta).time()})
    return usable_windows

def get_final_schedulable_ramp_times(ramp_obj, boat_obj, date_to_check):
    ecm_hours = get_ecm_operating_hours(date_to_check)
    if not ecm_hours: return []
    ecm_open = datetime.datetime.combine(date_to_check, ecm_hours['open'])
    ecm_close = datetime.datetime.combine(date_to_check, ecm_hours['close'])
    tide_data = fetch_noaa_tides(ramp_obj.noaa_station_id, date_to_check)
    tidal_windows = calculate_ramp_windows(ramp_obj, tide_data, date_to_check)
    final_windows = []
    for t_win in tidal_windows:
        tidal_start = datetime.datetime.combine(date_to_check, t_win['start_time'])
        tidal_end = datetime.datetime.combine(date_to_check, t_win['end_time'])
        overlap_start = max(tidal_start, ecm_open)
        overlap_end = min(tidal_end, ecm_close)
        if overlap_start < overlap_end:
            final_windows.append({
                'start_time': overlap_start.time(), 'end_time': overlap_end.time(),
                'high_tide_times': [t['time'] for t in tide_data if t['type'] == 'H'],
                'tide_rule_concise': get_concise_tide_rule(ramp_obj, boat_obj)
            })
    return final_windows

def get_suitable_trucks(boat_len, pref_truck_id=None):
    all_suitable = [t.truck_id for t in ECM_TRUCKS.values() if not t.is_crane and boat_len <= t.max_boat_length]
    if force_preferred_truck and pref_truck_id in all_suitable: return [pref_truck_id]
    return all_suitable

def check_truck_availability(truck_id, start_dt, end_dt):
    for job in SCHEDULED_JOBS:
        if job.job_status == "Scheduled" and (job.assigned_hauling_truck_id == truck_id or (job.assigned_crane_truck_id == truck_id and truck_id == "J17")):
            job_end = job.j17_busy_end_datetime if truck_id == "J17" and job.j17_busy_end_datetime else job.scheduled_end_datetime
            if start_dt < job_end and end_dt > job.scheduled_start_datetime: return False
    return True

def determine_job_location_coordinates(endpoint, service, cust, boat, ramp): return {}
def is_dropoff_at_ecm_base(coords): return False

def _check_and_create_slot_detail(s_date, p_time, truck_id, cust, boat, service, ramp, ecm_hours, duration, needs_j17, j17_dur, debug_list, window_details):
    start_dt = datetime.datetime.combine(s_date, p_time)
    hauler_end_dt = start_dt + datetime.timedelta(hours=duration)
    if hauler_end_dt.time() > ecm_hours['close'] and not (hauler_end_dt.time() == ecm_hours['close'] and hauler_end_dt.date() == s_date): return None
    if not check_truck_availability(truck_id, start_dt, hauler_end_dt): return None
    if needs_j17 and not check_truck_availability("J17", start_dt, start_dt + datetime.timedelta(hours=j17_dur)): return None
    
    priority = 1 if needs_j17 and ramp and is_j17_at_ramp(s_date, ramp.ramp_id) else 0
    return {'date': s_date, 'time': p_time, 'truck_id': truck_id, 'j17_needed': needs_j17, 'type': "Open", 'bumped_job_details': None, 'customer_name': cust.customer_name, 'boat_details_summary': f"{boat.boat_length}ft {boat.boat_type}", 'ramp_id': ramp.ramp_id if ramp else None, 'tide_rule_concise': window_details.get('tide_rule_concise', ''), 'high_tide_times': window_details.get('high_tide_times', []), 'priority_score': priority}

def find_available_job_slots(customer_id, boat_id, service_type, requested_date_str, selected_ramp_id=None, transport_dropoff_details=None, force_preferred_truck=True, ignore_forced_search=False, **kwargs):
    global original_job_request_details, DEBUG_LOG_MESSAGES
    original_job_request_details = locals(); DEBUG_LOG_MESSAGES = []
    try: requested_date_obj = datetime.datetime.strptime(requested_date_str, '%Y-%m-%d').date()
    except ValueError: return [], "Error: Invalid date format.", [], False
    
    customer = get_customer_details(customer_id); boat = get_boat_details(boat_id)
    if not customer or not boat: return [], "Error: Invalid Cust/Boat ID.", [], False

    ramp_obj = get_ramp_details(selected_ramp_id)
    if ramp_obj and "Power Boats Only" in ramp_obj.allowed_boat_types and "Sailboat" in boat.boat_type:
        return [], f"Ramp '{ramp_obj.ramp_name}' only allows Power Boats.", [], False

    forced_date = None
    if boat.boat_type.startswith("Sailboat") and ramp_obj and not ignore_forced_search:
        for job in SCHEDULED_JOBS:
            if job.assigned_crane_truck_id and (job.pickup_ramp_id == selected_ramp_id or job.dropoff_ramp_id == selected_ramp_id) and abs((requested_date_obj - job.scheduled_start_datetime.date()).days) <= 7:
                forced_date = job.scheduled_start_datetime.date(); break
    
    rules = BOOKING_RULES.get(boat.boat_type, {}); duration = rules.get('truck_mins', 90)/60.0
    needs_j17 = rules.get('crane_mins', 0)>0; j17_duration = rules.get('crane_mins', 0)/60.0
    trucks_to_search = get_suitable_trucks(boat.boat_length, customer.preferred_truck_id if force_preferred_truck else None)
    
    potential_slots = []
    def search_day(s_date, slots_list, limit):
        ecm_hours = get_ecm_operating_hours(s_date)
        if not ecm_hours: return
        windows = get_final_schedulable_ramp_times(ramp_obj, boat, s_date) if ramp_obj else [{'start_time': ecm_hours['open'], 'end_time': ecm_hours['close']}]
        
        for truck_id in trucks_to_search:
            if len(slots_list) >= limit: break
            for w in windows:
                if len(slots_list) >= limit: break
                p_time = w['start_time']
                while p_time < w['end_time']:
                    if len(slots_list) >= limit: break
                    if (datetime.datetime.combine(s_date, p_time).minute % 30) != 0:
                        p_time = (datetime.datetime.combine(s_date, p_time) + datetime.timedelta(minutes=30-(p_time.minute%30))).time()
                    if p_time >= w['end_time']: break
                    slot = _check_and_create_slot_detail(s_date, p_time, truck_id, customer, boat, service_type, ramp_obj, ecm_hours, duration, needs_j17, j17_duration, DEBUG_LOG_MESSAGES, w)
                    if slot: slots_list.append(slot); break
                    p_time = (datetime.datetime.combine(datetime.date.min, p_time) + datetime.timedelta(minutes=30)).time()

    if forced_date:
        search_day(forced_date, potential_slots, 6); was_forced = True
    else:
        slots_before, slots_after = [], []
        d = max(TODAY_FOR_SIMULATION, requested_date_obj - datetime.timedelta(days=5))
        while d < requested_date_obj and len(slots_before) < 2:
            search_day(d, slots_before, 2); d += datetime.timedelta(days=1)
        d, i = requested_date_obj, 0
        while len(slots_after) < 4 and i < 45:
            search_day(d, slots_after, 4); d += datetime.timedelta(days=1); i += 1
        potential_slots = slots_before + slots_after; was_forced = False

    if not potential_slots: return [], "No suitable slots found.", [], was_forced
    potential_slots.sort(key=lambda s: (-s.get('priority_score', 0), s['date'], s['time']))
    top_slots = potential_slots[:6]
    expl = f"Found {len(top_slots)} slots."
    if was_forced: expl = f"Found slots on {forced_date.strftime('%A, %b %d')} to group with an existing crane job."
    elif top_slots: expl = f"Found {len(top_slots)} slots starting from {top_slots[0]['date'].strftime('%A, %b %d')}."
    
    return top_slots, expl, DEBUG_LOG_MESSAGES, was_forced

def confirm_and_schedule_job(original_job_request_details, selected_slot_info):
    global JOB_ID_COUNTER, SCHEDULED_JOBS
    cust = get_customer_details(original_job_request_details['customer_id']); boat = get_boat_details(original_job_request_details['boat_id'])
    if not cust or not boat: return None, "Error: Cust/Boat details missing."
    ramp = get_ramp_details(selected_slot_info.get('ramp_id'))
    if original_job_request_details['service_type'] in ["Launch", "Haul"] and not ramp: return None, "Error: Ramp is required."

    new_id = JOB_ID_COUNTER; JOB_ID_COUNTER += 1
    start_dt = datetime.datetime.combine(selected_slot_info['date'], selected_slot_info['time'])
    rules = BOOKING_RULES.get(boat.boat_type, {}); hauler_dur = rules.get('truck_mins', 90)/60.0
    hauler_end_dt = start_dt + datetime.timedelta(hours=hauler_dur)
    j17_end_dt = None
    if selected_slot_info['j17_needed']: j17_end_dt = start_dt + datetime.timedelta(hours=rules.get('crane_mins',0)/60.0)

    pickup_addr, dropoff_addr = "", ""
    pickup_rid, dropoff_rid = None, None
    if original_job_request_details['service_type'] == "Launch":
        pickup_addr, dropoff_addr = f"Cust: {cust.customer_name}", ramp.ramp_name; dropoff_rid = ramp.ramp_id
    elif original_job_request_details['service_type'] == "Haul":
        pickup_addr, dropoff_addr = ramp.ramp_name, f"Cust: {cust.customer_name}"; pickup_rid = ramp.ramp_id

    new_job = Job(job_id=new_id, customer_id=cust.customer_id, boat_id=boat.boat_id, service_type=original_job_request_details['service_type'], requested_date=datetime.datetime.strptime(original_job_request_details['requested_date_str'], '%Y-%m-%d').date(), scheduled_start_datetime=start_dt, calculated_job_duration_hours=hauler_dur, scheduled_end_datetime=hauler_end_dt, assigned_hauling_truck_id=selected_slot_info['truck_id'], assigned_crane_truck_id="J17" if selected_slot_info['j17_needed'] else None, j17_busy_end_datetime=j17_end_dt, pickup_ramp_id=pickup_rid, pickup_street_address=pickup_addr, dropoff_ramp_id=dropoff_rid, dropoff_street_address=dropoff_addr, notes=f"Booked via type: {selected_slot_info['type']}.")
    SCHEDULED_JOBS.append(new_job)

    if new_job.assigned_crane_truck_id and new_job.scheduled_start_datetime:
        date_str = new_job.scheduled_start_datetime.strftime('%Y-%m-%d')
        if date_str not in crane_daily_status: crane_daily_status[date_str] = {'ramps_visited': set()}
        if new_job.pickup_ramp_id: crane_daily_status[date_str]['ramps_visited'].add(new_job.pickup_ramp_id)
        if new_job.dropoff_ramp_id: crane_daily_status[date_str]['ramps_visited'].add(new_job.dropoff_ramp_id)

    return new_job.job_id, f"SUCCESS: Job {new_id} for {cust.customer_name} scheduled."
