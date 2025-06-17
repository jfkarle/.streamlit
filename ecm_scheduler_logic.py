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
    if not isinstance(time_obj, datetime.time): return "InvalidTime"
    return time_obj.strftime('%I:%M %p').lstrip('0')

def get_concise_tide_rule(ramp_obj, boat_obj):
    if ramp_obj.tide_calculation_method == "AnyTide": return "Any Tide"
    if ramp_obj.tide_offset_hours1 is not None:
        return f"{float(ramp_obj.tide_offset_hours1):g}hrs +/- HT"
    return "Tide Rule N/A"

def is_j17_at_ramp(check_date, ramp_id):
    if not ramp_id: return False
    date_str = check_date.strftime('%Y-%m-%d')
    if date_str in crane_daily_status:
        return ramp_id in crane_daily_status[date_str].get('ramps_visited', set())
    return False

def calculate_ramp_windows(ramp_obj, boat_obj, tide_data_for_day, date_to_check):
    usable_windows = []
    tide_calc_method = ramp_obj.tide_calculation_method
    
    if tide_calc_method == "AnyTide":
        usable_windows.append({'start_time': datetime.time.min, 'end_time': datetime.time.max})
        return usable_windows

    if not tide_data_for_day:
        return []

    if "HoursAroundHighTide" in tide_calc_method:
        offset_val = ramp_obj.tide_offset_hours1
        if offset_val is None: return []
        
        offset_delta = datetime.timedelta(hours=float(offset_val))
        high_tides = [event['time'] for event in tide_data_for_day if event['type'] == 'H']
        
        for ht_time_obj in high_tides:
            high_tide_dt = datetime.datetime.combine(date_to_check, ht_time_obj)
            start_dt = high_tide_dt - offset_delta
            end_dt = high_tide_dt + offset_delta
            usable_windows.append({'start_time': start_dt.time(), 'end_time': end_dt.time()})
            
    usable_windows.sort(key=lambda x: x['start_time'])
    return usable_windows



# --- Configuration & Data Models ---
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

class Truck:
    def __init__(self, truck_id, truck_name, max_boat_length):
        self.truck_id = truck_id; self.truck_name = truck_name
        self.max_boat_length = max_boat_length; self.is_crane = "Crane" in truck_name

class Ramp:
    def __init__(self, ramp_id, ramp_name, noaa_station_id, tide_calculation_method="AnyTide", tide_offset_hours1=None, allowed_boat_types=["Powerboat", "Sailboat DT", "Sailboat MT"]):
        self.ramp_id = ramp_id; self.ramp_name = ramp_name; self.noaa_station_id = noaa_station_id
        self.tide_calculation_method = tide_calculation_method; self.tide_offset_hours1 = tide_offset_hours1
        self.allowed_boat_types = allowed_boat_types

class Customer:
    def __init__(self, customer_id, customer_name, preferred_truck_id=None, is_ecm_customer=False):
        self.customer_id = customer_id; self.customer_name = customer_name
        self.preferred_truck_id = preferred_truck_id; self.is_ecm_customer = is_ecm_customer

class Boat:
    def __init__(self, boat_id, customer_id, boat_type, boat_length, draft_ft=None):
        self.boat_id = boat_id; self.customer_id = customer_id; self.boat_type = boat_type
        self.boat_length = boat_length; self.draft_ft = draft_ft

class Job:
    def __init__(self, job_id, customer_id, boat_id, service_type, scheduled_start_datetime, **kwargs):
        self.job_id = job_id; self.customer_id = customer_id; self.boat_id = boat_id
        self.service_type = service_type; self.scheduled_start_datetime = scheduled_start_datetime
        self.job_status = "Scheduled"
        self.__dict__.update(kwargs)

class OperatingHoursEntry:
    def __init__(self, season, day_of_week, open_time, close_time):
        self.season = season; self.day_of_week = day_of_week; self.open_time = open_time; self.close_time = close_time

# --- Data Initialization ---
ECM_TRUCKS = { "S20/33": Truck("S20/33", "S20", 60), "S21/77": Truck("S21/77", "S21", 45), "S23/55": Truck("S23/55", "S23", 30), "J17": Truck("J17", "J17 (Crane)", 999)}
ECM_RAMPS = {
    "DuxburyHarbor": Ramp("DuxburyHarbor", "Duxbury Harbor", "8443970", "HoursAroundHighTide", 1.0, ["Powerboat"]),
    "GreenHarborTaylors": Ramp("GreenHarborTaylors", "Green Harbor", "8443970", "HoursAroundHighTide", 3.0),
    "ScituateHarborJericho": Ramp("ScituateHarborJericho", "Scituate Harbor", "8443970"),
    "PlymouthHarbor": Ramp("PlymouthHarbor", "Plymouth Harbor", "8443970", "HoursAroundHighTide", 3.0),
}
operating_hours_rules = [
    OperatingHoursEntry("Standard", 0, datetime.time(8, 0), datetime.time(16, 0)), OperatingHoursEntry("Standard", 1, datetime.time(8, 0), datetime.time(16, 0)),
    OperatingHoursEntry("Standard", 2, datetime.time(8, 0), datetime.time(16, 0)), OperatingHoursEntry("Standard", 3, datetime.time(8, 0), datetime.time(16, 0)),
    OperatingHoursEntry("Standard", 4, datetime.time(8, 0), datetime.time(16, 0)), OperatingHoursEntry("Busy", 0, datetime.time(7, 30), datetime.time(17, 30)),
    OperatingHoursEntry("Busy", 1, datetime.time(7, 30), datetime.time(17, 30)), OperatingHoursEntry("Busy", 2, datetime.time(7, 30), datetime.time(17, 30)),
    OperatingHoursEntry("Busy", 3, datetime.time(7, 30), datetime.time(17, 30)), OperatingHoursEntry("Busy", 4, datetime.time(7, 30), datetime.time(17, 30)),
    OperatingHoursEntry("BusySaturday", 5, datetime.time(7, 30), datetime.time(17, 30)),
]
LOADED_CUSTOMERS = {}
LOADED_BOATS = {}

def load_customers_and_boats_from_csv(filename="ECM Sample Cust.csv"):
    global LOADED_CUSTOMERS, LOADED_BOATS
    try:
        with open(filename, mode='r', encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            for i, row in enumerate(reader):
                cust_id = f"C{1001+i}"; boat_id = f"B{5001+i}"
                LOADED_CUSTOMERS[cust_id] = Customer(cust_id, row['customer_name'], row.get('preferred_truck'), row.get('is_ecm_boat','').lower()=='true')
                LOADED_BOATS[boat_id] = Boat(boat_id, cust_id, row['boat_type'], float(row['boat_length']), float(row['boat_draft'] or 0))
        return True
    except FileNotFoundError: return False

# --- Core Logic Functions ---
get_customer_details = LOADED_CUSTOMERS.get
get_boat_details = LOADED_BOATS.get
get_ramp_details = ECM_RAMPS.get

def get_ecm_operating_hours(date):
    season = "Busy" if date.month in [4,5,6,9,10] else "Standard"
    if season == "Busy" and date.weekday() == 5 and date.month in [5,9]: season = "BusySaturday"
    for rule in operating_hours_rules:
        if rule.season == season and rule.day_of_week == date.weekday():
            return {"open": rule.open_time, "close": rule.close_time}
    return None

def get_final_schedulable_ramp_times(ramp, boat, date):
    ecm_hours = get_ecm_operating_hours(date)
    if not ecm_hours: return []
    ecm_open = datetime.datetime.combine(date, ecm_hours['open'])
    ecm_close = datetime.datetime.combine(date, ecm_hours['close'])
    tide_data = fetch_noaa_tides(ramp.noaa_station_id, date)
    tidal_windows = calculate_ramp_windows(ramp, tide_data, date)
    final_windows = []
    for t_win in tidal_windows:
        tidal_start = datetime.datetime.combine(date, t_win['start_time'])
        tidal_end = datetime.datetime.combine(date, t_win['end_time'])
        overlap_start, overlap_end = max(tidal_start, ecm_open), min(tidal_end, ecm_close)
        if overlap_start < overlap_end:
            final_windows.append({
                'start_time': overlap_start.time(), 'end_time': overlap_end.time(),
                'high_tide_times': [t['time'] for t in tide_data if t['type'] == 'H'],
                'tide_rule_concise': get_concise_tide_rule(ramp, boat)
            })
    return final_windows

def get_suitable_trucks(boat_len, pref_truck_id=None, force_preferred=False):
    all_suitable = [t for t in ECM_TRUCKS.values() if not t.is_crane and boat_len <= t.max_boat_length]
    if force_preferred and pref_truck_id and any(t.truck_id == pref_truck_id for t in all_suitable):
        return [t for t in all_suitable if t.truck_id == pref_truck_id]
    return all_suitable

def check_truck_availability(truck_id, start_dt, end_dt):
    for job in SCHEDULED_JOBS:
        if job.job_status == "Scheduled" and (job.assigned_hauling_truck_id == truck_id or (job.assigned_crane_truck_id == truck_id and truck_id == "J17")):
            job_end = getattr(job, 'j17_busy_end_datetime', job.scheduled_end_datetime) if truck_id == "J17" else job.scheduled_end_datetime
            if start_dt < job_end and end_dt > job.scheduled_start_datetime: return False
    return True

def _check_and_create_slot_detail(s_date, p_time, truck, cust, boat, service, ramp, ecm_hours, duration, j17_duration, window):
    start_dt = datetime.datetime.combine(s_date, p_time)
    hauler_end_dt = start_dt + datetime.timedelta(hours=duration)
    if hauler_end_dt.time() > ecm_hours['close'] and not (hauler_end_dt.time() == ecm_hours['close'] and hauler_end_dt.date() == s_date): return None
    if not check_truck_availability(truck.truck_id, start_dt, hauler_end_dt): return None
    
    needs_j17 = BOOKING_RULES.get(boat.boat_type, {}).get('crane_mins', 0) > 0
    if needs_j17 and not check_truck_availability("J17", start_dt, start_dt + datetime.timedelta(hours=j17_duration)): return None
    
    return {
        'date': s_date, 'time': p_time, 'truck_id': truck.truck_id, 'j17_needed': needs_j17, 'type': "Open",
        'ramp_id': ramp.ramp_id if ramp else None, 'priority_score': 1 if needs_j17 and ramp and is_j17_at_ramp(s_date, ramp.ramp_id) else 0,
        **window
    }

def find_available_job_slots(customer_id, boat_id, service_type, requested_date_str, selected_ramp_id=None, force_preferred_truck=True, ignore_forced_search=False, **kwargs):
    try: requested_date_obj = datetime.datetime.strptime(requested_date_str, '%Y-%m-%d').date()
    except ValueError: return [], "Error: Invalid date format.", [], False
    customer = get_customer_details(customer_id); boat = get_boat_details(boat_id)
    if not customer or not boat: return [], "Error: Invalid Cust/Boat ID.", [], False
    ramp_obj = get_ramp_details(selected_ramp_id)
    if ramp_obj and boat.boat_type not in ramp_obj.allowed_boat_types:
        return [], f"Ramp '{ramp_obj.ramp_name}' does not allow {boat.boat_type}s.", [], False

    forced_date = None
    if boat.boat_type.startswith("Sailboat") and ramp_obj and not ignore_forced_search:
        for job in SCHEDULED_JOBS:
            if job.assigned_crane_truck_id and (getattr(job, 'pickup_ramp_id', None) == selected_ramp_id or getattr(job, 'dropoff_ramp_id', None) == selected_ramp_id) and abs((requested_date_obj - job.scheduled_start_datetime.date()).days) <= 7:
                forced_date = job.scheduled_start_datetime.date(); break
    
    rules = BOOKING_RULES.get(boat.boat_type, {}); duration = rules.get('truck_mins', 90)/60.0
    j17_duration = rules.get('crane_mins', 0)/60.0
    trucks = get_suitable_trucks(boat.boat_length, customer.preferred_truck_id, force_preferred_truck)
    if not trucks: return [], "No suitable trucks for this boat.", [], False
    
    def search_day(s_date, slots_list, limit):
        ecm_hours = get_ecm_operating_hours(s_date)
        if not ecm_hours: return
        windows = get_final_schedulable_ramp_times(ramp_obj, boat, s_date) if ramp_obj else [{'start_time': ecm_hours['open'], 'end_time': ecm_hours['close']}]
        if not customer.is_ecm_customer:
            min_start = (datetime.datetime.combine(s_date, ecm_hours['open']) + datetime.timedelta(hours=1.5)).time()
            windows = [{**w, 'start_time': max(w['start_time'], min_start)} for w in windows if max(w['start_time'], min_start) < w['end_time']]
        
        for truck in trucks:
            if len(slots_list) >= limit: break
            for w in windows:
                p_time, p_end = w['start_time'], w['end_time']
                while p_time < p_end:
                    if len(slots_list) >= limit: break
                    if (datetime.datetime.combine(s_date, p_time).minute % 30) != 0:
                        p_time = (datetime.datetime.combine(s_date, p_time) + datetime.timedelta(minutes=30-(p_time.minute%30))).time()
                    if p_time >= p_end: break
                    slot = _check_and_create_slot_detail(s_date, p_time, truck, customer, boat, service_type, ramp_obj, ecm_hours, duration, j17_duration, w)
                    if slot: slots_list.append(slot); break # Found slot for this truck in this window
                    p_time = (datetime.datetime.combine(datetime.date.min, p_time) + datetime.timedelta(minutes=30)).time()
    
    potential_slots, was_forced = [], False
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
        potential_slots = slots_before + slots_after

    if not potential_slots: return [], "No suitable slots found.", [], was_forced
    potential_slots.sort(key=lambda s: (-s.get('priority_score', 0), s['date'], s['time']))
    top_slots = potential_slots[:6]
    expl = f"Found {len(top_slots)} available slots."
    if was_forced: expl = f"Found slots on {forced_date.strftime('%A, %b %d')} to group with an existing crane job."
    elif top_slots: expl = f"Found {len(top_slots)} slots starting from {top_slots[0]['date'].strftime('%A, %b %d')}."
    
    return top_slots, expl, [], was_forced

def confirm_and_schedule_job(original_request, selected_slot):
    customer = get_customer_details(original_request['customer_id'])
    boat = get_boat_details(original_request['boat_id'])
    ramp = get_ramp_details(selected_slot.get('ramp_id'))
    if original_request['service_type'] in ["Launch", "Haul"] and not ramp: return None, "Error: Ramp is required."
    
    job_id = JOB_ID_COUNTER + 1; globals()['JOB_ID_COUNTER'] += 1
    start_dt = datetime.datetime.combine(selected_slot['date'], selected_slot['time'])
    rules = BOOKING_RULES.get(boat.boat_type, {})
    hauler_dur = rules.get('truck_mins', 90)/60.0
    hauler_end_dt = start_dt + datetime.timedelta(hours=hauler_dur)
    j17_end_dt = None
    if selected_slot['j17_needed']: j17_end_dt = start_dt + datetime.timedelta(hours=rules.get('crane_mins',0)/60.0)

    pickup_addr, dropoff_addr, pickup_rid, dropoff_rid = "", "", None, None
    if original_request['service_type'] == "Launch":
        pickup_addr, dropoff_addr, dropoff_rid = f"Cust: {customer.customer_name}", ramp.ramp_name, ramp.ramp_id
    elif original_request['service_type'] == "Haul":
        pickup_addr, dropoff_addr, pickup_rid = ramp.ramp_name, f"Cust: {customer.customer_name}", ramp.ramp_id

    new_job = Job(job_id=job_id, customer_id=customer.customer_id, boat_id=boat.boat_id, service_type=original_request['service_type'], scheduled_start_datetime=start_dt, assigned_hauling_truck_id=selected_slot['truck_id'], assigned_crane_truck_id="J17" if selected_slot['j17_needed'] else None, j17_busy_end_datetime=j17_end_dt, pickup_ramp_id=pickup_rid, pickup_street_address=pickup_addr, dropoff_ramp_id=dropoff_rid, dropoff_street_address=dropoff_addr, notes=f"Booked via type: {selected_slot['type']}.", requested_date=datetime.datetime.strptime(original_request['requested_date_str'], '%Y-%m-%d').date(), calculated_job_duration_hours=hauler_dur, scheduled_end_datetime=hauler_end_dt)
    SCHEDULED_JOBS.append(new_job)

    if new_job.assigned_crane_truck_id:
        date_str = new_job.scheduled_start_datetime.strftime('%Y-%m-%d')
        if date_str not in crane_daily_status: crane_daily_status[date_str] = {'ramps_visited': set()}
        if new_job.pickup_ramp_id: crane_daily_status[date_str]['ramps_visited'].add(new_job.pickup_ramp_id)
        if new_job.dropoff_ramp_id: crane_daily_status[date_str]['ramps_visited'].add(new_job.dropoff_ramp_id)

    return new_job.job_id, f"SUCCESS: Job {job_id} for {customer.customer_name} scheduled."
