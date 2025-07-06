import csv
import datetime
import requests
import random
from datetime import timedelta, time

# --- Data Models and Configuration ---

DEFAULT_TRUCK_OPERATING_HOURS = {
    "S20/33": { 0: (time(7, 0), time(15, 0)), 1: (time(7, 0), time(15, 0)), 2: (time(7, 0), time(15, 0)), 3: (time(7, 0), time(15, 0)), 4: (time(7, 0), time(15, 0)), 5: (time(8, 0), time(12, 0)), 6: None },
    "S21/77": { 0: (time(8, 0), time(16, 0)), 1: (time(8, 0), time(16, 0)), 2: (time(8, 0), time(16, 0)), 3: (time(8, 0), time(16, 0)), 4: (time(8, 0), time(16, 0)), 5: None, 6: None },
    "S23/55": { 0: (time(8, 0), time(17, 0)), 1: (time(8, 0), time(17, 0)), 2: (time(8, 0), time(17, 0)), 3: (time(8, 0), time(17, 0)), 4: (time(8, 0), time(17, 0)), 5: (time(7, 30), time(17, 30)), 6: None },
    "J17":    { 0: (time(8, 0), time(16, 0)), 1: (time(8, 0), time(16, 0)), 2: (time(8, 0), time(16, 0)), 3: (time(8, 0), time(16, 0)), 4: (time(8, 0), time(16, 0)), 5: None, 6: None }
}
CANDIDATE_CRANE_DAYS = { 'ScituateHarborJericho': [], 'PlymouthHarbor': [], 'WeymouthWessagusset': [], 'CohassetParkerAve': [] }
SCHEDULED_JOBS = []
PARKED_JOBS = {}
JOB_ID_COUNTER = 3000
BOOKING_RULES = {'Powerboat': {'truck_mins': 90, 'crane_mins': 0},'Sailboat DT': {'truck_mins': 180, 'crane_mins': 60},'Sailboat MT': {'truck_mins': 180, 'crane_mins': 90}}
crane_daily_status = {}
LOADED_CUSTOMERS, LOADED_BOATS = {}, {}

class Truck:
    def __init__(self, t_id, name, max_len): self.truck_id, self.truck_name, self.max_boat_length, self.is_crane = t_id, name, max_len, "Crane" in name
class Ramp:
    def __init__(self, r_id, name, station, tide_method="AnyTide", offset=None, boats=None):
        self.ramp_id, self.ramp_name, self.noaa_station_id, self.tide_calculation_method = r_id, name, station, tide_method
        self.tide_offset_hours1, self.allowed_boat_types = offset, boats or ["Powerboat", "Sailboat DT", "Sailboat MT"]
class Customer:
    def __init__(self, c_id, name, street, truck, is_ecm, line2, cityzip):
        self.customer_id, self.customer_name, self.street_address, self.preferred_truck_id = c_id, name, street, truck
        self.is_ecm_customer, self.home_line2, self.home_citystatezip = is_ecm, line2, cityzip
class Boat:
    def __init__(self, b_id, c_id, b_type, b_len, draft): self.boat_id, self.customer_id, self.boat_type, self.boat_length, self.draft_ft = b_id, c_id, b_type, b_len, draft
class Job:
    def __init__(self, **kwargs): self.job_status = "Scheduled"; self.__dict__.update(kwargs)

ECM_TRUCKS = { "S20/33": Truck("S20/33", "S20", 60), "S21/77": Truck("S21/77", "S21", 45), "S23/55": Truck("S23/55", "S23", 30), "J17": Truck("J17", "J17 (Crane)", 999)}
ECM_RAMPS = {
    "SandwichBasin": Ramp("SandwichBasin", "Sandwich Basin", "8447180", "AnyTide", None, ["Powerboat"]), "PlymouthHarbor": Ramp("PlymouthHarbor", "Plymouth Harbor", "8446493", "HoursAroundHighTide", 3.0),
    "CordagePark": Ramp("CordagePark", "Cordage Park (Plymouth)", "8446493", "HoursAroundHighTide", 1.5, ["Powerboat"]), "DuxburyHarbor": Ramp("DuxburyHarbor", "Duxbury Harbor (Town Pier)", "8446166", "HoursAroundHighTide", 1.0, ["Powerboat"]),
    "GreenHarborTaylors": Ramp("GreenHarborTaylors", "Green Harbor (Taylors)", "8446009", "HoursAroundHighTide", 3.0, ["Powerboat"]), "GreenHarborSafeHarbor": Ramp("GreenHarborSafeHarbor", "Safe Harbor (Green Harbor)", "8446009", "HoursAroundHighTide", 1.0, ["Powerboat"]),
    "FerryStreet": Ramp("FerryStreet", "Ferry Street MYC", "8446009", "HoursAroundHighTide", 3.0, ["Powerboat"]), "SouthRiverYachtYard": Ramp("SouthRiverYachtYard", "SRYY", "8446009", "HoursAroundHighTide", 2.0, ["Powerboat"]),
    "ScituateHarborJericho": Ramp("ScituateHarborJericho", "Scituate Harbor (Jericho Road)", "8445138", "AnyTideWithDraftRule"), "CohassetParkerAve": Ramp("CohassetParkerAve", "Cohasset Harbor (Parker Ave)", "8444762", "HoursAroundHighTide", 3.0),
    "HullASt": Ramp("HullASt", "Hull (A St, Sunset, Steamboat)", "8444351", "HoursAroundHighTide_WithDraftRule", 3.0), "HinghamHarbor": Ramp("HinghamHarbor", "Hingham Harbor", "8444662", "HoursAroundHighTide", 3.0),
    "WeymouthWessagusset": Ramp("WeymouthWessagusset", "Weymouth Harbor (Wessagusset)", "8444788", "HoursAroundHighTide", 3.0),
}
get_customer_details = LOADED_CUSTOMERS.get; get_boat_details = LOADED_BOATS.get; get_ramp_details = ECM_RAMPS.get

def fetch_noaa_tides_for_range(station_id, start_date, end_date):
    start_str, end_str = start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")
    params = {"product": "predictions", "application": "ecm-boat-scheduler", "begin_date": start_str, "end_date": end_str, "datum": "MLLW", "station": station_id, "time_zone": "lst_ldt", "units": "english", "interval": "hilo", "format": "json"}
    try:
        resp = requests.get("https://api.tidesandcurrents.noaa.gov/api/prod/datagetter", params=params, timeout=15)
        resp.raise_for_status()
        predictions = resp.json().get("predictions", [])
        grouped_tides = {}
        for tide in predictions:
            tide_dt = datetime.datetime.strptime(tide["t"], "%Y-%m-%d %H:%M"); date_key = tide_dt.date()
            if date_key not in grouped_tides: grouped_tides[date_key] = []
            grouped_tides[date_key].append({'type': tide["type"].upper(), 'time': tide_dt.time(), 'height': tide["v"]})
        return grouped_tides
    except Exception as e:
        print(f"ERROR fetching tides for station {station_id}: {e}"); return {}

def format_time_for_display(time_obj):
    return time_obj.strftime('%I:%M %p').lstrip('0') if isinstance(time_obj, datetime.time) else "InvalidTime"

def get_all_tide_times_for_ramp_and_date(ramp_obj, date_obj):
    if not ramp_obj or not ramp_obj.noaa_station_id:
        print(f"[ERROR] Ramp '{ramp_obj.ramp_name if ramp_obj else 'Unknown'}' missing NOAA station ID.")
        return {'H': [], 'L': []}
    tides_for_range = fetch_noaa_tides_for_range(ramp_obj.noaa_station_id, date_obj, date_obj)
    tide_data_for_day = tides_for_range.get(date_obj, [])
    all_tides = {'H': [], 'L': []}
    for tide_entry in tide_data_for_day:
        tide_type = tide_entry.get('type')
        if tide_type in ['H', 'L']:
            all_tides[tide_type].append(tide_entry)
    return all_tides

def load_candidate_days_from_file(filename="candidate_days.csv"):
    global CANDIDATE_CRANE_DAYS
    try:
        with open(filename, mode='r') as infile:
            reader = csv.DictReader(infile)
            for row in reader:
                if row['ramp_id'] in CANDIDATE_CRANE_DAYS:
                    CANDIDATE_CRANE_DAYS[row['ramp_id']].append({"date": datetime.datetime.strptime(row['date'], "%Y-%m-%d").date(), "high_tide_time": datetime.datetime.strptime(row['high_tide_time'], "%H:%M:%S").time()})
    except FileNotFoundError: print("CRITICAL ERROR: `candidate_days.csv` not found.")

def load_customers_and_boats_from_csv(filename="ECM Sample Cust.csv"):
    global LOADED_CUSTOMERS, LOADED_BOATS
    try:
        with open(filename, mode='r', encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            for i, row in enumerate(reader):
                cust_id, boat_id = f"C{1001+i}", f"B{5001+i}"
                LOADED_CUSTOMERS[cust_id] = Customer(cust_id, row['customer_name'], row.get('street_address', ''), row.get('preferred_truck'), row.get('is_ecm_boat', '').lower() in ['true', 'yes'], row.get('Bill to 2', ''), row.get('Bill to 3', ''))
                try: 
                    boat_length = float(row.get('boat_length', '0').strip())
                    boat_draft = float(row.get('boat_draft', '0').strip() or 0)
                except (ValueError, TypeError): continue
                LOADED_BOATS[boat_id] = Boat(boat_id, cust_id, row['boat_type'].strip(), boat_length, boat_draft)
        load_candidate_days_from_file()
        return True
    except FileNotFoundError: return False

def get_concise_tide_rule(ramp, boat):
    if ramp.tide_calculation_method == "AnyTide": return "Any Tide"
    if ramp.tide_calculation_method == "AnyTideWithDraftRule":
        return "Any Tide (<5' Draft)" if boat.draft_ft and boat.draft_ft < 5.0 else "3 hrs +/- High Tide (≥5' Draft)"
    return f"{float(ramp.tide_offset_hours1):g} hrs +/- HT" if ramp.tide_offset_hours1 else "Tide Rule N/A"

def calculate_ramp_windows(ramp, boat, tide_data, date):
    if ramp.tide_calculation_method == "AnyTide": return [{'start_time': time.min, 'end_time': time.max}]
    if ramp.tide_calculation_method == "AnyTideWithDraftRule" and boat.draft_ft and boat.draft_ft < 5.0: return [{'start_time': time.min, 'end_time': time.max}]
    offset_hours = 3.0 if ramp.tide_calculation_method == "AnyTideWithDraftRule" else float(ramp.tide_offset_hours1 or 0)
    if not tide_data or not offset_hours: return []
    offset = timedelta(hours=offset_hours)
    return [{'start_time': (datetime.datetime.combine(date, t['time']) - offset).time(), 'end_time': (datetime.datetime.combine(date, t['time']) + offset).time()} for t in tide_data if t['type']=='H']

def get_final_schedulable_ramp_times(ramp_obj, boat_obj, date_to_check, all_tides, truck_id, truck_hours_schedule):
    day_of_week = date_to_check.weekday()
    truck_hours = truck_hours_schedule.get(truck_id, {}).get(day_of_week)
    if not truck_hours: return []
    truck_open_dt, truck_close_dt = datetime.datetime.combine(date_to_check, truck_hours[0]), datetime.datetime.combine(date_to_check, truck_hours[1])
    if not ramp_obj: return [{'start_time': truck_hours[0], 'end_time': truck_hours[1], 'high_tide_times': [], 'tide_rule_concise': 'N/A'}]
    tide_data_for_day = all_tides.get(date_to_check, [])
    tidal_windows = calculate_ramp_windows(ramp_obj, boat_obj, tide_data_for_day, date_to_check)
    final_windows = []
    for t_win in tidal_windows:
        tidal_start_dt, tidal_end_dt = datetime.datetime.combine(date_to_check, t_win['start_time']), datetime.datetime.combine(date_to_check, t_win['end_time'])
        overlap_start, overlap_end = max(tidal_start_dt, truck_open_dt), min(tidal_end_dt, truck_close_dt)
        if overlap_start < overlap_end:
            final_windows.append({'start_time': overlap_start.time(), 'end_time': overlap_end.time(), 'high_tide_times': [t['time'] for t in tide_data_for_day if t['type'] == 'H'], 'tide_rule_concise': get_concise_tide_rule(ramp_obj, boat_obj)})
    return final_windows

def get_suitable_trucks(boat_len, pref_truck_id=None, force_preferred=False):
    all_suitable = [t for t in ECM_TRUCKS.values() if not t.is_crane and boat_len <= t.max_boat_length]
    if force_preferred and pref_truck_id and any(t.truck_id == pref_truck_id for t in all_suitable):
        return [t for t in all_suitable if t.truck_id == pref_truck_id]
    return all_suitable

def check_truck_availability(truck_id, start_dt, end_dt):
    for job in SCHEDULED_JOBS:
        if job.job_status == "Scheduled":
            job_start, job_end = None, None
            if getattr(job, 'assigned_hauling_truck_id', None) == truck_id: job_start, job_end = job.scheduled_start_datetime, job.scheduled_end_datetime
            elif getattr(job, 'assigned_crane_truck_id', None) == truck_id and truck_id == "J17": job_start, job_end = job.scheduled_start_datetime, job.j17_busy_end_datetime
            if job_start and job_end and start_dt < job_end and end_dt > job_start: return False
    return True

def _diagnose_failure_reasons(req_date, customer, boat, ramp_obj, service_type, truck_hours, manager_override):
    """
    Checks a specific date for schedulability and returns a list of human-readable
    reasons for failure. This is called when the main search finds no slots.
    """
    reasons = []
    
    suitable_trucks = get_suitable_trucks(boat.boat_length)
    if not suitable_trucks:
        reasons.append(f"**Boat Too Large:** No trucks in the fleet are rated for a boat of {boat.boat_length}ft.")
        return reasons

    trucks_working_that_day = [t for t in suitable_trucks if truck_hours.get(t.truck_id, {}).get(req_date.weekday()) is not None]
    if not trucks_working_that_day:
        reasons.append(f"**No Trucks on Duty:** No suitable trucks are scheduled to work on {req_date.strftime('%A, %B %d')}.")
        return reasons
        
    needs_j17 = BOOKING_RULES.get(boat.boat_type, {}).get('crane_mins', 0) > 0
    if needs_j17 and not manager_override and ramp_obj:
        date_str = req_date.strftime('%Y-%m-%d')
        if date_str in crane_daily_status:
            visited_ramps = crane_daily_status[date_str]['ramps_visited']
            if visited_ramps and ramp_obj.ramp_id not in visited_ramps:
                conflicting_ramp_name = list(visited_ramps)[0]
                reasons.append(f"**Crane Is Busy Elsewhere:** The J17 crane is already committed to **{conflicting_ramp_name}** on this date.")
    
    if ramp_obj:
        all_tides = fetch_noaa_tides_for_range(ramp_obj.noaa_station_id, req_date, req_date)
        any_window_found = False
        for truck in trucks_working_that_day:
            if get_final_schedulable_ramp_times(ramp_obj, boat, req_date, all_tides, truck.truck_id, truck_hours):
                any_window_found = True
                break
        if not any_window_found:
            reasons.append("**Tide Conditions Not Met:** No valid tide windows overlap with available truck working hours on this date.")
            
    if not reasons:
        reasons.append("**All Slots Booked:** All available time slots for suitable trucks are already taken on this date.")
        
    return reasons

### NEW CODE TO SUPPORT Cancel, Rebook, Park

def get_job_details(job_id):
    """Finds and returns a job object from the main schedule by its ID."""
    for job in SCHEDULED_JOBS:
        if job.job_id == job_id:
            return job
    return None

def get_parked_job_details(job_id):
    """Finds and returns a job object from the parked jobs dictionary."""
    return PARKED_JOBS.get(job_id)

def cancel_job(job_id):
    """
    Finds a job by its ID in the main schedule and removes it permanently.
    Returns True if successful, False otherwise.
    """
    job_to_cancel = get_job_details(job_id)
    if job_to_cancel:
        SCHEDULED_JOBS.remove(job_to_cancel)
        # Here you might also add logic to save the updated schedule to a file
        return True
    return False

def park_job(job_id):
    """
    Finds a job, removes it from the main schedule, and places it in the
    PARKED_JOBS dictionary for later rescheduling.
    Returns True if successful, False otherwise.
    """
    job_to_park = get_job_details(job_id)
    if job_to_park:
        SCHEDULED_JOBS.remove(job_to_park)
        PARKED_JOBS[job_id] = job_to_park
        return True
    return False

### END New code to support cancel, rebook, park
def _compile_truck_schedules(jobs):
    """
    Pre-processes the list of scheduled jobs into a simple dictionary
    for extremely fast conflict lookups.
    """
    schedule = {}
    for job in jobs:
        if job.job_status != "Scheduled":
            continue
        
        # Log busy time for the hauling truck
        hauler_id = getattr(job, 'assigned_hauling_truck_id', None)
        if hauler_id:
            if hauler_id not in schedule:
                schedule[hauler_id] = []
            schedule[hauler_id].append((job.scheduled_start_datetime, job.scheduled_end_datetime))
        
        # Log busy time for the crane, if applicable
        crane_id = getattr(job, 'assigned_crane_truck_id', None)
        if crane_id and hasattr(job, 'j17_busy_end_datetime') and job.j17_busy_end_datetime:
            if crane_id not in schedule:
                schedule[crane_id] = []
            schedule[crane_id].append((job.scheduled_start_datetime, job.j17_busy_end_datetime))
    return schedule

def check_truck_availability_optimized(truck_id, start_dt, end_dt, compiled_schedule):
    """
    Checks for conflicts against the pre-compiled schedule. This is much
    faster than iterating through the full job list every time.
    """
    # Check against the list of busy blocks for the given truck
    for busy_start, busy_end in compiled_schedule.get(truck_id, []):
        # A conflict exists if the new slot overlaps with a busy block
        if start_dt < busy_end and end_dt > busy_start:
            return False # Found an overlap
    return True # No conflicts
### New code to support tide efficiency score

def calculate_tide_efficiency_score(date_obj, ramp_obj, truck_operating_hours):
    """
    Calculates a penalty score for a given day based on tide times.
    A lower score is better. Penalties are added for low tides during work hours.
    """
    if not ramp_obj:
        return 0

    score = 0
    tide_data = get_all_tide_times_for_ramp_and_date(ramp_obj, date_obj)
    low_tides = tide_data.get('L', [])

    # Consider all trucks that could work at this ramp
    for truck_id in truck_operating_hours:
        work_hours = truck_operating_hours[truck_id].get(date_obj.weekday())
        if not work_hours or not low_tides:
            continue

        work_start_dt = datetime.datetime.combine(date_obj, work_hours[0])
        work_end_dt = datetime.datetime.combine(date_obj, work_hours[1])

        # Add a penalty for each low tide that occurs during the workday
        for tide in low_tides:
            tide_dt = datetime.datetime.combine(date_obj, tide['time'])
            if work_start_dt <= tide_dt <= work_end_dt:
                score += 5  # Add a significant penalty

    return score

### End new code to support tide efficiency score


def find_available_job_slots(customer_id, boat_id, service_type, requested_date_str,
                             selected_ramp_id=None, force_preferred_truck=True, num_suggestions_to_find=5,
                             manager_override=False, crane_look_back_days=7, crane_look_forward_days=60,
                             truck_operating_hours=None, **kwargs):
    """
    Finds and ranks available job slots with high performance by batching API calls
    and streamlining scoring calculations.
    """
    try:
        requested_date = datetime.datetime.strptime(requested_date_str, '%Y-%m-%d').date()
    except ValueError:
        return [], "Error: Invalid date format.", [], False

    customer, boat = get_customer_details(customer_id), get_boat_details(boat_id)
    if not customer or not boat:
        return [], "Invalid Customer/Boat ID.", ["Customer or boat not found in system."], False

    ramp_obj = get_ramp_details(selected_ramp_id)
    # ... (Your existing validation logic) ...

    rules = BOOKING_RULES.get(boat.boat_type, {})
    hauler_duration = timedelta(minutes=rules.get('truck_mins', 90))
    j17_duration = timedelta(minutes=rules.get('crane_mins', 0))
    needs_j17 = j17_duration.total_seconds() > 0
    suitable_trucks = get_suitable_trucks(boat.boat_length, customer.preferred_truck_id, force_preferred_truck)

    # --- High-Performance Refactor ---
    all_found_slots = []
    search_start_date = requested_date - timedelta(days=crane_look_back_days)
    search_end_date = requested_date + timedelta(days=crane_look_forward_days)
    date_range_days = (search_end_date - search_start_date).days
    
    # 1. Fetch all tide data for the entire search window in ONE API call
    all_tides = {}
    if ramp_obj:
        all_tides = fetch_noaa_tides_for_range(ramp_obj.noaa_station_id, search_start_date, search_end_date)

    # 2. Pre-calculate special crane days
    active_crane_days = set()
    candidate_crane_dates = set()
    if needs_j17 and not manager_override:
        active_crane_days = {datetime.datetime.strptime(d_str, '%Y-%m-%d').date() for d_str, status in crane_daily_status.items() if selected_ramp_id in status.get('ramps_visited', set())}
        candidate_crane_dates = {day['date'] for day in CANDIDATE_CRANE_DAYS.get(selected_ramp_id, [])}

    # 3. Loop through each day ONCE to find all possible slots
    for i in range(date_range_days + 1):
        check_date = search_start_date + timedelta(days=i)

        # Determine day priority
        priority = 2 # General Availability
        if check_date in active_crane_days: priority = 0
        elif check_date in candidate_crane_dates: priority = 1

        # Calculate day-level scores once
        tide_score = calculate_tide_efficiency_score(check_date, ramp_obj, truck_operating_hours)
        day_capacity_score = 0
        # (Day capacity logic can be enhanced here if needed, keeping it simple for now)

        for truck in suitable_trucks:
            windows = get_final_schedulable_ramp_times(ramp_obj, boat, check_date, all_tides, truck.truck_id, truck_operating_hours)
            for window in windows:
                p_time = window['start_time']
                end_time = window['end_time']
                while p_time < end_time:
                    slot_start_dt = datetime.datetime.combine(check_date, p_time)
                    if not check_truck_availability(truck.truck_id, slot_start_dt, slot_start_dt + hauler_duration):
                        p_time = (slot_start_dt + timedelta(minutes=30)).time(); continue
                    if needs_j17 and not check_truck_availability("J17", slot_start_dt, slot_start_dt + j17_duration):
                        p_time = (slot_start_dt + timedelta(minutes=30)).time(); continue
                    
                    time_of_day_score = 0 if p_time.hour < 12 else 1
                    final_score = tide_score + day_capacity_score + time_of_day_score

                    all_found_slots.append({
                        'date': check_date, 'time': p_time, 'truck_id': truck.truck_id,
                        'j17_needed': needs_j17, 'ramp_id': selected_ramp_id,
                        'score': final_score, 'priority': priority,
                        'tide_rule_concise': window.get('tide_rule_concise'),
                        'high_tide_times': window.get('high_tide_times', [])
                    })
                    p_time = (slot_start_dt + timedelta(minutes=30)).time()

    # 4. Process all found slots
    if not all_found_slots:
        failure_reasons = _diagnose_failure_reasons(requested_date, customer, boat, ramp_obj, service_type, truck_operating_hours, manager_override)
        return [], "No suitable slots could be found.", failure_reasons, False
    else:
        # Sort all found slots by priority, then score, then by distance from requested date
        all_found_slots.sort(key=lambda s: (s['priority'], s['score'], abs(s['date'] - requested_date)))
        
        # Deduplicate to show only the best slot per day
        final_slots, seen_dates = [], set()
        for slot in all_found_slots:
            if slot['date'] not in seen_dates:
                final_slots.append(slot)
                seen_dates.add(slot['date'])
            if len(final_slots) >= num_suggestions_to_find:
                break
        
        return final_slots, f"Found {len(final_slots)} best available slots.", [], False

def confirm_and_schedule_job(original_request, selected_slot, parked_job_to_remove=None):
    """
    Creates and schedules a new job. This version explicitly re-checks if a crane
    is needed instead of relying on a flag from the selected slot.
    """
    try:
        customer = get_customer_details(original_request['customer_id'])
        boat = get_boat_details(original_request['boat_id'])
        ramp = get_ramp_details(selected_slot.get('ramp_id'))

        if not customer or not boat:
            return None, "Error: Could not find Customer or Boat details."
        if original_request['service_type'] in ["Launch", "Haul"] and not ramp:
            return None, "Error: A ramp must be selected."

        tide_data = get_all_tide_times_for_ramp_and_date(ramp, selected_slot['date']) if ramp else {'H': [], 'L': []}

        # Use a global counter for unique IDs
        global JOB_ID_COUNTER
        job_id = JOB_ID_COUNTER + 1
        JOB_ID_COUNTER += 1

        start_dt = datetime.datetime.combine(selected_slot['date'], selected_slot['time'])

        # Explicitly check the boat type against the booking rules here
        rules = BOOKING_RULES.get(boat.boat_type, {})
        crane_is_required = rules.get('crane_mins', 0) > 0

        hauler_end_dt = start_dt + timedelta(minutes=rules.get('truck_mins', 90))
        j17_end_dt = start_dt + timedelta(minutes=rules.get('crane_mins', 0)) if crane_is_required else None

        pickup_addr, dropoff_addr, pickup_rid, dropoff_rid = "", "", None, None
        service_type = original_request['service_type']
        if service_type == "Launch":
            pickup_addr, dropoff_addr, dropoff_rid = "HOME", ramp.ramp_name if ramp else 'N/A', selected_slot.get('ramp_id')
        elif service_type == "Haul":
            pickup_addr, dropoff_addr, pickup_rid = ramp.ramp_name if ramp else 'N/A', "HOME", selected_slot.get('ramp_id')

        new_job = Job(
            job_id=job_id, customer_id=customer.customer_id, boat_id=boat.boat_id, service_type=service_type,
            scheduled_start_datetime=start_dt, scheduled_end_datetime=hauler_end_dt,
            assigned_hauling_truck_id=selected_slot['truck_id'],
            # Assign crane based on our new, reliable check
            assigned_crane_truck_id="J17" if crane_is_required else None,
            j17_busy_end_datetime=j17_end_dt,
            pickup_ramp_id=pickup_rid, dropoff_ramp_id=dropoff_rid,
            high_tides=tide_data.get('H', []), low_tides=tide_data.get('L', []),
            job_status="Scheduled", notes=f"Booked via type: {selected_slot.get('type', 'N/A')}.",
            pickup_street_address=pickup_addr, dropoff_street_address=dropoff_addr
        )
        SCHEDULED_JOBS.append(new_job)

        if new_job.assigned_crane_truck_id and (new_job.pickup_ramp_id or new_job.dropoff_ramp_id):
            date_str = new_job.scheduled_start_datetime.strftime('%Y-%m-%d')
            if date_str not in crane_daily_status:
                crane_daily_status[date_str] = {'ramps_visited': set()}
            crane_daily_status[date_str]['ramps_visited'].add(new_job.pickup_ramp_id or new_job.dropoff_ramp_id)

        # If this was a "Move" or "Reschedule," remove the old parked job
        if parked_job_to_remove:
            if parked_job_to_remove in PARKED_JOBS:
                del PARKED_JOBS[parked_job_to_remove]

        # This is the single, corrected return block for a successful operation
        message = f"SUCCESS: Job {new_job.job_id} for {customer.customer_name} scheduled for {start_dt.strftime('%A, %b %d at %I:%M %p')}."
        return new_job.job_id, message

    except Exception as e:
        # If anything goes wrong, return an error message
        return None, f"An error occurred: {e}"

def generate_random_jobs(num_to_generate, start_date, end_date, service_type_filter, truck_operating_hours):
    if not all((LOADED_CUSTOMERS, LOADED_BOATS, ECM_RAMPS)): return "Error: Master data not loaded."
    if start_date > end_date: return "Error: Start date cannot be after end date."
    success_count, fail_count, customer_ids, ramp_ids, date_range_days = 0, 0, list(LOADED_CUSTOMERS.keys()), list(ECM_RAMPS.keys()), (end_date - start_date).days
    for _ in range(num_to_generate):
        service_type = random.choice(["Launch", "Haul", "Transport"]) if service_type_filter.lower() == 'all' else service_type_filter
        random_customer_id = random.choice(customer_ids)
        boat = next((b for b in LOADED_BOATS.values() if b.customer_id == random_customer_id), None)
        if not boat: fail_count += 1; continue
        random_ramp_id = random.choice(ramp_ids) if service_type != "Transport" else None
        random_date = start_date + timedelta(days=random.randint(0, date_range_days))
        slots, _, _, _ = find_available_job_slots(
            customer_id=random_customer_id, boat_id=boat.boat_id, service_type=service_type,
            requested_date_str=random_date.strftime('%Y-%m-%d'), selected_ramp_id=random_ramp_id,
            force_preferred_truck=False, truck_operating_hours=truck_operating_hours
        )
        if slots:
            selected_slot = random.choice(slots)
            job_request = {'customer_id': random_customer_id, 'boat_id': boat.boat_id, 'service_type': service_type}
            new_job_id, _ = confirm_and_schedule_job(job_request, selected_slot)
            if new_job_id: success_count += 1
            else: fail_count += 1
        else: fail_count += 1
    return f"Bulk generation complete. Success: {success_count}. Failures: {fail_count}."

def calculate_scheduling_stats(all_customers, all_boats, scheduled_jobs):
    today = datetime.date.today()
    total_all_boats = len(all_boats)
    scheduled_customer_ids = {j.customer_id for j in scheduled_jobs if j.job_status == "Scheduled"}
    launched_customer_ids = {j.customer_id for j in scheduled_jobs if j.job_status == "Scheduled" and j.service_type == "Launch" and j.scheduled_start_datetime.date() < today}
    ecm_customer_ids = {c_id for c_id, cust in all_customers.items() if cust.is_ecm_customer}
    
    return {
        'all_boats': {'total': total_all_boats, 'scheduled': len(scheduled_customer_ids), 'launched': len(launched_customer_ids)},
        'ecm_boats': {'total': len(ecm_customer_ids), 'scheduled': len(scheduled_customer_ids.intersection(ecm_customer_ids)), 'launched': len(launched_customer_ids.intersection(ecm_customer_ids))}
    }

from collections import Counter

def analyze_job_distribution(scheduled_jobs, all_boats_map, all_ramps_map):
    """Analyzes scheduled jobs to find distributions by day, boat type, and ramp."""
    if not scheduled_jobs:
        return {
            'by_day': Counter(),
            'by_boat_type': Counter(),
            'by_ramp': Counter()
        }

    day_map = {0: 'Mon', 1: 'Tue', 2: 'Wed', 3: 'Thu', 4: 'Fri', 5: 'Sat', 6: 'Sun'}
    day_counter = Counter(day_map[job.scheduled_start_datetime.weekday()] for job in scheduled_jobs)

    boat_type_counter = Counter()
    for job in scheduled_jobs:
        boat = all_boats_map.get(job.boat_id)
        if boat:
            boat_type_counter[boat.boat_type] += 1
            
    ramp_counter = Counter()
    for job in scheduled_jobs:
        ramp_id = job.pickup_ramp_id or job.dropoff_ramp_id
        if ramp_id:
            ramp = all_ramps_map.get(ramp_id)
            if ramp:
                ramp_counter[ramp.ramp_name] += 1

    return {
        'by_day': day_counter,
        'by_boat_type': boat_type_counter,
        'by_ramp': ramp_counter
    }
