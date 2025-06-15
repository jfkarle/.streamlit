# ecm_scheduler_logic.py
# FINAL VERSION with Real-Time NOAA Tide Data

import csv
import datetime
import pandas as pd
import requests

def fetch_noaa_tides(station_id, date_to_check):
    """
    Fetches high/low tide predictions from the NOAA Tides and Currents API.
    This function is based on your successful implementation.
    """
    date_str = date_to_check.strftime("%Y%m%d")

    # --- NEW DEBUGGING/DEFENSIVE CODE START ---
    # Add these lines at the very beginning of the function
    try:
        # Check if requests.get is callable. If not, it will raise an AttributeError.
        if not callable(requests.get):
            raise AttributeError("requests.get is not callable")
        print(f"DEBUG: requests.get is callable within fetch_noaa_tides. Proceeding for station {station_id}.")
    except (NameError, AttributeError) as e:
        print(f"CRITICAL ERROR: 'requests' module or 'requests.get' is not properly loaded/defined within fetch_noaa_tides. Attempting re-import. Error: {e}")
        # Try to re-import requests specifically within the function, as a last resort
        try:
            import requests as requests_reimport # Use a different name to avoid conflict, if needed
            global requests # Declare intent to modify global 'requests'
            requests = requests_reimport # Reassign global 'requests'
            if not callable(requests.get):
                raise AttributeError("re-imported requests.get is still not callable")
            print("CRITICAL DEBUG: Successfully re-imported requests within function.")
        except Exception as re_e:
            print(f"CRITICAL ERROR: Failed to re-import requests within function: {re_e}")
            return [] # Cannot proceed without requests

    # --- NEW DEBUGGING/DEFENSIVE CODE END ---

    base = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
    params = {
        "product": "predictions",
        "application": "ecm-boat-scheduler",
        "begin_date": date_str,
        "end_date": date_str,
        "datum": "MLLW",
        "station": station_id,
        "time_zone": "lst_ldt",
        "units": "english",
        "interval": "hilo",
        "format": "json",
    }

    tide_events = []
    try:
        # The error points here:
        resp = requests.get(base, params=params, timeout=10) # This is line 36 in your current code structure
        resp.raise_for_status()
        data = resp.json().get("predictions", [])

        if data:
            print(f"DEBUG: Successfully received {len(data)} tide predictions for station {station_id}.")
        else:
            print(f"DEBUG: Received no tide predictions for station {station_id}.")

        for item in data:
            t = datetime.datetime.strptime(item["t"], "%Y-%m-%d %H:%M")
            typ = item["type"].upper()
            if typ in ["H", "L"]:
                tide_events.append({'type': typ, 'time': t.time()})

        print(f"DEBUG: Processed {len(tide_events)} high/low tide events for station {station_id}.")

    except requests.exceptions.RequestException as e:
        print(f"ERROR: Could not fetch real-time NOAA tide data for station {station_id}. Error: {e}")
        return []
    except (KeyError, ValueError) as e:
        print(f"ERROR: Could not parse NOAA tide data for station {station_id}. Error: {e}")
        return []

    return tide_events


# --- Configuration & Global Context ---
TODAY_FOR_SIMULATION = datetime.date(2025, 6, 2)
ECM_BASE_LOCATION = {"lat": 42.0762, "lon": -70.8069}
JOB_ID_COUNTER = 3000
SCHEDULED_JOBS = []
BOOKING_RULES = {
    'Powerboat': {'truck_mins': 90, 'crane_mins': 0},
    'Sailboat DT': {'truck_mins': 180, 'crane_mins': 60},
    'Sailboat MT': {'truck_mins': 180, 'crane_mins': 90}
}
crane_daily_status = {}

# --- Section 1: Data Models (Classes) ---
class Truck:
    def __init__(self, truck_id, truck_name, max_boat_boat_length, is_crane=False, home_base_address="43 Mattakeeset St, Pembroke MA"):
        self.truck_id = truck_id
        self.truck_name = truck_name
        self.max_boat_boat_length = max_boat_boat_length
        self.is_crane = is_crane
        self.home_base_address = home_base_address

class Ramp:
    def __init__(self, ramp_id, ramp_name, town, tide_rule_description,
                 tide_calculation_method, noaa_station_id,
                 tide_offset_hours1=None, tide_offset_hours2=None,
                 draft_restriction_ft=None, draft_restriction_tide_rule=None,
                 allowed_boat_types="Power and Sail", ramp_fee=None, operating_notes=None,
                 latitude=None, longitude=None):
        self.ramp_id = ramp_id
        self.ramp_name = ramp_name
        self.town = town
        self.tide_rule_description = tide_rule_description
        self.tide_calculation_method = tide_calculation_method
        self.noaa_station_id = noaa_station_id
        self.tide_offset_hours1 = tide_offset_hours1
        self.tide_offset_hours2 = tide_offset_hours2 if tide_offset_hours2 is not None else tide_offset_hours1
        self.draft_restriction_ft = draft_restriction_ft
        self.draft_restriction_tide_rule = draft_restriction_tide_rule
        self.allowed_boat_types = allowed_boat_types
        self.ramp_fee = ramp_fee
        self.operating_notes = operating_notes
        self.latitude = latitude
        self.longitude = longitude

class Customer:
    def __init__(self, customer_id, customer_name,
                 home_latitude=None, home_longitude=None,
                 preferred_truck_id=None, is_ecm_customer=False, is_safe_harbor_customer=False):
        self.customer_id = customer_id
        self.customer_name = customer_name
        self.home_latitude = home_latitude
        self.home_longitude = home_longitude
        self.preferred_truck_id = preferred_truck_id
        self.is_ecm_customer = is_ecm_customer
        self.is_safe_harbor_customer = is_safe_harbor_customer

class Boat:
    def __init__(self, boat_id, customer_id, boat_type, boat_length,
                 draft_ft=None, height_ft_keel_to_highest=None, keel_type=None, is_ecm_boat=None):
        self.boat_id = boat_id
        self.customer_id = customer_id
        self.boat_type = boat_type
        self.boat_length = boat_length
        self.draft_ft = draft_ft
        self.height_ft_keel_to_highest = height_ft_keel_to_highest
        self.keel_type = keel_type

    @property
    def is_ecm_boat(self):
        customer = get_customer_details(self.customer_id)
        return customer.is_ecm_customer if customer else False

class Job:
    def __init__(self, job_id, customer_id, boat_id, service_type, requested_date,
                 scheduled_start_datetime=None, calculated_job_duration_hours=None,
                 scheduled_end_datetime=None, assigned_hauling_truck_id=None,
                 assigned_crane_truck_id=None, j17_busy_end_datetime=None,
                 pickup_ramp_id=None, pickup_street_address=None,
                 dropoff_ramp_id=None, dropoff_street_address=None,
                 job_status="Pending", notes=None,
                 pickup_loc_coords=None, dropoff_loc_coords=None):
        self.job_id = job_id
        self.customer_id = customer_id
        self.boat_id = boat_id
        self.service_type = service_type
        self.requested_date = requested_date
        self.scheduled_start_datetime = scheduled_start_datetime
        self.calculated_job_duration_hours = calculated_job_duration_hours
        self.scheduled_end_datetime = scheduled_end_datetime
        self.assigned_hauling_truck_id = assigned_hauling_truck_id
        self.assigned_crane_truck_id = assigned_crane_truck_id
        self.j17_busy_end_datetime = j17_busy_end_datetime
        self.pickup_ramp_id = pickup_ramp_id
        self.pickup_street_address = pickup_street_address
        self.dropoff_ramp_id = dropoff_ramp_id
        self.dropoff_street_address = dropoff_street_address
        self.job_status = job_status
        self.notes = notes
        self.is_ecm_priority_job = False
        self.was_bumped = False
        self.bumped_from_job_id = None
        self.pickup_loc_coords = pickup_loc_coords
        self.dropoff_loc_coords = dropoff_loc_coords

class OperatingHoursEntry:
    def __init__(self, rule_id, season, day_of_week, open_time, close_time, notes=None):
        self.rule_id = rule_id
        self.season = season
        self.day_of_week = day_of_week
        self.open_time = open_time
        self.close_time = close_time
        self.notes = notes

# --- Utility & Data Loading Functions ---
def format_time_for_display(time_obj):
    if not isinstance(time_obj, datetime.time): return "InvalidTime"
    formatted_time = time_obj.strftime('%I:%M %p')
    if formatted_time.startswith('0'):
        return formatted_time[1:]
    return formatted_time

# --- Section 2: Business Configuration & Initial Data ---
ECM_TRUCKS = {
    "S20/33": Truck(truck_id="S20/33", truck_name="S20 (aka S33)", max_boat_boat_length=60),
    "S21/77": Truck(truck_id="S21/77", truck_name="S21 (aka S77)", max_boat_boat_length=45),
    "S23/55": Truck(truck_id="S23/55", truck_name="S23 (aka S55)", max_boat_boat_length=30),
    "J17": Truck(truck_id="J17", truck_name="J17 (Crane Truck)", max_boat_boat_length=None, is_crane=True)
}

ECM_RAMPS = {
    "SandwichBasin": Ramp("SandwichBasin", "Sandwich Basin", "Sandwich, MA", "Any tide", "AnyTide", "SandwichStation_mock", allowed_boat_types="Power Boats (RARE)"),
    "PlymouthHarbor": Ramp("PlymouthHarbor", "Plymouth Harbor", "Plymouth, MA", "3 hrs before and after high tide", "HoursAroundHighTide", "8446493", tide_offset_hours1=3.0),
    "CordagePark": Ramp("CordagePark", "Cordage Park (Plymouth)", "Plymouth, MA", "1.5 hr before and after high tide", "HoursAroundHighTide", "8446493", tide_offset_hours1=1.5, allowed_boat_types="Power Boats Only"),
    "DuxburyHarbor": Ramp("DuxburyHarbor", "Duxbury Harbor (Town Pier)", "Duxbury, MA", "1 hr before or after high tide", "HoursAroundHighTide", "8445672", tide_offset_hours1=1.0, allowed_boat_types="Power Boats Only"),
    "GreenHarborTaylors": Ramp("GreenHarborTaylors", "Green Harbor (Taylors)", "Marshfield, MA", "3 hrs before and after high tide", "HoursAroundHighTide", "8445071", tide_offset_hours1=3.0, allowed_boat_types="Power Boats"),
    "GreenHarborSafeHarbor": Ramp("GreenHarborSafeHarbor", "Safe Harbor (Green Harbor)", "Marshfield, MA", "1 hr before and after (only for Safe Harbor customers)", "HoursAroundHighTide", "8445071", tide_offset_hours1=1.0, allowed_boat_types="Power Boats only", operating_notes="Safe Harbor customers only"),
    "ScituateHarborJericho": Ramp("ScituateHarborJericho", "Scituate Harbor (Jericho Road)", "Scituate, MA", "Any tide; 5' draft or > needs 3 hrs around high tide", "AnyTideWithDraftRule", "8445138", draft_restriction_ft=5.0, draft_restriction_tide_rule="HoursAroundHighTide_Offset3"),
    "CohassetParkerAve": Ramp("CohassetParkerAve", "Cohasset Harbor (Parker Ave)", "Cohasset, MA", "3 hrs before or after high tide", "HoursAroundHighTide", "8444672", tide_offset_hours1=3.0),
    "HullASt": Ramp("HullASt", "Hull (A St, Sunset, Steamboat)", "Hull, MA", "3 hrs before or after high tide; 1.5 hr tide for 6' or > draft", "HoursAroundHighTide_WithDraftRule", "8444009", tide_offset_hours1=3.0, draft_restriction_ft=6.0, draft_restriction_tide_rule="HoursAroundHighTide_Offset1.5"),
    "HinghamHarbor": Ramp("HinghamHarbor", "Hingham Harbor", "Hingham, MA", "3 hrs before and after high tide", "HoursAroundHighTide", "8443971", tide_offset_hours1=3.0),
    "WeymouthWessagusset": Ramp("WeymouthWessagusset", "Weymouth Harbor (Wessagusset)", "Weymouth, MA", "3 hrs before and after high tide", "HoursAroundHighTide", "8443581", tide_offset_hours1=3.0),
}

operating_hours_rules = [
    OperatingHoursEntry(1, "Standard", 0, datetime.time(8, 0), datetime.time(16, 0)),
    OperatingHoursEntry(2, "Standard", 1, datetime.time(8, 0), datetime.time(16, 0)),
    OperatingHoursEntry(3, "Standard", 2, datetime.time(8, 0), datetime.time(16, 0)),
    OperatingHoursEntry(4, "Standard", 3, datetime.time(8, 0), datetime.time(16, 0)),
    OperatingHoursEntry(5, "Standard", 4, datetime.time(8, 0), datetime.time(16, 0)),
    OperatingHoursEntry(6, "Standard", 5, datetime.time(23, 58), datetime.time(23, 59), notes="Closed"),
    OperatingHoursEntry(7, "Standard", 6, datetime.time(23, 58), datetime.time(23, 59), notes="Closed"),
    OperatingHoursEntry(10, "Busy", 0, datetime.time(7, 30), datetime.time(17, 30)),
    OperatingHoursEntry(11, "Busy", 1, datetime.time(7, 30), datetime.time(17, 30)),
    OperatingHoursEntry(12, "Busy", 2, datetime.time(7, 30), datetime.time(17, 30)),
    OperatingHoursEntry(13, "Busy", 3, datetime.time(7, 30), datetime.time(17, 30)),
    OperatingHoursEntry(14, "Busy", 4, datetime.time(7, 30), datetime.time(17, 30)),
    OperatingHoursEntry(15, "Busy", 5, datetime.time(23, 58), datetime.time(23, 59), notes="Closed unless May or Sep"),
    OperatingHoursEntry(16, "Busy", 6, datetime.time(23, 58), datetime.time(23, 59), notes="Closed"),
    OperatingHoursEntry(20, "BusySaturday", 5, datetime.time(7, 30), datetime.time(17, 30)),
]

LOADED_CUSTOMERS = {}
LOADED_BOATS = {}
CUSTOMER_ID_FROM_CSV_COUNTER = 1000
BOAT_ID_FROM_CSV_COUNTER = 5000

def load_customers_and_boats_from_csv(csv_filename="ECM Sample Cust.csv"):
    global LOADED_CUSTOMERS, LOADED_BOATS, CUSTOMER_ID_FROM_CSV_COUNTER, BOAT_ID_FROM_CSV_COUNTER
    LOADED_CUSTOMERS.clear()
    LOADED_BOATS.clear()
    customer_name_to_id_map = {}
    current_cust_id = CUSTOMER_ID_FROM_CSV_COUNTER
    current_boat_id = BOAT_ID_FROM_CSV_COUNTER
    try:
        with open(csv_filename, mode='r', encoding='utf-8-sig') as infile:
            reader = csv.DictReader(infile)
            for original_row in reader:
                row = {key.lower().strip(): value for key, value in original_row.items()}
                try:
                    cust_name = row.get("customer_name")
                    if not cust_name:
                        continue
                    if cust_name in customer_name_to_id_map:
                        customer_id_for_this_boat = customer_name_to_id_map[cust_name]
                    else:
                        is_ecm = row.get("is_ecm_boat", "False").strip().lower() == 'true'
                        customer = Customer(
                            customer_id=current_cust_id,
                            customer_name=cust_name,
                            home_latitude=float(row["home_latitude"]) if row.get("home_latitude") else None,
                            home_longitude=float(row["home_longitude"]) if row.get("home_longitude") else None,
                            preferred_truck_id=row.get("preferred_truck"),
                            is_ecm_customer=is_ecm
                        )
                        LOADED_CUSTOMERS[current_cust_id] = customer
                        customer_name_to_id_map[cust_name] = current_cust_id
                        customer_id_for_this_boat = current_cust_id
                        current_cust_id += 1
                    boat_type = row.get("boat_type")
                    boat_len_str = row.get("boat_length")
                    if boat_type and boat_len_str:
                        boat_draft_str = row.get("boat_draft")
                        boat = Boat(
                            boat_id=current_boat_id,
                            customer_id=customer_id_for_this_boat,
                            boat_type=boat_type,
                            boat_length=float(boat_len_str),
                            draft_ft=float(boat_draft_str) if boat_draft_str and boat_draft_str.strip() else None)
                        LOADED_BOATS[current_boat_id] = boat
                        current_boat_id += 1
                except (ValueError, TypeError) as ve:
                    print(f"Warning: Skipping row due to data conversion error: {original_row} - Error: {ve}")
        return True
    except FileNotFoundError:
        print(f"Error: Customer CSV file '{csv_filename}' not found.")
        return False
    except Exception as e:
        print(f"An unexpected error occurred while reading '{csv_filename}': {e}")
        return False

def get_customer_details(customer_id):
    return LOADED_CUSTOMERS.get(customer_id)

def get_boat_details(boat_id):
    return LOADED_BOATS.get(boat_id)

def get_ramp_details(ramp_id_or_name):
    return ECM_RAMPS.get(ramp_id_or_name)

def get_season(date_to_check):
    return "Busy" if date_to_check.month in [4, 5, 6, 9, 10] else "Standard"

def get_ecm_operating_hours(date_to_check):
    day_of_week = date_to_check.weekday()
    month = date_to_check.month
    if day_of_week == 5 and month in [5, 9]:
        season_to_check = "BusySaturday"
    else:
        season_to_check = get_season(date_to_check)
    for rule in operating_hours_rules:
        if rule.season == season_to_check and rule.day_of_week == day_of_week:
            if rule.open_time.hour == 23 and rule.open_time.minute == 58:
                return None
            return {"open": rule.open_time, "close": rule.close_time}
    return None

# The fetch_noaa_tides function has been restored to its API-fetching version.
# The previous version using TIDE_DATA was removed.

def calculate_ramp_windows(ramp_obj, boat_obj, tide_data_for_day, date_to_check):
    usable_windows = []
    tide_calc_method = ramp_obj.tide_calculation_method
    offset1_val = ramp_obj.tide_offset_hours1
    offset2_val = ramp_obj.tide_offset_hours2
    if ramp_obj.draft_restriction_ft and boat_obj.draft_ft and boat_obj.draft_ft >= ramp_obj.draft_restriction_ft:
        if ramp_obj.ramp_id == "ScituateHarborJericho":
            tide_calc_method = "HoursAroundHighTide"
            offset1_val = 3.0
            offset2_val = 3.0
        elif ramp_obj.ramp_id == "HullASt":
            tide_calc_method = "HoursAroundHighTide"
            offset1_val = 1.5
            offset2_val = 1.5
    if tide_calc_method == "AnyTide":
        usable_windows.append({'start_time': datetime.time.min, 'end_time': datetime.time.max})
        return usable_windows
    if tide_calc_method == "AnyTideWithDraftRule" and not \
       (ramp_obj.draft_restriction_ft and boat_obj.draft_ft and boat_obj.draft_ft >= ramp_obj.draft_restriction_ft and \
        (ramp_obj.ramp_id == "ScituateHarborJericho" or ramp_obj.ramp_id == "HullASt")):
        usable_windows.append({'start_time': datetime.time.min, 'end_time': datetime.time.max})
        return usable_windows
    if not tide_data_for_day and "HoursAroundHighTide" in tide_calc_method:
        return []
    if "HoursAroundHighTide" in tide_calc_method:
        if offset1_val is None: return []
        offset1_delta = datetime.timedelta(hours=float(offset1_val))
        offset2_delta = datetime.timedelta(hours=float(offset2_val if offset2_val is not None else offset1_val))
        high_tides = [event['time'] for event in tide_data_for_day if event['type'] == 'H']
        for ht_time_obj in high_tides:
            high_tide_dt = datetime.datetime.combine(date_to_check, ht_time_obj)
            start_dt = high_tide_dt - offset1_delta
            end_dt = high_tide_dt + offset2_delta
            usable_windows.append({'start_time': start_dt.time(), 'end_time': end_dt.time()})
    usable_windows.sort(key=lambda x: x['start_time'])
    return usable_windows

def get_final_schedulable_ramp_times(ramp_obj, boat_obj, date_to_check):
    final_windows = []
    ecm_hours = get_ecm_operating_hours(date_to_check)
    if not ecm_hours: return []
    ecm_open_dt = datetime.datetime.combine(date_to_check, ecm_hours['open'])
    ecm_close_dt = datetime.datetime.combine(date_to_check, ecm_hours['close'])
    # This line now calls the API-fetching fetch_noaa_tides
    tide_data = fetch_noaa_tides(ramp_obj.noaa_station_id, date_to_check)
    tidal_windows = calculate_ramp_windows(ramp_obj, boat_obj, tide_data, date_to_check)
    if not tidal_windows: return []
    for t_window in tidal_windows:
        tidal_start_dt = datetime.datetime.combine(date_to_check, t_window['start_time'])
        tidal_end_dt = datetime.datetime.combine(date_to_check, t_window['end_time'])
        if tidal_end_dt < tidal_start_dt :
            overlap_start1 = max(tidal_start_dt, ecm_open_dt)
            overlap_end1 = min(datetime.datetime.combine(date_to_check, datetime.time.max), ecm_close_dt)
            if overlap_start1 < overlap_end1:
                final_windows.append({'start_time': overlap_start1.time(), 'end_time': overlap_end1.time()})
        else:
            overlap_start_dt = max(tidal_start_dt, ecm_open_dt)
            overlap_end_dt = min(tidal_end_dt, ecm_close_dt)
            if overlap_start_dt < overlap_end_dt:
                final_windows.append({'start_time': overlap_start_dt.time(), 'end_time': overlap_end_dt.time()})
    unique_final_windows = []
    if final_windows:
        final_windows.sort(key=lambda x: x['start_time'])
        for fw in final_windows:
            if not unique_final_windows or unique_final_windows[-1] != fw:
                unique_final_windows.append(fw)
    return unique_final_windows

def get_suitable_trucks(boat_boat_length, preferred_truck_id=None):
    suitable_trucks_list = []
    if preferred_truck_id and preferred_truck_id in ECM_TRUCKS:
        truck = ECM_TRUCKS[preferred_truck_id]
        if not truck.is_crane and (truck.max_boat_boat_length is None or boat_boat_length <= truck.max_boat_boat_length):
            suitable_trucks_list.append(truck.truck_id)
    for truck_id, truck in ECM_TRUCKS.items():
        if truck.is_crane: continue
        if truck_id not in suitable_trucks_list:
            if truck.max_boat_boat_length is None or boat_boat_length <= truck.max_boat_boat_length:
                suitable_trucks_list.append(truck.truck_id)
    return suitable_trucks_list

def check_truck_availability(truck_id_to_check, check_date, proposed_start_dt, proposed_end_dt):
    for job in SCHEDULED_JOBS:
        if job.scheduled_start_datetime is None or job.job_status != "Scheduled": continue
        job_date = job.scheduled_start_datetime.date()
        if job_date == check_date:
            existing_job_start_dt = job.scheduled_start_datetime
            existing_job_true_end_dt = job.scheduled_end_datetime
            truck_is_involved = False
            # THIS LINE HAS THE TYPO: 'truck_id_to_involved' should be 'truck_id_to_check'
            if job.assigned_hauling_truck_id == truck_id_to_check: # CORRECTED LINE
                truck_is_involved = True
            elif job.assigned_crane_truck_id == truck_id_to_check and truck_id_to_check == "J17": # CORRECTED LINE
                truck_is_involved = True
                if job.j17_busy_end_datetime:
                    existing_job_true_end_dt = job.j17_busy_end_datetime
            if truck_is_involved:
                if proposed_start_dt < existing_job_true_end_dt and proposed_end_dt > existing_job_start_dt:
                    return False
    return True

def get_last_scheduled_job_for_truck_on_date(truck_id, check_date):
    truck_jobs = [j for j in SCHEDULED_JOBS if j.scheduled_start_datetime and
                  j.scheduled_start_datetime.date() == check_date and
                  j.assigned_hauling_truck_id == truck_id and j.job_status == "Scheduled"]
    if not truck_jobs: return None
    truck_jobs.sort(key=lambda j: j.scheduled_start_datetime, reverse=True)
    return truck_jobs[0]

def determine_job_location_coordinates(endpoint_type, service_type, customer_obj, boat_obj, ramp_obj=None, other_address_details=None):
    if service_type == "Launch":
        return {"lat": customer_obj.home_latitude, "lon": customer_obj.home_longitude} if endpoint_type == "pickup" else \
               {"lat": ramp_obj.latitude, "lon": ramp_obj.longitude} if ramp_obj else {"lat":0,"lon":0}
    elif service_type == "Haul":
        if endpoint_type == "pickup":
            return {"lat": ramp_obj.latitude, "lon": ramp_obj.longitude} if ramp_obj else {"lat":0,"lon":0}
        else:
            if boat_obj.is_ecm_boat:
                return ECM_BASE_LOCATION
            return {"lat": customer_obj.home_latitude, "lon": customer_obj.home_longitude}
    elif service_type == "Transport":
        return {"lat": customer_obj.home_latitude, "lon": customer_obj.home_longitude}
    return {"lat":0,"lon":0}

def calculate_distance_miles(loc1_coords, loc2_coords):
    if not loc1_coords or not loc2_coords or None in loc1_coords.values() or None in loc2_coords.values(): return float('inf')
    if loc1_coords.get('mock_dist_type') == "far" or loc2_coords.get('mock_dist_type') == "far": return 25
    if loc1_coords.get('mock_dist_type') == "mid" or loc2_coords.get('mock_dist_type') == "mid": return 11
    if loc1_coords.get('mock_dist_type') == "close" or loc2_coords.get('mock_dist_type') == "close": return 5
    return 8

def get_job_at_slot(check_date, check_time, check_truck_id=None):
    check_dt_start = datetime.datetime.combine(check_date, check_time)
    for job in SCHEDULED_JOBS:
        if job.job_status != "Scheduled": continue
        if job.scheduled_start_datetime == check_dt_start:
            if check_truck_id:
                if job.assigned_hauling_truck_id == check_truck_id or \
                   (job.assigned_crane_truck_id == check_truck_id and check_truck_id == "J17"):
                    return job
            else: return job
    return None

def is_dropoff_at_ecm_base(dropoff_location_coords):
    if not dropoff_location_coords: return False
    return dropoff_location_coords.get('lat') == ECM_BASE_LOCATION['lat'] and \
           dropoff_location_coords.get('lon') == ECM_BASE_LOCATION['lon']

def _check_and_create_slot_detail(current_search_date, current_potential_start_time_obj,
                                   truck_id, customer, boat, service_type, ramp_obj,
                                   ecm_op_hours, job_duration_hours, needs_j17,
                                   j17_actual_busy_duration_hours, debug_log_list):
    debug_log_list.append(f"C&CSD: Check: {current_search_date.strftime('%a %m-%d')} {current_potential_start_time_obj.strftime('%I:%M%p')} Truck:{truck_id}")
    proposed_start_dt = datetime.datetime.combine(current_search_date, current_potential_start_time_obj)
    proposed_end_dt_hauler = proposed_start_dt + datetime.timedelta(hours=job_duration_hours)
    if proposed_end_dt_hauler.time() > ecm_op_hours['close'] and not (proposed_end_dt_hauler.time() == ecm_op_hours['close'] and proposed_end_dt_hauler.date() == current_search_date):
        debug_log_list.append(f"C&CSD: REJECT - Job ends at {proposed_end_dt_hauler.time()}, after close {ecm_op_hours['close']}")
        return None
    hauler_avail = check_truck_availability(truck_id, current_search_date, proposed_start_dt, proposed_end_dt_hauler)
    j17_avail = True
    if needs_j17:
        j17_end_dt = proposed_start_dt + datetime.timedelta(hours=j17_actual_busy_duration_hours)
        j17_avail = check_truck_availability("J17", current_search_date, proposed_start_dt, j17_end_dt)
    if not (hauler_avail and j17_avail):
        debug_log_list.append(f"C&CSD: REJECT - Truck/J17 Unavail. Hauler:{hauler_avail} J17:{j17_avail} (needed:{needs_j17})")
        return None
    slot_type = "Open"
    bumped_job_info = None
    is_ecm_c = customer.is_ecm_customer
    c_month = current_search_date.month
    is_busy_month = get_season(current_search_date) == "Busy"
    is_first_slot_of_day = (current_potential_start_time_obj == ecm_op_hours['open'])
    is_spring_l = (service_type=="Launch" and c_month in [3,4,5,6] and is_ecm_c)
    if is_spring_l and is_first_slot_of_day:
        ex_job = get_job_at_slot(current_search_date, current_potential_start_time_obj, truck_id)
        if ex_job and ex_job.customer_id != customer.customer_id and not get_customer_details(ex_job.customer_id).is_ecm_customer and ex_job.service_type == "Launch":
            slot_type, bumped_job_info = "BumpNonECM_SpringLaunch", {"job_id":ex_job.job_id, "customer_name":get_customer_details(ex_job.customer_id).customer_name}
    is_fall_h_ecm = (service_type=="Haul" and c_month in [8,9,10,11] and is_ecm_c and is_dropoff_at_ecm_base(determine_job_location_coordinates("dropoff",service_type,customer,boat,ramp_obj)))
    if is_fall_h_ecm and ramp_obj and current_potential_start_time_obj >= datetime.time(13,0):
        ex_job = get_job_at_slot(current_search_date, current_potential_start_time_obj, truck_id)
        if ex_job and ex_job.customer_id != customer.customer_id and not get_customer_details(ex_job.customer_id).is_ecm_customer and ex_job.service_type == "Haul":
            slot_type, bumped_job_info = "BumpNonECM_FallHaul", {"job_id":ex_job.job_id, "customer_name":get_customer_details(ex_job.customer_id).customer_name}
    if is_busy_month and service_type == "Haul" and is_ecm_c and is_first_slot_of_day:
        ex_job = get_job_at_slot(current_search_date, current_potential_start_time_obj, truck_id)
        if ex_job and ex_job.customer_id != customer.customer_id and not get_customer_details(ex_job.customer_id).is_ecm_customer and ex_job.service_type == "Haul":
            slot_type = "BumpNonECM_BusyHaul"
            bumped_job_info = {"job_id": ex_job.job_id, "customer_name": get_customer_details(ex_job.customer_id).customer_name}
    return {
        'date': current_search_date,
        'time': current_potential_start_time_obj,
        'truck_id': truck_id,
        'j17_needed': needs_j17,
        'type': slot_type,
        'bumped_job_details': bumped_job_info,
        'customer_name': customer.customer_name,
        'boat_details_summary': f"{boat.boat_length}ft {boat.boat_type}",
        'ramp_id': ramp_obj.ramp_id if ramp_obj else None
    }

def find_available_job_slots(customer_id, boat_id, service_type, requested_date_str,
                             selected_ramp_id=None, transport_dropoff_details=None,
                             start_after_slot_details=None,
                             force_preferred_truck=True, relax_ramp_constraint=False):
    global original_job_request_details, DEBUG_LOG_MESSAGES
    DEBUG_LOG_MESSAGES = [f"FindSlots Start: Cust({customer_id}) Boat({boat_id}) Svc({service_type}) ReqDate({requested_date_str}) Ramp({selected_ramp_id})"]
    original_job_request_details = {'transport_dropoff_details': transport_dropoff_details, 'customer_id': customer_id, 'boat_id': boat_id, 'service_type': service_type, 'selected_ramp_id': selected_ramp_id, 'requested_date_str': requested_date_str}
    try:
        requested_date_obj = datetime.datetime.strptime(requested_date_str, '%Y-%m-%d').date()
    except ValueError:
        DEBUG_LOG_MESSAGES.append("Error: Invalid date format.")
        return [], "Error: Invalid date format.", DEBUG_LOG_MESSAGES
    customer = get_customer_details(customer_id)
    boat = get_boat_details(boat_id)
    if not customer or not boat:
        DEBUG_LOG_MESSAGES.append("Error: Invalid Cust/Boat ID.")
        return [], "Error: Invalid Cust/Boat ID.", DEBUG_LOG_MESSAGES
    boat_type_for_rules = boat.boat_type
    if boat_type_for_rules == "Sailboat MD": boat_type_for_rules = "Sailboat DT"
    rules = BOOKING_RULES.get(boat_type_for_rules, {})
    job_duration_hours = rules.get('truck_mins', 90) / 60.0
    needs_j17 = rules.get('crane_mins', 0) > 0
    j17_actual_busy_duration_hours = rules.get('crane_mins', 0) / 60.0
    all_suitable_trucks = get_suitable_trucks(boat.boat_length, customer.preferred_truck_id)
    if not all_suitable_trucks:
        DEBUG_LOG_MESSAGES.append("Error: No suitable trucks.")
        return [], "Error: No suitable trucks.", DEBUG_LOG_MESSAGES
    trucks_to_search = []
    all_suitable_trucks = get_suitable_trucks(boat.boat_length, customer.preferred_truck_id)
    if force_preferred_truck:
        DEBUG_LOG_MESSAGES.append(f"Lever Active: Forcing search to preferred truck.")
        preferred_truck_id = customer.preferred_truck_id
        if not preferred_truck_id:
            return [], "No preferred truck is set for this customer. Cannot perform a strict truck search.", ["Strict search failed: No preferred truck."]
        if preferred_truck_id in all_suitable_trucks:
            trucks_to_search = [preferred_truck_id]
        else:
            return [], f"The customer's preferred truck ({preferred_truck_id}) is not suitable for this {boat.boat_length}ft boat. No slots can be found with the 'Strict Truck' constraint.", [f"Strict search failed: Preferred truck {preferred_truck_id} unsuitable."]
    else:
        trucks_to_search = all_suitable_trucks
        DEBUG_LOG_MESSAGES.append(f"Lever Inactive: Searching all suitable trucks: {trucks_to_search}")
    if not trucks_to_search:
         return [], "Error: No suitable trucks found matching the criteria.", ["No suitable trucks found for search phase."]
    ramps_to_search = []
    if relax_ramp_constraint and selected_ramp_id:
        # Assuming get_nearby_ramps exists elsewhere or is a mock
        # For now, this will cause a NameError if get_nearby_ramps is not defined
        # This part of the code needs a definition for get_nearby_ramps
        print("Warning: get_nearby_ramps is not defined in the provided code. Cannot relax ramp constraint.")
        ramps_to_search = [selected_ramp_id] # Defaulting to selected_ramp_id if relax not possible
        DEBUG_LOG_MESSAGES.append(f"Lever Active (but get_nearby_ramps undefined): Forcing search to ramp: {selected_ramp_id}")

    elif selected_ramp_id:
        ramps_to_search = [selected_ramp_id]
        DEBUG_LOG_MESSAGES.append(f"Lever Inactive: Forcing search to ramp: {selected_ramp_id}")
    elif service_type == "Transport":
        ramps_to_search = [None]
    today = TODAY_FOR_SIMULATION
    effective_search_start_date = requested_date_obj
    min_start_time_on_first_day = None
    if start_after_slot_details and start_after_slot_details.get('date'):
        effective_search_start_date = start_after_slot_details['date']
        if start_after_slot_details.get('time'):
            min_start_time_on_first_day = (datetime.datetime.combine(effective_search_start_date, start_after_slot_details['time']) + datetime.timedelta(minutes=1)).time()
    else:
        if requested_date_obj >= today + datetime.timedelta(days=7):
            effective_search_start_date = requested_date_obj - datetime.timedelta(days=3)
        if effective_search_start_date < today:
            effective_search_start_date = today
    search_end_limit_date = requested_date_obj + datetime.timedelta(days=30)
    DEBUG_LOG_MESSAGES.append(f"Search Window: {effective_search_start_date} to {search_end_limit_date}" + (f" (after {min_start_time_on_first_day})" if min_start_time_on_first_day else ""))
    potential_slots_collected = []
    MAX_POOL_SIZE = 20
    current_search_date = effective_search_start_date
    days_iterated = 0
    while current_search_date <= search_end_limit_date and len(potential_slots_collected) < MAX_POOL_SIZE and days_iterated < 45:
        ecm_op_hours = get_ecm_operating_hours(current_search_date)
        if not ecm_op_hours or (boat.boat_type in ["Sailboat MD", "Sailboat MT"] and current_search_date.weekday() == 5 and current_search_date.month not in [5, 9]):
            current_search_date += datetime.timedelta(days=1); days_iterated += 1; continue
        for current_ramp_id in ramps_to_search:
            ramp_obj = None; daily_windows = []
            if service_type in ["Launch", "Haul"]:
                ramp_obj = get_ramp_details(current_ramp_id)
                if not ramp_obj: continue
                daily_windows = get_final_schedulable_ramp_times(ramp_obj, boat, current_search_date)
            elif service_type == "Transport":
                daily_windows = [{'start_time': ecm_op_hours['open'], 'end_time': ecm_op_hours['close']}]
            if not daily_windows: continue
            is_non_ecm_cust = not customer.is_ecm_customer
            if is_non_ecm_cust:
                day_open_dt = datetime.datetime.combine(current_search_date, ecm_op_hours['open'])
                non_ecm_min_start_dt = day_open_dt + datetime.timedelta(hours=1.5)
                non_ecm_min_start_time = non_ecm_min_start_dt.time()
                delayed_windows = []
                for window in daily_windows:
                    new_start_time = max(window['start_time'], non_ecm_min_start_time)
                    if new_start_time < window['end_time']:
                        delayed_windows.append({'start_time': new_start_time, 'end_time': window['end_time']})
                daily_windows = delayed_windows
            for truck_id in trucks_to_search:
                if len(potential_slots_collected) >= MAX_POOL_SIZE: break
                for window in daily_windows:
                    if len(potential_slots_collected) >= MAX_POOL_SIZE: break
                    iter_start_time = window['start_time']
                    if current_search_date == effective_search_start_date and min_start_time_on_first_day and min_start_time_on_first_day > iter_start_time:
                        iter_start_time = min_start_time_on_first_day
                    potential_time = iter_start_time
                    while potential_time < window['end_time']:
                        if len(potential_slots_collected) >= MAX_POOL_SIZE: break
                        temp_dt = datetime.datetime.combine(current_search_date, potential_time)
                        if temp_dt.minute not in [0, 30]:
                            if temp_dt.minute < 30: temp_dt = temp_dt.replace(minute=30, second=0, microsecond=0)
                            else: temp_dt = (temp_dt + datetime.timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
                            potential_time = temp_dt.time()
                        if potential_time >= window['end_time']: break
                        if not any(s['date'] == current_search_date and s['time'] == potential_time and s['truck_id'] == truck_id for s in potential_slots_collected):
                            slot_detail = _check_and_create_slot_detail(current_search_date, potential_time, truck_id, customer, boat, service_type, ramp_obj, ecm_op_hours, job_duration_hours, needs_j17, j17_actual_busy_duration_hours, DEBUG_LOG_MESSAGES)
                            if slot_detail:
                                potential_slots_collected.append(slot_detail)
                                break
                        potential_time = (datetime.datetime.combine(datetime.date.min, potential_time) + datetime.timedelta(minutes=30)).time()
        if current_search_date == effective_search_start_date: min_start_time_on_first_day = None
        current_search_date += datetime.timedelta(days=1); days_iterated += 1
    if not potential_slots_collected:
        return [], "No suitable slots found with the current criteria.", DEBUG_LOG_MESSAGES
    potential_slots_collected.sort(key=lambda slot: (slot['date'], slot['time']))
    top_slots = potential_slots_collected[:6]
    if top_slots:
        explanation = f"Found {len(top_slots)} available slots starting from {top_slots[0]['date'].strftime('%A, %b %d')}."
    else:
        explanation = "No suitable slots found with the current criteria."
    return top_slots, explanation, DEBUG_LOG_MESSAGES

def confirm_and_schedule_job(original_job_request_details, selected_slot_info):
    global JOB_ID_COUNTER, SCHEDULED_JOBS
    customer = get_customer_details(original_job_request_details['customer_id'])
    boat = get_boat_details(original_job_request_details['boat_id'])
    if not customer or not boat: return None, "Error: Confirm - Cust/Boat details missing."
    new_job_id = JOB_ID_COUNTER; JOB_ID_COUNTER += 1
    scheduled_start_datetime = datetime.datetime.combine(selected_slot_info['date'], selected_slot_info['time'])
    job_duration_hours_hauler = 3.0 if boat.boat_type in ["Sailboat MD", "Sailboat MT"] else 1.5
    scheduled_end_datetime_hauler = scheduled_start_datetime + datetime.timedelta(hours=job_duration_hours_hauler)
    assigned_hauling_truck_id = selected_slot_info['truck_id']
    assigned_crane_truck_id = "J17" if selected_slot_info['j17_needed'] else None
    final_j17_busy_end_datetime = None
    if assigned_crane_truck_id == "J17":
        j17_busy_hours = 1.0 if boat.boat_type == "Sailboat MD" else (1.5 if boat.boat_type == "Sailboat MT" else 0)
        if j17_busy_hours > 0: final_j17_busy_end_datetime = scheduled_start_datetime + datetime.timedelta(hours=j17_busy_hours)
    service_type = original_job_request_details['service_type']
    selected_ramp_id = original_job_request_details.get('selected_ramp_id')
    ramp_obj = ECM_RAMPS.get(selected_ramp_id) if selected_ramp_id else None
    pickup_desc, dropoff_desc = "Default Pickup", "Default Dropoff"
    pickup_r_id, dropoff_r_id = None, None
    if service_type == "Launch":
        pickup_desc = f"Cust: {customer.customer_name} Home"
        dropoff_desc = ramp_obj.ramp_name if ramp_obj else "Selected Ramp"
        dropoff_r_id = selected_ramp_id
    elif service_type == "Haul":
        pickup_desc = ramp_obj.ramp_name if ramp_obj else "Selected Ramp"
        pickup_r_id = selected_ramp_id
        mock_dropoff_coords = determine_job_location_coordinates("dropoff", service_type, customer, boat, ramp_obj)
        dropoff_desc = ECM_TRUCKS[assigned_hauling_truck_id].home_base_address if is_dropoff_at_ecm_base(mock_dropoff_coords) else f"Cust: {customer.customer_name} Home"
    elif service_type == "Transport":
        pickup_desc = f"Cust: {customer.customer_name} Home"
        dropoff_desc = original_job_request_details.get('transport_dropoff_details',{}).get('address',"Cust Alt. Address")
    new_job = Job(job_id=new_job_id, customer_id=customer.customer_id, boat_id=boat.boat_id, service_type=service_type,
                  requested_date=datetime.datetime.strptime(original_job_request_details['requested_date_str'], '%Y-%m-%d').date(),
                  scheduled_start_datetime=scheduled_start_datetime, calculated_job_duration_hours=job_duration_hours_hauler,
                  scheduled_end_datetime=scheduled_end_datetime_hauler, assigned_hauling_truck_id=assigned_hauling_truck_id,
                  assigned_crane_truck_id=assigned_crane_truck_id, j17_busy_end_datetime=final_j17_busy_end_datetime,
                  pickup_ramp_id=pickup_r_id, pickup_street_address=pickup_desc,
                  dropoff_ramp_id=dropoff_r_id, dropoff_street_address=dropoff_desc,
                  job_status="Scheduled", notes=f"Booked via type: {selected_slot_info['type']}. ECM Boat: {customer.is_ecm_customer}")
    bump_notification = ""
    if selected_slot_info['type'] != "Open" and selected_slot_info['bumped_job_details']:
        bumped_job_id = selected_slot_info['bumped_job_details']['job_id']
        bumped_cust_name = selected_slot_info['bumped_job_details']['customer_name']
        bumped_updated = False
        for i, job in enumerate(SCHEDULED_JOBS):
            if job.job_id == bumped_job_id and job.job_status == "Scheduled":
                SCHEDULED_JOBS[i].job_status = "Bumped - Needs Reschedule"
                SCHEDULED_JOBS[i].notes = f"{job.notes or ''} Bumped by Job {new_job_id}."
                bump_notification = f"ALERT: Job {bumped_job_id} for {bumped_cust_name} now 'Bumped - Needs Reschedule'."
                bumped_updated = True; break
        if not bumped_updated: bump_notification = f"WARNING: Bump failed - Job {bumped_job_id} not found or not in 'Scheduled' state."
    SCHEDULED_JOBS.append(new_job)
    if new_job.assigned_crane_truck_id and new_job.scheduled_start_datetime:
        job_date_str = new_job.scheduled_start_datetime.strftime('%Y-%m-%d')
        if job_date_str not in crane_daily_status:
            crane_daily_status[job_date_str] = {'ramps_visited': set()}
        if new_job.dropoff_ramp_id:
            crane_daily_status[job_date_str]['ramps_visited'].add(new_job.dropoff_ramp_id)
        elif new_job.pickup_ramp_id:
            crane_daily_status[job_date_str]['ramps_visited'].add(new_job.pickup_ramp_id)
    success_msg = f"SUCCESS: Job {new_job_id} for {customer.customer_name} scheduled for {format_time_for_display(new_job.scheduled_start_datetime.time())} on {new_job.scheduled_start_datetime.date()}."
    final_msg = f"{success_msg} {bump_notification}".strip()
    return new_job.job_id, final_msg

def _mark_slots_in_grid(schedule_grid_truck_col, time_slots_dt_list,
                        job_actual_start_dt, job_actual_end_dt,
                        job_display_text, slot_status, job_id_for_ref,
                        time_increment_minutes):
    job_marked_as_started = False
    for i, slot_start_dt in enumerate(time_slots_dt_list):
        slot_end_dt = slot_start_dt + datetime.timedelta(minutes=time_increment_minutes)
        if slot_start_dt < job_actual_end_dt and slot_end_dt > job_actual_start_dt:
            schedule_grid_truck_col[i]["status"] = slot_status
            schedule_grid_truck_col[i]["job_id"] = job_id_for_ref
            if not job_marked_as_started:
                schedule_grid_truck_col[i]["display_text"] = job_display_text
                schedule_grid_truck_col[i]["is_start_of_job"] = True # This line was missing 'is_start_of_job = True'
                job_marked_as_started = True
            else:
                schedule_grid_truck_col[i]["display_text"] = " | | "
                schedule_grid_truck_col[i]["is_start_of_job"] = False # This line was missing 'is_start_of_job = False'
        # The problematic line was removed or correctly handled here.
        # It's usually better to ensure is_start_of_job is set within the if/else for clarity.

def prepare_daily_schedule_data(display_date,
                                original_job_request_details_for_potential=None,
                                potential_job_slot_info=None,
                                time_increment_minutes=30):
    global SCHEDULED_JOBS
    output_data = {
        "display_date_str": display_date.strftime("%Y-%m-%d %A"),
        "time_slots_labels": [],
        "truck_columns": ["S20/33", "S21/77", "S23/55", "J17"],
        "schedule_grid": {},
        "operating_hours_display": "Closed"
    }
    ecm_hours = get_ecm_operating_hours(display_date)
    if not ecm_hours:
        output_data["schedule_grid"] = {truck_id: [] for truck_id in output_data["truck_columns"]}
        return output_data
    output_data["operating_hours_display"] = \
        f"{format_time_for_display(ecm_hours['open'])} - {format_time_for_display(ecm_hours['close'])}"
    time_slots_datetime_objects = []
    current_dt_for_label = datetime.datetime.combine(display_date, ecm_hours['open'])
    day_end_dt_for_label = datetime.datetime.combine(display_date, ecm_hours['close'])
    while current_dt_for_label < day_end_dt_for_label:
        output_data["time_slots_labels"].append(format_time_for_display(current_dt_for_label.time()))
        time_slots_datetime_objects.append(current_dt_for_label)
        current_dt_for_label += datetime.timedelta(minutes=time_increment_minutes)
    num_time_slots = len(output_data["time_slots_labels"])
    if num_time_slots == 0: return output_data
    for truck_col_id in output_data["truck_columns"]:
        output_data["schedule_grid"][truck_col_id] = [
            {"status": "free", "job_id": None, "display_text": "", "is_start_of_job": False}
            for _ in range(num_time_slots)
        ]
    for job in SCHEDULED_JOBS:
        if job.scheduled_start_datetime and \
           job.scheduled_start_datetime.date() == display_date and \
           job.job_status == "Scheduled":
            customer = get_customer_details(job.customer_id)
            boat = get_boat_details(job.boat_id)
            cust_name = customer.customer_name if customer else f"CustID {job.customer_id}"
            boat_info = f"{boat.boat_length}ft {boat.boat_type}" if boat and hasattr(boat, 'boat_length') and hasattr(boat, 'boat_type') else "N/A"
            job_text = f"{cust_name} ({boat_info})"
            if job.assigned_hauling_truck_id in output_data["schedule_grid"]:
                _mark_slots_in_grid(
                    output_data["schedule_grid"][job.assigned_hauling_truck_id],
                    time_slots_datetime_objects,
                    job.scheduled_start_datetime,
                    job.scheduled_end_datetime,
                    job_text, "busy", job.job_id,
                    time_increment_minutes)
            if job.assigned_crane_truck_id == "J17" and job.j17_busy_end_datetime:
                if "J17" in output_data["schedule_grid"]:
                    _mark_slots_in_grid(
                        output_data["schedule_grid"]["J17"],
                        time_slots_datetime_objects,
                        job.scheduled_start_datetime,
                        job.j17_busy_end_datetime,
                        f"J17 for {cust_name}", "busy", job.job_id,
                        time_increment_minutes)
    if potential_job_slot_info and original_job_request_details_for_potential:
        pot_date = potential_job_slot_info['date']
        if pot_date == display_date:
            pot_start_dt = datetime.datetime.combine(display_date, potential_job_slot_info['time'])
            pot_customer = get_customer_details(original_job_request_details_for_potential.get('customer_id'))
            pot_boat = get_boat_details(original_job_request_details_for_potential.get('boat_id'))
            if pot_customer and pot_boat:
                pot_hauler_duration_hours = 3.0 if pot_boat.boat_type in ["Sailboat MD", "Sailboat MT"] else 1.5
                pot_hauler_end_dt = pot_start_dt + datetime.timedelta(hours=pot_hauler_duration_hours)
                potential_job_text = f"POTENTIAL: {pot_customer.customer_name} ({pot_boat.boat_length}ft {pot_boat.boat_type})"
                hauler_truck_id = potential_job_slot_info['truck_id']
                if hauler_truck_id in output_data["schedule_grid"]:
                    _mark_slots_in_grid(
                        output_data["schedule_grid"][hauler_truck_id],
                        time_slots_datetime_objects,
                        pot_start_dt, pot_hauler_end_dt,
                        potential_job_text, "potential", "POTENTIAL_JOB",
                        time_increment_minutes)
                if potential_job_slot_info.get('j17_needed'):
                    j17_busy_hours = 1.0 if pot_boat.boat_type == "Sailboat MD" else (1.5 if pot_boat.boat_type == "Sailboat MT" else 0)
                    if j17_busy_hours > 0:
                        pot_j17_end_dt = pot_start_dt + datetime.timedelta(hours=j17_busy_hours)
                        if "J17" in output_data["schedule_grid"]:
                            _mark_slots_in_grid(
                                output_data["schedule_grid"]["J17"],
                                time_slots_datetime_objects,
                                pot_start_dt, pot_j17_end_dt,
                                f"J17 for POTENTIAL: {pot_customer.customer_name}", "potential", "POTENTIAL_JOB",
                                time_increment_minutes)
    return output_data
