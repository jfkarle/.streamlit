# ecm_scheduler_logic.py
# Consolidated Python script for ECM Boat Hauling Scheduler

import csv
import datetime

# import requests # Only needed for a live fetch_noaa_tides; using a mock for now.

# --- Configuration & Global Context ---
TODAY_FOR_SIMULATION = datetime.date(2025, 6, 2) # Monday, June 2, 2025 (for consistent testing)
ECM_BASE_LOCATION = {"lat": 42.0762, "lon": -70.8069} # Approx. for 43 Mattakeeset St, Pembroke, MA
JOB_ID_COUNTER = 3000 # Initial Job ID for new jobs

# In-memory store for scheduled jobs (for this iteration)
SCHEDULED_JOBS = []

# --- Section 1: Data Models (Classes) ---
class Truck:
    def __init__(self, truck_id, truck_name, max_boat_length_ft, is_crane=False, home_base_address="43 Mattakeeset St, Pembroke MA"):
        self.truck_id = truck_id
        self.truck_name = truck_name
        self.max_boat_length_ft = max_boat_length_ft
        self.is_crane = is_crane
        self.home_base_address = home_base_address # Added for consistency

    def __repr__(self):
        return f"Truck(ID: {self.truck_id}, Name: {self.truck_name}, MaxLen: {self.max_boat_length_ft}, Crane: {self.is_crane})"

class Ramp:
    def __init__(self, ramp_id, ramp_name, town, tide_rule_description,
                 tide_calculation_method, noaa_station_id,
                 tide_offset_hours1=None, tide_offset_hours2=None,
                 draft_restriction_ft=None, draft_restriction_tide_rule=None, # Specific rule for draft
                 allowed_boat_types="Power and Sail", ramp_fee=None, operating_notes=None,
                 latitude=None, longitude=None): # Added lat/lon
        self.ramp_id = ramp_id
        self.ramp_name = ramp_name
        self.town = town
        self.tide_rule_description = tide_rule_description
        self.tide_calculation_method = tide_calculation_method # e.g., "HoursAroundHighTide", "AnyTide", "AnyTideWithDraftRule"
        self.noaa_station_id = noaa_station_id
        self.tide_offset_hours1 = tide_offset_hours1 # Primary offset (e.g., hours before)
        self.tide_offset_hours2 = tide_offset_hours2 if tide_offset_hours2 is not None else tide_offset_hours1 # Secondary offset (e.g., hours after)
        self.draft_restriction_ft = draft_restriction_ft
        self.draft_restriction_tide_rule = draft_restriction_tide_rule # Description of rule if draft met
        self.allowed_boat_types = allowed_boat_types
        self.ramp_fee = ramp_fee
        self.operating_notes = operating_notes
        self.latitude = latitude
        self.longitude = longitude

    def __repr__(self):
        return f"Ramp(ID: {self.ramp_id}, Name: {self.ramp_name}, Town: {self.town}, TideRule: {self.tide_rule_description})"

class Customer:
    def __init__(self, customer_id, customer_name,
                 home_latitude=None, home_longitude=None,
                 preferred_truck_id=None, is_ecm_customer=False, is_safe_harbor_customer=False): # is_safe_harbor_customer added
        self.customer_id = customer_id
        self.customer_name = customer_name
        self.home_latitude = home_latitude
        self.home_longitude = home_longitude
        self.preferred_truck_id = preferred_truck_id
        self.is_ecm_customer = is_ecm_customer # True if ECM stores their boat [user input]
        self.is_safe_harbor_customer = is_safe_harbor_customer # For Green Harbor Safe Harbor ramp rule

    def __repr__(self):
        return f"Customer(ID: {self.customer_id}, Name: {self.customer_name}, ECM_Cust: {self.is_ecm_customer})"

class Boat:
    def __init__(self, boat_id, customer_id, boat_type, length_ft,
                 draft_ft=None, height_ft_keel_to_highest=None, keel_type=None, is_ecm_boat=None):
        self.boat_id = boat_id
        self.customer_id = customer_id
        self.boat_type = boat_type # "Powerboat", "Sailboat MD" (Mast Down), "Sailboat MT" (Mast Trans.)
        self.length_ft = length_ft
        self.draft_ft = draft_ft
        self.height_ft_keel_to_highest = height_ft_keel_to_highest # For over-height check [user input]
        self.keel_type = keel_type # For sailboats: "Wing", "Bulb", "Full", "Centerboard" [user input]
        # self.is_ecm_boat = is_ecm_boat # Indicates if boat is stored at ECM (from Customer data usually)

    @property
    def is_ecm_boat(self): # Dynamically determine from customer if not set directly
        customer = get_customer_details(self.customer_id)
        return customer.is_ecm_customer if customer else False

    def __repr__(self):
        return f"Boat(ID: {self.boat_id}, CustID: {self.customer_id}, Type: {self.boat_type}, Len: {self.length_ft}ft)"

class Job:
    def __init__(self, job_id, customer_id, boat_id, service_type, requested_date,
                 scheduled_start_datetime=None, calculated_job_duration_hours=None,
                 scheduled_end_datetime=None, # For the hauling truck
                 assigned_hauling_truck_id=None,
                 assigned_crane_truck_id=None, # Will be "J17" if crane is used
                 j17_busy_end_datetime=None,   # Specific end time for J17's involvement
                 pickup_ramp_id=None, pickup_street_address=None,
                 dropoff_ramp_id=None, dropoff_street_address=None,
                 job_status="Pending", notes=None,
                 pickup_loc_coords=None, dropoff_loc_coords=None): # For mock location tracking

        self.job_id = job_id
        self.customer_id = customer_id
        self.boat_id = boat_id
        self.service_type = service_type # "Launch", "Haul", "Transport"
        self.requested_date = requested_date
        self.scheduled_start_datetime = scheduled_start_datetime
        self.calculated_job_duration_hours = calculated_job_duration_hours
        self.scheduled_end_datetime = scheduled_end_datetime # Hauler's end time
        self.assigned_hauling_truck_id = assigned_hauling_truck_id
        self.assigned_crane_truck_id = assigned_crane_truck_id
        self.j17_busy_end_datetime = j17_busy_end_datetime
        self.pickup_ramp_id = pickup_ramp_id
        self.pickup_street_address = pickup_street_address
        self.dropoff_ramp_id = dropoff_ramp_id
        self.dropoff_street_address = dropoff_street_address
        self.job_status = job_status # "Pending", "Scheduled", "Completed", "Canceled", "Bumped - Needs Reschedule"
        self.notes = notes
        self.is_ecm_priority_job = False # Can be set if it's an ECM boat getting priority
        self.was_bumped = False
        self.bumped_from_job_id = None
        self.pickup_loc_coords = pickup_loc_coords
        self.dropoff_loc_coords = dropoff_loc_coords

    def __repr__(self):
        crane_info = f", Crane: {self.assigned_crane_truck_id}" if self.assigned_crane_truck_id else ""
        j17_busy_info = f", J17 Free: {format_time_for_display(self.j17_busy_end_datetime.time())}" if self.j17_busy_end_datetime else ""
        start_time_str = format_time_for_display(self.scheduled_start_datetime.time()) if self.scheduled_start_datetime else "Not Set"
        return (f"Job(ID: {self.job_id}, Cust: {self.customer_id}, Svc: {self.service_type}, "
                f"Start: {self.scheduled_start_datetime.strftime('%Y-%m-%d %H:%M') if self.scheduled_start_datetime else 'N/A'}, "
                f"Truck: {self.assigned_hauling_truck_id}{crane_info}{j17_busy_info}, Status: {self.job_status})")

class OperatingHoursEntry:
    def __init__(self, rule_id, season, day_of_week, open_time, close_time, notes=None):
        self.rule_id = rule_id
        self.season = season # e.g., "Standard", "Peak", "MaySep"
        self.day_of_week = day_of_week # Monday=0, Sunday=6
        self.open_time = open_time   # datetime.time object
        self.close_time = close_time # datetime.time object
        self.notes = notes

    def __repr__(self):
        return (f"OpHours(Season: {self.season}, Day: {self.day_of_week}, "
                f"Open: {format_time_for_display(self.open_time)}, Close: {format_time_for_display(self.close_time)})")

# --- Section 2: Business Configuration & Initial Data ---
# For initial testing, this data is embedded. Later, it can be read from files.

ECM_TRUCKS = {
    "S20/33": Truck(truck_id="S20/33", truck_name="S20 (aka S33)", max_boat_length_ft=60),
    "S21/77": Truck(truck_id="S21/77", truck_name="S21 (aka S77)", max_boat_length_ft=50),
    "S23/55": Truck(truck_id="S23/55", truck_name="S23 (aka S55)", max_boat_length_ft=30),
    "J17": Truck(truck_id="J17", truck_name="J17 (Crane Truck)", max_boat_length_ft=None, is_crane=True)
}

ECM_RAMPS = {
    "SandwichBasin": Ramp("SandwichBasin", "Sandwich Basin", "Sandwich, MA", "Any tide", "AnyTide", "SandwichStation_mock", allowed_boat_types="Power Boats (RARE)"),
    "PlymouthHarbor": Ramp("PlymouthHarbor", "Plymouth Harbor", "Plymouth, MA", "3 hrs before and after high tide", "HoursAroundHighTide", "8446493", tide_offset_hours1=3.0),
    "CordagePark": Ramp("CordagePark", "Cordage Park (Plymouth)", "Plymouth, MA", "1.5 hr before and after high tide", "HoursAroundHighTide", "8446493", tide_offset_hours1=1.5, allowed_boat_types="Power Boats Only"),
    "DuxburyHarbor": Ramp("DuxburyHarbor", "Duxbury Harbor (Town Pier)", "Duxbury, MA", "1 hr before or after high tide", "HoursAroundHighTide", "8445672", tide_offset_hours1=1.0, allowed_boat_types="Power Boats Only"),
    "GreenHarborTaylors": Ramp("GreenHarborTaylors", "Green Harbor (Taylors)", "Marshfield, MA", "3 hrs before and after high tide", "HoursAroundHighTide", "8445071", tide_offset_hours1=3.0, allowed_boat_types="Power Boats"),
    "GreenHarborSafeHarbor": Ramp("GreenHarborSafeHarbor", "Safe Harbor (Green Harbor)", "Marshfield, MA", "1 hr before and after (only for Safe Harbor customers)", "HoursAroundHighTide", "8445071", tide_offset_hours1=1.0, allowed_boat_types="Power Boats only", operating_notes="Safe Harbor customers only"),
    "ScituateHarborJericho": Ramp("ScituateHarborJericho", "Scituate Harbor (Jericho Road)", "Scituate, MA", "Any tide; 5' draft or > needs 3 hrs around high tide", "AnyTideWithDraftRule", "8444992", draft_restriction_ft=5.0, draft_restriction_tide_rule="HoursAroundHighTide_Offset3"), # Special draft rule
    "CohassetParkerAve": Ramp("CohassetParkerAve", "Cohasset Harbor (Parker Ave)", "Cohasset, MA", "3 hrs before or after high tide", "HoursAroundHighTide", "8444672", tide_offset_hours1=3.0),
    "HullASt": Ramp("HullASt", "Hull (A St, Sunset, Steamboat)", "Hull, MA", "3 hrs before or after high tide; 1.5 hr tide for 6' or > draft", "HoursAroundHighTide_WithDraftRule", "8444009", tide_offset_hours1=3.0, draft_restriction_ft=6.0, draft_restriction_tide_rule="HoursAroundHighTide_Offset1.5"),
    "HinghamHarbor": Ramp("HinghamHarbor", "Hingham Harbor", "Hingham, MA", "3 hrs before and after high tide", "HoursAroundHighTide", "8443971", tide_offset_hours1=3.0),
    "WeymouthWessagusset": Ramp("WeymouthWessagusset", "Weymouth Harbor (Wessagusset)", "Weymouth, MA", "3 hrs before and after high tide", "HoursAroundHighTide", "8443581", tide_offset_hours1=3.0),
}

operating_hours_rules = [
    # Standard Season Rules (e.g., Jan-Apr, Oct-Dec, excluding specific May/Sep rules)
    OperatingHoursEntry(1, "Standard", 0, datetime.time(8, 0), datetime.time(16, 0)), # Mon
    OperatingHoursEntry(2, "Standard", 1, datetime.time(8, 0), datetime.time(16, 0)), # Tue
    OperatingHoursEntry(3, "Standard", 2, datetime.time(8, 0), datetime.time(16, 0)), # Wed
    OperatingHoursEntry(4, "Standard", 3, datetime.time(8, 0), datetime.time(16, 0)), # Thu
    OperatingHoursEntry(5, "Standard", 4, datetime.time(8, 0), datetime.time(16, 0)), # Fri
    OperatingHoursEntry(6, "Standard", 5, datetime.time(23, 58), datetime.time(23,59), notes="Effectively Closed"), # Sat CLOSED
    OperatingHoursEntry(7, "Standard", 6, datetime.time(23, 58), datetime.time(23,59), notes="Effectively Closed"), # Sun CLOSED

    # May & September Peak Rules (May=5, Sep=9)
    OperatingHoursEntry(10, "MaySep", 0, datetime.time(7, 30), datetime.time(17, 0)), # Mon
    OperatingHoursEntry(11, "MaySep", 1, datetime.time(7, 30), datetime.time(17, 0)), # Tue
    OperatingHoursEntry(12, "MaySep", 2, datetime.time(7, 30), datetime.time(17, 0)), # Wed
    OperatingHoursEntry(13, "MaySep", 3, datetime.time(7, 30), datetime.time(17, 0)), # Thu
    OperatingHoursEntry(14, "MaySep", 4, datetime.time(7, 30), datetime.time(17, 0)), # Fri
    OperatingHoursEntry(15, "MaySep", 5, datetime.time(7, 30), datetime.time(17, 30)), # Sat OPEN
    OperatingHoursEntry(16, "MaySep", 6, datetime.time(23, 58), datetime.time(23,59), notes="Effectively Closed"), # Sun CLOSED
]

# NEW: Global dictionaries for data loaded from CSV
LOADED_CUSTOMERS = {}
LOADED_BOATS = {}

# NEW: Counters for generating IDs from CSV data
CUSTOMER_ID_FROM_CSV_COUNTER = 1000 # Start from a different range than mocks
BOAT_ID_FROM_CSV_COUNTER = 5000   # Start from a different range

def load_customers_and_boats_from_csv(csv_filename="ECM Sample Cust.csv"):
    """
    Loads customer and associated boat data from a CSV file into
    LOADED_CUSTOMERS and LOADED_BOATS dictionaries.
    """
    global LOADED_CUSTOMERS, LOADED_BOATS, CUSTOMER_ID_FROM_CSV_COUNTER, BOAT_ID_FROM_CSV_COUNTER
    
    LOADED_CUSTOMERS.clear() # Clear previous loads
    LOADED_BOATS.clear()     # Clear previous loads
    
    # Reset counters or ensure they generate unique IDs if called multiple times
    current_cust_id = CUSTOMER_ID_FROM_CSV_COUNTER 
    current_boat_id = BOAT_ID_FROM_CSV_COUNTER

    try:
        with open(csv_filename, mode='r', encoding='utf-8-sig') as infile: # 'utf-8-sig' handles potential BOM
            reader = csv.DictReader(infile)
            if not reader.fieldnames:
                print(f"Error: CSV file '{csv_filename}' might be empty or headers are missing.")
                return False
            
            # Expected headers (adjust if your CSV is different)
            # "Customer Name", "Boat Type", "PREFERRED TRUCK", "Boat Length", "Boat Draft",
            # "Home Latitude", "Home Longitude", "Is ECM Boat"

            for row in reader:
                try:
                    # Customer Data
                    cust_name = row.get("Customer Name")
                    if not cust_name: # Skip row if essential data like name is missing
                        print(f"Warning: Skipping row due to missing Customer Name: {row}")
                        continue

                    pref_truck_id = row.get("PREFERRED TRUCK")
                    home_lat_str = row.get("Home Latitude")
                    home_lon_str = row.get("Home Longitude")
                    is_ecm_str = row.get("Is ECM Boat", "False") # Default to False if missing

                    home_lat = float(home_lat_str) if home_lat_str else None
                    home_lon = float(home_lon_str) if home_lon_str else None
                    is_ecm = is_ecm_str.strip().lower() == 'true'

                    customer = Customer(
                        customer_id=current_cust_id,
                        customer_name=cust_name,
                        home_latitude=home_lat,
                        home_longitude=home_lon,
                        preferred_truck_id=pref_truck_id if pref_truck_id in ECM_TRUCKS else None, # Validate truck ID
                        is_ecm_customer=is_ecm
                    )
                    LOADED_CUSTOMERS[current_cust_id] = customer

                    # Boat Data (assuming one boat per customer row in this CSV)
                    boat_type = row.get("Boat Type")
                    boat_len_str = row.get("Boat Length")
                    boat_draft_str = row.get("Boat Draft")
                    # height_ft_keel_to_highest and keel_type are not in this CSV structure,
                    # they would need to be added or handled as None/defaults.
                    
                    if boat_type and boat_len_str: # Basic check for essential boat data
                        boat_len = float(boat_len_str)
                        boat_draft = float(boat_draft_str) if boat_draft_str else None

                        boat = Boat(
                            boat_id=current_boat_id,
                            customer_id=current_cust_id, # Link to the customer we just created
                            boat_type=boat_type,
                            length_ft=boat_len,
                            draft_ft=boat_draft,
                            # height_ft_keel_to_highest, keel_type would be None or default here
                            # is_ecm_boat will be derived from customer via @property
                        )
                        LOADED_BOATS[current_boat_id] = boat
                        current_boat_id += 1
                    else:
                        print(f"Warning: Missing boat type or length for customer {cust_name}. Boat not created.")

                    current_cust_id += 1
                except ValueError as ve:
                    print(f"Warning: Skipping row due to data conversion error (e.g., non-numeric Lat/Lon/Length/Draft): {row} - Error: {ve}")
                except Exception as e:
                    print(f"Warning: Skipping row due to unexpected error: {row} - Error: {e}")
        
        print(f"Successfully loaded {len(LOADED_CUSTOMERS)} customers and {len(LOADED_BOATS)} boats from {csv_filename}.")
        return True

    except FileNotFoundError:
        print(f"Error: Customer CSV file '{csv_filename}' not found. Please ensure it's in the same directory as the script.")
        return False
    except Exception as e:
        print(f"An unexpected error occurred while reading '{csv_filename}': {e}")
        return False

def get_customer_details(customer_id):
    """Fetches customer details from the LOADED_CUSTOMERS dictionary."""
    customer = LOADED_CUSTOMERS.get(customer_id)
    if not customer:
        print(f"Warning: Customer ID {customer_id} not found in LOADED_CUSTOMERS.")
    return customer

def get_boat_details(boat_id):
    """Fetches boat details from the LOADED_BOATS dictionary."""
    boat = LOADED_BOATS.get(boat_id)
    if not boat:
        print(f"Warning: Boat ID {boat_id} not found in LOADED_BOATS.")
    return boat

def get_ramp_details(ramp_id_or_name): # Make sure this is the only definition
    """Fetches ramp details from the global ECM_RAMPS dictionary."""
    ramp = ECM_RAMPS.get(ramp_id_or_name)
    if not ramp:
        print(f"Warning: Ramp ID/Name '{ramp_id_or_name}' not found in ECM_RAMPS.")
    return ramp

# --- Section 3: Date & Time Utilities ---
def format_time_for_display(time_obj):
    if not isinstance(time_obj, datetime.time): return "InvalidTime"
    # Ensure lstrip('0') only runs if there's a leading zero from %I
    formatted_time = time_obj.strftime('%I:%M %p')
    if formatted_time.startswith('0'):
        return formatted_time[1:]
    return formatted_time

def get_season(date_to_check):
    month = date_to_check.month
    if month == 5 or month == 9:
        return "MaySep"
    # Add other season logic if needed, e.g., "Peak" vs "Standard"
    # For now, defaulting to "Standard" if not May/Sep
    return "Standard"

def get_ecm_operating_hours(date_to_check):
    season = get_season(date_to_check)
    day_of_week = date_to_check.weekday() # Monday=0, Sunday=6

    for rule in operating_hours_rules:
        if rule.season == season and rule.day_of_week == day_of_week:
            # Check for "Effectively Closed"
            if rule.open_time == datetime.time(23,58) and rule.close_time == datetime.time(23,59):
                return None
            return {"open": rule.open_time, "close": rule.close_time}
    return None # Should not happen if rules are comprehensive

# --- Section 4: NOAA Tide Data Fetching (Mocked for Standalone Testing) ---
def fetch_noaa_tides(station_id, date_to_check):
    """ MOCK: Returns sample tide data. Replace with live API call for production. """
    # print(f"    (MOCK fetch_noaa_tides for station {station_id} on {date_to_check})")
    # Based on common New England patterns, 2 high, 2 low tides typically.
    if station_id == "8446493": # Plymouth Harbor
        return [
            {'type': 'L', 'time': datetime.time(3, 15)},
            {'type': 'H', 'time': datetime.time(9, 30)},
            {'type': 'L', 'time': datetime.time(15, 45)},
            {'type': 'H', 'time': datetime.time(21, 50)}
        ]
    elif station_id == "8445672": # Duxbury
        return [
            {'type': 'L', 'time': datetime.time(4, 0)},
            {'type': 'H', 'time': datetime.time(10, 10)},
            {'type': 'L', 'time': datetime.time(16, 20)},
            {'type': 'H', 'time': datetime.time(22, 30)}
        ]
    # Add more mock stations as needed for testing other ramps
    # Default if station_id not specifically mocked:
    return [
        {'type': 'H', 'time': datetime.time(10, 0)},
        {'type': 'H', 'time': datetime.time(22, 15)}
    ]

# --- Section 5: Ramp Usable Window Calculation ---
def calculate_ramp_windows(ramp_obj, boat_obj, tide_data_for_day, date_to_check):
    usable_windows = []
    # Determine the effective tide rule & offsets
    tide_calc_method = ramp_obj.tide_calculation_method
    offset1_val = ramp_obj.tide_offset_hours1
    offset2_val = ramp_obj.tide_offset_hours2 # Will be same as offset1 if not explicitly set

    # Handle specific draft rule overrides from Ramp object
    if ramp_obj.draft_restriction_ft and boat_obj.draft_ft and boat_obj.draft_ft >= ramp_obj.draft_restriction_ft:
        if ramp_obj.ramp_id == "ScituateHarborJericho": # "5' draft or > needs 3 hrs"
            tide_calc_method = "HoursAroundHighTide"
            offset1_val = 3.0
            offset2_val = 3.0
        elif ramp_obj.ramp_id == "HullASt": # "1.5 hr tide for 6' or > draft"
            tide_calc_method = "HoursAroundHighTide"
            offset1_val = 1.5
            offset2_val = 1.5
        # Potentially map ramp_obj.draft_restriction_tide_rule to method/offsets if more generic rules are defined
    
    if tide_calc_method == "AnyTide":
        usable_windows.append({'start_time': datetime.time.min, 'end_time': datetime.time.max})
        return usable_windows
    
    if tide_calc_method == "AnyTideWithDraftRule" and not \
       (ramp_obj.draft_restriction_ft and boat_obj.draft_ft and boat_obj.draft_ft >= ramp_obj.draft_restriction_ft and \
        (ramp_obj.ramp_id == "ScituateHarborJericho" or ramp_obj.ramp_id == "HullASt")): # If draft rule didn't trigger override
        usable_windows.append({'start_time': datetime.time.min, 'end_time': datetime.time.max})
        return usable_windows

    if not tide_data_for_day and "HoursAroundHighTide" in tide_calc_method:
        return [] # No tide data, cannot calculate specific windows

    if "HoursAroundHighTide" in tide_calc_method:
        if offset1_val is None: return [] # Needs offset
        offset1_delta = datetime.timedelta(hours=float(offset1_val))
        offset2_delta = datetime.timedelta(hours=float(offset2_val if offset2_val is not None else offset1_val))
        
        high_tides = [event['time'] for event in tide_data_for_day if event['type'] == 'H']
        for ht_time_obj in high_tides:
            high_tide_dt = datetime.datetime.combine(date_to_check, ht_time_obj)
            start_dt = high_tide_dt - offset1_delta
            end_dt = high_tide_dt + offset2_delta
            usable_windows.append({
                'start_time': start_dt.time(),
                'end_time': end_dt.time()
            })
    # Add other tide_calculation_method logic here if needed (e.g., "BetweenLowAndHigh")
    
    # Sort windows by start time (though merging isn't implemented here yet)
    usable_windows.sort(key=lambda x: x['start_time'])
    return usable_windows

# --- Section 6: Final Schedulable Ramp Times ---
def get_final_schedulable_ramp_times(ramp_obj, boat_obj, date_to_check):
    final_windows = []
    ecm_hours = get_ecm_operating_hours(date_to_check)
    if not ecm_hours: return []
    ecm_open_dt = datetime.datetime.combine(date_to_check, ecm_hours['open'])
    ecm_close_dt = datetime.datetime.combine(date_to_check, ecm_hours['close'])

    tide_data = fetch_noaa_tides(ramp_obj.noaa_station_id, date_to_check)
    tidal_windows = calculate_ramp_windows(ramp_obj, boat_obj, tide_data, date_to_check)
    if not tidal_windows: return []

    for t_window in tidal_windows:
        tidal_start_dt = datetime.datetime.combine(date_to_check, t_window['start_time'])
        tidal_end_dt = datetime.datetime.combine(date_to_check, t_window['end_time'])
        # Handle overnight tidal windows by only considering portion on date_to_check
        if tidal_end_dt < tidal_start_dt : # Window crosses midnight, e.g. 22:00 - 02:00
            # Part 1: From tidal_start_dt to midnight
            overlap_start1 = max(tidal_start_dt, ecm_open_dt)
            overlap_end1 = min(datetime.datetime.combine(date_to_check, datetime.time.max), ecm_close_dt)
            if overlap_start1 < overlap_end1:
                final_windows.append({'start_time': overlap_start1.time(), 'end_time': overlap_end1.time()})
            # Part 2: From midnight to tidal_end_dt (if on same logical day for tidal event)
            # This part is tricky; for simplicity, we only take what falls on 'date_to_check'.
            # True overnight windows would need more complex handling if a single job spans them.
            # Given our day-by-day scheduling, we primarily care about the portion on date_to_check.
        else: # Window is within the same day
            overlap_start_dt = max(tidal_start_dt, ecm_open_dt)
            overlap_end_dt = min(tidal_end_dt, ecm_close_dt)
            if overlap_start_dt < overlap_end_dt:
                final_windows.append({
                    'start_time': overlap_start_dt.time(),
                    'end_time': overlap_end_dt.time()
                })
    
    unique_final_windows = [] # Simple de-duplication if any identical windows resulted
    if final_windows:
        final_windows.sort(key=lambda x: x['start_time'])
        for fw in final_windows:
            if not unique_final_windows or unique_final_windows[-1] != fw:
                unique_final_windows.append(fw)
    return unique_final_windows

# --- Section 7 & 8 (Combined): Main Scheduling Algorithm - Core Logic Helpers ---
def get_suitable_trucks(boat_length_ft, preferred_truck_id=None):
    suitable_trucks_list = []
    if preferred_truck_id and preferred_truck_id in ECM_TRUCKS:
        truck = ECM_TRUCKS[preferred_truck_id]
        if not truck.is_crane and (truck.max_boat_length_ft is None or boat_length_ft <= truck.max_boat_length_ft):
            suitable_trucks_list.append(truck.truck_id)
    for truck_id, truck in ECM_TRUCKS.items():
        if truck.is_crane: continue
        if truck_id not in suitable_trucks_list:
            if truck.max_boat_length_ft is None or boat_length_ft <= truck.max_boat_length_ft:
                suitable_trucks_list.append(truck.truck_id)
    if not suitable_trucks_list: print(f"Warning: No suitable hauler for boat {boat_length_ft}ft.")
    return suitable_trucks_list

def check_truck_availability(truck_id_to_check, check_date, proposed_start_dt, proposed_end_dt):
    for job in SCHEDULED_JOBS:
        if job.scheduled_start_datetime is None or job.job_status != "Scheduled": continue
        job_date = job.scheduled_start_datetime.date()
        if job_date == check_date:
            existing_job_start_dt = job.scheduled_start_datetime
            existing_job_true_end_dt = job.scheduled_end_datetime # Default for hauler

            truck_is_involved = False
            if job.assigned_hauling_truck_id == truck_id_to_check:
                truck_is_involved = True
            elif job.assigned_crane_truck_id == truck_id_to_check and truck_id_to_check == "J17":
                truck_is_involved = True
                if job.j17_busy_end_datetime: # Use J17's specific end time
                    existing_job_true_end_dt = job.j17_busy_end_datetime
                # else J17 conflict logic might use job.scheduled_end_datetime if specific j17 time missing
            
            if truck_is_involved:
                if proposed_start_dt < existing_job_true_end_dt and proposed_end_dt > existing_job_start_dt:
                    # print(f"Debug: Conflict for {truck_id_to_check} with Job {job.job_id}. Proposed: {proposed_start_dt.time()}-{proposed_end_dt.time()} vs Existing: {existing_job_start_dt.time()}-{existing_job_true_end_dt.time()}")
                    return False # Conflict
    return True # No conflicts

# Mock location/distance helpers (from Section 8)
def get_last_scheduled_job_for_truck_on_date(truck_id, check_date):
    truck_jobs = [j for j in SCHEDULED_JOBS if j.scheduled_start_datetime and 
                  j.scheduled_start_datetime.date() == check_date and 
                  j.assigned_hauling_truck_id == truck_id and j.job_status == "Scheduled"]
    if not truck_jobs: return None
    truck_jobs.sort(key=lambda j: j.scheduled_start_datetime, reverse=True)
    return truck_jobs[0]

def determine_job_location_coordinates(endpoint_type, service_type, customer_obj, boat_obj, ramp_obj=None, other_address_details=None):
    # Simplified Mock - replace with actual logic or API calls
    if service_type == "Launch":
        return {"lat": customer_obj.home_latitude, "lon": customer_obj.home_longitude} if endpoint_type == "pickup" else \
               {"lat": ramp_obj.latitude, "lon": ramp_obj.longitude} if ramp_obj else {"lat":0,"lon":0}
    elif service_type == "Haul":
        if endpoint_type == "pickup":
            return {"lat": ramp_obj.latitude, "lon": ramp_obj.longitude} if ramp_obj else {"lat":0,"lon":0}
        else: # dropoff
            # Check if ECM boat being hauled to ECM base
            if boat_obj.is_ecm_boat: # Assumes boat.is_ecm_boat is correctly set
                return ECM_BASE_LOCATION
            return {"lat": customer_obj.home_latitude, "lon": customer_obj.home_longitude}
    elif service_type == "Transport": # Highly dependent on actual addresses
        return {"lat": customer_obj.home_latitude, "lon": customer_obj.home_longitude} # Defaulting to customer home
    return {"lat":0,"lon":0} # Default fallback

def calculate_distance_miles(loc1_coords, loc2_coords): # MOCK
    if not loc1_coords or not loc2_coords or None in loc1_coords.values() or None in loc2_coords.values(): return float('inf')
    # Very rough approximation for testing; replace with real distance calculation
    # return abs(loc1_coords['lat'] - loc2_coords['lat']) * 69 + abs(loc1_coords['lon'] - loc2_coords['lon']) * 50 
    # For specific testing of rules:
    if loc1_coords.get('mock_dist_type') == "far" or loc2_coords.get('mock_dist_type') == "far": return 25 # >12 and >10
    if loc1_coords.get('mock_dist_type') == "mid" or loc2_coords.get('mock_dist_type') == "mid": return 11 # <12 but >10
    if loc1_coords.get('mock_dist_type') == "close" or loc2_coords.get('mock_dist_type') == "close": return 5 # <12 and <10
    return 8 # Default "close enough" for many tests

def get_job_at_slot(check_date, check_time, check_truck_id=None):
    check_dt_start = datetime.datetime.combine(check_date, check_time)
    for job in SCHEDULED_JOBS:
        if job.job_status != "Scheduled": continue
        if job.scheduled_start_datetime == check_dt_start:
            if check_truck_id:
                if job.assigned_hauling_truck_id == check_truck_id or \
                   (job.assigned_crane_truck_id == check_truck_id and check_truck_id == "J17"):
                    return job
            else: return job # Any job at this time if no truck specified
    return None

def is_dropoff_at_ecm_base(dropoff_location_coords):
    if not dropoff_location_coords: return False
    return dropoff_location_coords.get('lat') == ECM_BASE_LOCATION['lat'] and \
           dropoff_location_coords.get('lon') == ECM_BASE_LOCATION['lon']

# (Ensure all previous class definitions, global data, and helper functions are present above this)
# ... (Keep all code from Section 1 through the helper functions in Section 7/8) ...

# --- Section 11 (Revised): find_available_job_slots (with "Collect More, Then Sort & Pick") ---

import datetime

# Assume all previously defined classes, functions, and global data are available
# (As listed in previous sections, including mock data and other helpers)
# Ensure ECM_RAMPS, get_customer_details, get_boat_details, get_suitable_trucks,
# check_truck_availability, get_last_scheduled_job_for_truck_on_date,
# determine_job_location_coordinates, calculate_distance_miles, get_job_at_slot,
# is_dropoff_at_ecm_base, get_ecm_operating_hours, get_final_schedulable_ramp_times
# are all defined and accessible before this function.

# For context:
TODAY_FOR_SIMULATION = datetime.date(2025, 6, 2) # Monday, June 2, 2025

def _check_and_create_slot_detail(current_search_date, current_potential_start_time_obj,
                                  truck_id, customer, boat, service_type, ramp_obj, 
                                  ecm_op_hours, job_duration_hours, needs_j17, 
                                  j17_actual_busy_duration_hours):
    """
    Internal helper to check a specific slot and return its details if valid.
    Returns a slot_detail dictionary or None.
    """
    proposed_start_dt = datetime.datetime.combine(current_search_date, current_potential_start_time_obj)
    proposed_end_dt_hauler = proposed_start_dt + datetime.timedelta(hours=job_duration_hours)

    if proposed_end_dt_hauler.time() > ecm_op_hours['close'] and not (proposed_end_dt_hauler.time() == ecm_op_hours['close'] and proposed_end_dt_hauler.date() == current_search_date):
        return None 

    hauler_available = check_truck_availability(truck_id, current_search_date, proposed_start_dt, proposed_end_dt_hauler)
    j17_available = True
    if needs_j17:
        j17_end_dt = proposed_start_dt + datetime.timedelta(hours=j17_actual_busy_duration_hours)
        j17_available = check_truck_availability("J17", current_search_date, proposed_start_dt, j17_end_dt)

    if not (hauler_available and j17_available):
        return None

    passes_rules = True; slot_type = "Open"; bumped_job_info = None
    if current_potential_start_time_obj > datetime.time(15, 30): passes_rules = False
    
    if passes_rules:
        last_job = get_last_scheduled_job_for_truck_on_date(truck_id, current_search_date)
        if last_job and last_job.scheduled_end_datetime < proposed_start_dt:
            prev_drop_coords = determine_job_location_coordinates("dropoff", last_job.service_type, get_customer_details(last_job.customer_id), get_boat_details(last_job.boat_id), ECM_RAMPS.get(last_job.dropoff_ramp_id or last_job.pickup_ramp_id), getattr(last_job, 'dropoff_loc_coords', None))
            current_pickup_coords = determine_job_location_coordinates("pickup", service_type, customer, boat, ramp_obj, getattr(original_job_request_details, 'transport_dropoff_details', None))
            distance = calculate_distance_miles(prev_drop_coords, current_pickup_coords)

            if current_potential_start_time_obj >= datetime.time(14,30) and (last_job.scheduled_end_datetime.time() < datetime.time(13,30) or distance > 10):
                passes_rules = False
            if passes_rules and service_type == "Transport" and last_job.service_type == "Transport" and distance > 12:
                passes_rules = False
    
    if not passes_rules:
        return None

    is_ecm_c = customer.is_ecm_customer; c_month = current_search_date.month
    is_spring_l = (service_type=="Launch" and c_month in [3,4,5,6] and is_ecm_c)
    is_fall_h_ecm = (service_type=="Haul" and c_month in [8,9,10,11] and is_ecm_c and is_dropoff_at_ecm_base(determine_job_location_coordinates("dropoff",service_type,customer,boat,ramp_obj)))
    target_morn_start = ecm_op_hours['open']
    if is_spring_l and current_potential_start_time_obj == target_morn_start:
        ex_job = get_job_at_slot(current_search_date, target_morn_start, truck_id)
        if ex_job and ex_job.customer_id != customer.customer_id and not get_customer_details(ex_job.customer_id).is_ecm_customer and ex_job.service_type == "Launch":
            slot_type, bumped_job_info = "BumpNonECM_SpringLaunch", {"job_id":ex_job.job_id, "customer_name":get_customer_details(ex_job.customer_id).customer_name}
            
    if is_fall_h_ecm and ramp_obj and current_potential_start_time_obj >= datetime.time(13,0):
        ex_job = get_job_at_slot(current_search_date, current_potential_start_time_obj, truck_id)
        if ex_job and ex_job.customer_id != customer.customer_id and not get_customer_details(ex_job.customer_id).is_ecm_customer and ex_job.service_type == "Haul":
            slot_type, bumped_job_info = "BumpNonECM_FallHaul", {"job_id":ex_job.job_id, "customer_name":get_customer_details(ex_job.customer_id).customer_name}
            
    return {'date': current_search_date, 'time': current_potential_start_time_obj, 'truck_id': truck_id, 
            'j17_needed': needs_j17, 'type': slot_type, 'bumped_job_details': bumped_job_info,
            'customer_name': customer.customer_name, 'boat_details_summary': f"{boat.length_ft}ft {boat.boat_type}"}

def find_available_job_slots(customer_id, boat_id, service_type, requested_date_str,
                             selected_ramp_id=None, transport_dropoff_details=None,
                             start_after_slot_details=None):
    global original_job_request_details
    original_job_request_details = {'transport_dropoff_details': transport_dropoff_details, 
                                    'customer_id': customer_id, 'boat_id': boat_id,
                                    'service_type': service_type}
    
    # --- Initial Setup & Search Window ---
    today = TODAY_FOR_SIMULATION
    try: 
        requested_date_obj = datetime.datetime.strptime(requested_date_str, '%Y-%m-%d').date()
    except ValueError: return [], "Error: Invalid date format."
    
    customer = get_customer_details(customer_id)
    boat = get_boat_details(boat_id)
    if not customer or not boat: return [], "Error: Invalid Customer/Boat ID."

    # ... (height check and peak month notices) ...
    
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
    
    job_duration_hours = 3.0 if boat.boat_type in ["Sailboat MD","Sailboat MT"] else 1.5
    needs_j17 = boat.boat_type in ["Sailboat MD","Sailboat MT"]
    j17_actual_busy_duration_hours = 0
    if boat.boat_type == "Sailboat MD": j17_actual_busy_duration_hours = 1.0
    elif boat.boat_type == "Sailboat MT": j17_actual_busy_duration_hours = 1.5
    suitable_truck_ids = get_suitable_trucks(boat.length_ft, customer.preferred_truck_id)
    if not suitable_truck_ids: return [], "Error: No suitable trucks."

    collected_slots = []
    
    # --- Phase 0: J17 Co-location Search ---
    if needs_j17 and service_type in ["Launch", "Haul"] and selected_ramp_id and not start_after_slot_details:
        print(f"  Phase 0: Searching for J17 co-location opportunities at Ramp {selected_ramp_id}...")
        co_location_window_start = requested_date_obj - datetime.timedelta(days=14)
        if co_location_window_start < today: co_location_window_start = today
        co_location_window_end = requested_date_obj + datetime.timedelta(days=14)

        j17_engagements = {}
        for job in SCHEDULED_JOBS:
            if job.assigned_crane_truck_id == "J17" and job.scheduled_start_datetime and job.job_status == "Scheduled":
                job_date = job.scheduled_start_datetime.date()
                job_ramp_id = job.pickup_ramp_id or job.dropoff_ramp_id
                if co_location_window_start <= job_date <= co_location_window_end and job_ramp_id == selected_ramp_id:
                    j17_engagements[(job_date, job_ramp_id)] = True
        
        for job_date, ramp_id in sorted(list(j17_engagements.keys())):
            if len(collected_slots) >= 3: break
            ecm_op_hours = get_ecm_operating_hours(job_date)
            if not ecm_op_hours: continue
            if boat.boat_type in ["Sailboat MD", "Sailboat MT"] and job_date.weekday() == 5: continue
            
            ramp_obj = ECM_RAMPS.get(ramp_id)
            if not ramp_obj: continue
            
            daily_windows = get_final_schedulable_ramp_times(ramp_obj, boat, job_date)
            if not daily_windows: continue

            for truck_id in suitable_truck_ids:
                if len(collected_slots) >= 3: break
                for window in daily_windows:
                    if len(collected_slots) >= 3: break
                    potential_time = window['start_time']
                    while potential_time < window['end_time']:
                        temp_dt = datetime.datetime.combine(job_date, potential_time)
                        if temp_dt.minute not in [0, 30]:
                            if temp_dt.minute < 30: temp_dt = temp_dt.replace(minute=30, second=0, microsecond=0)
                            else: temp_dt = (temp_dt + datetime.timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
                        potential_time = temp_dt.time()
                        if potential_time >= window['end_time']: break
                        
                        slot_detail = _check_and_create_slot_detail(job_date, potential_time, truck_id, customer, boat, service_type, ramp_obj, ecm_op_hours, job_duration_hours, needs_j17, j17_actual_busy_duration_hours)
                        if slot_detail:
                            slot_detail['type'] = f"J17-Optimized ({slot_detail['type']})"
                            if not any(s['date'] == slot_detail['date'] and s['time'] == slot_detail['time'] and s['truck_id'] == slot_detail['truck_id'] for s in collected_slots):
                                collected_slots.append(slot_detail)
                                if len(collected_slots) >= 3: break
                        
                        potential_time = (datetime.datetime.combine(datetime.date.min, potential_time) + datetime.timedelta(minutes=30)).time()

    # --- Phase 1 & 2: Main Chronological Search ---
    if len(collected_slots) < 3:
        MAX_POOL_SIZE = 15 # Collect a larger pool for better sorting
        current_search_date = effective_search_start_date
        days_iterated = 0
        while current_search_date <= search_end_limit_date and len(collected_slots) < MAX_POOL_SIZE and days_iterated < 60:
            ecm_op_hours = get_ecm_operating_hours(current_search_date)
            if not ecm_op_hours:
                current_search_date += datetime.timedelta(days=1); days_iterated += 1; continue
            if boat.boat_type in ["Sailboat MD", "Sailboat MT"] and current_search_date.weekday() == 5:
                current_search_date += datetime.timedelta(days=1); days_iterated += 1; continue
            
            daily_windows = []; ramp_obj = None
            if service_type in ["Launch", "Haul"]:
                ramp_obj = ECM_RAMPS.get(selected_ramp_id)
                if not ramp_obj: break
                daily_windows = get_final_schedulable_ramp_times(ramp_obj, boat, current_search_date)
            elif service_type == "Transport":
                daily_windows = [{'start_time': ecm_op_hours['open'], 'end_time': ecm_op_hours['close']}]
            if not daily_windows:
                current_search_date += datetime.timedelta(days=1); days_iterated += 1; continue
                
            for truck_id in suitable_truck_ids:
                if len(collected_slots) >= MAX_POOL_SIZE: break
                for window in daily_windows:
                    if len(collected_slots) >= MAX_POOL_SIZE: break
                    iter_start_time = window['start_time']
                    if current_search_date == effective_search_start_date and min_start_time_on_first_day and min_start_time_on_first_day > iter_start_time:
                        iter_start_time = min_start_time_on_first_day
                    
                    potential_time = iter_start_time
                    while potential_time < window['end_time']:
                        if len(collected_slots) >= MAX_POOL_SIZE: break
                        temp_dt = datetime.datetime.combine(current_search_date, potential_time)
                        if temp_dt.minute not in [0,30]:
                            if temp_dt.minute < 30: temp_dt = temp_dt.replace(minute=30, second=0, microsecond=0)
                            else: temp_dt = (temp_dt + datetime.timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
                        potential_time = temp_dt.time()
                        if potential_time >= window['end_time']: break
                        
                        if not any(s['date'] == current_search_date and s['time'] == potential_time and s['truck_id'] == truck_id for s in collected_slots):
                            slot_detail = _check_and_create_slot_detail(current_search_date, potential_time, truck_id, customer, boat, service_type, ramp_obj, ecm_op_hours, job_duration_hours, needs_j17, j17_actual_busy_duration_hours)
                            if slot_detail:
                                collected_slots.append(slot_detail)
                        
                        potential_time = (datetime.datetime.combine(datetime.date.min, potential_time) + datetime.timedelta(minutes=30)).time()
            
            if current_search_date == effective_search_start_date: min_start_time_on_first_day = None
            current_search_date += datetime.timedelta(days=1); days_iterated += 1
            
    # --- Final Processing ---
    if not collected_slots: return [], "No suitable slots found within the search window."
    
    def sort_priority(slot):
        if "J17-Optimized" in slot['type']: return (0, slot['date'], slot['time'])
        return (1, slot['time'], slot['date'])
    
    collected_slots.sort(key=sort_priority)
    final_slots_to_present = collected_slots[:3]
    
    return final_slots_to_present, f"Showing top {len(final_slots_to_present)} slots (J17/Early-Day prioritized)."

# --- Section 10 (Revisited): confirm_and_schedule_job ---
def confirm_and_schedule_job(original_job_request_details, selected_slot_info):
    global JOB_ID_COUNTER, SCHEDULED_JOBS
    # print(f"\nAttempting to confirm: {selected_slot_info['type']}")
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

    # Simplified location details for job object
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
    success_msg = f"SUCCESS: Job {new_job_id} for {customer.customer_name} scheduled for {format_time_for_display(new_job.scheduled_start_datetime.time())} on {new_job.scheduled_start_datetime.date()}."
    final_msg = f"{success_msg} {bump_notification}".strip()
    # print(final_msg)
    return new_job.job_id, final_msg


# --- Section 9 (Detailed Implementation): prepare_daily_schedule_data ---
# This replaces your current placeholder for this function.

def _mark_slots_in_grid(schedule_grid_truck_col, time_slots_dt_list, 
                        job_actual_start_dt, job_actual_end_dt, 
                        job_display_text, slot_status, job_id_for_ref,
                        time_increment_minutes): # Added time_increment_minutes as direct arg

# This is the body for _mark_slots_in_grid
    # Ensure this block is indented correctly under your def _mark_slots_in_grid(...) line
    """
    Internal helper to mark time slots in a specific truck's column as busy/potential.
    """
    job_marked_as_started = False
    for i, slot_start_dt in enumerate(time_slots_dt_list):
        # Calculate the end of the current display slot
        slot_end_dt = slot_start_dt + datetime.timedelta(minutes=time_increment_minutes)

        # Check for overlap: current slot starts before job ends AND current slot ends after job starts
        if slot_start_dt < job_actual_end_dt and slot_end_dt > job_actual_start_dt:
            # This slot is covered by the job
            schedule_grid_truck_col[i]["status"] = slot_status
            schedule_grid_truck_col[i]["job_id"] = job_id_for_ref
            if not job_marked_as_started:
                schedule_grid_truck_col[i]["display_text"] = job_display_text
                schedule_grid_truck_col[i]["is_start_of_job"] = True
                job_marked_as_started = True
            else:
                schedule_grid_truck_col[i]["display_text"] = " | | " # Continuation marker
                schedule_grid_truck_col[i]["is_start_of_job"] = False

def prepare_daily_schedule_data(display_date, 
                                original_job_request_details_for_potential=None, 
                                potential_job_slot_info=None, 
                                time_increment_minutes=30):
    # ... full implementation of prepare_daily_schedule_data ...
    return output_data

    # 2. Initialize Grid Data Structure
    for truck_col_id in output_data["truck_columns"]:
        output_data["schedule_grid"][truck_col_id] = [
            {"status": "free", "job_id": None, "display_text": "", "is_start_of_job": False} 
            for _ in range(num_time_slots)
        ]

    # 3. Populate with Existing Confirmed Jobs
    for job in SCHEDULED_JOBS: # Assumes SCHEDULED_JOBS is the global list of Job objects
        if job.scheduled_start_datetime and \
           job.scheduled_start_datetime.date() == display_date and \
           job.job_status == "Scheduled":
            
            customer = get_customer_details(job.customer_id)
            boat = get_boat_details(job.boat_id)
            
            cust_name = customer.customer_name if customer else f"CustID {job.customer_id}"
            boat_info = f"{boat.length_ft}ft {boat.boat_type}" if boat else "N/A"
            # Example: "Seth Ohm => 30' Bear's Island"
            # For simplicity, using a generic display text for now.
            # You can customize this based on job.service_type, job.pickup_desc, job.dropoff_desc
            job_text = f"{cust_name} - {boat_info} ({job.service_type})"


            # Mark for Hauling Truck
            if job.assigned_hauling_truck_id in output_data["schedule_grid"]:
                _mark_slots_in_grid(
                    output_data["schedule_grid"][job.assigned_hauling_truck_id],
                    time_slots_datetime_objects,
                    job.scheduled_start_datetime,
                    job.scheduled_end_datetime, # Hauler's end time
                    job_text,
                    slot_status="busy",
                    job_id_for_ref=job.job_id
                )

            # Mark for J17 Crane
            if job.assigned_crane_truck_id == "J17" and job.j17_busy_end_datetime:
                if "J17" in output_data["schedule_grid"]: # Ensure J17 column exists
                    _mark_slots_in_grid(
                        output_data["schedule_grid"]["J17"],
                        time_slots_datetime_objects,
                        job.scheduled_start_datetime, # J17 starts with the job
                        job.j17_busy_end_datetime,    # J17's specific busy end time
                        job_text, # Could be "J17 for Job X" or similar
                        slot_status="busy",
                        job_id_for_ref=job.job_id
                    )
    
    # 4. Incorporate the "Potential" New Job (if provided)
    if potential_job_slot_info and original_job_request_details_for_potential:
        pot_date = potential_job_slot_info['date']
        # Ensure potential job is for the display_date
        if pot_date == display_date:
            pot_start_time_obj = potential_job_slot_info['time']
            pot_start_dt = datetime.datetime.combine(display_date, pot_start_time_obj)
            
            pot_customer = get_customer_details(original_job_request_details_for_potential['customer_id'])
            pot_boat = get_boat_details(original_job_request_details_for_potential['boat_id'])

            if pot_customer and pot_boat:
                pot_hauler_duration_hours = 3.0 if pot_boat.boat_type in ["Sailboat MD", "Sailboat MT"] else 1.5
                pot_hauler_end_dt = pot_start_dt + datetime.timedelta(hours=pot_hauler_duration_hours)
                
                pot_j17_needed = potential_job_slot_info['j17_needed']
                pot_j17_end_dt = None
                if pot_j17_needed:
                    j17_busy_hours = 0
                    if pot_boat.boat_type == "Sailboat MD": j17_busy_hours = 1.0
                    elif pot_boat.boat_type == "Sailboat MT": j17_busy_hours = 1.5
                    if j17_busy_hours > 0:
                        pot_j17_end_dt = pot_start_dt + datetime.timedelta(hours=j17_busy_hours)

                potential_job_text = f"POTENTIAL: {pot_customer.customer_name} - {pot_boat.length_ft}ft {pot_boat.boat_type} ({original_job_request_details_for_potential['service_type']})"
                potential_job_id = "POTENTIAL_JOB" # A unique identifier for this potential job

                # Mark for Potential Hauling Truck
                hauler_truck_id = potential_job_slot_info['truck_id']
                if hauler_truck_id in output_data["schedule_grid"]:
                    _mark_slots_in_grid(
                        output_data["schedule_grid"][hauler_truck_id],
                        time_slots_datetime_objects,
                        pot_start_dt,
                        pot_hauler_end_dt,
                        potential_job_text,
                        slot_status="potential",
                        job_id_for_ref=potential_job_id
                    )
                
                # Mark for Potential J17 Crane
                if pot_j17_needed and pot_j17_end_dt:
                    if "J17" in output_data["schedule_grid"]:
                         _mark_slots_in_grid(
                            output_data["schedule_grid"]["J17"],
                            time_slots_datetime_objects,
                            pot_start_dt,
                            pot_j17_end_dt,
                            potential_job_text, # Or "J17 for Potential Job"
                            slot_status="potential",
                            job_id_for_ref=potential_job_id
                        )
    return output_data

    # CORRECTED INDENTATION: These class definitions should NOT be inside the 'if Truck not in globals()' block.
    # They should be at the same level if they are also meant to be potentially re-defined mocks
    # for this specific testing block.
    # However, it's better if the __main__ block assumes the main classes at the top of the file are already defined.

    # For the purpose of THIS __main__ block, if you need to ensure these classes exist
    # because you might be running this snippet in isolation, then they should be defined
    # at this level of indentation, similar to Truck. But it's usually cleaner if __main__
    # assumes the main script's classes are available.

    # Let's assume for this block, if Truck wasn't defined, these others probably weren't either.
    # This is purely for making the __main__ block self-sufficient if run in isolation.
    # In your main ecm_scheduler_logic.py, these classes are defined ONCE at the top.

    if 'Ramp' not in globals(): # Example of how you might do it for all
        class Ramp:
            def __init__(self, ramp_id, ramp_name, town, tide_rule_description,
                         tide_calculation_method, noaa_station_id,
                         tide_offset_hours1=None, tide_offset_hours2=None,
                         draft_restriction_ft=None, draft_restriction_tide_rule=None,
                         allowed_boat_types="Power and Sail", ramp_fee=None, operating_notes=None,
                         latitude=None, longitude=None):
                self.ramp_id = ramp_id
                self.ramp_name = ramp_name
                # ... (rest of Ramp __init__ attributes) ...
                self.town = town
                self.tide_rule_description = tide_rule_description
                self.tide_calculation_method = tide_calculation_method
                self.noaa_station_id = noaa_station_id
                self.tide_offset_hours1 = tide_offset_hours1
                self.tide_offset_hours2 = tide_offset_hours2 if tide_offset_hours2 is not None else self.tide_offset_hours1
                self.draft_restriction_ft = draft_restriction_ft
                self.draft_restriction_tide_rule = draft_restriction_tide_rule
                self.allowed_boat_types = allowed_boat_types
                self.ramp_fee = ramp_fee
                self.operating_notes = operating_notes
                self.latitude = latitude
                self.longitude = longitude


    if 'Customer' not in globals():
        class Customer:
            def __init__(self, customer_id, customer_name,
                         home_latitude=None, home_longitude=None,
                         preferred_truck_id=None, is_ecm_customer=False, is_safe_harbor_customer=False):
                self.customer_id = customer_id
                # ... (rest of Customer __init__ attributes) ...
                self.customer_name = customer_name
                self.home_latitude = home_latitude
                self.home_longitude = home_longitude
                self.preferred_truck_id = preferred_truck_id
                self.is_ecm_customer = is_ecm_customer
                self.is_safe_harbor_customer = is_safe_harbor_customer


    if 'Boat' not in globals():
        class Boat:
            def __init__(self, boat_id, customer_id, boat_type, length_ft,
                         draft_ft=None, height_ft_keel_to_highest=None, keel_type=None, is_ecm_boat=None):
                self.boat_id = boat_id
                # ... (rest of Boat __init__ attributes) ...
                self.customer_id = customer_id
                self.boat_type = boat_type
                self.length_ft = length_ft
                self.draft_ft = draft_ft
                self.height_ft_keel_to_highest = height_ft_keel_to_highest
                self.keel_type = keel_type
                self._is_ecm_boat_direct = is_ecm_boat

            @property
            def is_ecm_boat(self):
                if self._is_ecm_boat_direct is not None:
                    return self._is_ecm_boat_direct
                # This get_customer_details inside the mock __main__ would need a mock version too
                # or rely on a globally defined one if this __main__ is part of the larger script.
                # For simplicity, if this is a truly isolated __main__ test for just one function,
                # you might hardcode this property or mock get_customer_details here too.
                # customer = get_customer_details(self.customer_id) 
                # return customer.is_ecm_customer if customer else False
                return False # Simplified for isolated mock

    if 'Job' not in globals():
        class Job:
            def __init__(self, job_id, customer_id, boat_id, service_type, requested_date,
                         scheduled_start_datetime=None, calculated_job_duration_hours=None,
                         scheduled_end_datetime=None, 
                         assigned_hauling_truck_id=None,
                         assigned_crane_truck_id=None, 
                         j17_busy_end_datetime=None,   
                         # ... (rest of Job __init__ parameters and assignments) ...
                         pickup_ramp_id=None, pickup_street_address=None,
                         dropoff_ramp_id=None, dropoff_street_address=None,
                         job_status="Pending", notes=None,
                         pickup_loc_coords=None, dropoff_loc_coords=None): 
                self.job_id = job_id
                self.customer_id = customer_id
                self.boat_id = boat_id
                self.service_type = service_type
                # ... (all other assignments) ...
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


            def __repr__(self):
                # ... (as defined previously, ensure format_time_for_display is available or mock it) ...
                # For this mock, let's simplify the __repr__ to avoid external dependencies
                return f"Job(ID: {self.job_id}, Cust: {self.customer_id}, Svc: {self.service_type}, Status: {self.job_status})"

    if 'OperatingHoursEntry' not in globals():
        class OperatingHoursEntry:
            def __init__(self, rule_id, season, day_of_week, open_time, close_time, notes=None):
                self.rule_id = rule_id
                # ... (rest of OperatingHoursEntry __init__ assignments) ...
                self.season = season
                self.day_of_week = day_of_week
                self.open_time = open_time
                self.close_time = close_time
                self.notes = notes

            # def __repr__(self): ... (simplified or ensure format_time_for_display)

            def __repr__(self):
                # ... (as defined previously) ...
                return (f"OpHours(Season: {self.season}, Day: {self.day_of_week}, "
                        f"Open: {format_time_for_display(self.open_time)}, Close: {format_time_for_display(self.close_time)})") # Assumes format_time_for_display is available
        
    SCHEDULED_JOBS = [] # Reset for test
    ECM_TRUCKS = { "S20/33": Truck("S20/33", "S20", 60), "S21/77": Truck("S21/77", "S21", 50), "S23/55": Truck("S23/55","S23",30), "J17": Truck("J17", "J17", None, True) }
    ALL_CUSTOMERS = { 1: Customer(1, "Test Customer 1"), 2: Customer(2, "Sailboat Customer") }
    ALL_BOATS = { 101: Boat(101, 1, "Powerboat", 30), 102: Boat(102, 2, "Sailboat MD", 40) }
    operating_hours_rules = [ OperatingHoursEntry(1, "Standard", 0, datetime.time(8,0), datetime.time(16,0)) ] # Mon 8-4
    def get_ecm_operating_hours(d): # Simplified
        if d.weekday() == 0: return {"open": datetime.time(8,0), "close": datetime.time(16,0)}
        return None
    def format_time_for_display(t): return t.strftime('%I:%M %p').lstrip('0') if isinstance(t, datetime.time) else "Invalid"
    def get_customer_details(cid): return ALL_CUSTOMERS.get(cid)
    def get_boat_details(bid): return ALL_BOATS.get(bid)
    # --- End Mocks ---

    # Add a sample scheduled job
    job_start = datetime.datetime(2025, 6, 2, 9, 0) # Monday June 2nd, 2025 at 9:00 AM
    SCHEDULED_JOBS.append(
        Job(job_id=1001, customer_id=1, boat_id=101, service_type="Launch", requested_date=datetime.date(2025,6,2),
            scheduled_start_datetime=job_start,
            calculated_job_duration_hours=1.5,
            scheduled_end_datetime=job_start + datetime.timedelta(hours=1.5),
            assigned_hauling_truck_id="S20/33",
            job_status="Scheduled"
        )
    )
    # Add a sailboat job involving J17
    job2_start = datetime.datetime(2025, 6, 2, 13, 0) # 1:00 PM
    SCHEDULED_JOBS.append(
        Job(job_id=1002, customer_id=2, boat_id=102, service_type="Haul", requested_date=datetime.date(2025,6,2),
            scheduled_start_datetime=job2_start,
            calculated_job_duration_hours=3.0, # Sailboat
            scheduled_end_datetime=job2_start + datetime.timedelta(hours=3.0),
            assigned_hauling_truck_id="S21/77",
            assigned_crane_truck_id="J17",
            j17_busy_end_datetime=job2_start + datetime.timedelta(hours=1.0), # J17 busy for 1hr for MD
            job_status="Scheduled"
        )
    )

    test_display_date = datetime.date(2025, 6, 2) # Same day as scheduled jobs

    # Test without a potential job
    print(f"\n--- Daily Schedule Data for {test_display_date} (No Potential Job) ---")
    schedule_data_existing = prepare_daily_schedule_data(test_display_date, time_increment_minutes=30)
    # For a clean print of the grid data (which can be large):
    # import json
    # print(json.dumps(schedule_data_existing, indent=2, default=str))
    print(f"Date: {schedule_data_existing['display_date_str']}, Hours: {schedule_data_existing['operating_hours_display']}")
    print("Time Slots:", schedule_data_existing['time_slots_labels'])
    for truck, slots in schedule_data_existing['schedule_grid'].items():
        print(f"  Truck {truck}:")
        for i, slot_info in enumerate(slots):
            if slot_info['status'] != 'free':
                print(f"    {schedule_data_existing['time_slots_labels'][i]}: {slot_info['status']} - {slot_info.get('display_text','')} (Job ID: {slot_info.get('job_id')})")


    # Test with a potential job
    # Original request that led to this potential slot (needed for customer/boat details)
    mock_original_request = {
        'customer_id': 1, 'boat_id': 101, 'service_type': "Transport", 
        'requested_date_str': "2025-06-02" 
    }
    mock_potential_slot = { # This would come from find_available_job_slots output
        'date': test_display_date, 
        'time': datetime.time(11, 0), 
        'truck_id': "S23/55", 
        'j17_needed': False, 
        'type': "Open", # Not used directly by prepare_daily_schedule_data, but good for context
        'customer_name': "Olivia (Non-ECM)", # This info is already in the slot from find_available_job_slots
        'boat_details_summary': "28ft Powerboat"
    }
    print(f"\n--- Daily Schedule Data for {test_display_date} (With Potential Job at {format_time_for_display(mock_potential_slot['time'])}) ---")
    schedule_data_potential = prepare_daily_schedule_data(test_display_date, 
                                                          original_job_request_details_for_potential=mock_original_request,
                                                          potential_job_slot_info=mock_potential_slot,
                                                          time_increment_minutes=30)
    # print(json.dumps(schedule_data_potential, indent=2, default=str))
    print(f"Date: {schedule_data_potential['display_date_str']}, Hours: {schedule_data_potential['operating_hours_display']}")
    for truck, slots in schedule_data_potential['schedule_grid'].items():
        print(f"  Truck {truck}:")
        for i, slot_info in enumerate(slots):
            if slot_info['status'] != 'free':
                 print(f"    {schedule_data_potential['time_slots_labels'][i]}: {slot_info['status']} - {slot_info.get('display_text','')} (Job ID: {slot_info.get('job_id')})")


# --- Main example for testing flow (Streamlit would call these functions) ---
if __name__ == '__main__':
    print(f"--- ECM Scheduler Logic Test ---")
    print(f"Simulating for date: {TODAY_FOR_SIMULATION.strftime('%Y-%m-%d %A')}")
    SCHEDULED_JOBS.clear() # Start with no jobs for a clean test run

    # Define a sample request
    job_request_1 = {
        'customer_id': 2, # James (ECM Priority)
        'boat_id': 102,     # Sailboat MD, 40ft
        'service_type': "Launch",
        'requested_date_str': "2025-06-09", # A Monday in June
        'selected_ramp_id': "PlymouthHarbor"
    }

    # 1. Find initial slots
    print("\n1. Finding initial slots...")
    slots_batch_1, msg1 = find_available_job_slots(**job_request_1)
    print(msg1)
    for slot in slots_batch_1: print(f"   - {slot}")

    if slots_batch_1:
        # 2. Simulate user "rolling forward"
        print("\n2. Rolling forward for more slots...")
        last_slot_b1 = slots_batch_1[-1]
        start_after_b1 = {'date': last_slot_b1['date'], 'time': last_slot_b1['time'], 'truck_id': last_slot_b1['truck_id']}
        slots_batch_2, msg2 = find_available_job_slots(**job_request_1, start_after_slot_details=start_after_b1)
        print(msg2)
        for slot in slots_batch_2: print(f"   - {slot}")

        # 3. Simulate user selecting a slot (e.g., the first one from batch 1)
        selected_slot_to_book = slots_batch_1[0]
        print(f"\n3. User selected slot: {selected_slot_to_book}")

        # 4. Conceptual: User would see a daily schedule preview
        #    (In Streamlit, you'd call prepare_daily_schedule_data and render it)
        print("\n4. Generating data for daily schedule preview (conceptual)...")
        preview_data = prepare_daily_schedule_data(selected_slot_to_book['date'], potential_job_details=selected_slot_to_book)
        print(f"   Preview data for {preview_data['display_date_str']}: Operating {preview_data['operating_hours_display']}")
        # print(f"   (Grid data: {preview_data['schedule_grid']})") # Potentially very verbose

        # 5. User confirms the job
        print("\n5. Confirming the selected job...")
        new_job_id, confirm_msg = confirm_and_schedule_job(job_request_1, selected_slot_to_book)
        print(confirm_msg)

        print("\n--- Current Scheduled Jobs ---")
        for job in SCHEDULED_JOBS:
            print(f"   - {job}")
        
        # 6. Try to find slots for another job to see if the first one blocks correctly
        job_request_2 = {
            'customer_id': 1, # Olivia (Non-ECM)
            'boat_id': 101,     # Powerboat, 28ft
            'service_type': "Launch",
            'requested_date_str': selected_slot_to_book['date'].strftime('%Y-%m-%d'), # Same day as first job
            'selected_ramp_id': "PlymouthHarbor"
        }
        print("\n6. Finding slots for a second job on the same day...")
        slots_for_req2, msg_req2 = find_available_job_slots(**job_request_2)
        print(msg_req2)
        for slot in slots_for_req2: print(f"   - {slot}")

    else:
        print("No initial slots found to proceed with further tests.")
