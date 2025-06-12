# app.py
# Your main Streamlit application file

import streamlit as st
import datetime
import csv
import ecm_scheduler_logic as ecm # Your logic file

st.set_page_config(layout="wide")

if 'data_loaded' not in st.session_state: # Simple flag to load only once per session
    if ecm.load_customers_and_boats_from_csv("ECM Sample Cust.csv"): # Use your actual filename
        st.session_state.data_loaded = True
        # Populate customer choices for the UI if needed here, or do it dynamically
    else:
        st.session_state.data_loaded = False
        st.error("Failed to load customer and boat data. Please check the CSV file and logs.")

# --- Initialize Session State Variables ---

if 'info_message' not in st.session_state: st.session_state.info_message = ""
if 'suggested_slot_history' not in st.session_state:
    st.session_state.suggested_slot_history = []
if 'current_batch_index' not in st.session_state:
    st.session_state.current_batch_index = -1
if 'current_job_request_details' not in st.session_state:
    st.session_state.current_job_request_details = None
if 'slot_for_confirmation_preview' not in st.session_state:
    st.session_state.slot_for_confirmation_preview = None
if 'no_more_slots_forward' not in st.session_state:
    st.session_state.no_more_slots_forward = False

# ... (st.set_page_config, session state initializations as before) ...

st.title("ECM Boat Hauling - Availability Scheduler")
st.sidebar.header("New Job Request")

# --- INSERT THE MESSAGE BLOCK HERE ---
# This block checks for a message on every rerun and displays it at the top.
if st.session_state.get('info_message'):
    st.info(st.session_state.info_message)
    st.session_state.info_message = "" # Clear the message after displaying it once
# --- END OF INSERTED BLOCK ---

# --- 1. Customer Name Search ---
customer_name_search_input = st.sidebar.text_input("Enter Customer Name (or part of it):", 
                                                   help="e.g., Olivia, James, Tho")

selected_customer_id = None
selected_customer_obj = None
customer_search_results = []

# ... (after imports and session state initialization in app.py) ...

# --- Load data at the start of the session (ensure this is called) ---
if 'data_loaded' not in st.session_state:
    if ecm.load_customers_and_boats_from_csv("ECM Sample Cust.csv"): # Or your actual CSV filename
        st.session_state.data_loaded = True
    else:
        st.session_state.data_loaded = False
        st.error("Failed to load customer and boat data. Please check CSV and logs.")
# ...

# --- In your Customer Name Search section ---
# ...
if customer_name_search_input:
    if st.session_state.get('data_loaded', False): # Check if data was loaded successfully
        # Search for customers (case-insensitive) from LOADED_CUSTOMERS
        for cust_id, cust_obj in ecm.LOADED_CUSTOMERS.items(): # <<< CHANGE HERE
            if cust_obj.customer_name and customer_name_search_input.lower() in cust_obj.customer_name.lower():
                customer_search_results.append(cust_obj)
        # ... (rest of your logic for handling search results)
    else:
        st.error("Customer data is not loaded. Cannot perform search.")
# ...
    
    if customer_search_results:
        if len(customer_search_results) == 1:
            selected_customer_obj = customer_search_results[0]
            selected_customer_id = selected_customer_obj.customer_id
            st.sidebar.success(f"Selected: {selected_customer_obj.customer_name}")
        else:
            # If multiple matches, provide a way to select one
            customer_options = {cust.customer_name: cust.customer_id for cust in customer_search_results}
            chosen_customer_name = st.sidebar.selectbox("Multiple matches found, please select:", 
                                                        options=list(customer_options.keys()))
            if chosen_customer_name:
                selected_customer_id = customer_options[chosen_customer_name]
                selected_customer_obj = ecm.get_customer_details(selected_customer_id) # Get the full object
    elif customer_name_search_input: # Input was typed but no results
        st.sidebar.warning("No customer found matching that name.")

# --- 2. Automatically Get Boat & Display Details (if customer is selected) ---
selected_boat_id = None
selected_boat_obj = None

if selected_customer_id: # Proceed only if a customer was successfully identified earlier
    # selected_customer_obj should already be defined from your customer search logic part
    
    customer_boats = [boat for boat_id, boat in ecm.LOADED_BOATS.items() if boat.customer_id == selected_customer_id]

# Sailboat MT BUG debug // In app.py

# --- 2. Automatically Get Boat & Display Details (if customer is selected) ---
selected_boat_id = None
selected_boat_obj = None

if selected_customer_id: # Proceed only if a customer was successfully identified earlier
    customer_boats = [boat for boat_id, boat in ecm.LOADED_BOATS.items() if boat.customer_id == selected_customer_id]

    # --- INSERT DEBUG BLOCK HERE ---
    with st.sidebar.expander("Show Debug Info for Boat Search"):
        st.write(f"Searching for boats for Customer ID: `{selected_customer_id}`")
        st.write(f"Found `{len(customer_boats)}` boat(s) for this customer.")
        if customer_boats:
            st.write("Details of found boat(s):")
            st.json([boat.__dict__ for boat in customer_boats]) # Show all data for the found boat(s)
    # --- END DEBUG BLOCK ---

    if customer_boats:
        selected_boat_obj = customer_boats[0] # Take the first boat
        selected_boat_id = selected_boat_obj.boat_id
        
        # ... (rest of the display logic as before) ...
    
    if customer_boats:
        selected_boat_obj = customer_boats[0] # Take the first boat
        selected_boat_id = selected_boat_obj.boat_id
        
        # Display details since we found a boat
        st.sidebar.markdown("---") # Separator before details
        st.sidebar.subheader("Selected Customer & Boat:")
        st.sidebar.write(f"**Customer:** {selected_customer_obj.customer_name}")
        ecm_status_str = "Yes" if selected_customer_obj.is_ecm_customer else "No"
        st.sidebar.write(f"**ECM Boat:** {ecm_status_str}")
        st.sidebar.write(f"**Boat Type:** {selected_boat_obj.boat_type}")
        st.sidebar.write(f"**Boat Length:** {selected_boat_obj.boat_length}ft")
        
        preferred_truck_id = selected_customer_obj.preferred_truck_id
        preferred_truck_name = "N/A" 

        if preferred_truck_id:
            truck_object = ecm.ECM_TRUCKS.get(preferred_truck_id) 
            if truck_object:
                preferred_truck_name = truck_object.truck_name 
            else:
                preferred_truck_name = f"Unknown Truck ID: {preferred_truck_id}" 
        
        st.sidebar.write(f"**Preferred Truck:** {preferred_truck_name}")
        st.sidebar.markdown("---") # Separator after details

    else: # No boats found for this selected customer
        st.sidebar.error(f"No boat found for customer: {selected_customer_obj.customer_name}")
        # You might want a separator here too, or handle it as part of an overall section structure
        # st.sidebar.markdown("---")

# --- Service Type, Requested Date, Ramp (Inputs remain similar) ---
service_type_options = ["Launch", "Haul", "Transport"]
service_type_input = st.sidebar.selectbox("Select Service Type:", service_type_options)

if selected_customer_id and selected_boat_id: # Only show these if we have a customer/boat
    default_requested_date = ecm.TODAY_FOR_SIMULATION + datetime.timedelta(days=7)
    requested_date_input = st.sidebar.date_input("Requested Date:", value=default_requested_date)

    selected_ramp_id_input = None
    if service_type_input in ["Launch", "Haul"]:
        ramp_options = list(ecm.ECM_RAMPS.keys()) 
        if ramp_options:
            selected_ramp_id_input = st.sidebar.selectbox("Select Ramp:", ramp_options, index=0)
        # ... (else for no ramps) ...
    
    transport_dropoff_input = None # As before
    if service_type_input == "Transport":
        transport_dropoff_input = st.sidebar.text_input("Transport Dropoff Address (Optional Info):", placeholder="e.g., 123 Other St, Town")
    
    st.sidebar.markdown("---")

# MODIFIED: "Find Available Slots" button logic completely updated
if st.sidebar.button("Find Best Slot (Strict)", key="find_strict"):
    if not selected_customer_id or not selected_boat_id:
        st.warning("Please select a customer and boat first.")
    else:
        # Clear previous results
        st.session_state.slot_for_confirmation_preview = None
        
        # Prepare job details for the logic function
        job_request_details = {
            'customer_id': selected_customer_id,
            'boat_id': selected_boat_id,
            'service_type': service_type_input,
            'requested_date_str': requested_date_input.strftime('%Y-%m-%d'),
            'selected_ramp_id': selected_ramp_id_input,
            'transport_dropoff_details': {'address': transport_dropoff_input} if transport_dropoff_input else None
        }
        
        # Call the logic function with STRICT lever settings
        slots, message, debug_log = ecm.find_available_job_slots(
            **job_request_details,
            force_preferred_truck=True,
            relax_ramp_constraint=False
        )
        
        # Store the single result (if any) for preview
        if slots:
            st.session_state.slot_for_confirmation_preview = slots[0]
            st.session_state.current_job_request_details = job_request_details
        
        st.session_state.info_message = message
        st.rerun()

    st.write("Generating daily schedule preview data (raw output for now):") # This line was already here
    daily_schedule_preview_data = ecm.prepare_daily_schedule_data(
        display_date=selected_for_preview['date'],
        # MODIFIED ARGUMENT NAME TO MATCH FUNCTION DEFINITION
        original_job_request_details_for_potential=original_request, 
        potential_job_slot_info=selected_for_preview,             
        time_increment_minutes=30
    )
    st.json(daily_schedule_preview_data) # This line was already here
    if st.button("CONFIRM THIS JOB", key="confirm_final_job"):
        # ... (rest of your confirmation logic) ...
        new_job_id, message = ecm.confirm_and_schedule_job(
            original_job_request_details=original_request,
            selected_slot_info=selected_for_preview
        )
        if new_job_id:
            st.success(f"Job Confirmed! {message}")
            st.session_state.suggested_slot_history = [] 
            st.session_state.current_batch_index = -1    
            st.session_state.slot_for_confirmation_preview = None 
            st.session_state.no_more_slots_forward = False 
            st.rerun()
        else:
            st.error(f"Failed to confirm job: {message}")
    if st.button("Cancel / Choose Another Slot", key="cancel_selection"):
        st.session_state.slot_for_confirmation_preview = None
        st.rerun()

# --- Optionally display all scheduled jobs (for testing) ---
if st.checkbox("Show All Currently Scheduled Jobs (In-Memory List for this Session)"):
    st.subheader("All Scheduled Jobs (Current Session):")
    if ecm.SCHEDULED_JOBS: # Assuming SCHEDULED_JOBS is accessible via ecm module
        
        display_data_for_table = []
        # Sort jobs by scheduled start time for consistent display
        sorted_jobs = sorted(
            [job for job in ecm.SCHEDULED_JOBS if job.scheduled_start_datetime], # Filter out jobs without a start time
            key=lambda j: j.scheduled_start_datetime
        )

        for job in sorted_jobs:
            customer = ecm.get_customer_details(job.customer_id)
            boat = ecm.get_boat_details(job.boat_id)
            
            customer_name = customer.customer_name if customer else "N/A"
            boat_type = boat.boat_type if boat else "N/A"
            boat_length = f"{boat.boat_length}ft" if boat and hasattr(boat, 'boat_length') else "N/A" # Added hasattr check
            
            destination = "N/A"
            # Determine destination based on service type
            if job.service_type == "Launch" and job.dropoff_ramp_id:
                ramp = ecm.get_ramp_details(job.dropoff_ramp_id)
                destination = ramp.ramp_name if ramp else job.dropoff_ramp_id
            elif job.dropoff_street_address: # For Haul or Transport if address is primary
                destination = job.dropoff_street_address
            elif job.service_type == "Haul" and job.dropoff_ramp_id: # Unlikely, Hauls usually to address
                 ramp = ecm.get_ramp_details(job.dropoff_ramp_id)
                 destination = ramp.ramp_name if ramp else job.dropoff_ramp_id

            relevant_high_tides_str = ""
            if job.service_type in ["Launch", "Haul"]:
                ramp_id_for_tide = job.pickup_ramp_id if job.service_type == "Haul" else job.dropoff_ramp_id
                if ramp_id_for_tide: # Ensure there is a ramp ID to check tides for
                    ramp_obj_for_tide = ecm.get_ramp_details(ramp_id_for_tide)
                    
                    if ramp_obj_for_tide and ramp_obj_for_tide.tide_calculation_method != "AnyTide":
                        job_date = job.scheduled_start_datetime.date()
                        # Ensure fetch_noaa_tides and get_ecm_operating_hours are robust for None returns
                        day_tides = ecm.fetch_noaa_tides(ramp_obj_for_tide.noaa_station_id, job_date)
                        ecm_op_hours_for_job_date = ecm.get_ecm_operating_hours(job_date)
                        
                        if day_tides and ecm_op_hours_for_job_date:
                            relevant_hts = []
                            ecm_open_dt_job = datetime.datetime.combine(job_date, ecm_op_hours_for_job_date['open'])
                            ecm_close_dt_job = datetime.datetime.combine(job_date, ecm_op_hours_for_job_date['close'])

                            for tide_event in day_tides:
                                if tide_event['type'] == 'H':
                                    ht_datetime = datetime.datetime.combine(job_date, tide_event['time'])
                                    
                                    offset1_hours = float(ramp_obj_for_tide.tide_offset_hours1 or 0)
                                    offset2_hours = float(ramp_obj_for_tide.tide_offset_hours2 or ramp_obj_for_tide.tide_offset_hours1 or 0)

                                    offset1 = datetime.timedelta(hours=offset1_hours)
                                    offset2 = datetime.timedelta(hours=offset2_hours)
                                    
                                    tidal_window_start = ht_datetime - offset1
                                    tidal_window_end = ht_datetime + offset2
                                    
                                    if max(tidal_window_start, ecm_open_dt_job) < min(tidal_window_end, ecm_close_dt_job):
                                        relevant_hts.append(ecm.format_time_for_display(tide_event['time']))
                            
                            if relevant_hts: relevant_high_tides_str = ", ".join(relevant_hts) + " HT"
                            else: relevant_high_tides_str = "No relevant HT in op hours"
                        elif not day_tides: relevant_high_tides_str = "Tide data N/A"
                        else: relevant_high_tides_str = "ECM Closed"
                    elif ramp_obj_for_tide and ramp_obj_for_tide.tide_calculation_method == "AnyTide":
                        relevant_high_tides_str = "Any Tide"
                    elif not ramp_obj_for_tide:
                        relevant_high_tides_str = "Ramp info N/A"
                else: # No ramp associated for tide check (e.g. Transport)
                    relevant_high_tides_str = "N/A (Transport)"


            job_info = {
                "Job ID": job.job_id,
                "Date": job.scheduled_start_datetime.strftime('%B %d, %Y'),
                "Start Time": ecm.format_time_for_display(job.scheduled_start_datetime.time()),
                "Customer": customer_name,
                "Boat Type": boat_type,
                "Length": boat_length,
                "Service": job.service_type,
                "Truck": job.assigned_hauling_truck_id,
                "J17": "Yes" if job.assigned_crane_truck_id else "No",
                "Destination/Location": destination, # Changed from "Destination" to match logbook context better for Launches too
                "Status": job.job_status,
                "Relevant High Tides": relevant_high_tides_str
            }
            display_data_for_table.append(job_info)
        
        if display_data_for_table:
            st.dataframe(display_data_for_table)
        else:
            st.write("No jobs with schedule details to display.") # Handles cases where SCHEDULED_JOBS might have unscheduled items
            
    else: # If ecm.SCHEDULED_JOBS is empty
        st.write("No jobs scheduled in the current session yet.")
