# app.py
# FINAL VERSION with Multi-Slot Selection & Levers

import streamlit as st
import datetime
import ecm_scheduler_logic as ecm
import pandas as pd

st.set_page_config(layout="wide")

# --- Session State Initialization ---
if 'data_loaded' not in st.session_state:
    if ecm.load_customers_and_boats_from_csv("ECM Sample Cust.csv"):
        st.session_state.data_loaded = True
    else:
        st.session_state.data_loaded = False
        st.error("Failed to load customer and boat data.")

# Initialize session state variables for the multi-slot workflow
if 'info_message' not in st.session_state: st.session_state.info_message = ""
if 'current_job_request' not in st.session_state: st.session_state.current_job_request = None
if 'found_slots' not in st.session_state: st.session_state.found_slots = []
if 'selected_slot' not in st.session_state: st.session_state.selected_slot = None

# --- Main App Layout ---
st.title("ECM Boat Hauling - Availability Scheduler")

# Display and clear info messages at the top
if st.session_state.info_message:
    st.info(st.session_state.info_message)
    st.session_state.info_message = ""

# --- Sidebar for Job Request ---
st.sidebar.header("New Job Request")

# --- 1. Customer Name Search ---
customer_name_search_input = st.sidebar.text_input("Enter Customer Name (or part of it):",
                                                   help="e.g., Olivia, James, Tho")
selected_customer_id = None
selected_customer_obj = None
customer_search_results = []
if customer_name_search_input:
    if st.session_state.get('data_loaded', False):
        for cust_id, cust_obj in ecm.LOADED_CUSTOMERS.items():
            if cust_obj.customer_name and customer_name_search_input.lower() in cust_obj.customer_name.lower():
                customer_search_results.append(cust_obj)
    else:
        st.error("Customer data is not loaded. Cannot perform search.")

    if customer_search_results:
        if len(customer_search_results) == 1:
            selected_customer_obj = customer_search_results[0]
            selected_customer_id = selected_customer_obj.customer_id
            st.sidebar.success(f"Selected: {selected_customer_obj.customer_name}")
        else:
            customer_options = {cust.customer_name: cust.customer_id for cust in customer_search_results}
            chosen_customer_name = st.sidebar.selectbox("Multiple matches found, please select:",
                                                        options=list(customer_options.keys()))
            if chosen_customer_name:
                selected_customer_id = customer_options[chosen_customer_name]
                selected_customer_obj = ecm.get_customer_details(selected_customer_id)
    elif customer_name_search_input:
        st.sidebar.warning("No customer found matching that name.")

# --- 2. Automatically Get Boat & Display Details ---
selected_boat_id = None
selected_boat_obj = None
if selected_customer_id:
    customer_boats = [boat for boat_id, boat in ecm.LOADED_BOATS.items() if boat.customer_id == selected_customer_id]
    if customer_boats:
        selected_boat_obj = customer_boats[0]
        selected_boat_id = selected_boat_obj.boat_id
        st.sidebar.markdown("---")
        st.sidebar.subheader("Selected Customer & Boat:")
        st.sidebar.write(f"**Customer:** {selected_customer_obj.customer_name}")
        st.sidebar.write(f"**ECM Boat:** {'Yes' if selected_customer_obj.is_ecm_customer else 'No'}")
        st.sidebar.write(f"**Boat Type:** {selected_boat_obj.boat_type}")
        st.sidebar.write(f"**Boat Length:** {selected_boat_obj.boat_length}ft")
        # Display preferred truck
        truck_name = "N/A"
        if selected_customer_obj.preferred_truck_id:
            truck = ecm.ECM_TRUCKS.get(selected_customer_obj.preferred_truck_id)
            if truck: truck_name = truck.truck_name
        st.sidebar.write(f"**Preferred Truck:** {truck_name}")
        st.sidebar.markdown("---")
    else:
        st.sidebar.error(f"No boat found for customer: {selected_customer_obj.customer_name}")

if selected_customer_id and selected_boat_id:
    service_type_input = st.sidebar.selectbox("Select Service Type:", ["Launch", "Haul", "Transport"])
    default_date = ecm.TODAY_FOR_SIMULATION + datetime.timedelta(days=7)
    requested_date_input = st.sidebar.date_input("Requested Date:", value=default_date)
    selected_ramp_id_input = None
    if service_type_input in ["Launch", "Haul"]:
        ramp_options = list(ecm.ECM_RAMPS.keys())
        selected_ramp_id_input = st.sidebar.selectbox("Select Ramp:", ramp_options)
    
    st.sidebar.markdown("---")

    # --- Phase 1: Strict Search Button ---
    if st.sidebar.button("Find Best Slot (Strict)", key="find_strict"):
        job_request = {
            'customer_id': selected_customer_obj.customer_id,
            'boat_id': selected_boat_obj.boat_id,
            'service_type': service_type_input,
            'requested_date_str': requested_date_input.strftime('%Y-%m-%d'),
            'selected_ramp_id': selected_ramp_id_input,
        }
        st.session_state.current_job_request = job_request

        # ADD THIS: Define your operating hours. This is correct.
        operating_hours = {'open': datetime.time(9, 0), 'close': datetime.time(17, 0)}

        # This is your corrected line. It's perfect.
        slots, message, _ = ecm.find_available_job_slots(**job_request,
                                                         force_preferred_truck=True,
                                                         relax_ramp_constraint=False,
                                                         ecm_op_hours=operating_hours)
        
        # --- FIX #1: DELETE THE DUPLICATE LINE BELOW ---
        # slots, message, _ = ecm.find_available_job_slots(**job_request, force_preferred_truck=True, relax_ramp_constraint=False)
        
        st.session_state.info_message = message
        st.session_state.found_slots = slots
        st.session_state.selected_slot = None
        st.rerun()

    # --- Levers for Alternative Search ---
    st.sidebar.markdown("---")
    st.sidebar.subheader("Not soon enough? Widen your search:")
    
    relax_truck_input = st.sidebar.checkbox("Relax Truck Constraint (use any suitable truck)", key="relax_truck")
    relax_ramp_input = st.sidebar.checkbox("Relax Ramp Constraint (search nearby ramps)", key="relax_ramp")

    if st.sidebar.button("Find Alternatives", key="find_relaxed"):
        # We need a current job request to find alternatives for
        if st.session_state.current_job_request:
            
            # You need the operating hours here as well!
            operating_hours = {'open': datetime.time(9, 0), 'close': datetime.time(17, 0)}

            # --- FIX #2: ADD 'ecm_op_hours' TO THIS FUNCTION CALL ---
            slots, message, _ = ecm.find_available_job_slots(
                **st.session_state.current_job_request, 
                force_preferred_truck=(not relax_truck_input), 
                relax_ramp_constraint=relax_ramp_input,
                ecm_op_hours=operating_hours  # <--- ADD THIS ARGUMENT
            )
            st.session_state.info_message = message
            st.session_state.found_slots = slots
            st.session_state.selected_slot = None # Reset selection
            st.rerun()
        else:
            st.sidebar.warning("Please find a strict slot first before searching for alternatives.")

# --- Main Area for Displaying Results and Confirmation ---

# This function will be called when a user clicks a "Select" button
def handle_slot_selection(slot_data):
    """Sets the chosen slot into the session state for confirmation."""
    st.session_state.selected_slot = slot_data

# --- Phase 2: Display Multiple Slot Options ---
if st.session_state.found_slots and not st.session_state.selected_slot:
    st.subheader("Please select your preferred slot:")
    
    # Create columns for a card-like layout
    cols = st.columns(3)
    
    for i, slot in enumerate(st.session_state.found_slots):
        col = cols[i % 3] # Cycle through the columns for layout
        with col:
            wwith st.container(border=True):
                # ... date, time, truck, ramp ...
                st.markdown(f"**Truck:** {truck_id}")
                if ramp_name != "N/A":
                    st.markdown(f"**Ramp:** {ramp_name}")

                # --- ADD THESE TWO LINES ---
                if slot.get('tide_rule_concise'):
                    st.markdown(f"**Tide Rule:** {slot['tide_rule_concise']}")
                if slot.get('high_tide_info'):
                    st.markdown(f"**{slot['high_tide_info']}**")
                
                # Button to select this specific slot
                st.button("Select this slot", key=f"select_slot_{i}", on_click=handle_slot_selection, args=(slot,))

                st.markdown(f"**Date:** {date_str}")
                st.markdown(f"**Time:** {time_str}")
                st.markdown(f"**Truck:** {truck_id}")
                if ramp_name != "N/A":
                    st.markdown(f"**Ramp:** {ramp_name}")
                
                # Button to select this specific slot
                st.button("Select this slot", key=f"select_slot_{i}", on_click=handle_slot_selection, args=(slot,))
    st.markdown("---")


# --- Phase 3: Display Confirmation Section for the CHOSEN Slot ---
if st.session_state.selected_slot:
    slot = st.session_state.selected_slot
    original_request = st.session_state.current_job_request
    
    slot_time_str = ecm.format_time_for_display(slot.get('time'))
    date_str = slot.get('date').strftime('%Y-%m-%d %A')
    
    st.subheader(f"Preview & Confirm Selection:")
    st.success(f"You are considering: **{date_str} at {slot_time_str}** with Truck **{slot.get('truck_id')}**.")
    if slot.get('j17_needed'):
        st.write("J17 Crane will also be assigned.")
    
    if st.button("CONFIRM THIS JOB", key="confirm_final_job"):
        new_job_id, message = ecm.confirm_and_schedule_job(original_job_request_details=original_request, selected_slot_info=slot)
        if new_job_id:
            st.success(f"Job Confirmed! {message}")
            # Clear all state variables for a fresh start
            st.session_state.found_slots = []
            st.session_state.selected_slot = None
            st.session_state.current_job_request = None
            st.rerun()
        else:
            st.error(f"Failed to confirm job: {message}")

# Handle case where no slots were found
elif st.session_state.get('current_job_request') and not st.session_state.found_slots:
    if st.session_state.info_message:
        st.warning(st.session_state.info_message)

# --- Display All Scheduled Jobs (your existing logic) ---
st.markdown("---")
if st.checkbox("Show All Currently Scheduled Jobs (In-Memory List for this Session)"):
    st.subheader("All Scheduled Jobs (Current Session):")
    if ecm.SCHEDULED_JOBS:
        # Helper function to add suffix to day (e.g., 1st, 2nd, 3rd, 4th)
        def get_day_with_suffix(d):
            return str(d) + ("th" if 11 <= d <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(d % 10, "th"))

        # Prepare a list of dictionaries with formatted and ordered data
        display_data = []
        for job in sorted(ecm.SCHEDULED_JOBS, key=lambda j: (j.scheduled_start_datetime is None, j.scheduled_start_datetime)):
            customer = ecm.get_customer_details(job.customer_id)
            
            if job.scheduled_start_datetime:
                day_str = get_day_with_suffix(job.scheduled_start_datetime.day)
                date_formatted = job.scheduled_start_datetime.strftime(f"%B {day_str}, %Y")
                time_formatted = ecm.format_time_for_display(job.scheduled_start_datetime.time())
            else:
                date_formatted = "Not Scheduled"
                time_formatted = "N/A"
            
            ramp_name = "N/A"
            ramp_id = job.pickup_ramp_id or job.dropoff_ramp_id
            if ramp_id:
                ramp = ecm.get_ramp_details(ramp_id)
                if ramp:
                    ramp_name = ramp.ramp_name

            # Define the structure and order of our columns for each job
            display_data.append({
                "Job ID": job.job_id,
                "Status": job.job_status,
                "Scheduled Date": date_formatted,
                "Scheduled Time": time_formatted,
                "Service": job.service_type,
                "Customer": customer.customer_name if customer else "N/A",
                "Truck": job.assigned_hauling_truck_id,
                "Crane": "Yes" if job.assigned_crane_truck_id else "No",
                "Ramp": ramp_name,
                "Notes": job.notes
            })
        
        # Create a pandas DataFrame from our prepared list
        df = pd.DataFrame(display_data)
        
        # Display the DataFrame in Streamlit, which respects the column order
        st.dataframe(df, use_container_width=True)
    else:
        st.write("No jobs scheduled in the current session yet.")
