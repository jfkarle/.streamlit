# app.py
# FINAL VERSION with Lever-Based Scheduling

import streamlit as st
import datetime
import ecm_scheduler_logic as ecm

st.set_page_config(layout="wide")

# --- Session State Initialization ---
if 'data_loaded' not in st.session_state:
    if ecm.load_customers_and_boats_from_csv("ECM Sample Cust.csv"):
        st.session_state.data_loaded = True
    else:
        st.session_state.data_loaded = False
        st.error("Failed to load customer and boat data.")

# We only need three state variables for this workflow
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

# This is your full, detailed customer and boat selection logic. It should remain.
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

# --- Inputs for the Job Request ---
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
        
        slots, message, _ = ecm.find_available_job_slots(**job_request, force_preferred_truck=True, relax_ramp_constraint=False)
        
        st.session_state.info_message = message
        st.session_state.found_slot = slots[0] if slots else None
        st.rerun()

# --- Main Area for Displaying Results and Levers ---
if st.session_state.found_slot:
    slot = st.session_state.found_slot
    original_request = st.session_state.current_job_request
    
    st.subheader("Best Available Slot Found:")
    
    slot_time_str = ecm.format_time_for_display(slot.get('time'))
    date_str = slot.get('date').strftime('%Y-%m-%d %A')
    ramp_name = ecm.get_ramp_details(slot.get('ramp_id')).ramp_name if slot.get('ramp_id') else "N/A"
    
    st.success(f"**{date_str} at {slot_time_str}** with Truck **{slot.get('truck_id', 'N/A')}** at **{ramp_name}**.")
    st.markdown("---")

    # --- Phase 2: Display Levers and "Find Alternatives" button ---
    st.sidebar.markdown("---")
    st.sidebar.subheader("Not soon enough? Widen your search:")
    
    relax_truck = st.sidebar.checkbox("Relax Truck Constraint (use any suitable truck)", key="relax_truck")
    relax_ramp = st.sidebar.checkbox("Relax Ramp Constraint (search nearby ramps)", key="relax_ramp")

    if st.sidebar.button("Find Alternatives", key="find_relaxed"):
        if original_request:
            slots, message, _ = ecm.find_available_job_slots(**original_request, force_preferred_truck=(not relax_truck), relax_ramp_constraint=relax_ramp)
            st.session_state.info_message = message
            st.session_state.found_slot = slots[0] if slots else None
            st.rerun()

    # --- Confirmation Section ---
    st.subheader(f"Preview & Confirm Selection:")
    st.write(f"You are considering: **{date_str} at {slot_time_str}** with Truck {slot.get('truck_id')}")
    if slot.get('j17_needed'):
        st.write("J17 Crane will also be assigned.")
    
    if st.button("CONFIRM THIS JOB", key="confirm_final_job"):
        new_job_id, message = ecm.confirm_and_schedule_job(original_job_request_details=original_request, selected_slot_info=slot)
        if new_job_id:
            st.success(f"Job Confirmed! {message}")
            st.session_state.found_slot = None
            st.session_state.current_job_request = None
            st.rerun()
        else:
            st.error(f"Failed to confirm job: {message}")

elif st.session_state.get('current_job_request'):
    if st.session_state.info_message:
        st.warning(st.session_state.info_message)

# --- Display All Scheduled Jobs (your existing logic) ---
if st.checkbox("Show All Currently Scheduled Jobs (In-Memory List for this Session)"):
    st.subheader("All Scheduled Jobs (Current Session):")
    if ecm.SCHEDULED_JOBS:
        # (Your full, detailed dataframe display logic should be pasted here)
        st.dataframe([job.__dict__ for job in ecm.SCHEDULED_JOBS]) # A simplified display
    else:
        st.write("No jobs scheduled in the current session yet.")
