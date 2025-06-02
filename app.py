# app.py
# Your main Streamlit application file

import streamlit as st
import datetime
import ecm_scheduler_logic as ecm # This imports all our functions and classes

st.set_page_config(layout="wide") # Use wide mode for more space
st.title("ECM Boat Hauling - Availability Scheduler")

st.sidebar.header("New Job Request")

# --- Get Inputs for a Job Request using Streamlit widgets in the sidebar ---
# For now, you'll manually enter IDs. Later, these could be dropdowns from loaded data.
customer_id_input = st.sidebar.number_input("Enter Customer ID:", min_value=1, value=1, step=1,
                                            help="Refer to mock ALL_CUSTOMERS in logic file for IDs (e.g., 1 for Olivia, 2 for James)")
boat_id_input = st.sidebar.number_input("Enter Boat ID:", min_value=101, value=101, step=1,
                                        help="Refer to mock ALL_BOATS in logic file for IDs (e.g., 101 for Olivia's boat, 102 for James's)")

service_type_options = ["Launch", "Haul", "Transport"]
service_type_input = st.sidebar.selectbox("Select Service Type:", service_type_options)

# Use today's date (from the logic file) as a sensible default start for date input
default_requested_date = ecm.TODAY_FOR_SIMULATION + datetime.timedelta(days=7)
requested_date_input = st.sidebar.date_input("Requested Date:", value=default_requested_date)

selected_ramp_id_input = None
if service_type_input in ["Launch", "Haul"]:
    # Get ramp names from our ECM_RAMPS dictionary for the dropdown
    ramp_options = list(ecm.ECM_RAMPS.keys()) # e.g., ["PlymouthHarbor", "DuxburyHarbor", ...]
    if ramp_options:
        selected_ramp_id_input = st.sidebar.selectbox("Select Ramp:", ramp_options)
    else:
        st.sidebar.warning("No ramps defined in ecm_scheduler_logic.py")
        selected_ramp_id_input = st.sidebar.text_input("Enter Ramp ID (if known):") # Fallback if needed

# Placeholder for transport dropoff details (not used by find_available_job_slots directly yet for finding, but for confirming)
transport_dropoff_input = None
if service_type_input == "Transport":
    transport_dropoff_input = st.sidebar.text_input("Transport Dropoff Address (Optional Info):", placeholder="e.g., 123 Other St, Town")

st.sidebar.markdown("---") # Separator

# --- Session State for "Roll Forward" and selected slot ---
# Streamlit's session state persists data across reruns for a single user session.
if 'last_suggested_slots' not in st.session_state:
    st.session_state.last_suggested_slots = []
if 'start_after_slot_for_next_search' not in st.session_state:
    st.session_state.start_after_slot_for_next_search = None
if 'current_job_request_details' not in st.session_state:
    st.session_state.current_job_request_details = None
if 'slot_for_confirmation_preview' not in st.session_state:
    st.session_state.slot_for_confirmation_preview = None


# --- Button to Find Slots ---
if st.sidebar.button("Find Available Slots", key="find_initial_slots"):
    st.session_state.last_suggested_slots = [] # Clear previous results for a new search
    st.session_state.start_after_slot_for_next_search = None # Reset roll forward state
    st.session_state.slot_for_confirmation_preview = None # Clear any pending confirmation

    # Store current request details in session state
    st.session_state.current_job_request_details = {
        'customer_id': customer_id_input,
        'boat_id': boat_id_input,
        'service_type': service_type_input,
        'requested_date_str': requested_date_input.strftime('%Y-%m-%d'),
        'selected_ramp_id': selected_ramp_id_input,
        'transport_dropoff_details': {'address': transport_dropoff_input} if transport_dropoff_input else None
    }

    # Call the main scheduling function from our logic file
    slots, message = ecm.find_available_job_slots(
        customer_id=customer_id_input,
        boat_id=boat_id_input,
        service_type=service_type_input,
        requested_date_str=requested_date_input.strftime('%Y-%m-%d'),
        selected_ramp_id=selected_ramp_id_input,
        transport_dropoff_details={'address': transport_dropoff_input} if transport_dropoff_input else None,
        start_after_slot_details=None # Initial search
    )
    st.session_state.last_suggested_slots = slots
    st.info(message) # Display messages like "Found X slots" or "No slots found"

# --- Button for "Roll Forward" ---
if st.session_state.last_suggested_slots and len(st.session_state.last_suggested_slots) > 0: # Show only if there are slots to roll from
    if st.sidebar.button("Next 3 Slots (Roll Forward)", key="roll_forward"):
        if st.session_state.last_suggested_slots:
            last_slot = st.session_state.last_suggested_slots[-1]
            st.session_state.start_after_slot_for_next_search = {
                'date': last_slot['date'],
                'time': last_slot['time'],
                'truck_id': last_slot['truck_id'] # Important to try and avoid suggesting same time with different truck
            }
            
            # Re-call find_available_job_slots with the start_after_slot_details
            slots, message = ecm.find_available_job_slots(
                **st.session_state.current_job_request_details, # Unpack the stored request details
                start_after_slot_details=st.session_state.start_after_slot_for_next_search
            )
            st.session_state.last_suggested_slots = slots # Update with new batch
            st.info(message)
            st.session_state.slot_for_confirmation_preview = None # Clear preview if rolling forward
        else:
            st.warning("No previous slots to roll forward from.")


# --- Display Suggested Slots and Allow Selection for Preview/Confirmation ---
if st.session_state.last_suggested_slots:
    st.subheader("Suggested Slots:")
    for i, slot in enumerate(st.session_state.last_suggested_slots):
        col1, col2, col3 = st.columns([4, 2, 2]) # Create columns for layout
        
        slot_time_str = ecm.format_time_for_display(slot['time'])
        date_str = slot['date'].strftime('%Y-%m-%d %A')
        truck_info = f"Truck: {slot['truck_id']}"
        if slot['j17_needed']:
            truck_info += " with J17"
        
        bump_info = ""
        if slot['type'] != "Open" and slot['bumped_job_details']:
            bump_info = f" (Potential Bump of Job ID: {slot['bumped_job_details']['job_id']} for {slot['bumped_job_details']['customer_name']})"
        
        col1.write(f"**Option {i+1}:** {date_str} at **{slot_time_str}**")
        col1.write(f"   {truck_info} - Type: {slot['type']}{bump_info}")
        col1.caption(f"   Customer: {slot['customer_name']}, Boat: {slot['boat_details_summary']}")


        # Button to select a slot for preview / then confirmation
        if col2.button(f"Preview & Confirm Slot {i+1}", key=f"select_slot_{i}"):
            st.session_state.slot_for_confirmation_preview = slot
            # When this button is clicked, Streamlit re-runs. The section below will pick it up.

        st.markdown("---") # Separator between slots

# --- Section to Display Schedule Preview and Confirm Job ---
if st.session_state.slot_for_confirmation_preview:
    selected_for_preview = st.session_state.slot_for_confirmation_preview
    original_request = st.session_state.current_job_request_details

    st.subheader(f"Preview & Confirm Selection:")
    preview_time_str = ecm.format_time_for_display(selected_for_preview['time'])
    preview_date_str = selected_for_preview['date'].strftime('%Y-%m-%d %A')
    st.write(f"You are considering: **{preview_date_str} at {preview_time_str}** with Truck {selected_for_preview['truck_id']}")
    if selected_for_preview['j17_needed']: st.write("J17 Crane will also be assigned.")
    if selected_for_preview['type'] != "Open" and selected_for_preview['bumped_job_details']:
        st.warning(f"This selection will BUMP Job ID: {selected_for_preview['bumped_job_details']['job_id']} "
                   f"for customer '{selected_for_preview['bumped_job_details']['customer_name']}'.")

    # Call prepare_daily_schedule_data to get data for the visual preview
    # For now, just display the raw data structure to verify
    st.write("Generating daily schedule preview data (raw output for now):")
    daily_schedule_preview_data = ecm.prepare_daily_schedule_data(
        display_date=selected_for_preview['date'],
        potential_job_details=selected_for_preview, # Pass the selected slot as potential
        time_increment_minutes=30 # Or 15 if you prefer more granularity
    )
    st.json(daily_schedule_preview_data) # Display the raw JSON-like data from the function

    if st.button("CONFIRM THIS JOB", key="confirm_final_job"):
        new_job_id, message = ecm.confirm_and_schedule_job(
            original_job_request_details=original_request,
            selected_slot_info=selected_for_preview
        )
        if new_job_id:
            st.success(f"Job Confirmed! {message}")
            # Clear selections after confirmation
            st.session_state.last_suggested_slots = []
            st.session_state.start_after_slot_for_next_search = None
            st.session_state.slot_for_confirmation_preview = None
            # Optionally, display the updated SCHEDULED_JOBS list
            # st.write("Current Scheduled Jobs:", ecm.SCHEDULED_JOBS)
        else:
            st.error(f"Failed to confirm job: {message}")
    
    if st.button("Cancel / Choose Another Slot", key="cancel_selection"):
        st.session_state.slot_for_confirmation_preview = None
        # This will make the script re-run, and the preview section will disappear.

# --- Optionally display all scheduled jobs (for testing) ---
if st.checkbox("Show All Currently Scheduled Jobs (In-Memory List for this Session)"):
    st.subheader("All Scheduled Jobs (Current Session):")
    if ecm.SCHEDULED_JOBS:
        for job_item in ecm.SCHEDULED_JOBS:
            # Use the __repr__ from the Job class or format it nicely
            st.text(str(job_item))
    else:
        st.write("No jobs scheduled in the current session yet.")
