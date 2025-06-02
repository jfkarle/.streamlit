# app.py
# Your main Streamlit application file

import streamlit as st
import datetime
import ecm_scheduler_logic as ecm # Your logic file

# --- Page Configuration (Optional, but good practice) ---
st.set_page_config(layout="wide") # Use wide mode for more space

# --- Initialize Session State Variables ---
# This should be done early in the script, after imports.
if 'suggested_slot_history' not in st.session_state:
    st.session_state.suggested_slot_history = [] # Will be a list of batches
if 'current_batch_index' not in st.session_state:
    st.session_state.current_batch_index = -1 # -1 means no valid batch displayed
if 'current_job_request_details' not in st.session_state:
    st.session_state.current_job_request_details = None
if 'slot_for_confirmation_preview' not in st.session_state:
    st.session_state.slot_for_confirmation_preview = None
if 'no_more_slots_forward' not in st.session_state: # Flag if roll forward finds nothing
    st.session_state.no_more_slots_forward = False
# --- End Session State Initialization ---

st.title("ECM Boat Hauling - Availability Scheduler")

# --- Sidebar for Inputs ---
st.sidebar.header("New Job Request")

# Get Inputs for a Job Request using Streamlit widgets in the sidebar
# ... (your customer_id_input, boat_id_input, service_type_input, etc. go here) ...
# (As defined in the previous app.py example I provided)
customer_id_input = st.sidebar.number_input("Enter Customer ID:", min_value=1, value=1, step=1,
                                            help="Refer to mock ALL_CUSTOMERS in logic file for IDs (e.g., 1 for Olivia, 2 for James)")
boat_id_input = st.sidebar.number_input("Enter Boat ID:", min_value=101, value=101, step=1,
                                        help="Refer to mock ALL_BOATS in logic file for IDs (e.g., 101 for Olivia's boat, 102 for James's)")
service_type_options = ["Launch", "Haul", "Transport"]
service_type_input = st.sidebar.selectbox("Select Service Type:", service_type_options)
default_requested_date = ecm.TODAY_FOR_SIMULATION + datetime.timedelta(days=7)
requested_date_input = st.sidebar.date_input("Requested Date:", value=default_requested_date)
selected_ramp_id_input = None
if service_type_input in ["Launch", "Haul"]:
    ramp_options = list(ecm.ECM_RAMPS.keys()) 
    if ramp_options:
        selected_ramp_id_input = st.sidebar.selectbox("Select Ramp:", ramp_options)
    else:
        st.sidebar.warning("No ramps defined in ecm_scheduler_logic.py")
        selected_ramp_id_input = st.sidebar.text_input("Enter Ramp ID (if known):")
transport_dropoff_input = None
if service_type_input == "Transport":
    transport_dropoff_input = st.sidebar.text_input("Transport Dropoff Address (Optional Info):", placeholder="e.g., 123 Other St, Town")
st.sidebar.markdown("---")


# --- Button to Find Slots (Initial Search) ---
# ... (your logic for the "Find Available Slots" button, which uses and sets session_state variables) ...
if st.sidebar.button("Find Available Slots", key="find_initial_slots"):
    st.session_state.suggested_slot_history = []
    st.session_state.current_batch_index = -1
    st.session_state.slot_for_confirmation_preview = None
    st.session_state.no_more_slots_forward = False 

    st.session_state.current_job_request_details = {
        'customer_id': customer_id_input, 
        'boat_id': boat_id_input,
        'service_type': service_type_input,
        'requested_date_str': requested_date_input.strftime('%Y-%m-%d'),
        'selected_ramp_id': selected_ramp_id_input,
        'transport_dropoff_details': {'address': transport_dropoff_input} if transport_dropoff_input else None
    }

    slots, message = ecm.find_available_job_slots(
        **st.session_state.current_job_request_details,
        start_after_slot_details=None
    )
    if slots:
        st.session_state.suggested_slot_history.append(slots)
        st.session_state.current_batch_index = 0
    st.info(message)


# --- Navigation Buttons (Roll Back / Roll Forward) ---
# ... (your logic for these buttons, which uses and sets session_state variables) ...
col_nav1, col_nav2 = st.sidebar.columns(2)
can_roll_back = st.session_state.current_batch_index > 0
if col_nav1.button("Prev. 3 Slots (Roll Back)", key="roll_back", disabled=not can_roll_back):
    st.session_state.current_batch_index -= 1
    st.session_state.slot_for_confirmation_preview = None 
    st.session_state.no_more_slots_forward = False 
can_roll_forward = st.session_state.current_batch_index != -1 and not st.session_state.no_more_slots_forward
if col_nav2.button("Next 3 Slots (Roll Forward)", key="roll_forward", disabled=not can_roll_forward):
    current_batch = st.session_state.suggested_slot_history[st.session_state.current_batch_index]
    if not current_batch: 
        st.warning("No current slots to roll forward from.")
    else:
        if st.session_state.current_batch_index < len(st.session_state.suggested_slot_history) - 1:
            st.session_state.current_batch_index += 1
        else: 
            last_slot = current_batch[-1]
            start_after_details = {
                'date': last_slot['date'],
                'time': last_slot['time'],
                'truck_id': last_slot['truck_id']
            }
            slots, message = ecm.find_available_job_slots(
                **st.session_state.current_job_request_details,
                start_after_slot_details=start_after_details
            )
            if slots:
                st.session_state.suggested_slot_history.append(slots)
                st.session_state.current_batch_index += 1
            else:
                st.session_state.no_more_slots_forward = True 
            st.info(message)
        st.session_state.slot_for_confirmation_preview = None 


# --- Display Suggested Slots ---
# ... (your logic to display slots, which reads from session_state) ...
if st.session_state.current_batch_index != -1 and st.session_state.suggested_slot_history:
    current_slots_to_display = st.session_state.suggested_slot_history[st.session_state.current_batch_index]
    st.subheader("Suggested Slots:")
    if not current_slots_to_display and st.session_state.no_more_slots_forward:
         st.write("No further slots available with the current criteria.")
    elif not current_slots_to_display: 
         st.write("No slots to display for this batch.")

    for i, slot in enumerate(current_slots_to_display):
        col1_disp, col2_disp, col3_disp = st.columns([4, 2, 2]) # Renamed to avoid conflict
        slot_time_str = ecm.format_time_for_display(slot['time'])
        date_str = slot['date'].strftime('%Y-%m-%d %A')
        truck_info = f"Truck: {slot['truck_id']}"
        if slot['j17_needed']: truck_info += " with J17"
        bump_info = f" (Potential Bump of Job ID: {slot['bumped_job_details']['job_id']} for {slot['bumped_job_details']['customer_name']})" if slot['type'] != "Open" and slot['bumped_job_details'] else ""
        
        col1_disp.write(f"**Option {st.session_state.current_batch_index * 3 + i + 1}:** {date_str} at **{slot_time_str}**")
        col1_disp.write(f"   {truck_info} - Type: {slot['type']}{bump_info}")
        col1_disp.caption(f"   Customer: {slot['customer_name']}, Boat: {slot['boat_details_summary']}")

        if col2_disp.button(f"Preview & Confirm Slot {st.session_state.current_batch_index * 3 + i + 1}", key=f"select_slot_batch_{st.session_state.current_batch_index}_item_{i}"):
            st.session_state.slot_for_confirmation_preview = slot
        st.markdown("---")


# --- Section to Display Schedule Preview and Confirm Job ---
# ... (your logic for preview and confirm, which reads from and sets session_state) ...
if st.session_state.slot_for_confirmation_preview:
    selected_for_preview = st.session_state.slot_for_confirmation_preview
    original_request = st.session_state.current_job_request_details
    st.subheader(f"Preview & Confirm Selection:")
    # ... (rest of preview and confirm logic from previous full app.py example) ...
    preview_time_str = ecm.format_time_for_display(selected_for_preview['time'])
    preview_date_str = selected_for_preview['date'].strftime('%Y-%m-%d %A')
    st.write(f"You are considering: **{preview_date_str} at {preview_time_str}** with Truck {selected_for_preview['truck_id']}")
    if selected_for_preview['j17_needed']: st.write("J17 Crane will also be assigned.")
    if selected_for_preview['type'] != "Open" and selected_for_preview['bumped_job_details']:
        st.warning(f"This selection will BUMP Job ID: {selected_for_preview['bumped_job_details']['job_id']} "
                   f"for customer '{selected_for_preview['bumped_job_details']['customer_name']}'.")
    st.write("Generating daily schedule preview data (raw output for now):")
    daily_schedule_preview_data = ecm.prepare_daily_schedule_data(
        display_date=selected_for_preview['date'],
        potential_job_details=selected_for_preview,
        time_increment_minutes=30 
    )
    st.json(daily_schedule_preview_data) 
    if st.button("CONFIRM THIS JOB", key="confirm_final_job"):
        new_job_id, message = ecm.confirm_and_schedule_job(
            original_job_request_details=original_request,
            selected_slot_info=selected_for_preview
        )
        if new_job_id:
            st.success(f"Job Confirmed! {message}")
            st.session_state.last_suggested_slots = []
            st.session_state.start_after_slot_for_next_search = None
            st.session_state.slot_for_confirmation_preview = None
        else:
            st.error(f"Failed to confirm job: {message}")
    if st.button("Cancel / Choose Another Slot", key="cancel_selection"):
        st.session_state.slot_for_confirmation_preview = None


# --- Optionally display all scheduled jobs (for testing) ---
# ... (your checkbox and display logic for ecm.SCHEDULED_JOBS) ...
if st.checkbox("Show All Currently Scheduled Jobs (In-Memory List for this Session)"):
    st.subheader("All Scheduled Jobs (Current Session):")
    if ecm.SCHEDULED_JOBS:
        for job_item in ecm.SCHEDULED_JOBS:
            st.text(str(job_item)) # Uses the __repr__ from the Job class
    else:
        st.write("No jobs scheduled in the current session yet.")
