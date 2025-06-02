# app.py
# Your main Streamlit application file

import streamlit as st
import datetime
import ecm_scheduler_logic as ecm # Your logic file

# --- Page Configuration (Optional, but good practice) ---
st.set_page_config(layout="wide")

# --- Initialize Session State Variables ---
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

# --- DEBUG LINES SET #1 (You have this already) ---
st.sidebar.markdown("---")
st.sidebar.write(f"DEBUG Initial: Index: {st.session_state.current_batch_index}, History len: {len(st.session_state.suggested_slot_history)}")
st.sidebar.markdown("---")
# --- END DEBUG LINES SET #1 ---

st.title("ECM Boat Hauling - Availability Scheduler")

# --- Sidebar for Inputs ---
st.sidebar.header("New Job Request")
customer_id_input = st.sidebar.number_input("Enter Customer ID:", min_value=1, value=1, step=1, help="e.g., 1 for Olivia, 2 for James")
boat_id_input = st.sidebar.number_input("Enter Boat ID:", min_value=101, value=101, step=1, help="e.g., 101 for Olivia's boat, 102 for James's")
service_type_options = ["Launch", "Haul", "Transport"]
service_type_input = st.sidebar.selectbox("Select Service Type:", service_type_options)
# Ensure ecm.TODAY_FOR_SIMULATION is accessible; if app.py and ecm_scheduler_logic.py are separate,
# you might need to define a default here or ensure ecm module is fully loaded.
# Assuming ecm.TODAY_FOR_SIMULATION is available:
default_requested_date = ecm.TODAY_FOR_SIMULATION + datetime.timedelta(days=7)
requested_date_input = st.sidebar.date_input("Requested Date:", value=default_requested_date)
selected_ramp_id_input = None
if service_type_input in ["Launch", "Haul"]:
    ramp_options = list(ecm.ECM_RAMPS.keys()) # Assuming ecm.ECM_RAMPS is available
    if ramp_options:
        selected_ramp_id_input = st.sidebar.selectbox("Select Ramp:", ramp_options, index=0)
    else:
        st.sidebar.warning("No ramps defined.")
        selected_ramp_id_input = st.sidebar.text_input("Enter Ramp ID:")
transport_dropoff_input = None
if service_type_input == "Transport":
    transport_dropoff_input = st.sidebar.text_input("Transport Dropoff Address (Optional Info):", placeholder="e.g., 123 Other St, Town")
st.sidebar.markdown("---")


# --- Button to Find Slots (Initial Search) ---
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
    st.rerun()

# --- DEBUG LINES SET #3 (Before Navigation Buttons) ---
st.sidebar.markdown("---")
st.sidebar.write(f"DEBUG Before Nav: Index: {st.session_state.current_batch_index}, History len: {len(st.session_state.suggested_slot_history)}")
st.sidebar.markdown("---")
# --- END DEBUG LINES SET #3 ---

# --- Navigation Buttons (Roll Back / Roll Forward) ---
col_nav1, col_nav2 = st.sidebar.columns(2)

can_roll_back = st.session_state.current_batch_index > 0
if col_nav1.button("Prev. 3 Slots (Roll Back)", key="roll_back", disabled=not can_roll_back):
    st.session_state.current_batch_index -= 1
    st.session_state.slot_for_confirmation_preview = None
    st.session_state.no_more_slots_forward = False
    st.rerun()

can_roll_forward = st.session_state.current_batch_index != -1 and not st.session_state.no_more_slots_forward
if col_nav2.button("Next 3 Slots (Roll Forward)", key="roll_forward", disabled=not can_roll_forward):
    if not st.session_state.current_job_request_details:
        st.warning("Please perform an initial search first by clicking 'Find Available Slots'.")
    elif st.session_state.current_batch_index == -1 or not st.session_state.suggested_slot_history:
         st.warning("No current slots to roll forward from. Perform an initial search.")
    else:
        current_batch_for_roll = st.session_state.suggested_slot_history[st.session_state.current_batch_index]
        if not current_batch_for_roll:
            st.warning("No current slots in active batch to roll forward from.")
        else:
            if st.session_state.current_batch_index < len(st.session_state.suggested_slot_history) - 1:
                st.session_state.current_batch_index += 1
                # --- DEBUG LINES SET #2 (Variant A - Next batch was already in history) ---
                st.write(f"DEBUG RollForward (from history): Index now: {st.session_state.current_batch_index}, History len: {len(st.session_state.suggested_slot_history)}")
                # --- END DEBUG LINES SET #2 ---
            else:
                last_slot = current_batch_for_roll[-1]
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
                    # --- DEBUG LINES SET #2 (Variant B - New slots fetched) ---
                    st.write(f"DEBUG RollForward (new fetch): New slots found. Index now: {st.session_state.current_batch_index}, History len: {len(st.session_state.suggested_slot_history)}")
                    # --- END DEBUG LINES SET #2 ---
                else:
                    st.session_state.no_more_slots_forward = True
                    # --- DEBUG LINES SET #2 (Variant C - No new slots fetched) ---
                    st.write(f"DEBUG RollForward (new fetch): No new slots. Index remains: {st.session_state.current_batch_index}, History len: {len(st.session_state.suggested_slot_history)}")
                    # --- END DEBUG LINES SET #2 ---
                st.info(message) # Display message from find_available_job_slots
            st.session_state.slot_for_confirmation_preview = None
            st.rerun()

# --- Display Suggested Slots ---
# (Your existing logic to display slots based on st.session_state.current_batch_index and st.session_state.suggested_slot_history)
# (Make sure this section is robust enough to handle an empty current_slots_to_display list if no_more_slots_forward is true)
if st.session_state.current_batch_index != -1 and st.session_state.suggested_slot_history:
    # Check if current_batch_index is valid for the history list
    if st.session_state.current_batch_index < len(st.session_state.suggested_slot_history):
        current_slots_to_display = st.session_state.suggested_slot_history[st.session_state.current_batch_index]
        st.subheader("Suggested Slots:")
        if not current_slots_to_display: # Could be an empty list if find_available_slots returned empty for this batch
             if st.session_state.no_more_slots_forward:
                 st.write("No further slots available with the current criteria.")
             else: # This case should ideally not be hit if roll forward properly manages no_more_slots_forward
                 st.write("No slots to display for this batch, but more might be available.")

        for i, slot in enumerate(current_slots_to_display):
            col1_disp, col2_disp = st.columns([5, 2]) # Adjusted columns for better spacing
            slot_time_str = ecm.format_time_for_display(slot['time'])
            date_str = slot['date'].strftime('%Y-%m-%d %A')
            truck_info = f"Truck: {slot['truck_id']}"
            if slot['j17_needed']: truck_info += " with J17"
            bump_info = f" (Potential Bump of Job ID: {slot['bumped_job_details']['job_id']} for {slot['bumped_job_details']['customer_name']})" if slot['type'] != "Open" and slot['bumped_job_details'] else ""
            
            col1_disp.write(f"**Option {st.session_state.current_batch_index * 3 + i + 1}:** {date_str} at **{slot_time_str}**")
            col1_disp.write(f"   {truck_info} - Type: {slot['type']}{bump_info}")
            col1_disp.caption(f"   Customer: {slot.get('customer_name', 'N/A')}, Boat: {slot.get('boat_details_summary', 'N/A')}") # Use .get for safety

            if col2_disp.button(f"Preview & Confirm Slot {st.session_state.current_batch_index * 3 + i + 1}", key=f"select_slot_batch_{st.session_state.current_batch_index}_item_{i}"):
                st.session_state.slot_for_confirmation_preview = slot
                st.rerun() # Rerun to show the preview section immediately
            st.markdown("---")
    elif st.session_state.no_more_slots_forward:
        st.subheader("Suggested Slots:")
        st.write("No further slots available with the current criteria.")


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
