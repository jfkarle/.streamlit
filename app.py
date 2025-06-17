# app.py
# FINAL, CORRECTED AND VERIFIED VERSION

import streamlit as st
import datetime
import ecm_scheduler_logic as ecm
import pandas as pd

st.set_page_config(layout="wide")

# --- Helper Functions ---
def format_tides_for_display(slot, ecm_hours):
    tide_times = slot.get('high_tide_times', [])
    if not tide_times: return ""
    if not ecm_hours or not ecm_hours.get('open'):
        return "HT: " + " / ".join([ecm.format_time_for_display(t) for t in tide_times])

    op_open, op_close = ecm_hours['open'], ecm_hours['close']
    def get_tide_relevance_score(tide_time):
        tide_dt = datetime.datetime.combine(datetime.date.today(), tide_time)
        open_dt = datetime.datetime.combine(datetime.date.today(), op_open)
        close_dt = datetime.datetime.combine(datetime.date.today(), op_close)
        if open_dt <= tide_dt <= close_dt: return 0, abs((tide_dt - open_dt).total_seconds())
        return 1, min(abs((tide_dt - open_dt).total_seconds()), abs((tide_dt - close_dt).total_seconds()))

    sorted_tides = sorted(tide_times, key=get_tide_relevance_score)
    if not sorted_tides: return ""
    primary_tide_str = ecm.format_time_for_display(sorted_tides[0])
    if len(sorted_tides) == 1: return f"**HIGH TIDE: {primary_tide_str}**"
    secondary_tides_str = " / ".join([ecm.format_time_for_display(t) for t in sorted_tides[1:]])
    return f"**HIGH TIDE: {primary_tide_str}** (and {secondary_tides_str.lower()})"

def handle_slot_selection(slot_data):
    st.session_state.selected_slot = slot_data

# --- Session State Initialization ---
def initialize_session_state():
    defaults = {
        'data_loaded': False, 'info_message': "", 'current_job_request': None,
        'found_slots': [], 'selected_slot': None, 'search_requested_date': None,
        'was_forced_search': False
    }
    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value
    
    if not st.session_state.data_loaded:
        if ecm.load_customers_and_boats_from_csv("ECM Sample Cust.csv"):
            st.session_state.data_loaded = True
        else:
            st.error("Failed to load customer and boat data.")

# --- Main App Execution ---
initialize_session_state()
st.title("ECM Boat Hauling - Availability Scheduler")

# --- NAVIGATION SIDEBAR ---
st.sidebar.title("Navigation")
app_mode = st.sidebar.radio("Go to", ["Schedule New Boat", "Reporting", "Settings"])

# --- PAGE 1: SCHEDULER ---
if app_mode == "Schedule New Boat":
    if st.session_state.info_message:
        st.info(st.session_state.info_message)
        st.session_state.info_message = ""

    # --- Sidebar for Job Request ---
    st.sidebar.header("New Job Request")
    customer_name_search_input = st.sidebar.text_input("Enter Customer Name:", help="e.g., Olivia, James, Tho")
    selected_customer_obj = None

    if customer_name_search_input:
        customer_search_results = [c for c in ecm.LOADED_CUSTOMERS.values() if customer_name_search_input.lower() in c.customer_name.lower()]
        if len(customer_search_results) == 1:
            selected_customer_obj = customer_search_results[0]
        elif len(customer_search_results) > 1:
            customer_options = {cust.customer_name: cust for cust in customer_search_results}
            chosen_name = st.sidebar.selectbox("Multiple matches, please select:", options=list(customer_options.keys()))
            selected_customer_obj = customer_options.get(chosen_name)
        else:
            st.sidebar.warning("No customer found.")
    
    if selected_customer_obj:
        st.sidebar.success(f"Selected: {selected_customer_obj.customer_name}")
        customer_boats = [b for b in ecm.LOADED_BOATS.values() if b.customer_id == selected_customer_obj.customer_id]
        if customer_boats:
            selected_boat_obj = customer_boats[0]
            st.sidebar.markdown("---")
            st.sidebar.subheader("Selected Customer & Boat:")
            st.sidebar.write(f"**Customer:** {selected_customer_obj.customer_name}")
            st.sidebar.write(f"**ECM Boat:** {'Yes' if selected_customer_obj.is_ecm_customer else 'No'}")
            st.sidebar.write(f"**Boat Type:** {selected_boat_obj.boat_type}")
            st.sidebar.write(f"**Boat Length:** {selected_boat_obj.boat_length}ft")
            truck_name = "N/A"
            if selected_customer_obj.preferred_truck_id and ecm.ECM_TRUCKS.get(selected_customer_obj.preferred_truck_id):
                truck_name = ecm.ECM_TRUCKS[selected_customer_obj.preferred_truck_id].truck_name
            st.sidebar.write(f"**Preferred Truck:** {truck_name}")
            st.sidebar.markdown("---")

            # --- Sidebar Job Inputs ---
            service_type_input = st.sidebar.selectbox("Select Service Type:", ["Launch", "Haul", "Transport"])
            default_date = ecm.TODAY_FOR_SIMULATION + datetime.timedelta(days=7)
            requested_date_input = st.sidebar.date_input("Requested Date:", value=default_date)
            selected_ramp_id_input = None
            if service_type_input in ["Launch", "Haul"]:
                ramp_options = list(ecm.ECM_RAMPS.keys())
                selected_ramp_id_input = st.sidebar.selectbox("Select Ramp:", ramp_options)
            
            st.sidebar.markdown("---")

            # --- Search Button Logic ---
            if st.sidebar.button("Find Best Slot (Strict)", key="find_strict"):
                job_request = {'customer_id': selected_customer_obj.customer_id, 'boat_id': selected_boat_obj.boat_id, 'service_type': service_type_input, 'requested_date_str': requested_date_input.strftime('%Y-%m-%d'), 'selected_ramp_id': selected_ramp_id_input}
                st.session_state.current_job_request = job_request
                st.session_state.search_requested_date = requested_date_input
                slots, message, _, was_forced = ecm.find_available_job_slots(**job_request)
                st.session_state.info_message, st.session_state.found_slots = message, slots
                st.session_state.selected_slot, st.session_state.was_forced_search = None, was_forced
                st.rerun()
        else:
            st.sidebar.error(f"No boat found for {selected_customer_obj.customer_name}.")

# In app.py, replace the main display logic block

# --- Main Area: Display Logic ---
if st.session_state.found_slots and not st.session_state.selected_slot:
    st.subheader("Please select your preferred slot:")
    
    cols = st.columns(3)
    for i, slot in enumerate(st.session_state.found_slots):
        with cols[i % 3]:
            with st.container(border=True):
                
                # Highlighting logic for the requested date
                if st.session_state.get('search_requested_date') and slot['date'] == st.session_state.search_requested_date:
                    st.markdown("""<div style='background-color:#F0FFF0;border-left:6px solid #2E8B57;padding:10px;border-radius:5px;margin-bottom:10px;'><h5 style='color:#2E8B57;margin:0;font-weight:bold;'>⭐ Requested Date</h5></div>""", unsafe_allow_html=True)
                
                # --- THIS IS THE NEW COMPACT DISPLAY LOGIC ---
                
                # 1. Define all variables
                date_str = slot['date'].strftime('%a, %b %d, %Y')
                time_str = ecm.format_time_for_display(slot.get('time'))
                truck_id = slot.get('truck_id', 'N/A')
                ramp_details = ecm.get_ramp_details(slot.get('ramp_id'))
                ramp_name = ramp_details.ramp_name if ramp_details else "N/A"
                ecm_hours = ecm.get_ecm_operating_hours(slot['date'])
                tide_display_str = format_tides_for_display(slot, ecm_hours).replace("**", "") # Remove markdown bolding
                tide_rule_str = slot.get('tide_rule_concise', '')
                
                # 2. Combine into a single HTML string with controlled spacing
                card_content_html = f"""
                <div style="line-height: 1.4;">
                    <p style="margin-bottom: 2px;"><strong>Date:</strong> {date_str}</p>
                    <p style="margin-bottom: 2px;"><strong>Tide Rule:</strong> {tide_rule_str}</p>
                    <p style="margin-bottom: 10px;"><strong>{tide_display_str}</strong></p>
                    <p style="margin-bottom: 2px;"><strong>Time:</strong> {time_str}</p>
                    <p style="margin-bottom: 2px;"><strong>Truck:</strong> {truck_id}</p>
                    <p style="margin-bottom: 2px;"><strong>Ramp:</strong> {ramp_name}</p>
                </div>
                """
                
                # 3. Render the HTML and the button
                st.markdown(card_content_html, unsafe_allow_html=True)
                st.button("Select this slot", key=f"select_slot_{i}", on_click=handle_slot_selection, args=(slot,))
                
    st.markdown("---")

elif st.session_state.selected_slot:
    # (The rest of your confirmation logic remains the same)
    # ...

    # 2. Display Confirmation Screen
    elif st.session_state.selected_slot:
        slot = st.session_state.selected_slot
        st.subheader("Preview & Confirm Selection:")
        st.success(f"You are considering: **{slot['date'].strftime('%Y-%m-%d %A')} at {ecm.format_time_for_display(slot.get('time'))}** with Truck **{slot.get('truck_id')}**.")
        if slot.get('j17_needed'): st.write("J17 Crane will also be assigned.")
        if st.button("CONFIRM THIS JOB", key="confirm_final_job"):
            new_job_id, message = ecm.confirm_and_schedule_job(st.session_state.current_job_request, slot)
            if new_job_id:
                st.success(f"Job Confirmed! {message}")
                for key in ['found_slots', 'selected_slot', 'current_job_request', 'search_requested_date', 'was_forced_search']:
                    st.session_state.pop(key, None)
                st.rerun()
            else:
                st.error(f"Failed to confirm job: {message}")

    # 3. Handle No Slots Found
    elif st.session_state.get('current_job_request') and not st.session_state.found_slots:
        if st.session_state.info_message:
            st.warning(st.session_state.info_message)

# --- PAGE 2: REPORTING ---
elif app_mode == "Reporting":
    st.header("Reporting Dashboard")
    st.info("This section is for viewing and exporting scheduled jobs.")
    st.subheader("All Scheduled Jobs (Current Session)")
    if ecm.SCHEDULED_JOBS:
        # Prepare data for display
        display_data = []
        for job in sorted(ecm.SCHEDULED_JOBS, key=lambda j: j.scheduled_start_datetime or datetime.datetime.max):
            customer = ecm.get_customer_details(getattr(job, 'customer_id', None))
            ramp_details = ecm.get_ramp_details(getattr(job, 'pickup_ramp_id', None) or getattr(job, 'dropoff_ramp_id', None))
            display_data.append({
                "Job ID": getattr(job, 'job_id', 'N/A'), "Status": getattr(job, 'job_status', 'N/A'),
                "Scheduled Date": job.scheduled_start_datetime.strftime("%B %d, %Y") if job.scheduled_start_datetime else "N/A",
                "Scheduled Time": ecm.format_time_for_display(job.scheduled_start_datetime.time()) if job.scheduled_start_datetime else "N/A",
                "Service": getattr(job, 'service_type', 'N/A'),
                "Customer": customer.customer_name if customer else "N/A",
                "Truck": getattr(job, 'assigned_hauling_truck_id', 'N/A'),
                "Crane": "Yes" if getattr(job, 'assigned_crane_truck_id', None) else "No",
                "Ramp": ramp_details.ramp_name if ramp_details else "N/A",
                "Notes": getattr(job, 'notes', '')
            })
        df = pd.DataFrame(display_data)
        st.dataframe(df, use_container_width=True)
        # Add a download button
        st.download_button("Download as CSV", df.to_csv(index=False), "scheduled_jobs.csv", "text/csv")
    else:
        st.write("No jobs scheduled in this session yet.")

# --- PAGE 3: SETTINGS ---
elif app_mode == "Settings":
    st.header("Application Settings")
    st.write("This section is under construction.")
    st.info("Here you can add widgets to modify business rules like operating hours or ramp tide offsets in the future.")
