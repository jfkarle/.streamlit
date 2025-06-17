# app.py
# FINAL, CORRECTED VERSION

import streamlit as st
import datetime
import ecm_scheduler_logic as ecm
import pandas as pd

st.set_page_config(layout="wide")

def format_tides_for_display(slot, ecm_hours):
    """
    Formats the tide display to emphasize the most relevant high tide.
    """
    tide_times = slot.get('high_tide_times', [])
    if not tide_times:
        return ""

    if not ecm_hours or not ecm_hours.get('open'):
        return "HT: " + " / ".join([ecm.format_time_for_display(t) for t in tide_times])

    op_open = ecm_hours['open']
    op_close = ecm_hours['close']
    
    def get_tide_relevance_score(tide_time):
        tide_dt = datetime.datetime.combine(datetime.date.today(), tide_time)
        open_dt = datetime.datetime.combine(datetime.date.today(), op_open)
        close_dt = datetime.datetime.combine(datetime.date.today(), op_close)

        if open_dt <= tide_dt <= close_dt:
            return 0, abs((tide_dt - open_dt).total_seconds())
        
        dist_to_open = abs((tide_dt - open_dt).total_seconds())
        dist_to_close = abs((tide_dt - close_dt).total_seconds())
        return 1, min(dist_to_open, dist_to_close)

    sorted_tides = sorted(tide_times, key=get_tide_relevance_score)
    primary_tide_str = ecm.format_time_for_display(sorted_tides[0])
    
    if len(sorted_tides) == 1:
        return f"**HIGH TIDE: {primary_tide_str}**"

    secondary_tides = [ecm.format_time_for_display(t) for t in sorted_tides[1:]]
    secondary_tides_str = " / ".join(secondary_tides)
    return f"**HIGH TIDE: {primary_tide_str}** (and {secondary_tides_str.lower()})"

# --- Session State Initialization ---
def initialize_session_state():
    if 'data_loaded' not in st.session_state:
        if ecm.load_customers_and_boats_from_csv("ECM Sample Cust.csv"):
            st.session_state.data_loaded = True
        else:
            st.session_state.data_loaded = False
            st.error("Failed to load customer and boat data.")
    
    # Initialize other variables if they don't exist
    for key, default_value in [('info_message', ""), ('current_job_request', None), 
                               ('found_slots', []), ('selected_slot', None), 
                               ('search_requested_date', None)]:
        if key not in st.session_state:
            st.session_state[key] = default_value

initialize_session_state()

# --- Main App Layout ---
st.title("ECM Boat Hauling - Availability Scheduler")

if st.session_state.info_message:
    st.info(st.session_state.info_message)
    st.session_state.info_message = ""

# --- Sidebar for Job Request ---
st.sidebar.header("New Job Request")

# --- 1. Customer Name Search ---
customer_name_search_input = st.sidebar.text_input("Enter Customer Name:", help="e.g., Olivia, James, Tho")
selected_customer_obj = None
if customer_name_search_input:
    customer_search_results = [c for c in ecm.LOADED_CUSTOMERS.values() if customer_name_search_input.lower() in c.customer_name.lower()]
    if len(customer_search_results) == 1:
        selected_customer_obj = customer_search_results[0]
        st.sidebar.success(f"Selected: {selected_customer_obj.customer_name}")
    elif len(customer_search_results) > 1:
        customer_options = {cust.customer_name: cust for cust in customer_search_results}
        chosen_name = st.sidebar.selectbox("Multiple matches, please select:", options=list(customer_options.keys()))
        selected_customer_obj = customer_options.get(chosen_name)
    else:
        st.sidebar.warning("No customer found.")

# --- 2. Boat & Job Details ---
if selected_customer_obj:
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

        service_type_input = st.sidebar.selectbox("Select Service Type:", ["Launch", "Haul", "Transport"])
        default_date = ecm.TODAY_FOR_SIMULATION + datetime.timedelta(days=7)
        requested_date_input = st.sidebar.date_input("Requested Date:", value=default_date)
        selected_ramp_id_input = None
        if service_type_input in ["Launch", "Haul"]:
            ramp_options = list(ecm.ECM_RAMPS.keys())
            selected_ramp_id_input = st.sidebar.selectbox("Select Ramp:", ramp_options)
        
        st.sidebar.markdown("---")

       if st.sidebar.button("Find Best Slot (Strict)", key="find_strict"):
        # --- THIS ENTIRE BLOCK IS NOW CORRECTLY INDENTED ---
        job_request = {
            'customer_id': selected_customer_obj.customer_id,
            'boat_id': selected_boat_obj.boat_id,
            'service_type': service_type_input,
            'requested_date_str': requested_date_input.strftime('%Y-%m-%d'),
            'selected_ramp_id': selected_ramp_id_input,
        }
        st.session_state.current_job_request = job_request
        st.session_state.search_requested_date = requested_date_input

        # Receive the new 'was_forced' flag
        slots, message, _, was_forced = ecm.find_available_job_slots(
            **job_request, 
            force_preferred_truck=True, 
            relax_ramp_constraint=False
        )
        
        st.session_state.info_message = message
        st.session_state.found_slots = slots
        st.session_state.selected_slot = None
        st.session_state.was_forced_search = was_forced # Save the flag
        st.rerun()
    
    st.session_state.info_message = message
    st.session_state.found_slots = slots
    st.session_state.selected_slot = None
    st.session_state.was_forced_search = was_forced # Save the flag
    st.rerun()

        # --- Levers for Alternative Search ---
    st.sidebar.markdown("---")
    st.sidebar.subheader("Not soon enough? Widen your search:")
    
    relax_truck_input = st.sidebar.checkbox("Relax Truck Constraint (use any suitable truck)", key="relax_truck")
    relax_ramp_input = st.sidebar.checkbox("Relax Ramp Constraint (search nearby ramps)", key="relax_ramp")

    if st.sidebar.button("Find Alternatives", key="find_relaxed"):
        # We need a current job request to find alternatives for
        if st.session_state.current_job_request:
            
            # Use the existing job request from session state
            req = st.session_state.current_job_request

            # --- THIS IS THE CORRECTED FUNCTION CALL ---
            slots, message, _ = ecm.find_available_job_slots(
                customer_id=req['customer_id'],
                boat_id=req['boat_id'],
                service_type=req['service_type'],
                requested_date_str=req['requested_date_str'],
                selected_ramp_id=req['selected_ramp_id'],
                force_preferred_truck=(not relax_truck_input), 
                relax_ramp_constraint=relax_ramp_input
            )

            st.session_state.info_message = message
            st.session_state.found_slots = slots
            st.session_state.selected_slot = None # Reset selection
            # Also save the date so highlighting works correctly on this search
            st.session_state.search_requested_date = datetime.datetime.strptime(req['requested_date_str'], '%Y-%m-%d').date()
            st.rerun()
        else:
            st.sidebar.warning("Please find a strict slot first before searching for alternatives.")


# --- Main Area for Displaying Results and Confirmation ---
def handle_slot_selection(slot_data):
    """Sets the chosen slot into the session state for confirmation."""
    st.session_state.selected_slot = slot_data

if st.session_state.found_slots and not st.session_state.selected_slot:
    st.subheader("Please select your preferred slot:")
    cols = st.columns(3)

    for i, slot in enumerate(st.session_state.found_slots):
        with cols[i % 3]:
            with st.container(border=True):
                if st.session_state.get('search_requested_date') and slot['date'] == st.session_state.search_requested_date:
                    st.markdown(
                        """
                        <div style="background-color: #F0FFF0; border-left: 6px solid #2E8B57; padding: 10px; border-radius: 5px; margin-bottom: 10px;">
                            <h5 style="color: #2E8B57; margin: 0; font-weight: bold;">⭐ Requested Date</h5>
                        </div>
                        """,
                        unsafe_allow_html=True
                    )
                
                date_str = slot['date'].strftime('%a, %b %d, %Y')
                time_str = ecm.format_time_for_display(slot.get('time'))
                truck_id = slot.get('truck_id', 'N/A')
                ramp_name = ecm.get_ramp_details(slot.get('ramp_id')).ramp_name if slot.get('ramp_id') else "N/A"
                ecm_hours = ecm.get_ecm_operating_hours(slot['date'])
                tide_display_str = format_tides_for_display(slot, ecm_hours)

                st.markdown(f"**Date:** {date_str}")
                if slot.get('tide_rule_concise'):
                    st.markdown(f"**Tide Rule:** {slot['tide_rule_concise']}")
                if tide_display_str:
                    st.markdown(tide_display_str)
                st.markdown(f"**Time:** {time_str}")
                st.markdown(f"**Truck:** {truck_id}")
                st.markdown(f"**Ramp:** {ramp_name}")
                
                st.button("Select this slot", key=f"select_slot_{i}", on_click=handle_slot_selection, args=(slot,))
    st.markdown("---")

elif st.session_state.selected_slot:
    slot = st.session_state.selected_slot
    original_request = st.session_state.current_job_request
    slot_time_str = ecm.format_time_for_display(slot.get('time'))
    date_str = slot.get('date').strftime('%Y-%m-%d %A')
    
    st.subheader("Preview & Confirm Selection:")
    st.success(f"You are considering: **{date_str} at {slot_time_str}** with Truck **{slot.get('truck_id')}**.")
    if slot.get('j17_needed'):
        st.write("J17 Crane will also be assigned.")
    
    if st.button("CONFIRM THIS JOB", key="confirm_final_job"):
        new_job_id, message = ecm.confirm_and_schedule_job(original_job_request_details=original_request, selected_slot_info=slot)
        if new_job_id:
            st.success(f"Job Confirmed! {message}")
            for key in ['found_slots', 'selected_slot', 'current_job_request', 'search_requested_date']:
                st.session_state.pop(key, None)
            st.rerun()
        else:
            st.error(f"Failed to confirm job: {message}")

elif st.session_state.get('current_job_request') and not st.session_state.found_slots:
    if st.session_state.info_message:
        st.warning(st.session_state.info_message)

# Display All Scheduled Jobs
st.markdown("---")
if st.checkbox("Show All Currently Scheduled Jobs"):
    st.subheader("All Scheduled Jobs (Current Session):")
    if ecm.SCHEDULED_JOBS:
        def get_day_with_suffix(d):
            return str(d) + ("th" if 11 <= d <= 13 else {1: "st", 2: "nd", 3: "rd"}.get(d % 10, "th"))

        display_data = []
        for job in sorted(ecm.SCHEDULED_JOBS, key=lambda j: (j.scheduled_start_datetime is None, j.scheduled_start_datetime)):
            customer = ecm.get_customer_details(job.customer_id)
            if job.scheduled_start_datetime:
                day_str = get_day_with_suffix(job.scheduled_start_datetime.day)
                date_formatted = job.scheduled_start_datetime.strftime(f"%B {day_str}, %Y")
                time_formatted = ecm.format_time_for_display(job.scheduled_start_datetime.time())
            else:
                date_formatted, time_formatted = "Not Scheduled", "N/A"
            
            ramp_name = "N/A"
            ramp_id = job.pickup_ramp_id or job.dropoff_ramp_id
            if ramp_id:
                ramp = ecm.get_ramp_details(ramp_id)
                if ramp: ramp_name = ramp.ramp_name

            display_data.append({
                "Job ID": job.job_id, "Status": job.job_status, "Scheduled Date": date_formatted,
                "Scheduled Time": time_formatted, "Service": job.service_type,
                "Customer": customer.customer_name if customer else "N/A",
                "Truck": job.assigned_hauling_truck_id,
                "Crane": "Yes" if job.assigned_crane_truck_id else "No",
                "Ramp": ramp_name, "Notes": job.notes
            })
        
        df = pd.DataFrame(display_data)
        st.dataframe(df, use_container_width=True)
    else:
        st.write("No jobs scheduled yet.")
