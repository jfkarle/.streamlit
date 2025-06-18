# app.py
# FINAL WORKING VERSION


import streamlit as st
import datetime
import ecm_scheduler_logic as ecm
import pandas as pd
from io import BytesIO
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors

# =============================================================================
# --- PDF Generation Functions ---
# =============================================================================

def _abbreviate_location(location_name):
    if not location_name:
        return ""
    if "Scituate" in location_name:
        return "Sci"
    if "Green Harbor" in location_name:
        return "Grn Hbr"
    if "Plymouth" in location_name:
        return "Plym"
    if "Duxbury" in location_name:
        return "Dux"
    parts = location_name.split(',')
    if len(parts) > 1:
        return parts[-1].strip()
    return location_name

def generate_daily_planner_pdf(report_date, jobs_for_day):
    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter

    planner_columns = ["S20/33", "S21/77", "S23/55", "S17"]
    column_map = {name: i for i, name in enumerate(planner_columns)}
    margin = 0.5 * inch
    time_col_width = 0.75 * inch
    content_width = width - (2 * margin) - time_col_width
    col_width = content_width / len(planner_columns)
    start_hour, end_hour = 8, 19
    top_y = height - margin - (0.5 * inch)
    bottom_y = margin + (0.5 * inch)
    content_height = top_y - bottom_y

    def get_y_for_time(t):
        total_minutes = (t.hour - start_hour) * 60 + t.minute
        return top_y - (total_minutes / ((end_hour - start_hour) * 60) * content_height)

    # Header
    day_of_year = report_date.timetuple().tm_yday
    days_in_year = 366 if (report_date.year % 4 == 0 and report_date.year % 100 != 0) or (report_date.year % 400 == 0) else 365
    days_remaining = days_in_year - day_of_year
    c.setFont("Helvetica", 9)
    c.drawRightString(width - margin, height - 0.4 * inch, f"{day_of_year}/{days_remaining}")
    date_str = report_date.strftime("%A, %B %d").upper()
    c.setFont("Helvetica-Bold", 12)
    c.drawRightString(width - margin, height - 0.6 * inch, date_str)

    # Column Headers
    c.setFont("Helvetica-Bold", 11)
    for i, name in enumerate(planner_columns):
        x_center = margin + time_col_width + i * col_width + col_width / 2
        c.drawCentredString(x_center, top_y + 10, name)

    for i in range(len(planner_columns) + 1):
        x = margin + time_col_width + i * col_width
        c.setLineWidth(0.5)
        c.line(x, top_y, x, bottom_y)
    c.line(margin, top_y, margin, bottom_y)

    # Time slots
    for hour in range(start_hour, end_hour + 1):
        for minute in [0, 15, 30, 45]:
            current_time = datetime.time(hour, minute)
            y = get_y_for_time(current_time)
            next_y = get_y_for_time((datetime.datetime.combine(datetime.date.today(), current_time) + datetime.timedelta(minutes=15)).time())
            label_y = (y + next_y) / 2
            c.setLineWidth(1.0 if minute == 0 else 0.25)
            c.line(margin, y, width - margin, y)
            if minute == 0:
                display_hour = hour if hour <= 12 else hour - 12
                c.setFont("Helvetica-Bold", 9)
                c.drawString(margin + 3, label_y - 3, str(display_hour))
                c.setFont("Helvetica", 7)
                c.drawString(margin + 18, label_y - 3, "00")
            else:
                c.setFont("Helvetica", 6)
                c.drawString(margin + 18, label_y - 2, f"{minute}")

    # Jobs
    for job in jobs_for_day:
        truck_id = getattr(job, 'assigned_hauling_truck_id', None)
        if getattr(job, 'assigned_crane_truck_id', None):
            truck_id = "S17"
        if truck_id not in column_map:
            continue
        col_index = column_map[truck_id]
        column_start_x = margin + time_col_width + col_index * col_width
        text_center_x = column_start_x + col_width / 2

        start_time = getattr(job, 'scheduled_start_datetime').time()
        end_time = getattr(job, 'scheduled_end_datetime').time()
        y_start = get_y_for_time(start_time)
        y_end = get_y_for_time(end_time)

        first_block_mid_y = (get_y_for_time(start_time) + get_y_for_time(
            (datetime.datetime.combine(datetime.date.today(), start_time) + datetime.timedelta(minutes=15)).time()
        )) / 2

        # Get customer name
        customer = ecm.get_customer_details(getattr(job, 'customer_id', None))
        customer_name = customer.customer_name.split()[-1] if customer else "Unknown"
        
        # Get boat info
        boat_length = getattr(job, 'boat_length', 0)
        boat_type = getattr(job, 'boat_type', '')
        boat_desc = f"{int(boat_length)}' {boat_type}"
        
        # Get location info
        pickup = getattr(job, 'pickup_street_address', '')
        dropoff = getattr(job, 'dropoff_street_address', '')
        location = f"{_abbreviate_location(pickup)}-{_abbreviate_location(dropoff)}"
        service_type = getattr(job, 'service_type', '')
        if service_type == "Launch":
            location = f"Launch-{_abbreviate_location(dropoff)}"
        elif service_type == "Haul":
            location = f"Haul-{_abbreviate_location(pickup)}"
        
        # --- Start of job block rendering ---
        # Compute exact text y-positions between grid lines
        dt_base = datetime.datetime.combine(datetime.date.today(), start_time)
        y0 = get_y_for_time(start_time)
        y1 = get_y_for_time((dt_base + datetime.timedelta(minutes=15)).time())
        y2 = get_y_for_time((dt_base + datetime.timedelta(minutes=30)).time())
        y3 = get_y_for_time((dt_base + datetime.timedelta(minutes=45)).time())
        
        line1_y = (y0 + y1) / 2  # Customer name
        line2_y = (y1 + y2) / 2  # Boat description
        line3_y = (y2 + y3) / 2  # Launch location
        
        # Draw the 3-line block between grid lines
        c.setFont("Helvetica-Bold", 8)
        c.drawCentredString(text_center_x, line1_y, customer_name)
        
        c.setFont("Helvetica", 7)
        c.drawCentredString(text_center_x, line2_y, boat_desc)
        c.drawCentredString(text_center_x, line3_y, location)
        
        # Vertical line starts below last line (after y3)
        y_bar_start = y3 + 6
        c.setLineWidth(2)
        c.line(text_center_x, y_bar_start, text_center_x, y_end)
        c.line(text_center_x - 3, y_end, text_center_x + 3, y_end)
        # --- End of job block rendering ---

    c.save()
    buffer.seek(0)
    return buffer

#=============================================================================
# --- Main Application Logic (Unchanged from your version) ---
#=============================================================================
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
initialize_session_state()
st.title("Marine Transportation")
st.sidebar.title("Navigation")
app_mode = st.sidebar.radio("Go to", ["Schedule New Boat", "Reporting", "Settings"])
if app_mode == "Schedule New Boat":
    if st.session_state.info_message:
        st.info(st.session_state.info_message)
        st.session_state.info_message = ""
    if st.session_state.get("confirmation_message") and not st.session_state.get("selected_slot"):
        st.success(f"✅ {st.session_state.confirmation_message}")
        if st.button("Schedule Another Job", key="schedule_another"):
            st.session_state.pop("confirmation_message", None)
            st.rerun()
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
            service_type_input = st.sidebar.selectbox("Select Service Type:", ["Launch", "Haul", "Transport"])
            default_date = ecm.TODAY_FOR_SIMULATION + datetime.timedelta(days=7)
            requested_date_input = st.sidebar.date_input("Requested Date:", value=default_date)
            selected_ramp_id_input = None
            if service_type_input in ["Launch", "Haul"]:
                ramp_options = list(ecm.ECM_RAMPS.keys())
                selected_ramp_id_input = st.sidebar.selectbox("Select Ramp:", ramp_options)
            st.sidebar.markdown("---")
            if st.sidebar.button("Find Best Slot (Strict)", key="find_strict"):
                job_request = {
                    'customer_id': selected_customer_obj.customer_id, 'boat_id': selected_boat_obj.boat_id,
                    'service_type': service_type_input, 'requested_date_str': requested_date_input.strftime('%Y-%m-%d'),
                    'selected_ramp_id': selected_ramp_id_input,
                }
                st.session_state.current_job_request = job_request
                st.session_state.search_requested_date = requested_date_input
                slots, message, _, was_forced = ecm.find_available_job_slots(**job_request)
                st.session_state.info_message, st.session_state.found_slots = message, slots
                st.session_state.selected_slot, st.session_state.was_forced_search = None, was_forced
                st.rerun()
        else:
            st.sidebar.error(f"No boat found for {selected_customer_obj.customer_name}.")
    if st.session_state.found_slots and not st.session_state.selected_slot:
        st.subheader("Please select your preferred slot:")
        cols = st.columns(3)
        for i, slot in enumerate(st.session_state.found_slots):
            with cols[i % 3]:
                with st.container(border=True):
                    if st.session_state.get('search_requested_date') and slot['date'] == st.session_state.search_requested_date:
                        st.markdown("""<div style='background-color:#F0FFF0;border-left:6px solid #2E8B57;padding:10px;border-radius:5px;margin-bottom:10px;'><h5 style='color:#2E8B57;margin:0;font-weight:bold;'>⭐ Requested Date</h5></div>""", unsafe_allow_html=True)
                    if slot.get('reason_for_suggestion'):
                        st.markdown(f"""<div style='background-color:#E3F2FD;border-left:6px solid #1E88E5;padding:10px;border-radius:5px;margin-bottom:10px;font-size:14px;'>💡 <b>Note:</b> {slot['reason_for_suggestion']}</div>""", unsafe_allow_html=True)
                    date_str = slot['date'].strftime('%a, %b %d, %Y')
                    time_str = ecm.format_time_for_display(slot.get('time'))
                    truck_id = slot.get('truck_id', 'N/A')
                    ramp_details = ecm.get_ramp_details(slot.get('ramp_id'))
                    ramp_name = ramp_details.ramp_name if ramp_details else "N/A"
                    ecm_hours = ecm.get_ecm_operating_hours(slot['date'])
                    tide_display_str = format_tides_for_display(slot, ecm_hours)
                    st.markdown(f"**Date:** {date_str}")
                    if slot.get('tide_rule_concise'): st.markdown(f"**Tide Rule:** {slot['tide_rule_concise']}")
                    if tide_display_str: st.markdown(tide_display_str)
                    st.markdown(f"**Time:** {time_str}")
                    st.markdown(f"**Truck:** {truck_id}")
                    st.markdown(f"**Ramp:** {ramp_name}")
                    st.button("Select this slot", key=f"select_slot_{i}", on_click=handle_slot_selection, args=(slot,))
        st.markdown("---")
    elif st.session_state.selected_slot:
        slot = st.session_state.selected_slot
        st.subheader("Preview & Confirm Selection:")
        st.success(f"You are considering: **{slot['date'].strftime('%Y-%m-%d %A')} at {ecm.format_time_for_display(slot.get('time'))}** with Truck **{slot.get('truck_id')}**.")
        if slot.get('j17_needed'): st.write("J17 Crane will also be assigned.")
        if st.button("CONFIRM THIS JOB", key="confirm_final_job"):
            new_job_id, message = ecm.confirm_and_schedule_job(st.session_state.current_job_request, slot)
            if new_job_id:
                st.session_state.confirmation_message = message
                for key in ['found_slots', 'selected_slot', 'current_job_request', 'search_requested_date', 'was_forced_search']:
                    st.session_state.pop(key, None)
                st.rerun()
            else:
                st.error(f"Failed to confirm job: {message}")
    elif st.session_state.get('current_job_request') and not st.session_state.found_slots:
        if st.session_state.info_message:
            st.warning(st.session_state.info_message)
elif app_mode == "Reporting":
    st.header("Reporting Dashboard")
    st.markdown("---")
    st.subheader("Daily Planner PDF Report")
    st.write("Select a single day to generate a PDF schedule in the classic planner format.")
    selected_date = st.date_input("Select a date for the report", value=datetime.date.today())
    if st.button("Generate Daily Planner PDF", key="generate_pdf"):
        jobs_for_selected_date = [
            job for job in ecm.SCHEDULED_JOBS 
            if job.scheduled_start_datetime.date() == selected_date
        ]
        if not jobs_for_selected_date:
            st.warning(f"No jobs found for {selected_date.strftime('%Y-%m-%d')}. The PDF would be empty.")
        else:
            with st.spinner("Creating your daily planner..."):
                pdf_buffer = generate_daily_planner_pdf(selected_date, jobs_for_selected_date)
                st.download_button(
                    label="✅ Download PDF Planner",
                    data=pdf_buffer,
                    file_name=f"Daily_Planner_{selected_date.strftime('%Y-%m-%d')}.pdf",
                    mime="application/pdf"
                )
    st.markdown("---")
    st.subheader("All Scheduled Jobs (Table View)")
    if ecm.SCHEDULED_JOBS:
        display_data = []
        for job in sorted(ecm.SCHEDULED_JOBS, key=lambda j: j.scheduled_start_datetime or datetime.datetime.max):
            customer = ecm.get_customer_details(getattr(job, 'customer_id', None))
            ramp = ecm.get_ramp_details(getattr(job, 'pickup_ramp_id', None) or getattr(job, 'dropoff_ramp_id', None))
            display_data.append({
                "Job ID": job.job_id, "Status": job.job_status,
                "Scheduled Date": job.scheduled_start_datetime.strftime("%Y-%m-%d") if job.scheduled_start_datetime else "N/A",
                "Scheduled Time": ecm.format_time_for_display(job.scheduled_start_datetime.time()) if job.scheduled_start_datetime else "N/A",
                "Service": job.service_type, "Customer": customer.customer_name if customer else "N/A",
                "Truck": job.assigned_hauling_truck_id, "Ramp": ramp.ramp_name if ramp else "N/A"
            })
        st.dataframe(pd.DataFrame(display_data))
    else:
        st.write("No jobs scheduled yet.")
elif app_mode == "Settings":
    st.header("Application Settings")
    st.write("This section is under construction.")
