import streamlit as st
import datetime
import ecm_scheduler_logic as ecm
import pandas as pd
import csv
import math
from reportlab.lib.pagesizes import letter
import calendar
from io import BytesIO

st.set_page_config(layout="wide")

# --- Helper Functions for UI ---

def create_gauge(value, max_value, label):
    """Generates an SVG string for a semi-circle gauge chart that displays an absolute value."""
    if max_value == 0: percent = 0
    else: percent = min(max(value / max_value, 0), 1)
    angle = percent * 180
    rads = math.radians(angle - 90)
    x, y = 50 + 40 * math.cos(rads), 50 + 40 * math.sin(rads)
    d = f"M 10 50 A 40 40 0 0 1 {x} {y}"
    fill_color = "#F44336"
    if percent >= 0.4: fill_color = "#FFC107"
    if percent >= 0.8: fill_color = "#4CAF50"
    main_text, sub_label = str(value), f"{label.upper()} OF {max_value}"
    return f'''
    <svg viewBox="0 0 100 65" style="width: 150px; height: 97px; overflow: visible;">
        <path d="M 10 50 A 40 40 0 0 1 90 50" stroke="#e0e0e0" stroke-width="10" fill="none" />
        <path d="{d}" stroke="{fill_color}" stroke-width="10" fill="none" />
        <text x="50" y="45" text-anchor="middle" font-size="20" font-weight="bold" fill="#333">{main_text}</text>
        <text x="50" y="60" text-anchor="middle" font-size="8" fill="#333">{sub_label}</text>
    </svg>
    '''

def format_tides_for_display(slot, truck_schedule):
    tide_times = slot.get('high_tide_times', [])
    if not tide_times: return ""
    truck_id, slot_date = slot.get('truck_id'), slot.get('date')
    op_hours = truck_schedule.get(truck_id, {}).get(slot_date.weekday()) if truck_id and slot_date else None
    if not op_hours: return "HT: " + " / ".join([ecm.format_time_for_display(t) for t in tide_times])
    op_open, op_close = op_hours[0], op_hours[1]
    def get_tide_relevance_score(tide_time):
        tide_dt = datetime.datetime.combine(datetime.date.today(), tide_time)
        open_dt, close_dt = datetime.datetime.combine(datetime.date.today(), op_open), datetime.datetime.combine(datetime.date.today(), op_close)
        return (0, abs((tide_dt - open_dt).total_seconds())) if open_dt <= tide_dt <= close_dt else (1, min(abs((tide_dt - open_dt).total_seconds()), abs((tide_dt - close_dt).total_seconds())))
    sorted_tides = sorted(tide_times, key=get_tide_relevance_score)
    if not sorted_tides: return ""
    primary_tide_str = ecm.format_time_for_display(sorted_tides[0])
    if len(sorted_tides) == 1: return f"**HIGH TIDE: {primary_tide_str}**"
    secondary_tides_str = " / ".join([ecm.format_time_for_display(t) for t in sorted_tides[1:]])
    return f"**HIGH TIDE: {primary_tide_str}** (and {secondary_tides_str.lower()})"

def handle_slot_selection(slot_data):
    st.session_state.selected_slot = slot_data

def display_crane_day_calendar(crane_days_for_ramp):
    candidate_dates, today = {d['date'] for d in crane_days_for_ramp}, datetime.date.today()
    _, cal_col, _ = st.columns([1, 2, 1])
    with cal_col, st.container(border=True):
        selected_month_str = st.selectbox("Select a month to view:", [(today + datetime.timedelta(days=30*i)).strftime("%B %Y") for i in range(6)])
        if not selected_month_str: return
        selected_month = datetime.datetime.strptime(selected_month_str, "%B %Y")
        st.subheader(f"Calendar for {selected_month_str}")
        header_cols = st.columns(7)
        for col, day_name in zip(header_cols, ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]):
            with col: st.markdown(f"<p style='text-align: center; font-weight: bold;'>{day_name}</p>", unsafe_allow_html=True)
        st.markdown("---")
        for week in calendar.Calendar().monthdatescalendar(selected_month.year, selected_month.month):
            cols = st.columns(7)
            for i, day in enumerate(week):
                if day.month != selected_month.month:
                    cols[i].markdown(f'<div style="padding:10px; border-radius:5px; background-color:#F0F2F6; height: 60px;"><p style="text-align: right; color: #D3D3D3;">{day.day}</p></div>', unsafe_allow_html=True)
                else:
                    is_candidate, is_today = day in candidate_dates, day == today
                    bg_color = "#E8F5E9" if is_candidate else "#FFFFFF"
                    border_color = "#1E88E5" if is_today else ("#4CAF50" if is_candidate else "#E0E0E0")
                    font_weight = "bold" if is_candidate or is_today else "normal"
                    cols[i].markdown(f'<div style="padding:10px; border-radius:5px; border: 2px solid {border_color}; background-color:{bg_color}; height: 60px;"><p style="text-align: right; font-weight: {font_weight}; color: black;">{day.day}</p></div>', unsafe_allow_html=True)

def _abbreviate_town(address):
    """
    Takes a full address string or a special keyword ('HOME') and returns
    a standardized three-letter abbreviation for the town.
    """
    if not address: return ""
    abbr_map = { "pembroke": "Pem", "scituate": "Sci", "green harbor": "GrH", "marshfield": "Mar", "cohasset": "Coh", "weymouth": "Wey", "plymouth": "Ply", "sandwich": "San", "duxbury": "Dux", "humarock": "Hum", "hingham": "Hin", "hull": "Hul" }
    if 'HOME' in address.upper(): return "Pem"
    address_lower = address.lower()
    for town, abbr in abbr_map.items():
        if town in address_lower: return abbr
    return address.title().split(',')[0][:3]

You're right, that's not working. The PDF is missing the entire time grid. This happens when the drawing loop inside the generate_daily_planner_pdf function fails.

The previous code had an error in how it handled the new 7:30 AM start time. I have corrected it below.

To fix this, please replace the entire generate_daily_planner_pdf function in your app.py file with this final, corrected version.

Python

def generate_daily_planner_pdf(report_date, jobs_for_day):
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.units import inch
    from reportlab.lib import colors

    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter
    planner_columns = ["S20/33", "S21/77", "S23/55", "J17"]
    column_map = {name: i for i, name in enumerate(planner_columns)}
    margin, time_col_width = 0.5 * inch, 0.75 * inch
    content_width = width - 2 * margin - time_col_width
    col_width = content_width / len(planner_columns)
    
    start_time_obj = datetime.time(7, 30)
    end_time_obj = datetime.time(17, 0)
    total_minutes_in_planner = (end_time_obj.hour * 60 + end_time_obj.minute) - (start_time_obj.hour * 60 + start_time_obj.minute)

    top_y, bottom_y = height - margin - 0.5 * inch, margin + 0.5 * inch
    content_height = top_y - bottom_y

    def get_y_for_time(t):
        minutes_into_day = (t.hour * 60 + t.minute) - (start_time_obj.hour * 60 + start_time_obj.minute)
        return top_y - ((minutes_into_day / total_minutes_in_planner) * content_height)

    high_tide_highlights, low_tide_highlights = [], []
    if jobs_for_day:
        ramp_id = getattr(jobs_for_day[0], 'pickup_ramp_id', None) or getattr(jobs_for_day[0], 'dropoff_ramp_id', None)
        if ramp_id:
            ramp_obj = ecm.get_ramp_details(ramp_id)
            all_tides = ecm.get_all_tide_times_for_ramp_and_date(ramp_obj, report_date)
            def round_time_to_15_min(t):
                total_minutes = t.hour * 60 + t.minute; rounded_minutes = int(round(total_minutes / 15.0) * 15)
                if rounded_minutes >= 24 * 60: rounded_minutes = (24 * 60) - 15
                h, m = divmod(rounded_minutes, 60); return datetime.time(h, m)
            high_tide_highlights = [round_time_to_15_min(t['time']) for t in all_tides.get('H', [])]
            low_tide_highlights = [round_time_to_15_min(t['time']) for t in all_tides.get('L', [])]
    
    c.setFont("Helvetica-Bold", 12)
    c.drawRightString(width - margin, height - 0.6 * inch, report_date.strftime("%A, %B %d").upper())
    for i, name in enumerate(planner_columns):
        c.setFont("Helvetica-Bold", 14); c.drawCentredString(margin + time_col_width + i * col_width + col_width / 2, top_y + 10, name)

    # --- Corrected Time Grid Drawing Loop ---
    for hour in range(start_time_obj.hour, end_time_obj.hour + 1):
        for minute in [0, 15, 30, 45]:
            current_time = datetime.time(hour, minute)
            if not (start_time_obj <= current_time <= end_time_obj):
                continue
            
            y = get_y_for_time(current_time)
            
            highlight_color = None
            if current_time in high_tide_highlights: highlight_color = colors.Color(1, 1, 0, alpha=0.4)
            elif current_time in low_tide_highlights: highlight_color = colors.Color(1, 0.6, 0.6, alpha=0.4)
            
            if highlight_color:
                next_q_hour_dt = datetime.datetime.combine(datetime.date.min, current_time) + datetime.timedelta(minutes=15)
                if next_q_hour_dt.time() <= end_time_obj:
                    y_next = get_y_for_time(next_q_hour_dt.time())
                    c.setFillColor(highlight_color)
                    c.rect(margin + time_col_width, y_next, content_width, y - y_next, fill=1, stroke=0)
            
            c.setStrokeColorRGB(0.7, 0.7, 0.7)
            c.setLineWidth(1.0 if minute == 0 else 0.25)
            c.line(margin, y, width - margin, y)
            
            if minute == 0:
                display_hour = hour if hour <= 12 else hour - 12
                c.setFont("Helvetica-Bold", 9); c.setFillColorRGB(0,0,0)
                c.drawString(margin + 3, y - 9, str(display_hour))

    c.setStrokeColorRGB(0,0,0)
    for i in range(len(planner_columns) + 1):
        x = margin + time_col_width + i * col_width; c.setLineWidth(0.5); c.line(x, top_y, x, bottom_y)
    c.line(margin, top_y, margin, bottom_y); c.line(width - margin, top_y, width - margin, bottom_y)

    for job in jobs_for_day:
        start_time, end_time = job.scheduled_start_datetime.time(), job.scheduled_end_datetime.time()
        y0, y_end = get_y_for_time(start_time), get_y_for_time(end_time)
        line1_y, line2_y, line3_y = y0 - 12, y0 - 22, y0 - 32
        customer, boat = ecm.get_customer_details(job.customer_id), ecm.get_boat_details(job.boat_id)
        if job.assigned_hauling_truck_id in column_map:
            col_index = column_map[job.assigned_hauling_truck_id]; text_x = margin + time_col_width + (col_index + 0.5) * col_width
            c.setFillColorRGB(0,0,0); c.setFont("Helvetica-Bold", 8); c.drawCentredString(text_x, line1_y, customer.customer_name)
            c.setFont("Helvetica", 7); c.drawCentredString(text_x, line2_y, f"{int(boat.boat_length)}' {boat.boat_type}")
            c.drawCentredString(text_x, line3_y, f"{_abbreviate_town(job.pickup_street_address)}-{_abbreviate_town(job.dropoff_street_address)}")
            c.setLineWidth(2); c.line(text_x, y0 - 40, text_x, y_end); c.line(text_x - 10, y_end, text_x + 10, y_end)
        if job.assigned_crane_truck_id and 'J17' in column_map:
            crane_col_index = column_map['J17']; crane_text_x = margin + time_col_width + (crane_col_index + 0.5) * col_width
            y_crane_end = get_y_for_time(job.j17_busy_end_datetime.time())
            c.setFillColorRGB(0,0,0); c.setFont("Helvetica-Bold", 8); c.drawCentredString(crane_text_x, line1_y, customer.customer_name.split()[-1])
            c.setFont("Helvetica", 7); c.drawCentredString(crane_text_x, line2_y, _abbreviate_town(job.dropoff_street_address))
            c.setLineWidth(2); c.line(crane_text_x, y0-40, crane_text_x, y_crane_end); c.line(crane_text_x-3, y_crane_end, crane_text_x+3, y_crane_end)

    c.save()
    buffer.seek(0)
    return buffer

def generate_multi_day_planner_pdf(start_date, end_date, jobs):
    from PyPDF2 import PdfMerger
    from io import BytesIO
    merger = PdfMerger()
    for single_date in (start_date + datetime.timedelta(n) for n in range((end_date - start_date).days + 1)):
        jobs_for_day = [j for j in jobs if j.scheduled_start_datetime.date() == single_date]
        if jobs_for_day:
            daily_pdf_buffer = generate_daily_planner_pdf(single_date, jobs_for_day)
            merger.append(daily_pdf_buffer)
    output = BytesIO()
    merger.write(output)
    merger.close()
    output.seek(0)
    return output

# --- Session State Initialization ---
def initialize_session_state():
    defaults = {
        'data_loaded': False, 'info_message': "", 'current_job_request': None, 'found_slots': [], 
        'selected_slot': None, 'search_requested_date': None, 'was_forced_search': False,
        'num_suggestions': 3, 'crane_look_back_days': 7, 'crane_look_forward_days': 60,
        'slot_page_index': 0, 'truck_operating_hours': ecm.DEFAULT_TRUCK_OPERATING_HOURS,
        'show_copy_dropdown': False
    }
    for key, default_value in defaults.items():
        if key not in st.session_state: st.session_state[key] = default_value
    if not st.session_state.data_loaded:
        if ecm.load_customers_and_boats_from_csv("ECM Sample Cust.csv"):
            st.session_state.data_loaded = True
        else: st.error("Failed to load customer and boat data.")

initialize_session_state()

# --- Main App Body ---
st.title("Marine Transportation")

with st.container(border=True):
    stats = ecm.calculate_scheduling_stats(ecm.LOADED_CUSTOMERS, ecm.LOADED_BOATS, ecm.SCHEDULED_JOBS)
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Overall Progress")
        c1, c2 = st.columns(2)
        with c1:
            st.markdown(create_gauge(stats['all_boats']['scheduled'], stats['all_boats']['total'], "Scheduled"), unsafe_allow_html=True)
        with c2:
            st.markdown(create_gauge(stats['all_boats']['launched'], stats['all_boats']['total'], "Launched"), unsafe_allow_html=True)
    with col2:
        st.subheader("ECM Boats")
        c1, c2 = st.columns(2)
        with c1: st.metric(label="Scheduled", value=stats['ecm_boats']['scheduled'], delta=f"/ {stats['ecm_boats']['total']} Total", delta_color="off")
        with c2: st.metric(label="Launched (to date)", value=stats['ecm_boats']['launched'], delta=f"/ {stats['ecm_boats']['scheduled']} Sched.", delta_color="off")
st.markdown("---")

st.sidebar.title("Navigation")
app_mode = st.sidebar.radio("Go to", ["Schedule New Boat", "Reporting", "Settings"])

# --- PAGE 1: SCHEDULER ---
if app_mode == "Schedule New Boat":
    if st.session_state.info_message:
        st.info(st.session_state.info_message); st.session_state.info_message = ""
    if st.session_state.get("confirmation_message"):
        st.success(f"✅ {st.session_state.confirmation_message}")
        if st.button("Schedule Another Job"): st.session_state.pop("confirmation_message", None); st.rerun()

    st.sidebar.header("New Job Request")
    customer_name_input = st.sidebar.text_input("Enter Customer Name:", help="e.g., Olivia, James, Tho")
    customer, boat = None, None

    if customer_name_input:
        results = [c for c in ecm.LOADED_CUSTOMERS.values() if customer_name_input.lower() in c.customer_name.lower()]
        if len(results) == 1: customer = results[0]
        elif len(results) > 1:
            options = {c.customer_name: c for c in results}
            customer = options.get(st.sidebar.selectbox("Multiple matches, please select:", options.keys()))
        else: st.sidebar.warning("No customer found.")

    if customer:
        st.sidebar.success(f"Selected: {customer.customer_name}")
        boat = next((b for b in ecm.LOADED_BOATS.values() if b.customer_id == customer.customer_id), None)
        if not boat: st.sidebar.error(f"No boat found for {customer.customer_name}."); st.stop()

    if customer and boat:
        st.sidebar.markdown("---"); st.sidebar.subheader("Selected Customer & Boat:")
        st.sidebar.write(f"**Customer:** {customer.customer_name}")
        st.sidebar.write(f"**ECM Boat:** {'Yes' if customer.is_ecm_customer else 'No'}")
        st.sidebar.write(f"**Boat Type:** {boat.boat_type}")
        st.sidebar.write(f"**Boat Length:** {boat.boat_length}ft")
        st.sidebar.write(f"**Preferred Truck:** {ecm.ECM_TRUCKS.get(customer.preferred_truck_id, type('',(object,),{'truck_name':'N/A'})()).truck_name}")
        st.sidebar.markdown("---")

        service_type = st.sidebar.selectbox("Select Service Type:", ["Launch", "Haul", "Transport"])
        req_date = st.sidebar.date_input("Requested Date:", datetime.date.today() + datetime.timedelta(days=7))
        ramp_id = st.sidebar.selectbox("Select Ramp:", list(ecm.ECM_RAMPS.keys())) if service_type in ["Launch", "Haul"] else None
        
        st.sidebar.markdown("---"); st.sidebar.subheader("Search Options")
        relax_truck = st.sidebar.checkbox("Relax Truck (Use any capable truck)")
        manager_override = st.sidebar.checkbox("MANAGER: Override Crane Day Block")

        if st.sidebar.button("Find Best Slot"):
            st.session_state.current_job_request = {'customer_id': customer.customer_id, 'boat_id': boat.boat_id, 'service_type': service_type, 'requested_date_str': req_date.strftime('%Y-%m-%d'), 'selected_ramp_id': ramp_id}
            st.session_state.search_requested_date = req_date
            st.session_state.slot_page_index = 0
            
            slots, msg, _, _ = ecm.find_available_job_slots(**st.session_state.current_job_request, num_suggestions_to_find=st.session_state.num_suggestions, crane_look_back_days=st.session_state.crane_look_back_days, crane_look_forward_days=st.session_state.crane_look_forward_days, truck_operating_hours=st.session_state.truck_operating_hours, force_preferred_truck=(not relax_truck), manager_override=manager_override)
            st.session_state.info_message, st.session_state.found_slots, st.session_state.selected_slot = msg, slots, None
            st.rerun()

    if st.session_state.found_slots and not st.session_state.selected_slot:
        st.subheader("Please select your preferred slot:")
        total_slots, page_index, slots_per_page = len(st.session_state.found_slots), st.session_state.slot_page_index, 3
        
        nav_cols = st.columns([1, 1, 5, 1, 1])
        nav_cols[0].button("⬅️ Prev", on_click=lambda: st.session_state.update(slot_page_index=page_index - slots_per_page), disabled=(page_index == 0), use_container_width=True)
        nav_cols[1].button("Next ➡️", on_click=lambda: st.session_state.update(slot_page_index=page_index + slots_per_page), disabled=(page_index + slots_per_page >= total_slots), use_container_width=True)
        if total_slots > 0: nav_cols[3].write(f"_{min(page_index + 1, total_slots)}-{min(page_index + slots_per_page, total_slots)} of {total_slots}_")
        st.markdown("---")

        cols = st.columns(3)
        for i, slot in enumerate(st.session_state.found_slots[page_index : page_index + slots_per_page]):
            with cols[i % 3]:
                container_style = "position:relative; padding:10px; border-radius:8px; border: 3px solid #FF8C00; background-color:#FFF8DC; box-shadow: 0px 4px 8px rgba(0,0,0,0.1); margin-bottom: 15px; height: 260px;" if i == 0 and page_index == 0 else "position:relative; padding:10px; border-radius:5px; border: 2px solid #E0E0E0; background-color:#FFFFFF; margin-bottom: 15px; height: 260px;"
                card_html = f'<div style="{container_style}">'
                if slot.get('is_active_crane_day') or slot.get('is_candidate_crane_day'):
                    tooltip = "Active Crane Day" if slot.get('is_active_crane_day') else "Candidate Crane Day"
                    card_html += f'<span title="{tooltip}" style="position:absolute; top:8px; right:10px; font-size: 24px; cursor: help;">⛵</span>'
                if st.session_state.search_requested_date and slot['date'] == st.session_state.search_requested_date:
                    card_html += "<div style='background-color:#F0FFF0;border-left:6px solid #2E8B57;padding:5px;border-radius:3px;margin-bottom:8px;'><h6 style='color:#2E8B57;margin:0;font-weight:bold;'>⭐ Requested Date</h6></div>"
                
                ramp_details = ecm.get_ramp_details(slot.get('ramp_id'))
                card_html += f"""
                    <p><b>Date:</b> {slot['date'].strftime('%a, %b %d, %Y')}</p>
                    <p><b>Time:</b> {ecm.format_time_for_display(slot.get('time'))}</p>
                    <p><b>Truck:</b> {slot.get('truck_id', 'N/A')}</p>
                    <p><b>Ramp:</b> {ramp_details.ramp_name if ramp_details else "N/A"}</p>
                    <p><b>Tide Rule:</b> {slot.get('tide_rule_concise', 'N/A')}</p>
                    <p>{format_tides_for_display(slot, st.session_state.truck_operating_hours)}</p>
                </div>"""
                st.html(card_html)
                st.button("Select this slot", key=f"select_slot_{page_index + i}", on_click=handle_slot_selection, args=(slot,), use_container_width=True)

    elif st.session_state.selected_slot:
        slot = st.session_state.selected_slot
        st.subheader("Preview & Confirm Selection:")
        st.success(f"Considering: **{slot['date'].strftime('%Y-%m-%d %A')} at {ecm.format_time_for_display(slot.get('time'))}** with Truck **{slot.get('truck_id')}**.")
        if st.button("CONFIRM THIS JOB"):
            new_job_id, message = ecm.confirm_and_schedule_job(st.session_state.current_job_request, slot)
            if new_job_id:
                st.session_state.confirmation_message = message
                for key in ['found_slots', 'selected_slot', 'current_job_request', 'search_requested_date']:
                    st.session_state.pop(key, None)
                st.rerun()
            else: st.error(f"Failed to confirm job: {message}")

# --- REPORTING PAGE ---
elif app_mode == "Reporting":
    st.header("Reporting Dashboard")
    tab1, tab2, tab3, tab4 = st.tabs(["Scheduled Jobs Overview", "Crane Day Calendar", "Scheduling Progress", "PDF Export Tools"])
    with tab1:
        st.subheader("All Scheduled Jobs")
        if ecm.SCHEDULED_JOBS:
            data = [{'Job ID': j.job_id, 'Date': j.scheduled_start_datetime.strftime("%Y-%m-%d"), 'Time': ecm.format_time_for_display(j.scheduled_start_datetime.time()), 'Service': j.service_type, 'Customer': ecm.get_customer_details(j.customer_id).customer_name, 'Truck': j.assigned_hauling_truck_id, 'Crane': j.assigned_crane_truck_id or ""} for j in sorted(ecm.SCHEDULED_JOBS, key=lambda j: j.scheduled_start_datetime)]
            st.dataframe(pd.DataFrame(data))
        else: st.write("No jobs scheduled.")
    with tab2:
        st.subheader("Crane Day Candidate Calendar")
        ramp = st.selectbox("Select a ramp:", list(ecm.CANDIDATE_CRANE_DAYS.keys()), key="cal_ramp_sel")
        if ramp: display_crane_day_calendar(ecm.CANDIDATE_CRANE_DAYS[ramp])
    with tab3:
        st.subheader("Scheduling Progress Report")
        stats = ecm.calculate_scheduling_stats(ecm.LOADED_CUSTOMERS, ecm.LOADED_BOATS, ecm.SCHEDULED_JOBS)
        st.markdown("#### Overall Progress"); c1,c2=st.columns(2); c1.metric("Boats Scheduled", stats['all_boats']['scheduled'], f"{stats['all_boats']['total']} Total"); c2.metric("Boats Launched", stats['all_boats']['launched'], f"{stats['all_boats']['scheduled']} Scheduled")
        st.markdown("#### ECM Boats"); c1,c2=st.columns(2); c1.metric("ECM Scheduled", stats['ecm_boats']['scheduled'], f"{stats['ecm_boats']['total']} Total"); c2.metric("ECM Launched", stats['ecm_boats']['launched'], f"{stats['ecm_boats']['scheduled']} Scheduled")
        st.markdown("---"); st.subheader("Download Detailed Status Report")
        report_data = []
        for boat in ecm.LOADED_BOATS.values():
            cust = ecm.get_customer_details(boat.customer_id)
            if not cust: continue
            services = [j.service_type for j in ecm.SCHEDULED_JOBS if j.customer_id == cust.customer_id and j.job_status == "Scheduled"]
            status = "Launched" if "Launch" in services else (f"Scheduled ({', '.join(services)})" if services else "Not Scheduled")
            report_data.append({"Customer": cust.customer_name, "Boat": f"{boat.boat_length}' {boat.boat_type}", "ECM": cust.is_ecm_customer, "Status": status})
        st.download_button("📥 Download Full Report (.csv)", pd.DataFrame(report_data).to_csv(index=False).encode('utf-8'), f"status_report_{datetime.date.today()}.csv", "text/csv")
    with tab4:
        st.subheader("Generate Daily Planner PDF")
        selected_date = st.date_input("Select date to export:", value=datetime.date.today(), key="daily_pdf_date_input")
        if st.button("📤 Generate PDF", key="generate_daily_pdf_button"):
            jobs_today = [j for j in ecm.SCHEDULED_JOBS if j.scheduled_start_datetime.date() == selected_date]
            if not jobs_today:
                st.warning("No jobs scheduled for that date.")
            else:
                pdf_buffer = generate_daily_planner_pdf(selected_date, jobs_today)
                st.download_button(
                    label="📥 Download Planner", data=pdf_buffer.getvalue(),
                    file_name=f"Daily_Planner_{selected_date}.pdf", mime="application/pdf",
                    key="download_daily_planner_button"
                )

        st.markdown("---")
        st.subheader("Export Multi-Day Planner")
        dcol1, dcol2 = st.columns(2)
        with dcol1:
            start_date = st.date_input("Start Date", value=datetime.date.today(), key="multi_start_date")
        with dcol2:
            end_date = st.date_input("End Date", value=datetime.date.today() + datetime.timedelta(days=5), key="multi_end_date")

        if st.button("📤 Generate Multi-Day Planner PDF", key="generate_multi_pdf_button"):
            if start_date > end_date:
                st.error("Start date must be before or equal to end date.")
            else:
                jobs_in_range = [j for j in ecm.SCHEDULED_JOBS if start_date <= j.scheduled_start_datetime.date() <= end_date]
                if not jobs_in_range:
                    st.warning("No jobs scheduled in this date range.")
                else:
                    merged_pdf = generate_multi_day_planner_pdf(start_date, end_date, jobs_in_range)
                    st.download_button(
                        label="📥 Download Multi-Day Planner", data=merged_pdf,
                        file_name=f"Planner_{start_date}_to_{end_date}.pdf", mime="application/pdf",
                        key="download_multi_planner_button"
                    )
# --- SETTINGS PAGE ---
elif app_mode == "Settings":
    st.header("Application Settings")
    tab1, tab2 = st.tabs(["Scheduling Rules", "Truck Schedules"])
    with tab1:
        st.subheader("Scheduling Defaults")
        st.session_state.num_suggestions = st.number_input("Number of Suggested Dates", min_value=1, max_value=6, value=st.session_state.num_suggestions, step=1)
        st.markdown("---"); st.subheader("Crane Job Search Window")
        c1,c2 = st.columns(2)
        c1.number_input("Days to search in PAST", min_value=0, max_value=30, value=st.session_state.crane_look_back_days, key="crane_look_back_days")
        c2.number_input("Days to search in FUTURE", min_value=7, max_value=180, value=st.session_state.crane_look_forward_days, key="crane_look_forward_days")
    with tab2:
        st.subheader("Truck & Crane Weekly Hours")
        truck_id = st.selectbox("Select a resource to edit:", list(st.session_state.truck_operating_hours.keys()))
        if truck_id:
            if st.button("Copy Schedule From..."): st.session_state.show_copy_dropdown = True
            if st.session_state.get('show_copy_dropdown'):
                source_truck = st.selectbox("Select source:", [t for t in st.session_state.truck_operating_hours if t != truck_id])
                if st.button("Apply Copy"):
                    st.session_state.truck_operating_hours[truck_id] = st.session_state.truck_operating_hours[source_truck]
                    st.session_state.show_copy_dropdown = False; st.rerun()
            st.markdown("---")
            with st.form(f"form_{truck_id}"):
                new_hours = {}
                for i, day in enumerate(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]):
                    current = st.session_state.truck_operating_hours.get(truck_id, {}).get(i)
                    is_working = current is not None
                    summary = f"{day}: {ecm.format_time_for_display(current[0])} - {ecm.format_time_for_display(current[1])}" if is_working else f"{day}: Off Duty"
                    with st.expander(summary):
                        c1,c2,c3 = st.columns([1,2,2])
                        working = c1.checkbox("Working", value=is_working, key=f"{truck_id}_{i}_w")
                        start, end = (current[0], current[1]) if is_working else (datetime.time(8,0), datetime.time(16,0))
                        new_start = c2.time_input("Start", value=start, key=f"{truck_id}_{i}_s", disabled=not working)
                        new_end = c3.time_input("End", value=end, key=f"{truck_id}_{i}_e", disabled=not working)
                        new_hours[i] = (new_start, new_end) if working else None
                if st.form_submit_button("Save Hours"):
                    st.session_state.truck_operating_hours[truck_id] = new_hours
                    st.success(f"Updated hours for {truck_id}."); st.rerun()
