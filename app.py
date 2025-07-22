import streamlit as st
import datetime
import ecm_scheduler_logic as ecm
import pandas as pd
import csv
import math
import os
import datetime
import ecm_scheduler_logic as ecm
import json

#from reportlab.lib.pagesizes import letter
import calendar
#from reportlab.lib import colors
from io import BytesIO
#from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
#from reportlab.lib.styles import getSampleStyleSheet
#from reportlab.graphics.shapes import Drawing
#from reportlab.graphics.charts.barcharts import VerticalBarChart
#from reportlab.graphics.charts.piecharts import Pie

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
    <svg viewBox="0 0 100 65" style="width: 150px;height: 97px; overflow: visible;">
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
    op_hours = truck_schedule.get(truck_id, {}).get(slot_date.weekday())
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
            with col: st.markdown(f"<p style='text-align: center;font-weight: bold;'>{day_name}</p>", unsafe_allow_html=True)
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
                    cols[i].markdown(f'<div style="padding:10px; border-radius:5px; border: 2px solid {border_color};background-color:{bg_color}; height: 60px;"><p style="text-align: right; font-weight: {font_weight}; color: black;">{day.day}</p></div>', unsafe_allow_html=True)

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
    end_time_obj = datetime.time(17, 30)
    total_minutes = (end_time_obj.hour * 60 + end_time_obj.minute) - (start_time_obj.hour * 60 + start_time_obj.minute)

    top_y = height - margin - 0.8 * inch
    bottom_y = margin + 0.5 * inch
    content_height = top_y - bottom_y

    def get_y_for_time(t):
        minutes_into_day = (t.hour * 60 + t.minute) - (start_time_obj.hour * 60 + start_time_obj.minute)
        return top_y - ((minutes_into_day / total_minutes) * content_height)

    high_tide_highlights, low_tide_highlights = [], []
    primary_high_tide = None
    if jobs_for_day:
        ramp_id = getattr(jobs_for_day[0], 'pickup_ramp_id', None) or getattr(jobs_for_day[0], 'dropoff_ramp_id', None)
        if ramp_id:
            ramp_obj = ecm.get_ramp_details(ramp_id)
            # Fetch tides for the specific day
            tide_data_for_day = ecm.fetch_noaa_tides_for_range(ramp_obj.noaa_station_id, report_date, report_date)
            all_tides = tide_data_for_day.get(report_date, [])
            
            # Process the fetched tides
            high_tides = [t for t in all_tides if t.get('type') == 'H']
            if high_tides:
                noon = datetime.datetime.combine(datetime.date.min, datetime.time(12,0))
                primary_high_tide = min(high_tides, key=lambda t: abs(datetime.datetime.combine(datetime.date.min, t['time']) - noon))

            def round_time(t):
                mins = t.hour * 60 + t.minute;rounded = int(round(mins / 15.0) * 15)
                return datetime.time(min(23, rounded // 60), rounded % 60)
            high_tide_highlights = [round_time(t['time']) for t in all_tides.get('H', [])]
            low_tide_highlights = [round_time(t['time']) for t in all_tides.get('L', [])]

    c.setFont("Helvetica-Bold", 12);c.drawRightString(width - margin, height - 0.6 * inch, report_date.strftime("%A, %B %d").upper())
    if primary_high_tide:
        tide_time_str = ecm.format_time_for_display(primary_high_tide['time'])
        tide_height_str = f"{float(primary_high_tide.get('height', 0)):.1f}'"
        c.setFont("Helvetica-Bold", 9);c.drawString(margin, height - 0.6 * inch, f"High Tide: {tide_time_str} ({tide_height_str})")

    for i, name in enumerate(planner_columns):
        c.setFont("Helvetica-Bold", 14);c.drawCentredString(margin + time_col_width + i * col_width + col_width / 2, top_y + 10, name)

    c.setFont("Helvetica-Bold", 9)
    c.drawString(margin + 3, top_y - 9, "7:30")

    for hour in range(start_time_obj.hour + 1, end_time_obj.hour + 1):
        # --- UPDATED HIGHLIGHT LOGIC ---
        hour_highlight_color = None
        for m_check in [0, 15, 30, 45]:
            check_time = datetime.time(hour, m_check)
            if check_time in high_tide_highlights:
                hour_highlight_color = colors.Color(1, 1, 0, alpha=0.4)
                break
            elif check_time in low_tide_highlights:
                hour_highlight_color = colors.Color(1, 0.6, 0.6, alpha=0.4)
                break

        for minute in [0, 15, 30, 45]:
            current_time = datetime.time(hour, minute)
            if not (start_time_obj <= current_time <= end_time_obj): continue
            y = get_y_for_time(current_time)

            c.setStrokeColorRGB(0.7, 0.7, 0.7)
            c.setLineWidth(1.0 if minute == 0 else 0.25)
            c.line(margin, y, width - margin, y)

            if minute == 0:
                if hour_highlight_color:
                    c.setFillColor(hour_highlight_color)
                    c.rect(margin + 1, y - 11, time_col_width - 2, 13, fill=1, stroke=0)
                display_hour = hour if hour <= 12 else hour - 12
                c.setFont("Helvetica-Bold", 9);c.setFillColorRGB(0,0,0)
                c.drawString(margin + 3, y - 9, str(display_hour))

    c.setStrokeColorRGB(0,0,0)
    for i in range(len(planner_columns) + 1):
        x = margin + time_col_width + i * col_width;c.setLineWidth(0.5); c.line(x, top_y, x, bottom_y)
    c.line(margin, top_y, margin, bottom_y);c.line(width - margin, top_y, width - margin, bottom_y)
    c.line(margin, bottom_y, width - margin, bottom_y);c.line(margin, top_y, width - margin, top_y)

    for job in jobs_for_day:
        start_time, end_time = job.scheduled_start_datetime.time(), job.scheduled_end_datetime.time()
        if start_time < start_time_obj: start_time = start_time_obj
        y0, y_end = get_y_for_time(start_time), get_y_for_time(end_time)
        line1_y, line2_y, line3_y = y0 - 15, y0 - 25, y0 - 35
        customer, boat = ecm.get_customer_details(job.customer_id), ecm.get_boat_details(job.boat_id)
        if job.assigned_hauling_truck_id in column_map:
            col_index = column_map[job.assigned_hauling_truck_id]; text_x = margin + time_col_width + (col_index + 0.5) * col_width
            c.setFillColorRGB(0,0,0);c.setFont("Helvetica-Bold", 8); c.drawCentredString(text_x, line1_y, customer.customer_name)
            c.setFont("Helvetica", 7);c.drawCentredString(text_x, line2_y, f"{int(boat.boat_length)}' {boat.boat_type}")
            c.drawCentredString(text_x, line3_y, f"{ecm._abbreviate_town(job.pickup_street_address)}-{ecm._abbreviate_town(job.dropoff_street_address)}")
            c.setLineWidth(2);c.line(text_x, y0 - 45, text_x, y_end); c.line(text_x - 10, y_end, text_x + 10, y_end)
        if job.assigned_crane_truck_id and 'J17' in column_map:
            crane_col_index = column_map['J17'];crane_text_x = margin + time_col_width + (crane_col_index + 0.5) * col_width
            y_crane_end = get_y_for_time(job.j17_busy_end_datetime.time())
            c.setFillColorRGB(0,0,0);c.setFont("Helvetica-Bold", 8); c.drawCentredString(crane_text_x, line1_y, customer.customer_name.split()[-1])
            c.setFont("Helvetica", 7);c.drawCentredString(crane_text_x, line2_y, ecm._abbreviate_town(job.dropoff_street_address))
            c.setLineWidth(2); c.line(crane_text_x, y0-45, crane_text_x, y_crane_end);c.line(crane_text_x-3, y_crane_end, crane_text_x+3, y_crane_end)

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

#### Detailed report generation

def generate_progress_report_pdf(stats, analysis):
    """Generates a multi-page PDF progress report with stats, charts, and tables."""
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()

    # --- Page 1: Title and Executive Summary ---
    story.append(Paragraph("ECM Season Progress Report", styles['h1']))
    story.append(Paragraph(f"Generated on: {datetime.date.today().strftime('%B %d, %Y')}", styles['Normal']))
    story.append(Spacer(1, 24))

    story.append(Paragraph("Executive Summary", styles['h2']))
    story.append(Spacer(1, 12))

    # Overall Stats
    total_boats = stats['all_boats']['total']
    scheduled_boats = stats['all_boats']['scheduled']
    launched_boats = stats['all_boats']['launched']
    percent_scheduled = (scheduled_boats / total_boats * 100) if total_boats > 0 else 0
    percent_launched = (launched_boats / total_boats * 100) if total_boats > 0 else 0

    summary_data = [
        ['Metric', 'Value'],
        ['Total Boats in Fleet:', f'{total_boats}'],
        ['Boats Scheduled:', f'{scheduled_boats} ({percent_scheduled:.0f}%)'],
        ['Boats Launched (to date):', f'{launched_boats} ({percent_launched:.0f}%)'],
        ['Boats Remaining to Schedule:', f'{total_boats - scheduled_boats}'],
    ]
    summary_table = Table(summary_data, colWidths=[200, 100])
    summary_table.setStyle(TableStyle([
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('ALIGN', (1,1), (-1,-1), 'RIGHT'),
        ('GRID', (0,0), (-1,-1), 0.5, colors.grey)
    ]))
    story.append(summary_table)
    story.append(Spacer(1, 24))

    # --- Page 2: Analytics ---
    story.append(PageBreak())
    story.append(Paragraph("Scheduling Analytics", styles['h2']))
    story.append(Spacer(1, 12))

    # Jobs by Day Chart
    if analysis['by_day']:
        story.append(Paragraph("Jobs by Day of Week", styles['h3']))
        drawing = Drawing(400, 200)
        day_data = [tuple(v for k,v in sorted(analysis['by_day'].items()))]
        day_names = [k for k,v in sorted(analysis['by_day'].items())]
        bc = VerticalBarChart()
        bc.x = 50;bc.y = 50; bc.height = 125; bc.width = 300
        bc.data = day_data
        bc.categoryAxis.categoryNames = day_names
        drawing.add(bc)
        story.append(drawing)
        story.append(Spacer(1, 12))

    # Jobs by Ramp Chart
    if analysis['by_ramp']:
        story.append(Paragraph("Jobs by Ramp", styles['h3']))
        drawing = Drawing(400, 200)
        ramp_data = [tuple(v for k,v in sorted(analysis['by_ramp'].items()))]
        ramp_names = [k for k,v in sorted(analysis['by_ramp'].items())]
        bc_ramp = VerticalBarChart()
        bc_ramp.x = 50;bc_ramp.y = 50; bc_ramp.height = 125; bc_ramp.width = 300
        bc_ramp.data = ramp_data
        bc_ramp.categoryAxis.categoryNames = ramp_names
        bc_ramp.categoryAxis.labels.angle = 45 # Angle labels to fit
        drawing.add(bc_ramp)
        story.append(drawing)

    # --- Page 3+: Detailed List ---
    story.append(PageBreak())
    story.append(Paragraph("Detailed Boat Status", styles['h2']))
    story.append(Spacer(1, 12))

    table_data = [["Customer Name", "Boat Details", "ECM?", "Scheduling Status"]]
    for boat in ecm.LOADED_BOATS.values():
        cust = ecm.get_customer_details(boat.customer_id)
        if not cust: continue
        services = [j.service_type for j in ecm.SCHEDULED_JOBS if j.customer_id == cust.customer_id and j.job_status == "Scheduled"]
        status = "Launched" if "Launch" in services else (f"Scheduled ({', '.join(services)})" if services else "Not Scheduled")
        table_data.append([
            Paragraph(cust.customer_name, styles['Normal']),
            Paragraph(f"{boat.boat_length}' {boat.boat_type}", styles['Normal']),
            "Yes" if cust.is_ecm_customer else "No",
            status
        ])

    detail_table = Table(table_data, colWidths=[150, 150, 50, 150])
    detail_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.grey),
        ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke),
        ('ALIGN', (0,0), (-1,-1), 'LEFT'),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0,0), (-1,0), 12),
        ('BACKGROUND', (0,1), (-1,-1), colors.beige),
        ('GRID', (0,0), (-1,-1), 1, colors.black)
    ]))
    story.append(detail_table)

    doc.build(story)
    buffer.seek(0)
    return buffer

#### NEW CANCEL, REBOOK, PARK FUNCTINOALITY

# PASTE THE FOLLOWING TWO FUNCTIONS HERE

def show_scheduler_page():
    """
    Displays the entire Schedule New Boat page and handles new jobs, moved jobs,
    and the new seasonal return trip prompt.
    """
    # --- EXISTING LOGIC: Handle Seasonal Return Trip Prompt ---
    if job_info := st.session_state.get('last_seasonal_job'):
        st.success(st.session_state.get('confirmation_message', "Job Scheduled!"))
        st.markdown("---")
        
        opposite_service = "Haul" if job_info['original_service'] == "Launch" else "Launch"
        st.info(f"**Would you like to schedule the corresponding '{opposite_service}' for this boat?**")

        def setup_return_trip():
            st.session_state.selected_customer_id = job_info['customer_id']
            st.session_state.rebooking_details = {
                'service_type': opposite_service,
                'customer_id': job_info['customer_id'],
                'boat_id': job_info['boat_id']
                }
            st.session_state.last_seasonal_job = None
            st.session_state.confirmation_message = None

        def finish_scheduling():
            st.session_state.last_seasonal_job = None
            st.session_state.confirmation_message = None
            st.session_state.selected_customer_id = None
            st.session_state.selected_boat_id = None
            st.session_state.customer_search_input = ""

        col1, col2, _ = st.columns([1.5, 1, 3])
        col1.button(f"🗓️ Yes, Schedule {opposite_service}", on_click=setup_return_trip, use_container_width=True)
        col2.button("No, Finish", on_click=finish_scheduling, use_container_width=True)
        return

    # --- DEFINE CALLBACKS (with updates for boat selection) ---
    def schedule_another():
        st.session_state.pop("confirmation_message", None)
        st.session_state.selected_customer_id = None
        st.session_state.selected_boat_id = None
        st.session_state.customer_search_input = ""

    def select_customer(cust_id):
        st.session_state.selected_customer_id = cust_id
        st.session_state.selected_boat_id = None # Reset boat selection
        st.session_state.customer_search_input = ecm.LOADED_CUSTOMERS.get(cust_id).customer_name

    def clear_selection():
        st.session_state.selected_customer_id = None
        st.session_state.selected_boat_id = None
        st.session_state.customer_search_input = ""
    
    # --- EXISTING LOGIC: Message Handling ---
    if info_msg := st.session_state.get('info_message'):
        st.info(info_msg)
        if reasons := st.session_state.get('failure_reasons'):
            for reason in reasons:
                st.warning(reason) # Display each reason
        # Clear the messages
        st.session_state.info_message = ""
        st.session_state.failure_reasons = []

    if st.session_state.get("confirmation_message"):
        st.success(f"✅ {st.session_state.confirmation_message}")
        st.button("Schedule Another Job", on_click=schedule_another)
        return

    # --- REFACTORED SIDEBAR UI ---
    st.sidebar.header("New Job Request")
    
    customer = None
    boat = None

    # --- STEP 1: Customer Search ---
    if not st.session_state.get('selected_customer_id'):
        st.session_state.customer_search_input = st.sidebar.text_input(
            "Search for Customer or Boat ID:", value=st.session_state.get('customer_search_input', ''),
            placeholder="e.g., 'Olivia' or 'B5001'"
        )
        search_term = st.session_state.customer_search_input.lower().strip()
        if search_term:
            # (Your existing search logic is good)
            customer_results = [c for c in ecm.LOADED_CUSTOMERS.values() if search_term in c.customer_name.lower()]
            boat_results = [b for b in ecm.LOADED_BOATS.values() if search_term in str(b.boat_id).lower()]
            customers_from_boat_search = [ecm.LOADED_CUSTOMERS.get(b.customer_id) for b in boat_results if b and ecm.LOADED_CUSTOMERS.get(b.customer_id)]
            combined_customers = {c.customer_id: c for c in customer_results}
            for c in customers_from_boat_search:
                if c: combined_customers[c.customer_id] = c
            
            sorted_customers = sorted(combined_customers.values(), key=lambda c: c.customer_name)
            if sorted_customers:
                st.sidebar.write("---")
                with st.sidebar.container(height=250):
                    for cust in sorted_customers:
                        st.button(f"{cust.customer_name}", key=f"select_{cust.customer_id}", on_click=select_customer, args=(cust.customer_id,), use_container_width=True)
            else:
                st.sidebar.warning("No matches found.")
    
    # --- STEP 2: Boat Selection (after customer is selected) ---
    if st.session_state.get('selected_customer_id'):
        customer = ecm.LOADED_CUSTOMERS.get(st.session_state.selected_customer_id)
        if not customer:
            clear_selection()
            st.rerun()

        st.sidebar.text_input("Selected Customer:", value=customer.customer_name, disabled=True)
        st.sidebar.button("Clear Selection", on_click=clear_selection, use_container_width=True)
        
        boats_for_customer = [b for b in ecm.LOADED_BOATS.values() if b.customer_id == customer.customer_id]

        if not boats_for_customer:
            st.sidebar.error(f"No boats found for {customer.customer_name}.")
        elif len(boats_for_customer) == 1:
            st.session_state.selected_boat_id = boats_for_customer[0].boat_id
        else:
            boat_options = {f"{b.boat_length}' {b.boat_type} (ID: {b.boat_id})": b.boat_id for b in boats_for_customer}
            boat_options_with_prompt = {"-- Select a boat --": None, **boat_options}
            selected_boat_str = st.sidebar.selectbox("Select Boat:", options=boat_options_with_prompt.keys())
            st.session_state.selected_boat_id = boat_options_with_prompt[selected_boat_str]

    # --- STEP 3: Scheduling Form (after boat is selected) ---
    if customer and st.session_state.get('selected_boat_id'):
        boat = ecm.LOADED_BOATS.get(st.session_state.selected_boat_id)
        if not boat:
            st.sidebar.error("Selected boat not found. Please re-select.")
            st.session_state.selected_boat_id = None # Clear invalid boat ID
        else:
            st.sidebar.markdown("---")
            st.sidebar.subheader("Selected Customer & Boat:")
            st.sidebar.write(f"**Customer:** {customer.customer_name}")
            st.sidebar.write(f"**ECM Boat:** {'Yes' if boat.is_ecm_boat else 'No'}")
            st.sidebar.write(f"**Boat Type:** {boat.boat_type}")
            st.sidebar.write(f"**Boat Length:** {boat.boat_length}ft")
            st.sidebar.write(f"**Preferred Truck:** {boat.preferred_truck_id or 'N/A'}") # Now from boat
            st.sidebar.markdown("---")

            rebooking_details = st.session_state.get('rebooking_details')
            service_type_options = ["Launch", "Haul", "Transport"]
            service_index = service_type_options.index(rebooking_details['service_type']) if rebooking_details and rebooking_details.get('service_type') in service_type_options else 0
            service_type = st.sidebar.selectbox("Select Service Type:", service_type_options, index=service_index)
            # ... (previous code) ...
            req_date = st.sidebar.date_input("Requested Date:", datetime.date.today() + datetime.timedelta(days=90))
            
            ramp_id = None
            if service_type in ["Launch", "Haul"]:
                # --- START of block to replace ---
                ramp_options = list(ecm.ECM_RAMPS.keys())
                ramp_index = ramp_options.index(rebooking_details['ramp_id']) if rebooking_details and rebooking_details.get('ramp_id') in ramp_options else 0
                ramp_id = st.sidebar.selectbox("Select Ramp:", ramp_options, index=ramp_index, format_func=lambda ramp_id: ecm.ECM_RAMPS[ramp_id].ramp_name)
                # --- END of block to replace ---
            
            st.sidebar.markdown("---")
            st.sidebar.subheader("Search Options")
            # ... (rest of the code) ...
            relax_truck = st.sidebar.checkbox("Relax Truck (Use any capable truck)")
            manager_override = st.sidebar.checkbox("MANAGER: Override Crane Day Block")
            if st.sidebar.button("Find Best Slot"):
                st.session_state.current_job_request = {'customer_id': customer.customer_id, 'boat_id': boat.boat_id, 'service_type': service_type, 'requested_date_str': req_date.strftime('%Y-%m-%d'), 'selected_ramp_id': ramp_id}
                st.session_state.search_requested_date = req_date
                st.session_state.slot_page_index = 0
                slots, msg, reasons, _ = ecm.find_available_job_slots(**st.session_state.current_job_request, num_suggestions_to_find=st.session_state.num_suggestions, crane_look_back_days=st.session_state.crane_look_back_days, crane_look_forward_days=st.session_state.crane_look_forward_days, truck_operating_hours=ecm.TRUCK_OPERATING_HOURS, force_preferred_truck=(not relax_truck), manager_override=manager_override, prioritize_sailboats=st.session_state.get('sailboat_priority_enabled', True))
                st.session_state.info_message = msg
                st.session_state.found_slots = slots
                st.session_state.selected_slot = None
                st.session_state.failure_reasons = reasons # Store the reasons
                st.rerun()

    if st.session_state.found_slots and not st.session_state.selected_slot:
        # This block is now correctly indented
        st.subheader("Please select your preferred slot:")
        total_slots, page_index, slots_per_page = len(st.session_state.found_slots), st.session_state.slot_page_index, 3

        # --- This navigation part is unchanged ---
        nav_cols = st.columns([1, 1, 5, 1, 1])
        nav_cols[0].button("⬅️ Prev", on_click=lambda: st.session_state.update(slot_page_index=page_index - slots_per_page), disabled=(page_index == 0), use_container_width=True)
        nav_cols[1].button("Next ➡️", on_click=lambda: st.session_state.update(slot_page_index=page_index + slots_per_page), disabled=(page_index + slots_per_page >= total_slots), use_container_width=True)
        if total_slots > 0: 
            nav_cols[3].write(f"_{min(page_index + 1, total_slots)}-{min(page_index + slots_per_page, total_slots)} of {total_slots}_")
        st.markdown("---")
        
        # --- The results display loop has been rebuilt ---
        cols = st.columns(3)
        for i, slot in enumerate(st.session_state.found_slots[page_index : page_index + slots_per_page]):
            with cols[i % 3]:
                # Use a container to recreate the card effect
                with st.container(border=True):
                    # Display special message for the requested date
                    if st.session_state.search_requested_date and slot['date'] == st.session_state.search_requested_date:
                        st.success("⭐ Requested Date")

                    # Display the main slot info using st.write
                    ramp_details = ecm.get_ramp_details(slot.get('ramp_id'))
                    st.markdown(f"""
                    **Date:** {slot['date'].strftime('%a, %b %d, %Y')}  
                    **Time:** {ecm.format_time_for_display(slot.get('time'))}  
                    **Truck:** {slot.get('truck_id', 'N/A')}  
                    **Ramp:** {ramp_details.ramp_name if ramp_details else "N/A"}
                    """)
                    st.caption(f"Tide Rule: {slot.get('tide_rule_concise', 'N/A')}")
                    st.markdown(format_tides_for_display(slot, st.session_state.truck_operating_hours), unsafe_allow_html=True)
                    
                    # This is the new block that adds the clickable expander
                    if 'debug_trace' in slot:
                        with st.expander("Show Calculation Details"):
                            st.json(slot['debug_trace'])
                    
                    # Put the button at the bottom of the card
                    st.button("Select this slot", key=f"select_slot_{page_index + i}", on_click=handle_slot_selection, args=(slot,), use_container_width=True)
    
    elif st.session_state.selected_slot:
        slot = st.session_state.selected_slot
        st.subheader("Preview & Confirm Selection:")
        st.success(f"Considering: **{slot['date'].strftime('%Y-%m-%d %A')} at {ecm.format_time_for_display(slot.get('time'))}** with Truck **{slot.get('truck_id')}**.")
        
        if st.button("CONFIRM THIS JOB"):
            parked_job_id_to_remove = st.session_state.get('rebooking_details', {}).get('parked_job_id')
            new_job_id, message = ecm.confirm_and_schedule_job(st.session_state.current_job_request, slot, parked_job_to_remove=parked_job_id_to_remove)
            if new_job_id:
                st.session_state.confirmation_message = message
                service_type = st.session_state.current_job_request.get('service_type')
                if service_type in ["Launch", "Haul"]:
                    st.session_state.last_seasonal_job = {
                        "customer_id": st.session_state.current_job_request.get('customer_id'),
                        "boat_id": st.session_state.current_job_request.get('boat_id'),
                        "original_service": service_type
                    }
                for key in ['found_slots', 'selected_slot', 'current_job_request', 'search_requested_date', 'rebooking_details']:
                    st.session_state.pop(key, None)
                st.rerun()
            else: 
                st.error(f"Failed to confirm job: {message}")
                
def show_reporting_page():
    """
    Displays the entire Reporting dashboard, including all original tabs and
    interactive job management with a confirmation step for cancellation.
    """
    st.header("Reporting Dashboard")

    # --- Action Callbacks ---
    def move_job(job_id):
        job = ecm.get_job_details(job_id)
        if not job: return
        ecm.park_job(job_id)
        st.session_state.selected_customer_id = job.customer_id
        st.session_state.rebooking_details = {
            'parked_job_id': job.job_id, 'customer_id': job.customer_id,
            'service_type': job.service_type, 'ramp_id': job.dropoff_ramp_id or job.pickup_ramp_id
        }
        st.session_state.info_message = f"Rebooking job for {ecm.get_customer_details(job.customer_id).customer_name}. Please find a new slot."
        st.session_state.app_mode_switch = "Schedule New Boat"

    def park_job(job_id):
        ecm.park_job(job_id)
        st.toast(f"🅿️ Job #{job_id} has been parked.", icon="🅿️")

    def reschedule_parked_job(parked_job_id):
        job = ecm.get_parked_job_details(parked_job_id)
        if not job: return
        st.session_state.selected_customer_id = job.customer_id
        st.session_state.rebooking_details = {
            'parked_job_id': job.job_id, 'customer_id': job.customer_id,
            'service_type': job.service_type, 'ramp_id': job.dropoff_ramp_id or job.pickup_ramp_id
        }
        st.session_state.info_message = f"Rescheduling parked job for {ecm.get_customer_details(job.customer_id).customer_name}. Please select a new slot."
        st.session_state.app_mode_switch = "Schedule New Boat"

    def prompt_for_cancel(job_id):
        st.session_state.job_to_cancel = job_id

    def clear_cancel_prompt():
        st.session_state.job_to_cancel = None

    def cancel_job_confirmed():
        job_id = st.session_state.get('job_to_cancel')
        if job_id:
            ecm.cancel_job(job_id)
            st.toast(f"🗑️ Job #{job_id} has been permanently cancelled.", icon="🗑️")
            clear_cancel_prompt()

    # --- UI Layout ---
    tab_keys = ["Scheduled Jobs", "Crane Day Calendar", "Progress", "PDF Exports", "Parked Jobs"]
    tab1, tab2, tab3, tab4, tab5 = st.tabs(tab_keys)

    with tab1:
        st.subheader("Scheduled Jobs Overview")
        if ecm.SCHEDULED_JOBS:
            # Header for the jobs list
            cols = st.columns((2, 1, 2, 1, 1, 3))
            fields = ["Date/Time", "Service", "Customer", "Haul Truck", "Crane", "Actions"]
            for col, field in zip(cols, fields):
                col.markdown(f"**{field}**")
            st.markdown("---")

            # Display a row for each scheduled job
            for j in sorted(ecm.SCHEDULED_JOBS, key=lambda j: j.scheduled_start_datetime or datetime.datetime.max):
                customer = ecm.get_customer_details(j.customer_id)
                if not customer:
                    continue # Skip this job if the customer can't be found
                
                cols = st.columns((2, 1, 2, 1, 1, 3))
                
                # Check if the datetime exists before trying to format it
                if j.scheduled_start_datetime:
                    cols[0].write(j.scheduled_start_datetime.strftime("%a, %b %d @ %I:%M%p"))
                else:
                    cols[0].warning("No Date Set") # Show a warning for bad data

                cols[1].write(j.service_type)
                cols[2].write(customer.customer_name)
                cols[3].write(j.assigned_hauling_truck_id or "—")
                cols[4].write(j.assigned_crane_truck_id or "—")

                # Actions column with cancel confirmation logic
                with cols[5]:
                    if st.session_state.get('job_to_cancel') == j.job_id:
                        st.warning("Are you sure?")
                        btn_cols = st.columns(2)
                        btn_cols[0].button("✅ Yes, Cancel", key=f"confirm_cancel_{j.job_id}", on_click=cancel_job_confirmed, use_container_width=True, type="primary")
                        btn_cols[1].button("❌ No", key=f"deny_cancel_{j.job_id}", on_click=clear_cancel_prompt, use_container_width=True)
                    else:
                        btn_cols = st.columns(3)
                        btn_cols[0].button("Move", key=f"move_{j.job_id}", on_click=move_job, args=(j.job_id,), use_container_width=True)
                        btn_cols[1].button("Park", key=f"park_{j.job_id}", on_click=park_job, args=(j.job_id,), use_container_width=True)
                        btn_cols[2].button("Cancel", key=f"cancel_{j.job_id}", on_click=prompt_for_cancel, args=(j.job_id,), type="primary", use_container_width=True)
        else:
            st.write("No jobs scheduled.")
    
    with tab2:
        st.subheader("Crane Day Candidate Calendar")
        ramp_options = list(ecm.CANDIDATE_CRANE_DAYS.keys())
        if ramp_options:
            ramp = st.selectbox("Select a ramp:", ramp_options, key="cal_ramp_sel")
            if ramp: display_crane_day_calendar(ecm.CANDIDATE_CRANE_DAYS[ramp])
        else:
            st.warning("No crane day data available.")

    with tab3:
        st.subheader("Scheduling Progress Report")
        stats = ecm.calculate_scheduling_stats(ecm.LOADED_CUSTOMERS, ecm.LOADED_BOATS, ecm.SCHEDULED_JOBS)
        st.markdown("#### Overall Progress")
        c1, c2 = st.columns(2)
        c1.metric("Boats Scheduled", f"{stats['all_boats']['scheduled']} / {stats['all_boats']['total']}")
        c2.metric("Boats Launched (to date)", f"{stats['all_boats']['launched']} / {stats['all_boats']['total']}")
        st.markdown("#### ECM Boats")
        c1, c2 = st.columns(2)
        c1.metric("ECM Scheduled", f"{stats['ecm_boats']['scheduled']} / {stats['ecm_boats']['total']}")
        c2.metric("ECM Launched (to date)", f"{stats['ecm_boats']['launched']} / {stats['ecm_boats']['total']}")
        st.markdown("---")
        st.subheader("Download Formatted PDF Report")
        if st.button("📊 Generate PDF Report"):
            with st.spinner("Generating your report..."):
                analysis = ecm.analyze_job_distribution(ecm.SCHEDULED_JOBS, ecm.LOADED_BOATS, ecm.ECM_RAMPS)
                pdf_buffer = generate_progress_report_pdf(stats, analysis)
                #st.download_button(label="📥 Download Report (.pdf)", data=pdf_buffer, file_name=f"progress_report_{datetime.date.today()}.pdf", mime="application/pdf")

    with tab4:
        st.subheader("Generate Daily Planner PDF")
        selected_date = st.date_input("Select date to export:", value=datetime.date.today(), key="daily_pdf_date_input")
        if st.button("📤 Generate PDF", key="generate_daily_pdf_button"):
            jobs_today = [j for j in ecm.SCHEDULED_JOBS if j.scheduled_start_datetime.date() == selected_date]
            if not jobs_today:
                st.warning("No jobs scheduled for that date.")
            else:
                pdf_buffer = generate_daily_planner_pdf(selected_date, jobs_today)
                #st.download_button(label="📥 Download Planner", data=pdf_buffer.getvalue(), file_name=f"Daily_Planner_{selected_date}.pdf", mime="application/pdf", key="download_daily_planner_button")

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
                    #st.download_button(label="📥 Download Multi-Day Planner", data=merged_pdf, file_name=f"Planner_{start_date}_to_{end_date}.pdf", mime="application/pdf", key="download_multi_planner_button")

    with tab5:
        st.subheader("🅿️ Parked Jobs")
        st.info("These jobs have been removed from the schedule and are waiting to be re-booked. Reschedule them from here.")
        if ecm.PARKED_JOBS:
            # Header
            cols = st.columns((2, 2, 1, 2))
            fields = ["Customer", "Boat", "Service", "Actions"]
            for col, field in zip(cols, fields):
                col.markdown(f"**{field}**")
            st.markdown("---")

            # Parked Job Rows
            for job_id, job in ecm.PARKED_JOBS.items():
                customer = ecm.get_customer_details(job.customer_id)
                boat = ecm.get_boat_details(job.boat_id)
                cols = st.columns((2, 2, 1, 2))
                cols[0].write(customer.customer_name)
                cols[1].write(f"{boat.boat_length}' {boat.boat_type}")
                cols[2].write(job.service_type)
                with cols[3]:
                    st.button("Reschedule", key=f"reschedule_{job.job_id}", on_click=reschedule_parked_job, args=(job.job_id,), use_container_width=True)
        else:
            st.write("No jobs are currently parked.")


def show_settings_page():
    st.header("Application Settings")
    tab_list = ["Scheduling Rules", "Truck Schedules", "Developer Tools", "Tide Charts"]
    tab1, tab2, tab3, tab4 = st.tabs(tab_list)

    with tab1:
        st.subheader("Scheduling Defaults")
        st.session_state.num_suggestions = st.number_input("Number of Suggested Dates to Return", min_value=1, max_value=10, value=st.session_state.get('num_suggestions', 3), step=1)
        
        # --- NEW TOGGLE SWITCH ---
        st.markdown("---")
        st.subheader("Advanced Logic")
        st.toggle(
            "Prioritize Sailboats on Prime Tide Days",
            value=st.session_state.get('sailboat_priority_enabled', True),
            key='sailboat_priority_enabled',
            help="When enabled, the scheduler will give a large bonus to sailboats on days with favorable high tides, making them 'outbid' powerboats for those slots. When disabled, all boats are treated equally."
        )
        # --- END OF NEW SWITCH ---

        st.markdown("---")
        st.subheader("Crane Job Search Window")
        c1,c2 = st.columns(2)
        c1.number_input("Days to search in PAST", min_value=0, max_value=30, value=st.session_state.get('crane_look_back_days', 7), key="crane_look_back_days")
        c2.number_input("Days to search in FUTURE", min_value=7, max_value=180, value=st.session_state.get('crane_look_forward_days', 60), key="crane_look_forward_days")

    with tab2:
        st.subheader("Truck & Crane Weekly Hours")
        st.info("NOTE: Changes made here are saved permanently to the database.")

        schedule_to_edit = ecm.TRUCK_OPERATING_HOURS
        truck_id = st.selectbox("Select a resource to edit:", list(schedule_to_edit.keys()))
        
        if truck_id:
            st.markdown("---")
            with st.form(f"form_{truck_id}"):
                st.write(f"**Editing hours for {truck_id}**")
                
                # This dictionary will hold the new hours from the form inputs
                new_hours = {}
                
                days_of_week = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                for i, day_name in enumerate(days_of_week):
                    # Get the current hours for this day from the loaded schedule
                    current_hours = schedule_to_edit.get(truck_id, {}).get(i)
                    is_working = current_hours is not None
                    
                    # Set default times for the time_input widgets
                    start_time, end_time = current_hours if is_working else (datetime.time(8, 0), datetime.time(16, 0))

                    # Display a summary in the expander title
                    summary = f"{day_name}: {ecm.format_time_for_display(start_time)} - {ecm.format_time_for_display(end_time)}" if is_working else f"{day_name}: Off Duty"
                    
                    with st.expander(summary):
                        col1, col2, col3 = st.columns([1, 2, 2])
                        working = col1.checkbox("Working", value=is_working, key=f"{truck_id}_{i}_working")
                        
                        new_start = col2.time_input("Start", value=start_time, key=f"{truck_id}_{i}_start", disabled=not working)
                        new_end = col3.time_input("End", value=end_time, key=f"{truck_id}_{i}_end", disabled=not working)
                        
                        # Store the result for this day
                        new_hours[i] = (new_start, new_end) if working else None
                
                if st.form_submit_button("Save Hours"):
                    # Call the new function to update the database
                    success, message = ecm.update_truck_schedule(truck_id, new_hours)

                    if success:
                        # Also update the in-memory data to match, avoiding a full reload
                        ecm.TRUCK_OPERATING_HOURS[truck_id] = new_hours
                        st.success(message)
                        st.rerun()
                    else:
                        st.error(message)

    with tab3:
        st.subheader("QA & Data Generation Tools")
        st.write("This tool creates random, valid jobs to populate the calendar for testing.")
        num_jobs_to_gen = st.number_input("Number of jobs to generate:", min_value=1, max_value=100, value=25, step=1)
        service_type_input = st.selectbox("Type of jobs to create:", ["All", "Launch", "Haul", "Transport"])
        dcol1, dcol2 = st.columns(2)
        start_date_input = dcol1.date_input("Start of date range:", datetime.date(2025, 4, 15))
        end_date_input = dcol2.date_input("End of date range:", datetime.date(2025, 7, 1))
        if st.button("Generate Random Jobs"):
            if start_date_input > end_date_input:
                st.error("Start date cannot be after end date.")
            else:
                with st.spinner(f"Generating {num_jobs_to_gen} jobs..."):
                   summary = ecm.generate_random_jobs(
                        num_jobs_to_gen, 
                        start_date_input, 
                        end_date_input, 
                        service_type_input, 
                        st.session_state.truck_operating_hours
                    )
                st.success(summary)
                st.info("Navigate to the 'Reporting' page to see the newly generated jobs.")

    with tab4:
        st.subheader("Monthly Tide Chart for Scituate Harbor")

        col1, col2 = st.columns(2)
        with col1:
            current_year = datetime.date.today().year
            year_options = list(range(current_year - 1, current_year + 4))
            default_year_index = year_options.index(2025) if 2025 in year_options else 2
            selected_year = st.selectbox("Select Year:", options=year_options, index=default_year_index)
        with col2:
            month_names = list(calendar.month_name)[1:]
            selected_month_name = st.selectbox("Select Month:", options=month_names, index=8)

        # Callback to set the selected day in session state
        def select_day(date_obj):
            st.session_state.selected_tide_day = date_obj

        # Fetch data for the whole month
        month_index = month_names.index(selected_month_name) + 1
        tide_data = ecm.get_monthly_tides_for_scituate(selected_year, month_index)

        if not tide_data:
            st.warning("Could not retrieve tide data. The NOAA API might be unavailable.")
        else:
            # --- Create the Calendar Grid ---
            st.markdown("---")
            cal = calendar.Calendar()
            cal_data = cal.monthdatescalendar(selected_year, month_index)

            # Display day of the week headers
            header_cols = st.columns(7)
            for i, day_name in enumerate(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]):
                header_cols[i].markdown(f"<p style='text-align: center; font-weight: bold;'>{day_name}</p>", unsafe_allow_html=True)
            st.divider()

            # Display the calendar days
            for week in cal_data:
                cols = st.columns(7)
                for i, day in enumerate(week):
                    with cols[i]:
                        if day.month != month_index:
                            st.container(height=55, border=False) # Empty placeholder for days not in month
                        else:
                            # Use a button to make the day selectable
                            st.button(
                                str(day.day),
                                key=f"day_{day}",
                                on_click=select_day,
                                args=(day,),
                                use_container_width=True,
                                type="secondary" if st.session_state.selected_tide_day != day else "primary"
                            )
            st.divider()

            # --- Display Tide Details for Selected Day ---
            if selected_day := st.session_state.get('selected_tide_day'):
                if selected_day.year == selected_year and selected_day.month == month_index:
                    day_str = selected_day.strftime("%A, %B %d, %Y")
                    st.subheader(f"Tides for: {day_str}")

                    tides_for_day = tide_data.get(selected_day, [])
                    if not tides_for_day:
                        st.write("No tide data available for this day.")
                    else:
                        high_tides = [f"{ecm.format_time_for_display(t['time'])} ({float(t['height']):.1f}')" for t in tides_for_day if t['type'] == 'H']
                        low_tides = [f"{ecm.format_time_for_display(t['time'])} ({float(t['height']):.1f}')" for t in tides_for_day if t['type'] == 'L']
                        tide_col1, tide_col2 = st.columns(2)
                        tide_col1.metric("🌊 High Tides", " / ".join(high_tides) if high_tides else "N/A")
                        tide_col2.metric("💧 Low Tides", " / ".join(low_tides) if low_tides else "N/A")


# --- Session State Initialization ---

def initialize_session_state():
    defaults = {
        'data_loaded': False, 'info_message': "", 'current_job_request': None, 'found_slots': [],
        'selected_slot': None, 'search_requested_date': None, 'was_forced_search': False,
        'num_suggestions': 3, 'crane_look_back_days': 7, 'crane_look_forward_days': 60,
        'slot_page_index': 0, 'truck_operating_hours': ecm.TRUCK_OPERATING_HOURS,
        'show_copy_dropdown': False,
        'customer_search_input': '',
        'selected_customer_id': None,
        'selected_boat_id': None, # <-- THIS LINE WAS MISSING
        'job_to_cancel': None,
        'selected_tide_day': None, 
        'sailboat_priority_enabled': True,
        'last_seasonal_job': None
    }
    
    for key, default_value in defaults.items():
        if key not in st.session_state: st.session_state[key] = default_value
            
    if not st.session_state.get('data_loaded'):
        ecm.load_all_data_from_sheets()
        st.session_state.data_loaded = True
        
initialize_session_state()

# --- Main App Body ---
st.title("ECM Logistics")

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

# PASTE THIS REPLACEMENT BLOCK AT THE END OF YOUR FILE

st.sidebar.title("Navigation")

page_options = ["Schedule New Boat", "Reporting", "Settings"]

# This corrected logic correctly determines the page index.
# It prioritizes a programmatic switch, then the radio button's own
# state, and finally defaults to the first page on the first run.
if switch_to := st.session_state.get("app_mode_switch"):
    try:
        index = page_options.index(switch_to)
    except ValueError:
        index = 0
    del st.session_state.app_mode_switch
elif radio_state := st.session_state.get("app_mode_radio"):
    try:
        index = page_options.index(radio_state)
    except ValueError:
        index = 0
else:
    index = 0

app_mode = st.sidebar.radio(
    "Go to",
    page_options,
    index=index,
    key="app_mode_radio" # The key is essential for remembering the state
)


# Call the new functions based on the selected mode
if app_mode == "Schedule New Boat":
    show_scheduler_page()
    with st.expander("Show Debug Log for Last Slot Search", expanded=False):
        st.text_area("Debug Output:", "\n".join(ecm.DEBUG_MESSAGES), height=500, key="debug_log_text_area")
elif app_mode == "Reporting":
    show_reporting_page()
elif app_mode == "Settings":
    # Just call the function. That's it.
    show_settings_page()



