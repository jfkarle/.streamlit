# app.py
# FINAL, ENHANCED VERSION WITH PDF INTEGRATION

import streamlit as st
import datetime
import ecm_scheduler_logic as ecm
import pandas as pd
from io import BytesIO
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch

st.set_page_config(layout="wide")

app_mode = st.sidebar.radio("Go to", ["Schedule New Boat", "Reporting", "Settings"])

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

def _abbreviate_town(address):
    if not address:
        return ""
    address = address.lower()
    for k, v in {
        "scituate": "Sci", "green harbor": "Grn", "marshfield": "Mfield", "cohasset": "Coh",
        "weymouth": "Wey", "plymouth": "Ply", "sandwich": "Sand", "duxbury": "Dux",
        "humarock": "Huma", "pembroke": "Pembroke", "ecm": "Pembroke"
    }.items():
        if k in address: return v
    return address.title().split(',')[0]

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
    start_hour, end_hour = 7, 18
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

    # Columns
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
        truck_id = getattr(job, 'assigned_crane_truck_id', None) or getattr(job, 'assigned_hauling_truck_id', None)
        if truck_id not in column_map:
            continue
        col_index = column_map[truck_id]
        column_start_x = margin + time_col_width + col_index * col_width
        text_center_x = column_start_x + col_width / 2

        start_time = job.scheduled_start_datetime.time()
        end_time = job.scheduled_end_datetime.time()
        dt_base = datetime.datetime.combine(datetime.date.today(), start_time)
        y0 = get_y_for_time(start_time)
        y1 = get_y_for_time((dt_base + datetime.timedelta(minutes=15)).time())
        y2 = get_y_for_time((dt_base + datetime.timedelta(minutes=30)).time())
        y3 = get_y_for_time((dt_base + datetime.timedelta(minutes=45)).time())

        line1_y = (y0 + y1) / 2
        line2_y = (y1 + y2) / 2
        line3_y = (y2 + y3) / 2

        customer = ecm.get_customer_details(getattr(job, 'customer_id', None))
        customer_name = customer.customer_name.split()[-1] if customer and customer.customer_name else "Unknown"

        boat_length = getattr(job, 'boat_length', None)
        boat_type = getattr(job, 'boat_type', '')
        boat_desc = f"{int(boat_length)}' {boat_type}" if boat_length else boat_type or "Unknown"

        origin = getattr(job, 'pickup_street_address', '') or ''
        dest = getattr(job, 'dropoff_street_address', '') or ''
        location_label = f"{_abbreviate_town(origin)}-{_abbreviate_town(dest)}"

        c.setFont("Helvetica-Bold", 8)
        c.drawCentredString(text_center_x, line1_y, customer_name)
        c.setFont("Helvetica", 7)
        c.drawCentredString(text_center_x, line2_y, boat_desc)
        c.drawCentredString(text_center_x, line3_y, location_label)

        y_bar_start = y3 + 6
        y_end = get_y_for_time(end_time)
        c.setLineWidth(2)
        c.line(text_center_x, y_bar_start, text_center_x, y_end)
        c.line(text_center_x - 3, y_end, text_center_x + 3, y_end)

    c.save()
    buffer.seek(0)
    return buffer

# --- REPORTING EXTENSION ---
if app_mode == "Reporting":
    st.header("Reporting Dashboard")
    st.subheader("Generate Daily Planner PDF")
    selected_date = st.date_input("Select date for report", value=datetime.date.today())
    if st.button("Generate Daily Planner PDF"):
        jobs_for_day = [j for j in ecm.SCHEDULED_JOBS if j.scheduled_start_datetime.date() == selected_date]
        if not jobs_for_day:
            st.warning("No jobs scheduled for this date.")
        else:
            pdf_buffer = generate_daily_planner_pdf(selected_date, jobs_for_day)
            st.download_button(
                label="📥 Download PDF Planner",
                data=pdf_buffer,
                file_name=f"Daily_Planner_{selected_date}.pdf",
                mime="application/pdf"
            )
