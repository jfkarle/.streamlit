import streamlit as st
import datetime
import ecm_scheduler_logic as ecm
import pandas as pd
from io import BytesIO
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch

def _abbreviate_town(address):
    if not address:
        return ""
    address = address.lower()
    if "scituate" in address:
        return "Sci"
    if "green harbor" in address:
        return "Grn"
    if "marshfield" in address:
        return "Mfield"
    if "cohasset" in address:
        return "Coh"
    if "weymouth" in address:
        return "Wey"
    if "plymouth" in address:
        return "Ply"
    if "sandwich" in address:
        return "Sand"
    if "duxbury" in address:
        return "Dux"
    if "humarock" in address:
        return "Huma"
    if "pembroke" in address or "ecm" in address:
        return "Pembroke"
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

    day_of_year = report_date.timetuple().tm_yday
    days_in_year = 366 if (report_date.year % 4 == 0 and report_date.year % 100 != 0) or (report_date.year % 400 == 0) else 365
    days_remaining = days_in_year - day_of_year
    c.setFont("Helvetica", 9)
    c.drawRightString(width - margin, height - 0.4 * inch, f"{day_of_year}/{days_remaining}")
    date_str = report_date.strftime("%A, %B %d").upper()
    c.setFont("Helvetica-Bold", 12)
    c.drawRightString(width - margin, height - 0.6 * inch, date_str)

    c.setFont("Helvetica-Bold", 11)
    for i, name in enumerate(planner_columns):
        x_center = margin + time_col_width + i * col_width + col_width / 2
        c.drawCentredString(x_center, top_y + 10, name)

    for i in range(len(planner_columns) + 1):
        x = margin + time_col_width + i * col_width
        c.setLineWidth(0.5)
        c.line(x, top_y, x, bottom_y)
    c.line(margin, top_y, margin, bottom_y)

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
        if boat_length and isinstance(boat_length, (int, float)) and boat_length > 0:
            boat_desc = f"{int(boat_length)}' {boat_type}"
        else:
            boat_desc = boat_type or "Unknown"

        origin = getattr(job, 'pickup_street_address', '') or ''
        dest = getattr(job, 'dropoff_street_address', '') or ''
        origin_abbr = _abbreviate_town(origin)
        dest_abbr = _abbreviate_town(dest)
        location_label = f"{origin_abbr}-{dest_abbr}"

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

st.title("Daily Planner PDF Generator")
st.write("Select a date to generate the daily boat delivery planner.")

selected_date = st.date_input("Select date", value=datetime.date.today())
if st.button("Generate PDF"):
    jobs = [job for job in ecm.SCHEDULED_JOBS if job.scheduled_start_datetime.date() == selected_date]
    if not jobs:
        st.warning("No jobs scheduled on this day.")
    else:
        pdf = generate_daily_planner_pdf(selected_date, jobs)
        st.download_button("📥 Download PDF", data=pdf, file_name=f"Daily_Planner_{selected_date}.pdf", mime="application/pdf")
