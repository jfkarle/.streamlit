# app.py
# FINAL, CORRECTED AND VERIFIED VERSION

import streamlit as st
import datetime
import ecm_scheduler_logic as ecm
import pandas as pd
from io import BytesIO
# --- ADDED LIBRARIES FOR PDF GENERATION ---
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors

# =============================================================================
# --- PDF Generation Functions ---
# =============================================================================

def _abbreviate_location(location_name):
    """Helper function to create location abbreviations per user rules."""
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
    """
    Creates a high-fidelity PDF file in the style of the user-provided daily planner.
    """
    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter

    # --- 1. Define High-Fidelity Planner Layout ---
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
        total_minutes_from_start = (t.hour - start_hour) * 60 + t.minute
        total_planner_minutes = (end_hour - start_hour) * 60
        return top_y - (total_minutes_from_start / total_planner_minutes * content_height)

    # --- 2. Draw PDF Header ---
    day_of_year = report_date.timetuple().tm_yday
    days_in_year = 366 if (report_date.year % 4 == 0 and report_date.year % 100 != 0) or (report_date.year % 400 == 0) else 365
    days_remaining = days_in_year - day_of_year
    
    c.setFont("Helvetica", 9)
    c.drawRightString(width - margin, height - 0.4 * inch, f"{day_of_year}/{days_remaining}")
    
    date_str = report_date.strftime("%A, %B %d").upper()
    c.setFont("Helvetica-Bold", 12)
    c.drawRightString(width - margin, height - 0.6 * inch, date_str)


    # --- 3. Draw High-Fidelity Grid ---
    c.setFont("Helvetica-Bold", 10)
    for i, col_name in enumerate(planner_columns):
        x_center = margin + time_col_width + (i * col_width) + (col_width / 2)
        c.drawCentredString(x_center, top_y + 8, col_name)

    c.setLineWidth(0.5)
    for i in range(len(planner_columns) + 1):
        x = margin + time_col_width + (i * col_width)
        c.line(x, top_y, x, bottom_y)
    c.line(margin, top_y, margin, bottom_y)
    
    for hour in range(start_hour, end_hour + 1):
        for minute in [0, 15, 30, 45]:
            current_time = datetime.time(hour, minute)
            if current_time > datetime.time(end_hour, 0): continue
            
            y = get_y_for_time(current_time)
            
            if minute == 0:
                c.setLineWidth(1.0)
                c.setFont("Helvetica-Bold", 10)
                c.drawString(margin + 5, y - 4, str(hour))
                c.setFont("Helvetica", 8)
                c.drawString(margin + 20, y - 3, "00")
            else:
                c.setLineWidth(0.25)
                c.setFont("Helvetica", 7)
                c.drawString(margin + 20, y - 2, str(minute))
                
            c.line(margin, y, width - margin, y)

    # --- 4. Place Jobs and Duration Lines onto the Grid ---
    for job in jobs_for_day:
        truck_id = job.assigned_hauling_truck_id
        if job.assigned_crane_truck_id:
             truck_id = "S17"

        if truck_id not in column_map:
            continue

        col_index = column_map[truck_id]
        job_start_time = job.scheduled_start_datetime.time()
        job_end_time = job.scheduled_end_datetime.time()

        # --- NEW, SIMPLIFIED & CORRECTED LOGIC FOR POSITIONING ---
        column_start_x = margin + time_col_width + (col_index * col_width)
        line_x = column_start_x + (col_width * 0.2)
        text_center_x = column_start_x + (col_width * 0.6)

        job_slot_top_y = get_y_for_time(job_start_time)
        next_slot_time = (datetime.datetime.combine(datetime.date.today(), job_start_time) + datetime.timedelta(minutes=15)).time()
        job_slot_bottom_y = get_y_for_time(next_slot_time)
        job_slot_center_y = job_slot_bottom_y + ((job_slot_top_y - job_slot_bottom_y) / 2)

        customer = ecm.get_customer_details(job.customer_id)
        boat = ecm.get_boat_details(job.boat_id)
        last_name = customer.customer_name.split(' ')[-1] if ' ' in customer.customer_name else customer.customer_name
        origin = _abbreviate_location(job.pickup_street_address)
        destination = _abbreviate_location(job.dropoff_street_address)
        
        location_text = f"{origin}-{destination}"
        if job.service_type == "Launch":
            location_text = f"Launch-{destination}"
        elif job.service_type == "Haul":
            location_text = f"Haul-{origin}"
        
        c.setFont("Helvetica", 11)
        line_height = 13

        c.drawCentredString(text_center_x, job_slot_center_y + line_height, last_name)
        c.drawCentredString(text_center_x, job_slot_center_y, f"{int(boat.boat_length)}' {boat.boat_type}")
        c.drawCentredString(text_center_x, job_slot_center_y - line_height, location_text)

        text_block_bottom_y = job_slot_center_y - line_height - (line_height / 2)
        
        y_start_for_line = text_block_bottom_y
        y_end_for_line = get_y_for_time(job_end_time)
        
        c.setLineWidth(1.0)
        c.setStrokeColorRGB(0.1, 0.1, 0.1)
        
        c.line(line_x, y_start_for_line, line_x, y_end_for_line)
        c.line(line_x - 3, y_end_for_line, line_x + 3, y_end_for_line)
        c.setStrokeColorRGB(0,0,0)

    c.save()
    buffer.seek(0)
    return buffer

#=============================================================================
# --- Main Application Logic (No changes needed below this line) ---
#=============================================================================

def format_tides_for_display(slot, ecm_hours):
    # ... (rest of the file is unchanged)
    # ...
