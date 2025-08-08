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
import uuid
from geopy.geocoders import Nominatim
from datetime import timezone
from reportlab.lib.pagesizes import letter
import calendar
from reportlab.lib import colors
from io import BytesIO
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.graphics.shapes import Drawing
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.charts.piecharts import Pie

st.set_page_config(layout="wide")

# --- Helper Functions for UI ---

from ecm_scheduler_logic import load_all_data_from_sheets
load_all_data_from_sheets()   # this populates LOADED_BOATS, etc.

st.sidebar.write(f"🔍 Loaded {len(ecm.LOADED_BOATS)} boats from Supabase")

# --- Helper Functions for UI ---

import uuid # Make sure to import uuid at the top of app.py

class SlotDetail:
    """A wrapper class to make slot dictionaries easier to use in the UI."""
    def __init__(self, slot_dict):
        self.raw_data = slot_dict
        
        # --- Basic Attributes ---
        self.date = slot_dict.get('date')
        self.time = slot_dict.get('time')
        self.truck_id = slot_dict.get('truck_id')
        self.ramp_id = slot_dict.get('ramp_id')

        # --- Derived Attributes for Display ---
        if self.date and self.time:
            self.start_datetime = datetime.datetime.combine(self.date, self.time)
        else:
            self.start_datetime = None
        
        truck_obj = ecm.ECM_TRUCKS.get(self.truck_id)
        self.truck_name = truck_obj.truck_name if truck_obj else "N/A"
        
        ramp_obj = ecm.get_ramp_details(self.ramp_id)
        self.ramp_name = ramp_obj.ramp_name if ramp_obj else "N/A"
        
        self.slot_id = str(uuid.uuid4())
        self.customer_id = slot_dict.get('customer_id', 'N/A')
        self.boat_id = slot_dict.get('boat_id', 'N/A')
        self.service_type = slot_dict.get('service_type', 'N/A')
        
    @property
    def confirmation_text(self):
        customer = ecm.get_customer_details(self.customer_id)
        boat = ecm.get_boat_details(self.boat_id)
        
        return (
            f"You are about to schedule a **{self.service_type}** for **{customer.customer_name}**'s "
            f"{boat.boat_length}' {boat.boat_type} on **{self.start_datetime.strftime('%A, %B %d, %Y')}** "
            f"at **{self.start_datetime.strftime('%I:%M %p')}** using **{self.truck_name}** at **{self.ramp_name}**."
        )

    # This allows the object to still be used like a dictionary,
    # which is needed by the `confirm_and_schedule_job` function.
    def __getitem__(self, key):
        return self.raw_data[key]
    
    def get(self, key, default=None):
        return self.raw_data.get(key, default)

# ... (the rest of your helper functions)

def create_gauge(value, max_value, label):
    """Generates an SVG string for a semi-circle gauge chart that displays an absolute value."""
    if max_value == 0: 
        percent = 0
    else: 
        percent = min(max(value / max_value, 0), 1)

    # If the value is zero, create an empty path so no colored arc is drawn.
    if value == 0:
        d = ""
    else:
        # --- THIS IS THE CORRECTED MATH ---
        # Map the percentage to an angle from -180 (left) to 0 (right) degrees
        angle_deg = -180 + (percent * 180)
        rads = math.radians(angle_deg)
        
        # Calculate the end point of the arc
        x = 50 + 40 * math.cos(rads)
        y = 50 + 40 * math.sin(rads)
        
        # Determine if the arc should be drawn the "long way" (for >50%)
        large_arc_flag = 1 if percent > 0.5 else 0
        
        # The start point of the arc is fixed at (10, 50)
        d = f"M 10 50 A 40 40 0 {large_arc_flag} 1 {x} {y}"

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
        # Change these lines to make them timezone-aware (UTC)
        # We use slot_date for the date component to ensure consistency with the job's date
        tide_dt = datetime.datetime.combine(slot_date, tide_time, tzinfo=timezone.utc)
        open_dt = datetime.datetime.combine(slot_date, op_open, tzinfo=timezone.utc)
        close_dt = datetime.datetime.combine(slot_date, op_close, tzinfo=timezone.utc)

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
    planner_columns = ["S20/33", "S21/77", "S23/55", "S17"]
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

    # --- This section is unchanged, for context ---
    high_tide_highlights, low_tide_highlights = [], []
    primary_high_tide = None
    if jobs_for_day:
        ramp_id_for_tides = None
        first_job = jobs_for_day[0]
        if first_job.pickup_ramp_id: ramp_id_for_tides = first_job.pickup_ramp_id
        elif first_job.dropoff_ramp_id: ramp_id_for_tides = first_job.dropoff_ramp_id
        else: ramp_id_for_tides = "3000001" 

        if ramp_id_for_tides:
            ramp_obj = ecm.get_ramp_details(ramp_id_for_tides)
            if ramp_obj:
                all_tides_for_date = ecm.fetch_noaa_tides_for_range(ramp_obj.noaa_station_id, report_date, report_date)
                tide_data_for_day = all_tides_for_date.get(report_date, [])
                high_tides_full_data = [t for t in tide_data_for_day if t.get('type') == 'H']
                low_tides_full_data = [t for t in tide_data_for_day if t.get('type') == 'L']
                if high_tides_full_data:
                    noon = datetime.datetime.combine(report_date, datetime.time(12,0), tzinfo=timezone.utc)
                    primary_high_tide = min(high_tides_full_data, key=lambda t: abs(datetime.datetime.combine(report_date, t['time'], tzinfo=timezone.utc) - noon))
                def round_time(t):
                    mins = t.hour * 60 + t.minute;rounded = int(round(mins / 15.0) * 15)
                    return datetime.time(min(23, rounded // 60), rounded % 60)
                high_tide_highlights = [round_time(t['time']) for t in high_tides_full_data]
                low_tide_highlights = [round_time(t['time']) for t in low_tides_full_data]
    # --- End of unchanged section ---

    c.setFont("Helvetica-Bold", 12);c.drawRightString(width - margin, height - 0.6 * inch, report_date.strftime("%A, %B %d").upper())
    if primary_high_tide:
        tide_time_str = ecm.format_time_for_display(primary_high_tide['time'])
        tide_height_str = f"{float(primary_high_tide.get('height', 0)):.1f}'"
        c.setFont("Helvetica-Bold", 9);c.drawString(margin, height - 0.6 * inch, f"High Tide: {tide_time_str} ({tide_height_str})")

    for i, name in enumerate(planner_columns):
        c.setFont("Helvetica-Bold", 14);c.drawCentredString(margin + time_col_width + i * col_width + col_width / 2, top_y + 10, name)

    # Drawing the grid (unchanged)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(margin + 3, top_y - 9, "7:30")
    for hour in range(start_time_obj.hour + 1, end_time_obj.hour + 1):
        hour_highlight_color = None
        for m_check in [0, 15, 30, 45]:
            check_time = datetime.time(hour, m_check)
            if check_time in high_tide_highlights: hour_highlight_color = colors.Color(1, 1, 0, alpha=0.4); break
            elif check_time in low_tide_highlights: hour_highlight_color = colors.Color(1, 0.6, 0.6, alpha=0.4); break
        for minute in [0, 15, 30, 45]:
            current_time = datetime.time(hour, minute)
            if not (start_time_obj <= current_time <= end_time_obj): continue
            y = get_y_for_time(current_time)
            c.setStrokeColorRGB(0.7, 0.7, 0.7); c.setLineWidth(1.0 if minute == 0 else 0.25); c.line(margin, y, width - margin, y)
            if minute == 0:
                if hour_highlight_color:
                    c.setFillColor(hour_highlight_color); c.rect(margin + 1, y - 11, time_col_width - 2, 13, fill=1, stroke=0)
                display_hour = hour if hour <= 12 else hour - 12
                c.setFont("Helvetica-Bold", 9);c.setFillColorRGB(0,0,0); c.drawString(margin + 3, y - 9, str(display_hour))
    c.setStrokeColorRGB(0,0,0)
    for i in range(len(planner_columns) + 1):
        x = margin + time_col_width + i * col_width;c.setLineWidth(0.5); c.line(x, top_y, x, bottom_y)
    c.line(margin, top_y, margin, bottom_y);c.line(width - margin, top_y, width - margin, bottom_y)
    c.line(margin, bottom_y, width - margin, bottom_y);c.line(margin, top_y, width - margin, top_y)

    # --- THIS SECTION IS CORRECTED ---
    # Create a map to look up truck names, ensuring the keys are STRINGS
    id_to_name_map = {str(t.truck_id): t.truck_name for t in ecm.ECM_TRUCKS.values()}

    for job in jobs_for_day:
        start_time, end_time = job.scheduled_start_datetime.time(), job.scheduled_end_datetime.time()
        if start_time < start_time_obj: start_time = start_time_obj
        y0, y_end = get_y_for_time(start_time), get_y_for_time(end_time)
        line1_y, line2_y, line3_y = y0 - 15, y0 - 25, y0 - 35
        customer, boat = ecm.get_customer_details(job.customer_id), ecm.get_boat_details(job.boat_id)
        
        # Look up the truck name, ensuring the job's truck ID is also treated as a STRING
        hauling_truck_name = id_to_name_map.get(str(job.assigned_hauling_truck_id))
        
        if hauling_truck_name and hauling_truck_name in column_map:
            col_index = column_map[hauling_truck_name]
            text_x = margin + time_col_width + (col_index + 0.5) * col_width
            c.setFillColorRGB(0,0,0);c.setFont("Helvetica-Bold", 8); c.drawCentredString(text_x, line1_y, customer.customer_name)
            c.setFont("Helvetica", 7);c.drawCentredString(text_x, line2_y, f"{int(boat.boat_length)}' {boat.boat_type}")
            c.drawCentredString(text_x, line3_y, f"{ecm._abbreviate_town(job.pickup_street_address)}-{ecm._abbreviate_town(job.dropoff_street_address)}")
            c.setLineWidth(2);c.line(text_x, y0 - 45, text_x, y_end); c.line(text_x - 10, y_end, text_x + 10, y_end)

        # Do the same for the crane, ensuring the ID is a STRING
        crane_truck_name = id_to_name_map.get(str(job.assigned_crane_truck_id))
        if crane_truck_name and crane_truck_name in column_map:
            crane_col_index = column_map[crane_truck_name]
            crane_text_x = margin + time_col_width + (crane_col_index + 0.5) * col_width
            y_crane_end = get_y_for_time(job.S17_busy_end_datetime.time())
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
        jobs_for_day = [j for j in jobs if j.scheduled_start_datetime and j.scheduled_start_datetime.date() == single_date]
        
        if jobs_for_day:
            daily_pdf_buffer = generate_daily_planner_pdf(single_date, jobs_for_day)
            merger.append(daily_pdf_buffer)
            
    output = BytesIO()
    if len(merger.pages) > 0:
        merger.write(output)
    merger.close()
    output.seek(0)
    return output

#### Detailed report generation

def generate_progress_report_pdf(stats, dist_analysis, eff_analysis):
    """
    Generates a multi-page PDF progress report with stats, charts, and tables.
    -- MODIFIED to group boats by scheduled vs. unscheduled status. --
    """
    buffer = BytesIO()
    from reportlab.lib.pagesizes import letter
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.graphics.shapes import Drawing
    from reportlab.graphics.charts.barcharts import VerticalBarChart

    doc = SimpleDocTemplate(buffer, pagesize=letter)
    story = []
    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name='Justify', alignment=1))

    # --- Page 1 & 2 & 3 (Executive Summary, Analytics, Efficiency) remain unchanged ---
    story.append(Paragraph("ECM Season Progress Report", styles['h1']))
    story.append(Paragraph(f"Generated on: {datetime.date.today().strftime('%B %d, %Y')}", styles['Normal']))
    story.append(Spacer(1, 24))
    story.append(Paragraph("Executive Summary", styles['h2']))
    story.append(Spacer(1, 12))
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
    summary_table.setStyle(TableStyle([('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'), ('ALIGN', (1,1), (-1,-1), 'RIGHT'), ('GRID', (0,0), (-1,-1), 0.5, colors.grey)]))
    story.append(summary_table)
    story.append(Spacer(1, 24))
    story.append(PageBreak())
    story.append(Paragraph("Scheduling Analytics", styles['h2']))
    story.append(Spacer(1, 12))
    if dist_analysis.get('by_day'):
        story.append(Paragraph("Jobs by Day of Week", styles['h3']))
        drawing = Drawing(400, 200)
        day_data = [tuple(v for k,v in sorted(dist_analysis['by_day'].items()))]
        day_names = [k for k,v in sorted(dist_analysis['by_day'].items())]
        bc = VerticalBarChart()
        bc.x = 50; bc.y = 50; bc.height = 125; bc.width = 300
        bc.data = day_data
        bc.categoryAxis.categoryNames = day_names
        drawing.add(bc)
        story.append(drawing)
    story.append(PageBreak())
    story.append(Paragraph("Fleet Efficiency Report", styles['h2']))
    story.append(Spacer(1, 12))
    if eff_analysis and eff_analysis.get("total_truck_days", 0) > 0:
        story.append(Paragraph("<b><u>Truck Day Utilization</u></b>", styles['h3']))
        low_util_pct = (eff_analysis['low_utilization_days'] / eff_analysis['total_truck_days'] * 100)
        story.append(Paragraph(f"<b>Days with Low Utilization (≤ 2 jobs):</b> {eff_analysis['low_utilization_days']} of {eff_analysis['total_truck_days']} total truck-days ({low_util_pct:.0f}%)", styles['Normal']))
        story.append(Spacer(1, 6))
        story.append(Paragraph("<i><b>Insight:</b> This is the most critical metric to watch. A high percentage indicates that trucks are frequently dispatched for only one or two jobs, leading to inefficiency. The goal is to reduce this number by scheduling more clustered, multi-job days.</i>", styles['Italic']))
        story.append(Spacer(1, 24))
        story.append(Paragraph("<b><u>Travel Efficiency</u></b>", styles['h3']))
        story.append(Paragraph(f"<b>Average Travel Time Between Jobs (Deadhead):</b> {eff_analysis['avg_deadhead_per_day']:.0f} minutes per day", styles['Normal']))
        story.append(Spacer(1, 6))
        story.append(Paragraph("<i><b>Insight:</b> This measures the average time drivers spend traveling empty between the yard and the first job, and between subsequent jobs. Lowering this number by creating geographically logical routes (e.g., Scituate -> Marshfield) directly reduces fuel costs and wasted driver time.</i>", styles['Italic']))
        story.append(Spacer(1, 24))
        story.append(Paragraph("<b><u>Productivity and Timing</u></b>", styles['h3']))
        story.append(Paragraph(f"<b>Overall Driver Efficiency:</b> {eff_analysis['efficiency_percent']:.1f}%", styles['Normal']))
        story.append(Paragraph("<i>↳ This is the percentage of a driver's 'on-the-clock' time (from first job start to last job end) that is spent actively working on a job. A higher number is better.</i>", styles['Italic']))
        story.append(Spacer(1, 12))
        story.append(Paragraph(f"<b>Days with Excellent Timing:</b> {eff_analysis['excellent_timing_days']}", styles['Normal']))
        story.append(Paragraph("<i>↳ These are ideal days: starting before 9 AM, completing 3+ jobs, and finishing by 3 PM. This is the target for a highly productive day.</i>", styles['Italic']))
    else:
        story.append(Paragraph("Not enough job data exists to generate an efficiency report.", styles['Normal']))
    
    # --- Page 4+: Detailed Boat Status (MODIFIED) ---
    story.append(PageBreak())
    story.append(Paragraph("Detailed Boat Status", styles['h2']))
    story.append(Spacer(1, 12))

    # Create two lists to hold boats based on their status
    scheduled_rows = []
    unscheduled_rows = []
    
    # Pre-calculate scheduled services for all customers for efficiency
    scheduled_services_by_cust = {}
    for job in ecm.SCHEDULED_JOBS:
        if job.job_status == "Scheduled":
            scheduled_services_by_cust.setdefault(job.customer_id, []).append(job.service_type)

    # Loop through all boats once and sort them into the two lists
    for boat in sorted(ecm.LOADED_BOATS.values(), key=lambda b: (ecm.get_customer_details(b.customer_id).customer_name if ecm.get_customer_details(b.customer_id) else "")):
        cust = ecm.get_customer_details(boat.customer_id)
        if not cust: continue

        services = scheduled_services_by_cust.get(cust.customer_id, [])
        status = "Launched" if "Launch" in services else (f"Scheduled ({', '.join(services)})" if services else "Not Scheduled")
        
        row_data = [
            Paragraph(cust.customer_name, styles['Normal']),
            Paragraph(f"{boat.boat_length}' {boat.boat_type}", styles['Normal']),
            "Yes" if boat.is_ecm_boat else "No",
            status
        ]

        if status == "Not Scheduled":
            unscheduled_rows.append(row_data)
        else:
            scheduled_rows.append(row_data)
    
    # Define a single table style to be reused
    table_style = TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.grey),
        ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke),
        ('ALIGN', (0,0), (-1,-1), 'LEFT'),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('BOTTOMPADDING', (0,0), (-1,0), 12),
        ('BACKGROUND', (0,1), (-1,-1), colors.beige),
        ('GRID', (0,0), (-1,-1), 1, colors.black)
    ])
    table_headers = [["Customer Name", "Boat Details", "ECM?", "Scheduling Status"]]

    # Add the "Scheduled Boats" table to the report
    if scheduled_rows:
        story.append(Paragraph("Scheduled Boats", styles['h3']))
        story.append(Spacer(1, 6))
        scheduled_table = Table(table_headers + scheduled_rows, colWidths=[150, 150, 50, 150])
        scheduled_table.setStyle(table_style)
        story.append(scheduled_table)
        story.append(Spacer(1, 24))

    # Add the "Unscheduled Boats" table to the report
    if unscheduled_rows:
        story.append(Paragraph("Unscheduled Boats", styles['h3']))
        story.append(Spacer(1, 6))
        unscheduled_table = Table(table_headers + unscheduled_rows, colWidths=[150, 150, 50, 150])
        unscheduled_table.setStyle(table_style)
        story.append(unscheduled_table)

    doc.build(story)
    buffer.seek(0)
    return buffer

def show_scheduler_page():
    """
    Displays the entire Schedule New Boat page and handles the new interactive conflict warning.
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

    # --- DEFINE CALLBACKS ---
    def schedule_another():
        st.session_state.pop("confirmation_message", None)
        st.session_state.selected_customer_id = None
        st.session_state.selected_boat_id = None
        st.session_state.customer_search_input = ""

    def select_customer(cust_id):
        st.session_state.selected_customer_id = cust_id
        st.session_state.selected_boat_id = None
        st.session_state.customer_search_input = ecm.LOADED_CUSTOMERS[cust_id].customer_name

    def clear_selection():
        st.session_state.selected_customer_id = None
        st.session_state.selected_boat_id = None
        st.session_state.customer_search_input = ""

    # --- NEW: CONFLICT WARNING UI (CORRECTED) ---
    if conflict_job := st.session_state.get('conflict_warning_details'):
        customer = ecm.get_customer_details(conflict_job.customer_id)
        customer_name = customer.customer_name if customer else "This customer"
        
        st.warning(f"""
        **⚠️ Scheduling Conflict Detected** {customer_name} is already scheduled for a **{conflict_job.service_type}** service on **{conflict_job.scheduled_start_datetime.strftime('%A, %B %d, %Y')}**.

        Scheduling the same service again within 30 days is unusual.
        """)
        
        def override_conflict():
            st.session_state.conflict_override_acknowledged = True
            st.session_state.conflict_warning_details = None

        def cancel_conflict():
            st.session_state.conflict_override_acknowledged = False
            st.session_state.conflict_warning_details = None

        c1, c2, _ = st.columns([1.5, 1.5, 3])
        c1.button("Continue Anyway (Override)", on_click=override_conflict, type="primary", use_container_width=True)
        c2.button("Change Request", on_click=cancel_conflict, use_container_width=True)
        return  # halt until user chooses

    # --- EXISTING LOGIC: Message Handling ---
    if info_msg := st.session_state.get('info_message'):
        st.info(info_msg)
        if reasons := st.session_state.get('failure_reasons'):
            for reason in reasons:
                st.warning(reason)
        st.session_state.info_message = ""
        st.session_state.failure_reasons = []

    if st.session_state.get("confirmation_message"):
        st.success(f"✅ {st.session_state.confirmation_message}")
        st.button("Schedule Another Job", on_click=schedule_another)
        return

    # --- SIDEBAR UI: New Job Request ---
    st.sidebar.header("New Job Request")
    customer = None
    boat = None

    # 1️⃣ Search input if no customer selected
    if not st.session_state.get('selected_customer_id'):
        st.session_state.customer_search_input = st.sidebar.text_input(
            "Search for Customer or Boat ID:",
            value=st.session_state.get('customer_search_input', ''),
            placeholder="e.g. 'Olivia' or 'B5001'"
        )
        search_term = st.session_state.customer_search_input.lower().strip()
        if search_term:
            # match on customer name and boat ID
            cust_matches = [c for c in ecm.LOADED_CUSTOMERS.values() if search_term in c.customer_name.lower()]
            boat_matches = [b for b in ecm.LOADED_BOATS.values() if search_term in str(b.boat_id)]
            # also include those boats' customers
            for b in boat_matches:
                cust = ecm.LOADED_CUSTOMERS.get(b.customer_id)
                if cust and cust not in cust_matches:
                    cust_matches.append(cust)
            unique = {c.customer_id: c for c in cust_matches}
            sorted_custs = sorted(unique.values(), key=lambda c: c.customer_name)
            if sorted_custs:
                st.sidebar.write("---")
                with st.sidebar.container(height=250):
                    for c in sorted_custs:
                        st.button(c.customer_name,
                                 key=f"select_{c.customer_id}",
                                 on_click=lambda cid=c.customer_id: select_customer(cid),
                                 use_container_width=True)
            else:
                st.sidebar.warning("No matches found.")

    # 2️⃣ Once a customer is selected
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
            boat_opts = {f"{b.boat_length}' {b.boat_type} (ID: {b.boat_id})": b.boat_id for b in boats_for_customer}
            opts_with_prompt = {"-- Select a boat --": None, **boat_opts}
            choice = st.sidebar.selectbox("Select Boat:", list(opts_with_prompt.keys()))
            st.session_state.selected_boat_id = opts_with_prompt[choice]

        # Show details of the boat currently selected for scheduling
        if st.session_state.get('selected_boat_id'):
            boat = ecm.LOADED_BOATS.get(st.session_state.selected_boat_id)
            if boat:
                st.sidebar.markdown("---")
                st.sidebar.markdown("**Boat Details:**")
                st.sidebar.markdown(f"- **Type:** {boat.boat_type or 'N/A'}")
                st.sidebar.markdown(f"- **Length:** {boat.boat_length or 'N/A'}'")
                st.sidebar.markdown(f"- **Draft:** {boat.draft_ft or 'N/A'}'")

                # Safely get the ramp name to prevent errors
                ramp_obj = ecm.ECM_RAMPS.get(boat.preferred_ramp_id)
                ramp_name = ramp_obj.ramp_name if ramp_obj else "N/A"
                st.sidebar.markdown(f"- **Preferred Ramp:** {ramp_name}")
    
    # SCHEDULING FORM: once customer + boat chosen
    if customer and st.session_state.get('selected_boat_id'):
        boat = ecm.LOADED_BOATS.get(st.session_state.selected_boat_id)
        service_type = st.sidebar.selectbox("Service Type:", ["Launch", "Haul", "Sandblast", "Paint"])
        req_date = st.sidebar.date_input("Requested Date:", min_value=None)
        override = st.sidebar.checkbox("Ignore Scheduling Conflict?", False)

        # --- THIS IS THE REPLACED RAMP SELECTION LOGIC ---
        # First get the list of ramps that are compatible with the boat type
        # Ensure it's a list to safely use the .index() method
        available_ramp_ids = list(ecm.find_available_ramps_for_boat(boat, ecm.ECM_RAMPS))

        # Determine the default index for the selectbox
        default_ramp_index = 0
        if boat.preferred_ramp_id and boat.preferred_ramp_id in available_ramp_ids:
            default_ramp_index = available_ramp_ids.index(boat.preferred_ramp_id)

        # Use that list and default index to populate the selectbox
        selected_ramp_id = st.sidebar.selectbox(
            "Ramp:",
            options=available_ramp_ids,
            index=default_ramp_index,  # This sets the default value
            format_func=lambda ramp_id: ecm.ECM_RAMPS[ramp_id].ramp_name
        )
        # --- END OF REPLACED SECTION ---

        # Initialize slots to an empty list before the button check to prevent UnboundLocalError
        slot_dicts = []
        msg = ""
        warnings = []
        forced = False

        if st.sidebar.button("Find Best Slot"):
            # Reverted to the general-purpose slot finding function because the
            # new crane-specific logic was incomplete and causing the search to fail.
            slot_dicts, msg, warnings, forced = ecm.find_available_job_slots(
                customer_id=customer.customer_id,
                boat_id=boat.boat_id,
                service_type=service_type,
                requested_date_str=req_date.strftime("%Y-%m-%d"),
                selected_ramp_id=selected_ramp_id,
                num_suggestions_to_find=st.session_state.get('num_suggestions', 3),
                manager_override=override
            )

            # Process the results and update the session state
            st.session_state.found_slots = [SlotDetail(s) for s in slot_dicts]
            st.session_state.failure_reasons = warnings
            st.session_state.was_forced_search = forced
            st.session_state.current_job_request = {
                'customer_id': customer.customer_id,
                'boat_id': boat.boat_id,
                'service_type': service_type,
                'requested_date_str': req_date.strftime("%Y-%m-%d"),
                'ignore_forced_search': override
            }
            st.session_state.search_requested_date = req_date
            st.session_state.info_message = msg
            st.session_state.conflict_warning_details = None

    # --- SLOT DISPLAY AND PAGINATION ---
    if st.session_state.get('found_slots') and not st.session_state.get('selected_slot'):
        found = st.session_state.found_slots
        total = len(found)
        page = st.session_state.get('slot_page_index', 0)
        per_page = 3

        st.subheader("Select a Slot:")
        cols = st.columns([1,1,5,1,1])
        cols[0].button("← Prev", on_click=lambda: st.session_state.update(slot_page_index=max(page-1, 0)))
        cols[1].button("Next →", on_click=lambda: st.session_state.update(slot_page_index=min(page+1, (total-1)//per_page)))
        if total:
            cols[3].write(f"{page*per_page+1}–{min((page+1)*per_page, total)} of {total}")
        st.markdown("---")
        
        for slot in found[page*per_page:(page+1)*per_page]:
            with st.container(border=True):
                # Using columns for a structured layout within the card
                col1, col2, col3, col4 = st.columns([1, 2, 4, 2])
                
                # Column 1: Ramp Info
                with col1:
                    st.metric("Ramp", slot.ramp_name)
                
                # Column 2: Date & Time Info
                with col2:
                    st.metric("Date", slot.start_datetime.strftime("%b %d, %Y"))
                    st.metric("Time", slot.start_datetime.strftime("%I:%M %p"))

                # Column 3: Tide and Truck Details
                with col3:
                    st.markdown(f"**Tide Rule:** {slot.raw_data.get('tide_rule_concise', 'N/A')}")
                    tide_times = slot.raw_data.get('high_tide_times', [])
                    tide_str = " / ".join([ecm.format_time_for_display(t) for t in tide_times]) or 'N/A'
                    st.markdown(f"**High Tides:** {tide_str}")

                # Column 4: Select Button
                with col4:
                    st.button("Select", key=f"sel_{slot.slot_id}", use_container_width=True, on_click=lambda s=slot: st.session_state.__setitem__('selected_slot', s))
                    
    # --- PREVIEW & CONFIRM SELECTION ---
    if st.session_state.get('selected_slot'):
        slot = st.session_state.selected_slot
        st.subheader("Preview & Confirm Job")
        st.success(slot.confirmation_text)

        if st.button("CONFIRM THIS JOB"):
            parked_to_remove = st.session_state.get('rebooking_details', {}).get('parked_job_id')
            new_id, message = ecm.confirm_and_schedule_job(
                st.session_state.current_job_request,
                slot,
                parked_job_to_remove=parked_to_remove
            )
            if new_id:
                st.session_state.confirmation_message = message
                svc = st.session_state.current_job_request['service_type']
                if svc in ["Launch", "Haul"]:
                    st.session_state.last_seasonal_job = {
                        'customer_id': customer.customer_id,
                        'boat_id': boat.boat_id,
                        'original_service': svc
                    }
                # clear state for a fresh start
                for key in ['found_slots', 'selected_slot', 'current_job_request',
                            'search_requested_date', 'rebooking_details',
                            'failure_reasons', 'was_forced_search']:
                    st.session_state.pop(key, None)

                st.button("🔄 Schedule Another Job", on_click=schedule_another)
                
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
            # --- THIS BLOCK IS CORRECTED ---
            # Create a map to look up truck names from IDs
            id_to_name_map = {t.truck_id: t.truck_name for t in ecm.ECM_TRUCKS.values()}

            cols = st.columns((2, 1, 2, 1, 1, 3))
            fields = ["Date/Time", "Service", "Customer", "Haul Truck", "Crane", "Actions"]
            for col, field in zip(cols, fields):
                col.markdown(f"**{field}**")
            st.markdown("---")

            sorted_jobs = sorted(ecm.SCHEDULED_JOBS, key=lambda j: j.scheduled_start_datetime or datetime.datetime.max.replace(tzinfo=datetime.timezone.utc))
            for j in sorted_jobs:
                customer = ecm.get_customer_details(j.customer_id)
                if not customer:
                    continue # SAFETY CHECK

                cols = st.columns((2, 1, 2, 1, 1, 3))
                if j.scheduled_start_datetime:
                    cols[0].write(j.scheduled_start_datetime.strftime("%a, %b %d @ %I:%M%p"))
                else:
                    cols[0].warning("No Date Set")

                cols[1].write(j.service_type)
                cols[2].write(customer.customer_name)
                
                # Look up and display the truck names instead of IDs
                cols[3].write(id_to_name_map.get(j.assigned_hauling_truck_id, "—"))
                cols[4].write(id_to_name_map.get(j.assigned_crane_truck_id, "—"))

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
        st.info("Note: This calendar is not yet populated with data.")
        # The logic below is safe but will show nothing until CANDIDATE_CRANE_DAYS is populated
        ramp_options = list(ecm.CANDIDATE_CRANE_DAYS.keys())
        if ramp_options:
            ramp = st.selectbox("Select a ramp:", ramp_options, key="cal_ramp_sel")
            if ramp and ecm.CANDIDATE_CRANE_DAYS.get(ramp):
                display_crane_day_calendar(ecm.CANDIDATE_CRANE_DAYS[ramp])
            else:
                st.write("No crane day data for this ramp.")
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
                dist_analysis = ecm.analyze_job_distribution(ecm.SCHEDULED_JOBS, ecm.LOADED_BOATS, ecm.ECM_RAMPS)
                eff_analysis = ecm.perform_efficiency_analysis(ecm.SCHEDULED_JOBS)
                pdf_buffer = generate_progress_report_pdf(stats, dist_analysis, eff_analysis)
                st.download_button(label="📥 Download Report (.pdf)", data=pdf_buffer, file_name=f"progress_report_{datetime.date.today()}.pdf", mime="application/pdf")

    with tab4:
        st.subheader("Generate Daily Planner PDF")
        selected_date = st.date_input("Select date to export:", value=datetime.date.today(), key="daily_pdf_date_input")
        if st.button("📤 Generate Daily PDF", key="generate_daily_pdf_button"):
            jobs_today = [j for j in ecm.SCHEDULED_JOBS if j.scheduled_start_datetime and j.scheduled_start_datetime.date() == selected_date]
            if not jobs_today:
                st.warning("No jobs scheduled for that date.")
            else:
                pdf_buffer = generate_daily_planner_pdf(selected_date, jobs_today)
                st.download_button(label="📥 Download Daily Planner", data=pdf_buffer.getvalue(), file_name=f"Daily_Planner_{selected_date}.pdf", mime="application/pdf", key="download_daily_planner_button")

        st.markdown("---") # Add a separator

        # --- RESTORED MULTI-DAY FUNCTIONALITY ---
        st.subheader("Generate Multi-Day Planner PDF")
        d_col1, d_col2 = st.columns(2)
        start_date = d_col1.date_input("Start date:", datetime.date.today())
        end_date = d_col2.date_input("End date:", datetime.date.today() + datetime.timedelta(days=6))

        if st.button("📤 Generate Multi-Day PDF", key="generate_multi_day_pdf_button"):
            if start_date > end_date:
                st.error("Start date cannot be after end date.")
            else:
                jobs_in_range = [j for j in ecm.SCHEDULED_JOBS if j.scheduled_start_datetime and start_date <= j.scheduled_start_datetime.date() <= end_date]
                if not jobs_in_range:
                    st.warning("No jobs scheduled in that date range.")
                else:
                    pdf_buffer = generate_multi_day_planner_pdf(start_date, end_date, jobs_in_range)
                    st.download_button(label="📥 Download Multi-Day Planner", data=pdf_buffer.getvalue(), file_name=f"Multi_Day_Planner_{start_date}_to_{end_date}.pdf", mime="application/pdf", key="download_multi_day_planner_button")

    with tab5:
        st.subheader("🅿️ Parked Jobs")
        st.info("These jobs have been removed from the schedule and are waiting to be re-booked.")
        if ecm.PARKED_JOBS:
            cols = st.columns((2, 2, 1, 2))
            fields = ["Customer", "Boat", "Service", "Actions"]
            for col, field in zip(cols, fields):
                col.markdown(f"**{field}**")
            st.markdown("---")

            for job_id, job in ecm.PARKED_JOBS.items():
                customer = ecm.get_customer_details(job.customer_id)
                boat = ecm.get_boat_details(job.boat_id)
                
                # --- THIS IS THE CORRECTED PART ---
                if not customer or not boat:
                    continue # SAFETY CHECK: Skip this job if customer or boat data is missing

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
        st.session_state.num_suggestions = st.number_input(
            "Number of suggestions to find per request",
            min_value=1,
            max_value=10,
            value=st.session_state.get('num_suggestions', 3),
            step=1
        )
        st.markdown("---")
        st.subheader("Geographic Rules")
        st.number_input(
            "Max Distance Between Jobs (miles)",
            min_value=5,
            max_value=100,
            value=st.session_state.get('max_job_distance', 10),
            key='max_job_distance',
            help="Enforces that a truck's next job must be within this many miles of its previous job's location."
        )
        st.markdown("---")
        st.info("NOTE: Changes made here are saved permanently to the database.")
        name_to_id_map = {t.truck_name: t.truck_id for t in ecm.ECM_TRUCKS.values()}
        all_truck_names = sorted(name_to_id_map.keys())
        selected_truck_name = st.selectbox("Select a truck to edit:", all_truck_names)
        if selected_truck_name:
            selected_truck_id = name_to_id_map[selected_truck_name]
            st.markdown("---")
            with st.form(f"form_{selected_truck_name.replace('/', '_')}"):
                st.write(f"**Editing hours for {selected_truck_name}**")
                new_hours = {}
                days_of_week = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
                for i, day_name in enumerate(days_of_week):
                    current_hours = ecm.TRUCK_OPERATING_HOURS.get(selected_truck_id, {}).get(i)
                    is_working = current_hours is not None
                    start_time, end_time = current_hours if is_working else (datetime.time(8, 0), datetime.time(16, 0))
                    summary = (
                        f"{day_name}: {ecm.format_time_for_display(start_time)} – {ecm.format_time_for_display(end_time)}"
                        if is_working else
                        f"{day_name}: Off Duty"
                    )
                    with st.expander(summary):
                        col1, col2, col3 = st.columns([1, 2, 2])
                        working = col1.checkbox("Working", value=is_working, key=f"{selected_truck_name}_{i}_working")
                        new_start = col2.time_input(
                            "Start",
                            value=start_time,
                            key=f"{selected_truck_name}_{i}_start",
                            disabled=not working
                        )
                        new_end = col3.time_input(
                            "End",
                            value=end_time,
                            key=f"{selected_truck_name}_{i}_end",
                            disabled=not working
                        )
                        new_hours[i] = (new_start, new_end) if working else None
                if st.form_submit_button("Save Hours"):
                    success, message = ecm.update_truck_schedule(selected_truck_name, new_hours)
                    if success:
                        ecm._data_from_sheets()
                        st.success(message)
                        st.rerun()
                    else:
                        st.error(message)

    with tab2:
        st.subheader("Developer Tools & Overrides")
        # ... (other developer tools here)

    with tab3:
        st.subheader("QA & Data Generation Tools")
        num_jobs_to_gen = st.number_input("Total number of jobs to simulate:", min_value=1, max_value=200, value=50, step=1)
        if st.button("Simulate Job Requests"):
            with st.spinner(f"Simulating {num_jobs_to_gen} job requests..."):
                summary = ecm.simulate_job_requests(
                    total_jobs_to_gen=num_jobs_to_gen,
                    truck_hours=st.session_state.truck_operating_hours
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
        def select_day(date_obj):
            st.session_state.selected_tide_day = date_obj
        month_index = month_names.index(selected_month_name) + 1

        tide_data = ecm.get_monthly_tides_for_scituate(selected_year, month_index)
        if not tide_data:
            st.warning("Could not retrieve tide data.")
        else:
            cal = calendar.Calendar()
            cal_data = cal.monthdatescalendar(selected_year, month_index)
            # Header row
            header_cols = st.columns(7)
            for i, day_name in enumerate(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]):
                header_cols[i].markdown(
                    f"<p style='text-align: center; font-weight: bold;'>{day_name}</p>",
                    unsafe_allow_html=True
                )
            st.divider()
            # Calendar grid
            for week in cal_data:
                cols = st.columns(7)
                for i, day in enumerate(week):
                    with cols[i]:
                        if day.month != month_index:
                            st.container(height=55, border=False)
                        else:
                            st.button(
                                str(day.day),
                                key=f"day_{day}",
                                on_click=select_day,
                                args=(day,),
                                use_container_width=True,
                                type="primary" if st.session_state.get('selected_tide_day') == day else "secondary"
                            )
            st.divider()

            if selected_day := st.session_state.get('selected_tide_day'):
                if selected_day.year == selected_year and selected_day.month == month_index:
                    day_str = selected_day.strftime("%A, %B %d, %Y")
                    st.subheader(f"Tides for: {day_str}")
                    tides_for_day = tide_data.get(selected_day, [])
                    if not tides_for_day:
                        st.write("No tide data available for this day.")
                    else:
                        high_tides = [
                            f"{ecm.format_time_for_display(t['time'])} ({float(t['height']):.1f}')"
                            for t in tides_for_day if t['type'] == 'H'
                        ]
                        low_tides = [
                            f"{ecm.format_time_for_display(t['time'])} ({float(t['height']):.1f}')"
                            for t in tides_for_day if t['type'] == 'L'
                        ]
                        tide_col1, tide_col2 = st.columns(2)
                        tide_col1.metric(
                            "🌊 High Tides",
                            " / ".join(high_tides) if high_tides else "N/A"
                        )
                        tide_col2.metric(
                            "💧 Low Tides",
                            " / ".join(low_tides) if low_tides else "N/A"
                        )


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
        'ramp_tide_blackout_enabled': True, # Add this
        'scituate_powerboat_priority_enabled': True,
        'dynamic_duration_enabled': False,
        'max_job_distance': 10,'last_seasonal_job': None
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
        with c1:
            # Use gauge for ECM Scheduled
            st.markdown(create_gauge(stats['ecm_boats']['scheduled'], stats['ecm_boats']['total'], "Scheduled"), unsafe_allow_html=True)
        with c2:
            # Use gauge for ECM Launched
            st.markdown(create_gauge(stats['ecm_boats']['launched'], stats['ecm_boats']['total'], "Launched"), unsafe_allow_html=True)

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
