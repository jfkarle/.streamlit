from io import BytesIO
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch

def _abbreviate_town(address):
    if not address: return ""
    address = address.lower()
    for town, abbr in {
        "scituate": "Sci", "green harbor": "Grn", "marshfield": "Mfield", "cohasset": "Coh",
        "weymouth": "Wey", "plymouth": "Ply", "sandwich": "Sand", "duxbury": "Dux",
        "humarock": "Huma", "pembroke": "Pembroke", "ecm": "Pembroke"
    }.items():
        if town in address: return abbr
    return address.title().split(',')[0]

def generate_daily_planner_pdf(report_date, jobs_for_day):
    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter
    planner_columns = ["S20/33", "S21/77", "S23/55", "S17"]
    column_map = {name: i for i, name in enumerate(planner_columns)}
    margin = 0.5 * inch
    time_col_width = 0.75 * inch
    content_width = width - 2 * margin - time_col_width
    col_width = content_width / len(planner_columns)
    start_hour, end_hour = 7, 18
    top_y = height - margin - 0.5 * inch
    bottom_y = margin + 0.5 * inch
    content_height = top_y - bottom_y

    def get_y_for_time(t):
        total_minutes = (t.hour - start_hour) * 60 + t.minute
        return top_y - (total_minutes / ((end_hour - start_hour) * 60) * content_height)

    # Header
    day_of_year = report_date.timetuple().tm_yday
    days_in_year = 366 if (report_date.year % 4 == 0 and report_date.year % 100 != 0) or (report_date.year % 400 == 0) else 365
    c.setFont("Helvetica", 9)
    c.drawRightString(width - margin, height - 0.4 * inch, f"{day_of_year}/{days_in_year - day_of_year}")
    c.setFont("Helvetica-Bold", 12)
    c.drawRightString(width - margin, height - 0.6 * inch, report_date.strftime("%A, %B %d").upper())

    # Column headers
    c.setFont("Helvetica-Bold", 11)
    for i, name in enumerate(planner_columns):
        x_center = margin + time_col_width + i * col_width + col_width / 2
        c.drawCentredString(x_center, top_y + 10, name)

    # Vertical and horizontal lines
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

    # Job bars
    for job in jobs_for_day:
        start_time = getattr(job, 'scheduled_start_datetime').time()
        end_time = getattr(job, 'scheduled_end_datetime').time()
        
        y0 = get_y_for_time(start_time)
        y_end = get_y_for_time(end_time)

        line1_y_text = y0 - 18 
        line2_y_text = line1_y_text - 12
        line3_y_text = line2_y_text - 10

        y_bar_start = line3_y_text - 5 

        customer = ecm.get_customer_details(getattr(job, 'customer_id', None))
        customer_full_name = customer.customer_name if customer and hasattr(customer, 'customer_name') else "Unknown Customer"
        customer_last_name = customer_full_name.split()[-1] if customer_full_name != "Unknown Customer" else "Unknown"

        boat_id = getattr(job, 'boat_id', None)
        boat = ecm.LOADED_BOATS.get(boat_id) if boat_id else None
        boat_type = getattr(boat, 'boat_type', '') if boat else ''
        
        # --- CORRECTED LINE IS HERE ---
        is_sailboat_job = boat_type.lower() == 'sailboat'

        origin_address = getattr(job, 'pickup_street_address', '') or ''
        dest_address = getattr(job, 'dropoff_street_address', '') or ''
        if customer and hasattr(customer, 'street_address'):
            if origin_address.upper() == 'HOME':
                origin_address = customer.street_address
            if dest_address.upper() == 'HOME':
                dest_address = customer.street_address
        origin_abbr = _abbreviate_town(origin_address)
        dest_abbr = _abbreviate_town(dest_address)
        
        # --- Draw Hauling Truck Information (if assigned) ---
        truck_id = getattr(job, 'assigned_hauling_truck_id', None)
        if truck_id in column_map:
            col_index = column_map[truck_id]
            column_start_x = margin + time_col_width + col_index * col_width
            text_center_x = column_start_x + col_width / 2

            c.setFont("Helvetica-Bold", 8)
            c.drawCentredString(text_center_x, line1_y_text, customer_full_name) 
            
            if boat:
                boat_length = getattr(boat, 'boat_length', None)
                boat_desc = f"{int(boat_length)}' {boat_type}".strip() if boat_length and isinstance(boat_length, (int, float)) and boat_length > 0 else boat_type or "Unknown Boat"
            else:
                boat_desc = "Unknown Boat"
            
            c.setFont("Helvetica", 7)
            c.drawCentredString(text_center_x, line2_y_text, boat_desc) 

            location_label_truck = f"{origin_abbr}-{dest_abbr}"
            c.drawCentredString(text_center_x, line3_y_text, location_label_truck) 

            c.setLineWidth(2)
            c.line(text_center_x, y_bar_start, text_center_x, y_end)
            c.line(text_center_x - 3, y_end, text_center_x + 3, y_end)

        # --- Draw Crane Information (S17 column) ONLY IF it's a sailboat job ---
        if is_sailboat_job and 'S17' in column_map:
            col_index_crane = column_map['S17']
            column_start_x_crane = margin + time_col_width + col_index_crane * col_width
            text_center_x_crane = column_start_x_crane + col_width / 2

            c.setFont("Helvetica-Bold", 8)
            c.drawCentredString(text_center_x_crane, line1_y_text, customer_last_name)

            town_for_crane = dest_abbr 
            c.setFont("Helvetica", 7)
            c.drawCentredString(text_center_x_crane, line2_y_text, town_for_crane)
            
            c.setLineWidth(2)
            # NOTE: Uses y_bar_start which is calculated from the single text block height
            # and y_end from the overall job time. This keeps the bars aligned.
            c.line(text_center_x_crane, y_bar_start, text_center_x_crane, y_end)
            c.line(text_center_x_crane - 3, y_end, text_center_x_crane + 3, y_end)

    c.setLineWidth(1.0)
    c.line(margin, bottom_y, width - margin, bottom_y)

    c.save()
    buffer.seek(0)
    return buffer
