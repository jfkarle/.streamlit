import datetime
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors

# --- Sample Job Data (This would come from your ECM) ---
# In a real application, you would fetch this from your ECM database/API
sample_jobs = [
    {
        "description": "Quarterly financial data validation",
        "start_time": datetime.time(9, 15),
        "end_time": datetime.time(10, 0),
        "category": "FINANCE"
    },
    {
        "description": "Onboarding call with Client X",
        "start_time": datetime.time(11, 0),
        "end_time": datetime.time(11, 30),
        "category": "CLIENTS"
    },
    {
        "description": "Server patch and reboot cycle",
        "start_time": datetime.time(14, 0),
        "end_time": datetime.time(15, 45),
        "category": "IT OPS"
    },
    {
        "description": "Review marketing campaign proofs",
        "start_time": datetime.time(9, 30),
        "end_time": datetime.time(10, 15),
        "category": "MARKETING"
    }
]

# --- PDF Generation Logic ---
def create_daily_report_pdf(filename, report_date, jobs_data, categories):
    """
    Generates a PDF report formatted like a daily planner.

    Args:
        filename (str): The name of the PDF file to create.
        report_date (datetime.date): The date for the report.
        jobs_data (list): A list of job dictionaries.
        categories (list): A list of category strings for the columns.
    """
    c = canvas.Canvas(filename, pagesize=letter)
    width, height = letter  # Get page dimensions

    # --- Define Layout Constants ---
    margin = 0.75 * inch
    time_col_width = 1.25 * inch
    content_width = width - margin - margin - time_col_width
    col_width = content_width / len(categories)
    start_hour, end_hour = 8, 19 # 8 AM to 7 PM
    row_height = (height - (2 * margin)) / ((end_hour - start_hour) * 4)

    # --- Helper function to calculate Y position from time ---
    def get_y_for_time(t):
        hour_offset = t.hour - start_hour
        minute_offset = t.minute / 15
        return height - margin - ((hour_offset * 4 + minute_offset) * row_height)

    # --- 1. Draw Header ---
    c.setFont("Helvetica-Bold", 14)
    c.drawString(margin, height - 0.5 * inch, f"Daily Job Report")
    
    day_of_year = report_date.timetuple().tm_yday
    days_in_year = 366 if (report_date.year % 4 == 0 and report_date.year % 100 != 0) or (report_date.year % 400 == 0) else 365
    days_remaining = days_in_year - day_of_year
    
    c.setFont("Helvetica", 10)
    date_str = report_date.strftime("%A, %B %d, %Y").upper()
    c.drawRightString(width - margin, height - 0.5 * inch, date_str)
    c.drawRightString(width - margin, height - 0.65 * inch, f"Day {day_of_year}/{days_remaining}")

    # --- 2. Draw Grid and Time Column ---
    top_y = height - margin
    bottom_y = margin
    
    # Draw data column headers
    c.setFont("Helvetica-Bold", 9)
    for i, category in enumerate(categories):
        x = margin + time_col_width + (i * col_width)
        c.drawString(x + 5, top_y + 5, category)
        
    # Draw vertical lines
    for i in range(len(categories) + 1):
        x = margin + time_col_width + (i * col_width)
        c.line(x, top_y, x, bottom_y)
    c.line(margin, top_y, margin, bottom_y) # Leftmost line
    
    # Draw horizontal lines and time labels
    c.setFont("Helvetica", 8)
    for hour in range(start_hour, end_hour + 1):
        for quarter in range(4):
            time_offset = (hour - start_hour) * 4 + quarter
            y = top_y - (time_offset * row_height)
            
            # Draw faint horizontal lines
            if quarter != 0:
                c.setStrokeColorRGB(0.8, 0.8, 0.8)
                c.line(margin, y, width - margin, y)
            
            # Time labels
            c.setStrokeColorRGB(0, 0, 0) # Reset color
            current_time = datetime.time(hour, quarter * 15)
            if quarter == 0:
                c.setFont("Helvetica-Bold", 10)
                c.drawString(margin + 5, y - 10, current_time.strftime("%I:00"))
                c.setFont("Helvetica", 8)
                c.drawString(margin + 45, y - 10, current_time.strftime("%p").lower())
            else:
                c.drawString(margin + 20, y - 8, current_time.strftime("%M"))
    
    c.setStrokeColorRGB(0, 0, 0)
    c.line(margin, top_y, width - margin, top_y) # Top border line
    c.line(margin, bottom_y, width - margin, bottom_y) # Bottom border line

    # --- 3. Place Job Data onto the Grid ---
    c.setFont("Helvetica", 9)
    category_map = {name: i for i, name in enumerate(categories)}
    
    for job in jobs_data:
        if job['category'] not in category_map:
            continue # Skip jobs with no assigned column

        start_y = get_y_for_time(job['start_time'])
        end_y = get_y_for_time(job['end_time'])
        col_index = category_map[job['category']]
        
        x_pos = margin + time_col_width + (col_index * col_width)
        rect_height = start_y - end_y
        
        # Draw a colored rectangle representing the job's duration
        c.setFillColorRGB(0.9, 0.95, 1.0) # Light blue
        c.rect(x_pos, end_y, col_width, rect_height, stroke=0, fill=1)
        
        # Add the job description text
        c.setFillColorRGB(0,0,0) # Black text
        text_object = c.beginText(x_pos + 5, start_y - 12)
        text_object.setFont("Helvetica", 9)
        # Simple text wrapping
        words = job['description'].split()
        line = ""
        for word in words:
            if c.stringWidth(line + word, "Helvetica", 9) < col_width - 10:
                line += word + " "
            else:
                text_object.textLine(line)
                line = word + " "
        text_object.textLine(line)
        c.drawText(text_object)

    c.save()
    print(f"Successfully created report: {filename}")


# --- Run the report generator ---
if __name__ == "__main__":
    report_date_today = datetime.date.today()
    # The columns you want on your report
    report_categories = ["FINANCE", "CLIENTS", "IT OPS", "MARKETING"] 
    output_filename = "daily_job_report.pdf"
    
    create_daily_report_pdf(output_filename, report_date_today, sample_jobs, report_categories)
