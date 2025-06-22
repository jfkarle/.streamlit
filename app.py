import streamlit as st
import datetime
import ecm_scheduler_logic as ecm
import pandas as pd

st.set_page_config(layout="wide")

# --- Helper Functions ---
def format_tides_for_display(slot, ecm_hours):
    tide_times = slot.get('high_tide_times', [])
    if not tide_times: return ""
    if not ecm_hours or not ecm_hours.get('open'):
        return "HT: " + " / ".join([ecm.format_time_for_display(t) for t in tide_times])

    # CORRECTED LINE HERE
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

from io import BytesIO
from PyPDF2 import PdfMerger

def generate_multi_day_planner_pdf(start_date, end_date, jobs):
    merger = PdfMerger()
    for single_date in (start_date + datetime.timedelta(n) for n in range((end_date - start_date).days + 1)):
        jobs_for_day = [j for j in jobs if j.scheduled_start_datetime.date() == single_date]
        if jobs_for_day:
            daily_pdf = generate_daily_planner_pdf(single_date, jobs_for_day)
            merger.append(daily_pdf)
    output = BytesIO()
    merger.write(output)
    merger.close()
    output.seek(0)
    return output
    

########################################################################################
### BEGIN  PDF Page Generation Tool AFTER Helper function BEFORE Session State Init ###
########################################################################################

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
    from io import BytesIO
    from reportlab.pdfgen import canvas
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.units import inch
    import datetime

    row_height = 30  # points per row
    
    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=letter)
    width, height = letter
    planner_columns = ["S20/33", "S21/77", "S23/55", "J17"]
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

    # Header
    day_of_year = report_date.timetuple().tm_yday
    days_in_year = 366 if (report_date.year % 4 == 0 and report_date.year % 100 != 0) or (report_date.year % 400 == 0) else 365
    c.setFont("Helvetica", 9)
    c.drawRightString(width - margin, height - 0.4 * inch, f"{day_of_year}/{days_in_year - day_of_year}")
    c.setFont("Helvetica-Bold", 12)
    c.drawRightString(width - margin, height - 0.6 * inch, report_date.strftime("%A, %B %d").upper())

    # Column headers
    c.setFont("Helvetica-Bold", 14)
    for i, name in enumerate(planner_columns):
        x_center = margin + time_col_width + i * col_width + col_width / 2
        c.drawCentredString(x_center, top_y + 10, name)

    # Horizontal lines and time labels
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

    # Vertical lines
    for i in range(len(planner_columns) + 1):
        x = margin + time_col_width + i * col_width
        c.setLineWidth(0.5)
        c.line(x, top_y, x, bottom_y)
    c.line(margin, top_y, margin, bottom_y)

    for job in jobs_for_day:
        start_time = getattr(job, 'scheduled_start_datetime').time()
        end_time = getattr(job, 'scheduled_end_datetime').time()

        y0 = get_y_for_time(start_time)
        y_end = get_y_for_time(end_time)

        line1_y_text = y0 - 8
        line2_y_text = line1_y_text - 10
        line3_y_text = line2_y_text - 10
        y_bar_start = line3_y_text - 4

        customer = ecm.get_customer_details(getattr(job, 'customer_id', None))
        customer_full_name = customer.customer_name if customer and hasattr(customer, 'customer_name') else "Unknown Customer"
        customer_last_name = customer_full_name.split()[-1] if customer_full_name != "Unknown Customer" else "Unknown"

        boat_id = getattr(job, 'boat_id', None)
        boat = ecm.LOADED_BOATS.get(boat_id) if boat_id else None
        boat_type = getattr(boat, 'boat_type', '') if boat else ''
        assigned_crane = getattr(job, 'assigned_crane_truck_id', '') or ''
        is_sailboat_job = boat and 'sailboat' in boat_type.lower() and 'j17' in assigned_crane.lower()

        origin_address = getattr(job, 'pickup_street_address', '') or ''
        dest_address = getattr(job, 'dropoff_street_address', '') or ''
        if customer and hasattr(customer, 'street_address'):
            if origin_address.upper() == 'HOME':
                origin_address = customer.street_address
            if dest_address.upper() == 'HOME':
                dest_address = customer.street_address
        origin_abbr = _abbreviate_town(origin_address)
        dest_abbr = _abbreviate_town(dest_address)

        truck_id = getattr(job, 'assigned_hauling_truck_id', None)
        if truck_id in column_map:
            col_index = column_map[truck_id]
            column_start_x = margin + time_col_width + col_index * col_width
            text_center_x = column_start_x + col_width / 2

            c.setFont("Helvetica-Bold", 8)
            c.drawCentredString(text_center_x, line1_y_text, customer_full_name)
            boat_length = getattr(boat, 'boat_length', None)
            boat_desc = f"{int(boat_length)}' {boat_type}".strip() if boat_length and isinstance(boat_length, (int, float)) and boat_length > 0 else boat_type or "Unknown Boat"
            c.setFont("Helvetica", 7)
            c.drawCentredString(text_center_x, line2_y_text, boat_desc)
            c.drawCentredString(text_center_x, line3_y_text, f"{origin_abbr}-{dest_abbr}")
            c.setLineWidth(2)
            c.line(text_center_x, y_bar_start, text_center_x, y_end)
            c.line(text_center_x - 3, y_end, text_center_x + 3, y_end)

    if is_sailboat_job and 'J17' in column_map:
        col_index_crane = column_map['J17']
        column_start_x_crane = margin + time_col_width + col_index_crane * col_width
        text_center_x_crane = column_start_x_crane + col_width / 2
    
        # Set fonts and write LASTNAME
        c.setFont("Helvetica-Bold", 8)
        c.drawCentredString(text_center_x_crane, line1_y_text, customer_last_name)
    
        # Write DEST TOWN
        c.setFont("Helvetica", 7)
        c.drawCentredString(text_center_x_crane, line2_y_text, dest_abbr)
    
        # If Sailboat MT, write TRANSPORT
        if 'mt' in boat_type.lower():
            c.drawCentredString(text_center_x_crane, line3_y_text, "TRANSPORT")
            j17_duration = datetime.timedelta(minutes=90)
            y_bar_start_crane = line3_y_text - 5
        else:
            j17_duration = datetime.timedelta(minutes=60)
            y_bar_start_crane = line2_y_text - 15
    
        # Compute crane end time
        crane_end_time = (datetime.datetime.combine(report_date, start_time) + j17_duration).time()
        crane_end_index = (crane_end_time.hour - 7) * 4 + crane_end_time.minute // 15
        y_crane_end = top_y - crane_end_index * row_height
    
        # Draw vertical bar
        c.setLineWidth(2)
        c.line(text_center_x_crane, y_bar_start_crane, text_center_x_crane, y_crane_end)
        c.line(text_center_x_crane - 3, y_crane_end, text_center_x_crane + 3, y_crane_end)

    c.save()
    buffer.seek(0)
    return buffer

########################################################################################
### END PDF Page Generation Tool AFTER Helper function BEFORE Session State Init ###
########################################################################################

# This is the patched code that includes:
# - Cancel by customer name
# - Reschedule (rebook)
# - Audit log with change tracking
# - AgGrid reporting with column filtering, hiding, grouping

from datetime import datetime as dt
from st_aggrid import AgGrid, GridOptionsBuilder

CANCELED_JOBS_AUDIT_LOG = []

# Function to cancel a job by customer name
def cancel_job_by_customer_name(customer_name):
    job_to_cancel = None
    for job in SCHEDULED_JOBS:
        customer = get_customer_details(job.customer_id)
        if customer and customer.customer_name.lower() == customer_name.lower():
            job_to_cancel = job
            break
    if job_to_cancel:
        audit_entry = {
            "Customer": customer.customer_name,
            "Original Date": job_to_cancel.scheduled_start_datetime.strftime("%Y-%m-%d"),
            "Original Time": job_to_cancel.scheduled_start_datetime.strftime("%H:%M"),
            "Original Truck": job_to_cancel.assigned_hauling_truck_id,
            "Original Ramp": get_ramp_details(job_to_cancel.pickup_ramp_id or job_to_cancel.dropoff_ramp_id).ramp_name,
            "Action": "Canceled",
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        CANCELED_JOBS_AUDIT_LOG.append(audit_entry)
        SCHEDULED_JOBS.remove(job_to_cancel)
        return True, audit_entry
    return False, None

# Function to reschedule a customer to a new slot
def reschedule_customer(customer_name, new_slot):
    canceled, audit_entry = cancel_job_by_customer_name(customer_name)
    if not canceled:
        return False, "Customer not found."

    customer = next((c for c in LOADED_CUSTOMERS.values() if c.customer_name.lower() == customer_name.lower()), None)
    boat = next((b for b in LOADED_BOATS.values() if b.customer_id == customer.customer_id), None)
    if not customer or not boat:
        return False, "Customer or boat not found."

    # Create new job
    new_job_request = {
        'customer_id': customer.customer_id,
        'boat_id': boat.boat_id,
        'service_type': "Launch",  # or detect based on prior job?
        'requested_date_str': new_slot['date'].strftime('%Y-%m-%d'),
        'selected_ramp_id': new_slot['ramp_id'],
    }
    job_id, _ = confirm_and_schedule_job(new_job_request, new_slot)
    audit_entry['Action'] = "Rescheduled"
    audit_entry['New Date'] = new_slot['date'].strftime('%Y-%m-%d')
    audit_entry['New Time'] = new_slot['time'].strftime('%H:%M')
    audit_entry['New Truck'] = new_slot['truck_id']
    audit_entry['New Ramp'] = get_ramp_details(new_slot['ramp_id']).ramp_name
    return True, audit_entry

# Function to show audit log in AgGrid
def display_cancel_audit_log():
    if not CANCELED_JOBS_AUDIT_LOG:
        st.warning("No jobs have been canceled or rescheduled yet.")
        return

    df = pd.DataFrame(CANCELED_JOBS_AUDIT_LOG)
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_pagination(paginationAutoPageSize=True)
    gb.configure_default_column(groupable=True, value=True, enableRowGroup=True, editable=False)
    gb.configure_column("Timestamp", hide=False)
    gb.configure_side_bar()
    gb.configure_auto_height(autoHeight=True)
    gb.configure_grid_options(domLayout='normal')
    gridOptions = gb.build()

    st.subheader("📜 List of Canceled / Rescheduled Jobs")
    AgGrid(df, gridOptions=gridOptions, enable_enterprise_modules=True, height=300, theme="alpine")


# --- Session State Initialization ---
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

# --- Main App Execution ---
initialize_session_state()
st.title("Marine Transportation")

# --- NAVIGATION SIDEBAR ---
st.sidebar.title("Navigation")
app_mode = st.sidebar.radio("Go to", ["Schedule New Boat", "Reporting", "Settings"])

# --- PAGE 1: SCHEDULER ---
if app_mode == "Schedule New Boat":
    if st.session_state.info_message:
        st.info(st.session_state.info_message)
        st.session_state.info_message = ""

    # Display confirmation after rerun (TOP LEVEL)
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
            default_date = dt.now().date() + datetime.timedelta(days=7)
            requested_date_input = st.sidebar.date_input("Requested Date:", value=default_date)
            selected_ramp_id_input = None
            if service_type_input in ["Launch", "Haul"]:
                ramp_options = list(ecm.ECM_RAMPS.keys())
                selected_ramp_id_input = st.sidebar.selectbox("Select Ramp:", ramp_options)
            
            st.sidebar.markdown("---")

            # --- START OF MODIFIED BLOCK ---
            
            # 1. ADD CHECKBOXES FOR RELAXATION OPTIONS
            relax_truck_input = st.sidebar.checkbox("Relax Truck (Use any capable truck)")
            relax_ramp_input = st.sidebar.checkbox("Relax Ramp (Search other nearby ramps)") # Note: This will search all ramps.
            
            # 2. CHANGE BUTTON AND UPDATE THE LOGIC IT CALLS
            if st.sidebar.button("Find Best Slot", key="find_slots"):
                job_request = {
                    'customer_id': selected_customer_obj.customer_id,
                    'boat_id': selected_boat_obj.boat_id,
                    'service_type': service_type_input,
                    'requested_date_str': requested_date_input.strftime('%Y-%m-%d'),
                    'selected_ramp_id': selected_ramp_id_input,
                }
                st.session_state.current_job_request = job_request
                st.session_state.search_requested_date = requested_date_input
                
                # This is the key change: we now pass the checkbox values to the search function.
                # If "Relax Truck" is checked, force_preferred_truck becomes False.
                slots, message, _, was_forced = ecm.find_available_job_slots(
                    **job_request,
                    force_preferred_truck=(not relax_truck_input), 
                    relax_ramp=relax_ramp_input
                )
                
                st.session_state.info_message, st.session_state.found_slots = message, slots
                st.session_state.selected_slot, st.session_state.was_forced_search = None, was_forced
                st.rerun()
            # --- END OF MODIFIED BLOCK ---
        
        else:
            st.sidebar.error(f"No boat found for {selected_customer_obj.customer_name}.")

    # --- Main Area Display Logic ---
    if st.session_state.found_slots and not st.session_state.selected_slot:
        st.subheader("Please select your preferred slot:")
    
        cols = st.columns(3)
        for i, slot in enumerate(st.session_state.found_slots):
            with cols[i % 3]:
                with st.container(border=True):
                    if st.session_state.get('search_requested_date') and slot['date'] == st.session_state.search_requested_date:
                        st.markdown("""<div style='background-color:#F0FFF0;border-left:6px solid #2E8B57;padding:10px;border-radius:5px;margin-bottom:10px;'><h5 style='color:#2E8B57;margin:0;font-weight:bold;'>⭐ Requested Date</h5></div>""", unsafe_allow_html=True)
    
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
                    if slot.get('j17_needed'):
                        st.markdown("**Crane:** J17")
                    st.markdown(f"**Ramp:** {ramp_name}")
                    st.button("Select this slot", key=f"select_slot_{i}", on_click=handle_slot_selection, args=(slot,))
        st.markdown("---")
    
    elif st.session_state.selected_slot:
        # Confirmation Screen Logic
        slot = st.session_state.selected_slot
        st.subheader("Preview & Confirm Selection:")
        st.success(f"You are considering: **{slot['date'].strftime('%Y-%m-%d %A')} at {ecm.format_time_for_display(slot.get('time'))}** with Truck **{slot.get('truck_id')}**.")
        if slot.get('j17_needed'):
            st.write("J17 Crane will also be assigned.")
    
        if st.button("CONFIRM THIS JOB", key="confirm_final_job"):
            new_job_id, message = ecm.confirm_and_schedule_job(st.session_state.current_job_request, slot)
    
            if new_job_id:
                st.session_state.confirmation_message = message
                for key in ['found_slots', 'selected_slot', 'current_job_request', 'search_requested_date', 'was_forced_search']:
                    st.session_state.pop(key, None)
                st.rerun()
            else:
                st.session_state.pop("confirmation_message", None)
                st.error(f"❌ Failed to confirm job: {message}")
    
    elif st.session_state.get("confirmation_message"):
        st.success(f"✅ {st.session_state.confirmation_message}")
        if st.button("Schedule Another Job", key="schedule_another"):
            st.session_state.pop("confirmation_message", None)
            st.rerun()
    
    elif st.session_state.get('current_job_request') and not st.session_state.found_slots:
        if st.session_state.info_message:
            st.warning(st.session_state.info_message)

# --- PAGE 2: REPORTING ---
elif app_mode == "Reporting":
    st.header("Reporting Dashboard")
    st.info("This section is for viewing and exporting scheduled jobs.")

    # --- Job Table ---
    st.subheader("All Scheduled Jobs (Current Session)")
    if ecm.SCHEDULED_JOBS:
        display_data = []
        for job in sorted(ecm.SCHEDULED_JOBS, key=lambda j: j.scheduled_start_datetime or datetime.datetime.max):
            customer = ecm.get_customer_details(getattr(job, 'customer_id', None))
            ramp = ecm.get_ramp_details(getattr(job, 'pickup_ramp_id', None) or getattr(job, 'dropoff_ramp_id', None))
            
            # Separate Truck and Crane info for Sailboat jobs on the dashboard display
            truck_info = job.assigned_hauling_truck_id if job.assigned_hauling_truck_id else "N/A"
            crane_info = job.assigned_crane_truck_id if job.assigned_crane_truck_id else "N/A"

            # Check if it's a sailboat job requiring both truck and crane
            boat_id = getattr(job, 'boat_id', None)
            boat = ecm.LOADED_BOATS.get(boat_id) if boat_id else None
            is_sailboat = boat and getattr(boat, 'boat_type', '').lower() == 'sailboat'

            if is_sailboat and crane_info != "N/A":
                # First row for Truck
                display_data.append({
                    "Job ID": job.job_id,
                    "Status": job.job_status,
                    "Scheduled Date": job.scheduled_start_datetime.strftime("%Y-%m-%d") if job.scheduled_start_datetime else "N/A",
                    "Scheduled Time": ecm.format_time_for_display(job.scheduled_start_datetime.time()) if job.scheduled_start_datetime else "N/A",
                    "Service": job.service_type,
                    "Customer": customer.customer_name if customer else "N/A",
                    "Truck": truck_info,
                    "Crane": "", # Intentionally empty for the truck row
                    "Ramp": ramp.ramp_name if ramp else "N/A"
                })
                # Second row for Crane (distinct, consecutive row)
                display_data.append({
                    "Job ID": "", # Empty for continuity
                    "Status": "",
                    "Scheduled Date": "",
                    "Scheduled Time": "",
                    "Service": "",
                    "Customer": "",
                    "Truck": "", # Intentionally empty for the crane row
                    "Crane": crane_info,
                    "Ramp": "" # Empty for continuity
                })
            else:
                # Standard job (or sailboat job without crane)
                display_data.append({
                    "Job ID": job.job_id,
                    "Status": job.job_status,
                    "Scheduled Date": job.scheduled_start_datetime.strftime("%Y-%m-%d") if job.scheduled_start_datetime else "N/A",
                    "Scheduled Time": ecm.format_time_for_display(job.scheduled_start_datetime.time()) if job.scheduled_start_datetime else "N/A",
                    "Service": job.service_type,
                    "Customer": customer.customer_name if customer else "N/A",
                    "Truck": truck_info,
                    "Crane": crane_info if crane_info != "N/A" else "",
                    "Ramp": ramp.ramp_name if ramp else "N/A"
                })
        st.dataframe(pd.DataFrame(display_data))
    else:
        st.write("No jobs scheduled yet.")

    # --- Single Day Planner Export ---
    st.subheader("Generate Daily Planner PDF")
    selected_date = st.date_input("Select date to export:", value=datetime.date.today())
    if st.button("📤 Generate PDF"):
        jobs_today = [j for j in ecm.SCHEDULED_JOBS if j.scheduled_start_datetime.date() == selected_date]
        if not jobs_today:
            st.warning("No jobs scheduled for that date.")
        else:
            pdf_buffer = generate_daily_planner_pdf(selected_date, jobs_today)
            st.download_button(
                label="📥 Download Planner",
                data=pdf_buffer.getvalue(),  # ← this is the fix
                file_name=f"Daily_Planner_{selected_date}.pdf",
                mime="application/pdf"
            )

    # --- Multi-Day Export Tool ---
    st.subheader("Export Multi-Day Planner")
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=datetime.date.today(), key="multi_start")
    with col2:
        end_date = st.date_input("End Date", value=datetime.date.today() + datetime.timedelta(days=5), key="multi_end")

    if st.button("📤 Generate Multi-Day Planner PDF"):
        if start_date > end_date:
            st.error("Start date must be before or equal to end date.")
        else:
            jobs_in_range = [j for j in ecm.SCHEDULED_JOBS if start_date <= j.scheduled_start_datetime.date() <= end_date]
            if not jobs_in_range:
                st.warning("No jobs scheduled in this date range.")
            else:
                merged_pdf = generate_multi_day_planner_pdf(start_date, end_date, jobs_in_range)
                st.download_button(
                    label="📥 Download Multi-Day Planner",
                    data=merged_pdf,
                    file_name=f"Planner_{start_date}_to_{end_date}.pdf",
                    mime="application/pdf"
                )

