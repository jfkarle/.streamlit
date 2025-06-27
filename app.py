import streamlit as st
import datetime
import ecm_scheduler_logic as ecm
import pandas as pd
import csv
from io import BytesIO
from PyPDF2 import PdfMerger
from st_aggrid import AgGrid, GridOptionsBuilder
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
import calendar

#This line MUST remain under the IMPORTS section up here
st.set_page_config(layout="wide")



# --- NEW CALENDAR DISPLAY FUNCTION ---
def display_crane_day_calendar(crane_days_for_ramp):
    """
    Generates a more visually defined and compact monthly calendar,
    highlighting crane days and the current date.
    """
    candidate_dates = {d['date'] for d in crane_days_for_ramp}
    today = datetime.date.today()

    # Use columns to create a centered, narrower layout for the calendar
    _ , cal_col, _ = st.columns([1, 2, 1])

    with cal_col:
        with st.container(border=True): # Add a border around the entire component
            # --- Month Selector ---
            selected_month_str = st.selectbox(
                "Select a month to view:",
                [(today + datetime.timedelta(days=30*i)).strftime("%B %Y") for i in range(6)]
            )
            if not selected_month_str:
                return

            selected_month = datetime.datetime.strptime(selected_month_str, "%B %Y")
            st.subheader(f"Calendar for {selected_month_str}")

            # --- Calendar Header (Days of the Week) ---
            days_of_week = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            header_cols = st.columns(7)
            for col, day_name in zip(header_cols, days_of_week):
                with col:
                    st.markdown(f"<p style='text-align: center; font-weight: bold;'>{day_name}</p>", unsafe_allow_html=True)
            st.markdown("---", help=None) # A thin separator line

            # --- Calendar Body (Dates) ---
            cal = calendar.Calendar()
            month_days = cal.monthdatescalendar(selected_month.year, selected_month.month)

            for week in month_days:
                cols = st.columns(7)
                for i, day in enumerate(week):
                    # Style days that are not in the current month differently
                    if day.month != selected_month.month:
                        cols[i].markdown(
                            f"""
                            <div style="padding:10px; border-radius:5px; background-color:#F0F2F6; height: 60px;">
                                <p style="text-align: right; color: #D3D3D3;">{day.day}</p>
                            </div>
                            """, unsafe_allow_html=True)
                    else:
                        day_str = str(day.day)
                        is_candidate = day in candidate_dates
                        is_today = day == today

                        # Define styles based on the day's properties
                        bg_color = "#E8F5E9" if is_candidate else "#FFFFFF" # Light green for candidates
                        # Blue border for today, green for other candidates, light grey for regular days
                        border_color = "#1E88E5" if is_today else ("#4CAF50" if is_candidate else "#E0E0E0")
                        font_weight = "bold" if is_candidate or is_today else "normal"

                        # Render the styled day cell
                        cols[i].markdown(
                            f"""
                            <div style="padding:10px; border-radius:5px; border: 2px solid {border_color}; background-color:{bg_color}; height: 60px;">
                                <p style="text-align: right; font-weight: {font_weight}; color: black;">{day_str}</p>
                            </div>
                            """, unsafe_allow_html=True)
# --- Helper Functions ---

def format_tides_for_display(slot, ecm_hours):
    tide_times = slot.get('high_tide_times', [])
    if not tide_times:
        return ""
    if not ecm_hours or not ecm_hours.get('open'):
        return "HT: " + " / ".join([ecm.format_time_for_display(t) for t in tide_times])

    op_open, op_close = ecm_hours['open'], ecm_hours['close']

    def get_tide_relevance_score(tide_time):
        tide_dt = datetime.datetime.combine(datetime.date.today(), tide_time)
        open_dt = datetime.datetime.combine(datetime.date.today(), op_open)
        close_dt = datetime.datetime.combine(datetime.date.today(), op_close)
        if open_dt <= tide_dt <= close_dt:
            return 0, abs((tide_dt - open_dt).total_seconds())
        return 1, min(abs((tide_dt - open_dt).total_seconds()), abs((tide_dt - close_dt).total_seconds()))

    sorted_tides = sorted(tide_times, key=get_tide_relevance_score)
    if not sorted_tides:
        return ""
    primary_tide_str = ecm.format_time_for_display(sorted_tides[0])
    if len(sorted_tides) == 1:
        return f"**HIGH TIDE: {primary_tide_str}**"
    secondary_tides_str = " / ".join([ecm.format_time_for_display(t) for t in sorted_tides[1:]])
    return f"**HIGH TIDE: {primary_tide_str}** (and {secondary_tides_str.lower()})"

def handle_slot_selection(slot_data):
    st.session_state.selected_slot = slot_data

def generate_multi_day_planner_pdf(start_date, end_date, jobs):
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


# --- PDF Page Generation Tool ---

def _abbreviate_town(address):
    """
    Takes a full address string or a special keyword ('HOME') and returns
    a standardized three-letter abbreviation for the town.
    """
    if not address:
        return ""

    # The abbreviation mapping dictionary
    abbr_map = {
        "pembroke": "Pem",
        "scituate": "Sci",
        "green harbor": "GrH",
        "marshfield": "Mar",
        "cohasset": "Coh",
        "weymouth": "Wey",
        "plymouth": "Ply",
        "sandwich": "San",
        "duxbury": "Dux",
        "humarock": "Hum",
        "hingham": "Hin",
        "hull": "Hul"
    }

    # Check for the special 'HOME' case first
    if 'HOME' in address.upper():
        return "Pem"

    # For all other addresses, find the matching town
    address_lower = address.lower()
    for town, abbr in abbr_map.items():
        if town in address_lower:
            return abbr
            
    # As a fallback for unknown addresses, return the first 3 letters
    return address.title().split(',')[0][:3]

def generate_daily_planner_pdf(report_date, jobs_for_day):
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

    # --- Get Tide Data from the first scheduled job for the day ---
    high_tides, low_tides = [], []
    if jobs_for_day:
        first_job = jobs_for_day[0]
        high_tides = getattr(first_job, 'high_tides', [])
        low_tides = getattr(first_job, 'low_tides', [])

    def get_y_for_time(t):
        total_minutes = (t.hour - start_hour) * 60 + t.minute
        return top_y - (total_minutes / ((end_hour - start_hour) * 60) * content_height)

    # --- Header & Column Drawing ---
    c.setFont("Helvetica-Bold", 12)
    c.drawRightString(width - margin, height - 0.6 * inch, report_date.strftime("%A, %B %d").upper())
    for i, name in enumerate(planner_columns):
        c.setFont("Helvetica-Bold", 14)
        c.drawCentredString(margin + time_col_width + i * col_width + col_width / 2, top_y + 10, name)
    
    # --- Time Labels & Grid with CORRECT Highlighting ---
    for hour in range(start_hour, end_hour + 1):
        for minute in [0, 15, 30, 45]:
            current_time = datetime.time(hour, minute)
            y = get_y_for_time(current_time)
            label_y = get_y_for_time((datetime.datetime.combine(datetime.date.today(), current_time) + datetime.timedelta(minutes=7.5)).time())
            
            # Draw grid line
            c.setLineWidth(1.0 if minute == 0 else 0.25)
            c.line(margin, y, width - margin, y)

            # Check if the current 15-minute mark is a tide time
            def is_tide_time(tide_list):
                for t in tide_list:
                    if t['time'].hour == hour and (t['time'].minute // 15) * 15 == minute:
                        return True
                return False

            is_high_tide = is_tide_time(high_tides)
            is_low_tide = is_tide_time(low_tides)
            
            # Set highlight color ONLY for the text
            text_color = colors.black
            if is_high_tide:
                text_color = colors.orange # Using orange for visibility on white
            elif is_low_tide:
                text_color = colors.red
            
            c.setFillColor(text_color)
            
            # Draw the time labels
            if minute == 0:
                display_hour = hour if hour <= 12 else hour - 12
                c.setFont("Helvetica-Bold", 9)
                c.drawString(margin + 3, label_y - 3, str(display_hour))
                c.setFont("Helvetica", 7)
                c.drawString(margin + 18, label_y - 3, "00")
            else:
                c.setFont("Helvetica", 6)
                c.drawString(margin + 18, label_y - 2, f"{minute:02d}")
            
            # IMPORTANT: Reset color to black after drawing
            c.setFillColor(colors.black)

    # --- Vertical and Side Borders ---
    for i in range(len(planner_columns) + 1):
        x = margin + time_col_width + i * col_width; c.setLineWidth(0.5); c.line(x, top_y, x, bottom_y)
    c.line(margin, top_y, margin, bottom_y)
    
    # --- Job Entries --- (Your formatting is preserved)
    for job in jobs_for_day:
        # ... (This entire section is the same as your correct version)
        start_time = getattr(job, 'scheduled_start_datetime').time(); end_time = getattr(job, 'scheduled_end_datetime').time()
        y0, y_end = get_y_for_time(start_time), get_y_for_time(end_time)
        line1_y, line2_y, line3_y, line4_y = y0 - 8, y0 - 18, y0 - 28, y0 - 38; y_bar_start = y0 - 42
        customer = ecm.get_customer_details(getattr(job, 'customer_id', None)); boat = ecm.get_boat_details(getattr(job, 'boat_id', None))
        truck_id = getattr(job, 'assigned_hauling_truck_id', None)
        if truck_id in column_map:
            col_index = column_map[truck_id]; text_center_x = margin + time_col_width + (col_index + 0.5) * col_width
            c.setFont("Helvetica-Bold", 8); c.drawCentredString(text_center_x, line1_y, customer.customer_name)
            c.setFont("Helvetica", 7); c.drawCentredString(text_center_x, line2_y, f"{int(boat.boat_length)}' {boat.boat_type}")
            c.drawCentredString(text_center_x, line3_y, f"(Draft: {boat.draft_ft}')")
            c.drawCentredString(text_center_x, line4_y, f"{_abbreviate_town(getattr(job, 'pickup_street_address', ''))}-{_abbreviate_town(getattr(job, 'dropoff_street_address', ''))}")
            c.setLineWidth(2); c.line(text_center_x, y_bar_start, text_center_x, y_end); c.line(text_center_x - 3, y_end, text_center_x + 3, y_end)
        if getattr(job, 'assigned_crane_truck_id') and 'J17' in column_map:
            crane_col_index = column_map['J17']; text_center_x_crane = margin + time_col_width + (crane_col_index + 0.5) * col_width
            y_crane_end = get_y_for_time(getattr(job, 'j17_busy_end_datetime').time())
            c.setFont("Helvetica-Bold", 8); c.drawCentredString(text_center_x_crane, line1_y, customer.customer_name.split()[-1])
            c.setFont("Helvetica", 7); c.drawCentredString(text_center_x_crane, line2_y, _abbreviate_town(getattr(job, 'dropoff_street_address', '')))
            if 'mt' in boat.boat_type.lower(): c.drawCentredString(text_center_x_crane, line3_y, "TRANSPORT")
            c.setLineWidth(2); c.line(text_center_x_crane, y_bar_start, text_center_x_crane, y_crane_end); c.line(text_center_x_crane - 3, y_crane_end, text_center_x_crane + 3, y_crane_end)

    # --- FIX #2: Draw the Bottom Border LAST ---
    # This line was missing from the previous attempt's loop
    c.setLineWidth(1.0)
    c.line(margin, bottom_y, width - margin, bottom_y)
        
    c.save()
    buffer.seek(0)
    return buffer

# --- Cancel, Rebook, and Audit ---

CANCELED_JOBS_AUDIT_LOG = []

def cancel_job_by_customer_name(customer_name):
    job_to_cancel = None
    customer_details = None
    for job in ecm.SCHEDULED_JOBS:
        customer = ecm.get_customer_details(job.customer_id)
        if customer and customer.customer_name.lower() == customer_name.lower():
            job_to_cancel = job
            customer_details = customer
            break
    if job_to_cancel:
        ramp = ecm.get_ramp_details(job_to_cancel.pickup_ramp_id or job_to_cancel.dropoff_ramp_id)
        audit_entry = {
            "Customer": customer_details.customer_name,
            "Original Date": job_to_cancel.scheduled_start_datetime.strftime("%Y-%m-%d"),
            "Original Time": job_to_cancel.scheduled_start_datetime.strftime("%H:%M"),
            "Original Truck": job_to_cancel.assigned_hauling_truck_id,
            "Original Ramp": ramp.ramp_name if ramp else "N/A",
            "Action": "Canceled",
            "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        CANCELED_JOBS_AUDIT_LOG.append(audit_entry)
        ecm.SCHEDULED_JOBS.remove(job_to_cancel)
        return True, audit_entry
    return False, None

def reschedule_customer(customer_name, new_slot):
    canceled, audit_entry = cancel_job_by_customer_name(customer_name)
    if not canceled:
        return False, "Customer not found."

    customer = next((c for c in ecm.LOADED_CUSTOMERS.values() if c.customer_name.lower() == customer_name.lower()), None)
    boat = next((b for b in ecm.LOADED_BOATS.values() if b.customer_id == customer.customer_id), None) if customer else None
    if not customer or not boat:
        return False, "Customer or boat not found for rescheduling."

    # Create new job request
    new_job_request = {
        'customer_id': customer.customer_id,
        'boat_id': boat.boat_id,
        'service_type': "Launch",  # Assuming Launch, might need to be smarter
        'requested_date_str': new_slot['date'].strftime('%Y-%m-%d'),
        'selected_ramp_id': new_slot['ramp_id'],
    }
    job_id, _ = ecm.confirm_and_schedule_job(new_job_request, new_slot)
    ramp_details = ecm.get_ramp_details(new_slot['ramp_id'])
    audit_entry['Action'] = "Rescheduled"
    audit_entry['New Date'] = new_slot['date'].strftime('%Y-%m-%d')
    audit_entry['New Time'] = new_slot['time'].strftime('%H:%M')
    audit_entry['New Truck'] = new_slot['truck_id']
    audit_entry['New Ramp'] = ramp_details.ramp_name if ramp_details else "N/A"
    return True, audit_entry

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


def find_next_available_slot_after(date_obj, customer_id, boat_id, service_type, selected_ramp_id, relax_truck, relax_ramp):
    import datetime as dt
    max_search_days = 45
    next_date = date_obj + dt.timedelta(days=1)
    for _ in range(max_search_days):
        date_str = next_date.strftime('%Y-%m-%d')
        slots, message, _, _ = ecm.find_available_job_slots(
            customer_id=customer_id,
            boat_id=boat_id,
            service_type=service_type,
            requested_date_str=date_str,
            selected_ramp_id=selected_ramp_id,
            force_preferred_truck=(not relax_truck),
            relax_ramp=relax_ramp,
            ignore_forced_search=True
        )
        if slots:
            return slots[0]
        next_date += dt.timedelta(days=1)
    return None
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
app_mode = st.sidebar.radio("Go to", ["Schedule New Boat", "Reporting", "Cancel Job", "Settings"])

# --- PAGE 1: SCHEDULER ---
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
    selected_boat_obj = None

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
        else:
            st.sidebar.error(f"No boat found for {selected_customer_obj.customer_name}.")
            st.stop()

    def validate_and_correct_customer_data(customer, boat):
        missing_fields = []
        if not boat.boat_type: missing_fields.append("Boat Type")
        if not boat.boat_length or boat.boat_length <= 0: missing_fields.append("Boat Length")
        if boat.draft_ft is None or boat.draft_ft <= 0: missing_fields.append("Boat Draft")
        if not customer.preferred_truck_id: missing_fields.append("Preferred Truck")
        if customer.is_ecm_customer not in [True, False]: missing_fields.append("ECM Boat Flag")

        if not missing_fields:
            return True

        st.warning("🚨 The following fields are missing: " + ", ".join([f"**{field}**" for field in missing_fields]))
        with st.form("edit_customer_data_form"):
            new_boat_type = st.selectbox("Boat Type", ["Powerboat", "Sailboat MT", "Sailboat DT"], index=0)
            new_length = st.number_input("Boat Length (ft)", min_value=1.0, value=float(boat.boat_length) if boat.boat_length else 20.0)
            new_draft = st.number_input("Boat Draft (ft)", min_value=0.5, value=float(boat.draft_ft) if boat.draft_ft else 2.0)
            new_ecm_flag = st.radio("Is ECM Boat?", [True, False], index=0)
            new_truck = st.selectbox("Preferred Truck", list(ecm.ECM_TRUCKS.keys()))
            submitted = st.form_submit_button("Update & Continue")

        if submitted:
            boat.boat_type, boat.boat_length, boat.draft_ft = new_boat_type, new_length, new_draft
            customer.is_ecm_customer, customer.preferred_truck_id = new_ecm_flag, new_truck

            updated_rows = []
            fieldnames = []
            with open("ECM Sample Cust.csv", "r", encoding='utf-8-sig') as infile:
                reader = csv.DictReader(infile)
                fieldnames = reader.fieldnames
                for row in reader:
                    if row["customer_name"] == customer.customer_name:
                        row.update({
                            "boat_type": new_boat_type, "boat_length": str(new_length),
                            "boat_draft": str(new_draft), "is_ecm_boat": "TRUE" if new_ecm_flag else "FALSE",
                            "preferred_truck": new_truck
                        })
                    updated_rows.append(row)

            with open("ECM Sample Cust.csv", "w", newline='', encoding='utf-8-sig') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(updated_rows)

            st.success("Customer record updated. Re-running scheduling search...")
            st.rerun()
        return False

    if selected_customer_obj and selected_boat_obj:
        if validate_and_correct_customer_data(selected_customer_obj, selected_boat_obj):
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
            st.sidebar.subheader("Search Options")
            relax_truck_input = st.sidebar.checkbox("Relax Truck (Use any capable truck)")
            relax_ramp_input = st.sidebar.checkbox("Relax Ramp (Search other nearby ramps)")
            
            st.sidebar.markdown("---")
            st.sidebar.subheader("Advanced Settings")
            
            # --- NEW: Master feature toggle ---
            # This creates the toggle and ensures the backend variable stays in sync with the UI
            crane_logic_toggle = st.sidebar.toggle("Enable Crane Day Logic", value=ecm.CRANE_DAY_LOGIC_ENABLED, key="crane_logic_master_toggle")
            ecm.CRANE_DAY_LOGIC_ENABLED = crane_logic_toggle 
            
            # --- NEW: Manager override checkbox ---
            manager_override_input = st.sidebar.checkbox("MANAGER: Override Crane Day Block")
            
            
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
            
                slots, message, warning_msgs, was_forced = ecm.find_available_job_slots(
                    # --- NEW: Pass the number of suggestions from session state ---
                    num_suggestions_to_find=st.session_state.get('num_suggestions', 3),
                    
                    **job_request,
                    force_preferred_truck=(not relax_truck_input),
                    relax_ramp=relax_ramp_input,
                    manager_override=manager_override_input
                )
            
                st.session_state.info_message = message
                st.session_state.found_slots = slots
                st.session_state.warning_msgs = warning_msgs
                st.session_state.selected_slot = None
                st.session_state.was_forced_search = was_forced
                st.rerun()

    if st.session_state.found_slots and not st.session_state.selected_slot:
        st.subheader("Please select your preferred slot:")
        cols = st.columns(3)
        for i, slot in enumerate(st.session_state.found_slots):
            with cols[i % 3]:
                with st.container(border=True):
                    if st.session_state.get('search_requested_date') and slot['date'] == st.session_state.search_requested_date:
                        st.markdown("""<div style='background-color:#F0FFF0;border-left:6px solid #2E8B57;padding:10px;border-radius:5px;margin-bottom:10px;'><h5 style='color:#2E8B57;margin:0;font-weight:bold;'>⭐ Requested Date</h5></div>""", unsafe_allow_html=True)
                        # --- NEW: Display warnings for alternate slots ---
                        if slot.get('is_alternate_ramp'):
                            st.warning(f"⚠️ Alternate Ramp Used")
                        if slot.get('is_alternate_truck'):
                            st.warning(f"⚠️ Alternate Truck Used")

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
                    if slot.get('j17_needed'): st.markdown(f"**Crane:** J17")
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


if st.session_state.info_message and "crane job" in st.session_state.info_message.lower() and not st.session_state.found_slots:
    forced_date = st.session_state.search_requested_date
    st.error(f"No suitable time slots available on that crane grouping day ({forced_date.strftime('%A, %b %d')}).")

    next_slot = find_next_available_slot_after(
        forced_date,
        selected_customer_obj.customer_id,
        selected_boat_obj.boat_id,
        service_type_input,
        selected_ramp_id_input,
        relax_truck_input,
        relax_ramp_input
    )

    if next_slot:
        slot_date_str = next_slot['date'].strftime('%A, %b %d')
        slot_time_str = ecm.format_time_for_display(next_slot['time'])
        ramp_details = ecm.get_ramp_details(next_slot['ramp_id'])
        ramp_name = ramp_details.ramp_name if ramp_details else "N/A"
        st.info(f"Next available slot: {slot_date_str} at {slot_time_str} with Truck {next_slot['truck_id']} at Ramp {ramp_name}.")

        if st.button("Select This Slot", key="select_next_available_slot"):
            st.session_state.selected_slot = next_slot
            st.rerun()

        if st.button("Search Next Available Slot", key="search_next_slot"):
            even_next_slot = find_next_available_slot_after(
                next_slot['date'],
                selected_customer_obj.customer_id,
                selected_boat_obj.boat_id,
                service_type_input,
                selected_ramp_id_input,
                relax_truck_input,
                relax_ramp_input
            )
            if even_next_slot:
                st.session_state.selected_slot = even_next_slot
                st.rerun()
            else:
                st.error("No more available slots found within the search range.")
    elif st.session_state.get('current_job_request') and not st.session_state.found_slots:
        if st.session_state.info_message:
            st.warning(st.session_state.info_message)

# --- PAGE 2: REPORTING ---
elif app_mode == "Reporting":
    st.header("Reporting Dashboard")
    st.info("This section is for viewing and exporting scheduled jobs.")
    st.subheader("All Scheduled Jobs (Current Session)")
    st.markdown("---")
    st.subheader("Crane Day Candidate Calendar")
    
    # Create a list of the crane-specific ramps
    crane_ramp_options = list(ecm.CANDIDATE_CRANE_DAYS.keys())
    
    selected_ramp_for_calendar = st.selectbox(
        "Select a ramp to see its Candidate Crane Days:",
        options=crane_ramp_options
    )
    
    if selected_ramp_for_calendar:
        # Get the list of candidate day objects for the selected ramp
        candidate_days_for_selected_ramp = ecm.CANDIDATE_CRANE_DAYS[selected_ramp_for_calendar]
        
        # Call our new function to display the calendar
        display_crane_day_calendar(candidate_days_for_selected_ramp)
    
    st.markdown("---")

    if ecm.SCHEDULED_JOBS:
        display_data = []
        sorted_jobs = sorted(ecm.SCHEDULED_JOBS, key=lambda j: j.scheduled_start_datetime or datetime.datetime.max)
        for job in sorted_jobs:
            customer = ecm.get_customer_details(getattr(job, 'customer_id', None))
            ramp = ecm.get_ramp_details(getattr(job, 'pickup_ramp_id', None) or getattr(job, 'dropoff_ramp_id', None))
            
            truck_info = job.assigned_hauling_truck_id if job.assigned_hauling_truck_id else "N/A"
            crane_info = job.assigned_crane_truck_id if job.assigned_crane_truck_id else "N/A"
            
            boat_id = getattr(job, 'boat_id', None)
            boat = ecm.LOADED_BOATS.get(boat_id) if boat_id else None
            is_sailboat = boat and 'sailboat' in getattr(boat, 'boat_type', '').lower()

            display_data.append({
                "Job ID": job.job_id, "Status": job.job_status,
                "Scheduled Date": job.scheduled_start_datetime.strftime("%Y-%m-%d") if job.scheduled_start_datetime else "N/A",
                "Scheduled Time": ecm.format_time_for_display(job.scheduled_start_datetime.time()) if job.scheduled_start_datetime else "N/A",
                "Service": job.service_type, "Customer": customer.customer_name if customer else "N/A",
                "Truck": truck_info, "Crane": crane_info if crane_info != "N/A" else "",
                "Ramp": ramp.ramp_name if ramp else "N/A"
            })
        st.dataframe(pd.DataFrame(display_data))
    else:
        st.write("No jobs scheduled yet.")

    st.subheader("Generate Daily Planner PDF")
    selected_date = st.date_input("Select date to export:", value=datetime.date.today())
    if st.button("📤 Generate PDF"):
        jobs_today = [j for j in ecm.SCHEDULED_JOBS if j.scheduled_start_datetime.date() == selected_date]
        if not jobs_today:
            st.warning("No jobs scheduled for that date.")
        else:
            pdf_buffer = generate_daily_planner_pdf(selected_date, jobs_today)
            st.download_button(
                label="📥 Download Planner", data=pdf_buffer.getvalue(),
                file_name=f"Daily_Planner_{selected_date}.pdf", mime="application/pdf"
            )

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
                    label="📥 Download Multi-Day Planner", data=merged_pdf,
                    file_name=f"Planner_{start_date}_to_{end_date}.pdf", mime="application/pdf"
                )

# --- PAGE 3: SETTINGS ---

elif app_mode == "Cancel Job":
    st.header("Cancel a Scheduled Job")

    # --- Option 1: Search by Customer Name ---
    st.subheader("Search by Customer Name")
    name_input = st.text_input("Start typing customer name:")

    matched_customers = [c for c in ecm.LOADED_CUSTOMERS.values() if name_input.lower() in c.customer_name.lower()]

    selected_customer = None
    if matched_customers:
        customer_names = [c.customer_name for c in matched_customers]
        chosen_name = st.selectbox("Select customer to cancel:", customer_names)
        selected_customer = next(c for c in matched_customers if c.customer_name == chosen_name)

    if selected_customer:
        scheduled_job = next((j for j in ecm.SCHEDULED_JOBS if j.customer_id == selected_customer.customer_id), None)
        if scheduled_job:
            st.write(f"**Scheduled Job for {selected_customer.customer_name}:**")
            st.write(f"- Date: {scheduled_job.scheduled_start_datetime.date()}")
            st.write(f"- Time: {ecm.format_time_for_display(scheduled_job.scheduled_start_datetime.time())}")
            st.write(f"- Truck: {scheduled_job.assigned_hauling_truck_id}")
            ramp_obj = ecm.get_ramp_details(scheduled_job.pickup_ramp_id or scheduled_job.dropoff_ramp_id)
            ramp_name = ramp_obj.ramp_name if ramp_obj else "N/A"
            st.write(f"- Ramp: {ramp_name}")

            if st.button("Cancel This Job", key="cancel_by_name"):
                success, audit = cancel_job_by_customer_name(selected_customer.customer_name)
                if success:
                    st.success(f"✅ Job canceled for {selected_customer.customer_name}")
                else:
                    st.error("Failed to cancel job.")
        else:
            st.warning("This customer has no scheduled job.")

    st.markdown("---")
    # --- Option 2: Select from Full Scheduled Jobs Report ---
    st.subheader("Select Job from Full Schedule")
    jobs_data = []
    for job in ecm.SCHEDULED_JOBS:
        customer = ecm.get_customer_details(job.customer_id)
        jobs_data.append({
            "Customer": customer.customer_name if customer else "Unknown",
            "Date": job.scheduled_start_datetime.date(),
            "Time": ecm.format_time_for_display(job.scheduled_start_datetime.time()),
            "Truck": job.assigned_hauling_truck_id,
            "Ramp": ecm.get_ramp_details(job.pickup_ramp_id or job.dropoff_ramp_id).ramp_name if (job.pickup_ramp_id or job.dropoff_ramp_id) else "N/A"
        })
    if jobs_data:
        df_jobs = pd.DataFrame(jobs_data)
        selected_customer_to_cancel = st.selectbox("Select Customer:", df_jobs["Customer"].tolist())

        if st.button("Cancel Selected Job", key="cancel_from_table"):
            success, audit = cancel_job_by_customer_name(selected_customer_to_cancel)
            if success:
                st.success(f"✅ Job canceled for {selected_customer_to_cancel}")
            else:
                st.error("Failed to cancel job.")
    else:
        st.warning("No jobs scheduled.")
elif app_mode == "Settings":
    st.header("Application Settings")
    
    st.subheader("Scheduling Defaults")
    
    # Initialize the session state key if it doesn't exist
    if 'num_suggestions' not in st.session_state:
        st.session_state.num_suggestions = 3

    # Create the number input and link it to the session state
    st.session_state.num_suggestions = st.number_input(
        "Number of Suggested Dates to Return",
        min_value=3,
        max_value=6,
        value=st.session_state.num_suggestions,
        step=1,
        help="Choose how many different date options to see when searching for a slot (default is 3)."
    )
    
    st.success(f"Search results will now show {st.session_state.num_suggestions} suggestions.")
