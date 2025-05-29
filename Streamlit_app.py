import streamlit as st
import uuid
from datetime import datetime
import os
import pandas as pd

# For Google Sheets connection
from streamlit_gsheets import GSheetsConnection # Ensure this is imported

# --- Configuration ---
DATETIME_FORMAT = "%Y-%m-%d %H:%M"
CSV_CUSTOMER_IMPORT_FILE = "ECM Sample Cust.csv" # Your CSV file in GitHub

# --- Customer Class ---
class Customer:
    def __init__(self, customer_name, boat_type, boat_length, boat_draft,
                 home_latitude, home_longitude, is_ecm_boat,
                 phone="", email="", address="", customer_id=None):

        self.customer_id = str(customer_id) if customer_id and str(customer_id).strip() else str(uuid.uuid4())
        self.customer_name = str(customer_name)
        self.phone = str(phone)
        self.email = str(email)
        self.address = str(address)
        self.boat_type = str(boat_type)
        try:
            self.boat_length = float(boat_length) if pd.notna(boat_length) else 0.0
        except (ValueError, TypeError):
            self.boat_length = 0.0
        try:
            self.boat_draft = float(boat_draft) if pd.notna(boat_draft) else 0.0
        except (ValueError, TypeError):
            self.boat_draft = 0.0
        try:
            self.home_latitude = float(home_latitude) if pd.notna(home_latitude) else 0.0
        except (ValueError, TypeError):
            self.home_latitude = 0.0
        try:
            self.home_longitude = float(home_longitude) if pd.notna(home_longitude) else 0.0
        except (ValueError, TypeError):
            self.home_longitude = 0.0

        if isinstance(is_ecm_boat, str):
            self.is_ecm_boat = is_ecm_boat.strip().upper() == 'TRUE'
        elif pd.isna(is_ecm_boat):
            self.is_ecm_boat = False
        else:
            self.is_ecm_boat = bool(is_ecm_boat)

    def to_sheet_row(self):
        # Order must match Google Sheet headers exactly
        return [
            self.customer_id, self.customer_name, self.phone, self.email, self.address,
            self.boat_type, self.boat_length, self.boat_draft, self.home_latitude,
            self.home_longitude, self.is_ecm_boat
        ]

    @staticmethod
    def from_sheet_row(row_dict):
        # Assumes row_dict has keys matching the *exact* headers from your Google Sheet
        return Customer(
            customer_id=row_dict.get('customer_id'),
            customer_name=row_dict.get('Customer Name'),
            phone=row_dict.get('phone', ""),
            email=row_dict.get('email', ""),
            address=row_dict.get('address', ""),
            boat_type=row_dict.get('Boat Type'),
            boat_length=row_dict.get('Boat Length', 0.0),
            boat_draft=row_dict.get('Boat Draft', 0.0),
            home_latitude=row_dict.get('Home Latitude', 0.0),
            home_longitude=row_dict.get('Home Longitude', 0.0),
            is_ecm_boat=row_dict.get('Is ECM Boat', False)
        )

# --- Google Sheets Data Manager for Customers ---
class CustomerGSheetsManager:
    CUSTOMERS_SHEET_NAME = "Customers" # Tab name in your Google Sheet
    # THIS LIST MUST EXACTLY MATCH THE HEADERS IN ROW 1 OF YOUR GOOGLE SHEET
    EXPECTED_HEADERS = [
        'customer_id', 'Customer Name', 'phone', 'email', 'address',
        'Boat Type', 'Boat Length', 'Boat Draft', 'Home Latitude',
        'Home Longitude', 'Is ECM Boat'
    ]

    def __init__(self):
        self.conn = None
        self.gs_worksheet = None
        try:
            self.conn = st.connection("gsheets", type=GSheetsConnection)
            # Attempt to get the worksheet and check headers
            self.gs_worksheet = self.conn.session.worksheet(self.CUSTOMERS_SHEET_NAME)
            
            actual_headers = self.gs_worksheet.row_values(1) # Get headers from the first row
            if not actual_headers : # Sheet might be completely blank
                st.warning(f"The '{self.CUSTOMERS_SHEET_NAME}' sheet appears to be completely blank. Attempting to set headers.")
                self.gs_worksheet.update([self.EXPECTED_HEADERS], value_input_option='USER_ENTERED') # Write headers
                st.info(f"Headers set in '{self.CUSTOMERS_SHEET_NAME}'. Please refresh if you see this for the first time.")
            elif actual_headers[:len(self.EXPECTED_HEADERS)] != self.EXPECTED_HEADERS:
                 # Compare only up to the length of expected headers, in case there are extra columns in sheet
                st.error(f"Header mismatch in '{self.CUSTOMERS_SHEET_NAME}' sheet. App Expected (first {len(self.EXPECTED_HEADERS)}): {self.EXPECTED_HEADERS}, Sheet Found: {actual_headers}. Please correct the Google Sheet headers to match the expected ones, including exact spelling and case.")
                self.conn = None # Invalidate connection if headers are critically wrong
        except Exception as e:
            st.error(f"Error initializing CustomerGSheetsManager: {e}. Check Google Sheet name, tab name, permissions, and API enablement.")
            self.conn = None


    def get_all_customers(self):
        if not self.conn or not self.gs_worksheet:
            st.warning("Google Sheets connection not available or worksheet not loaded.")
            return []
        try:
            # Use conn.read() which returns a DataFrame, more robust for type handling
            data_df = self.conn.read(
                worksheet=self.CUSTOMERS_SHEET_NAME,
                usecols=list(range(len(self.EXPECTED_HEADERS))), # Read only expected number of columns
                ttl=5 # Cache for 5 seconds
            )

            if data_df.empty:
                return []

            # Ensure DataFrame columns match expected headers for safety during processing
            # This step might be redundant if conn.read correctly uses headers or if we rename columns
            # For now, let's assume conn.read attempts to use the first row as headers
            # And if they don't match, from_sheet_row will have issues.
            # It's safer to ensure the DataFrame columns are what Customer.from_sheet_row expects.
            
            # Make a defensive copy before renaming if necessary
            # data_df_processed = data_df.copy()
            # If conn.read() doesn't automatically use the correct headers for column names,
            # you might need to assign them or read differently.
            # Assuming conn.read() uses the first row as headers correctly.

            data_df = data_df.dropna(subset=[self.EXPECTED_HEADERS[0]], how='all') # Drop rows where primary key (customer_id) is entirely NaN

            customers = []
            for record_dict in data_df.to_dict('records'):
                # The record_dict keys will be the actual column headers from the sheet.
                # Customer.from_sheet_row is designed to use these exact headers.
                customers.append(Customer.from_sheet_row(record_dict))
            return customers
        except Exception as e:
            st.error(f"Error reading customers from Google Sheet: {e}")
            st.exception(e)
            return []

    def add_customer_obj(self, customer_obj: Customer):
        if not self.conn or not self.gs_worksheet:
            st.error("Connection not established. Cannot add customer.")
            return False
        try:
            if customer_obj.email and customer_obj.email.strip(): # Only check if email is provided
                all_data = self.gs_worksheet.get_all_records() # Returns list of dicts
                existing_emails = {str(row.get('email', '')).lower() for row in all_data if row.get('email')}
                if customer_obj.email.lower() in existing_emails:
                    st.warning(f"Customer with email '{customer_obj.email}' already exists. Not adding.")
                    return False
            
            new_row_data = customer_obj.to_sheet_row()
            self.gs_worksheet.append_row(new_row_data, value_input_option='USER_ENTERED')
            st.success(f"Customer '{customer_obj.customer_name}' added to Google Sheet.")
            return True
        except Exception as e:
            st.error(f"Error adding customer to Google Sheet: {e}")
            st.exception(e)
            return False

    def update_customer_by_id(self, customer_id_to_update, updated_customer_obj: Customer):
        if not self.conn or not self.gs_worksheet:
            st.error("Connection not established. Cannot update customer.")
            return False
        try:
            cell = None
            try:
                cell = self.gs_worksheet.find(customer_id_to_update, in_cols=1) # Find in the first column (customer_id)
            except Exception: # gspread.exceptions.CellNotFound
                st.error(f"Customer with ID '{customer_id_to_update}' not found in sheet for update.")
                return False
            
            if cell:
                row_to_update_values = updated_customer_obj.to_sheet_row()
                end_column_letter = chr(ord('A') + len(self.EXPECTED_HEADERS) - 1)
                range_to_update = f'A{cell.row}:{end_column_letter}{cell.row}'
                self.gs_worksheet.update(range_to_update, [row_to_update_values], value_input_option='USER_ENTERED')
                st.success(f"Customer '{updated_customer_obj.customer_name}' (ID: {customer_id_to_update}) updated.")
                return True
            else:
                st.error(f"Customer with ID '{customer_id_to_update}' could not be found (cell not found).")
                return False
        except Exception as e:
            st.error(f"Error updating customer (ID: {customer_id_to_update}): {e}")
            st.exception(e)
            return False

# --- Streamlit UI Application ---
def streamlit_main():
    st.set_page_config(layout="wide", page_title="Boat Hauling Automator")
    st.title("🚤 Boat Hauling Business Automator")
    st.write(f"Current Time: {datetime.now().strftime(DATETIME_FORMAT)} (Location: Pembroke, MA)")


    if 'customer_manager' not in st.session_state:
        st.session_state.customer_manager = CustomerGSheetsManager()
    customer_manager = st.session_state.customer_manager
    
    if not customer_manager.conn or not customer_manager.gs_worksheet:
        st.error("Failed to connect to Customer Database (Google Sheets). Please check configurations and secrets. App functionality will be limited.")
        # Optionally, you could disable menu options if connection fails
        # return # or allow limited operation

    menu_options = [
        "Home", "Add New Customer", "List/View All Customers",
        "Import Customers from CSV"
        # Add Job-related options later
    ]
    menu_choice = st.sidebar.selectbox("Navigation", menu_options)

    if menu_choice == "Home":
        st.header("Welcome!")
        st.write("Select an option from the sidebar.")
        if customer_manager.conn and customer_manager.gs_worksheet:
            try:
                all_customers = customer_manager.get_all_customers()
                st.metric("Total Customers (Google Sheet)", len(all_customers))
            except Exception as e:
                st.error(f"Could not retrieve customer count: {e}")
        else:
            st.info("Customer data unavailable due to connection issues.")

    elif menu_choice == "Add New Customer":
        st.header("➕ Add New Customer")
        if not customer_manager.conn or not customer_manager.gs_worksheet:
            st.error("Cannot add customer: No connection to Google Sheets.")
        else:
            with st.form("add_customer_gform", clear_on_submit=True):
                st.subheader("Customer Info")
                customer_name = st.text_input("Customer Name*", help="e.g., John Doe")
                phone = st.text_input("Phone", help="e.g., 555-123-4567")
                email = st.text_input("Email", help="e.g., john.doe@example.com")
                address = st.text_area("Address", help="e.g., 123 Main St, Anytown, USA")

                st.subheader("Boat Info")
                boat_type = st.text_input("Boat Type*", help="e.g., Powerboat, Sailboat MD, Sailboat MT")
                boat_length = st.number_input("Boat Length (ft)*", min_value=1.0, value=25.0, format="%.1f")
                boat_draft = st.number_input("Boat Draft (ft)", min_value=0.0, value=3.0, format="%.1f")
                
                st.subheader("Location & Other")
                home_latitude = st.number_input("Home Latitude", format="%.6f", value=42.078000)
                home_longitude = st.number_input("Home Longitude", format="%.6f", value=-70.710000)
                is_ecm_boat = st.checkbox("Is ECM Boat?", value=False)
                
                submitted = st.form_submit_button("Add Customer")

                if submitted:
                    if not all([customer_name, boat_type, boat_length > 0]):
                        st.error("Please fill in required fields: Customer Name, Boat Type, Boat Length.")
                    else:
                        new_customer = Customer(
                            customer_name=customer_name, phone=phone, email=email, address=address,
                            boat_type=boat_type, boat_length=boat_length, boat_draft=boat_draft,
                            home_latitude=home_latitude, home_longitude=home_longitude,
                            is_ecm_boat=is_ecm_boat
                        )
                        customer_manager.add_customer_obj(new_customer)

    elif menu_choice == "List/View All Customers":
        st.header("👥 List of Customers")
        if not customer_manager.conn or not customer_manager.gs_worksheet:
            st.error("Cannot list customers: No connection to Google Sheets.")
        else:
            all_customers = customer_manager.get_all_customers()
            if not all_customers:
                st.info("No customers found in the Google Sheet.")
            else:
                st.write(f"Found {len(all_customers)} customer(s).")
                for customer in sorted(all_customers, key=lambda c: c.customer_name):
                    exp_title = f"{customer.customer_name} ({customer.boat_type}, {customer.boat_length}ft)"
                    with st.expander(exp_title):
                        st.markdown(f"**ID:** `{customer.customer_id}`")
                        cols = st.columns(2)
                        cols[0].markdown(f"**Phone:** {customer.phone if customer.phone else 'N/A'}")
                        cols[0].markdown(f"**Email:** {customer.email if customer.email else 'N/A'}")
                        cols[0].markdown(f"**Address:** {customer.address if customer.address else 'N/A'}")
                        cols[1].markdown(f"**Boat Type:** {customer.boat_type}")
                        cols[1].markdown(f"**Boat Length:** {customer.boat_length} ft")
                        cols[1].markdown(f"**Boat Draft:** {customer.boat_draft} ft")
                        cols[0].markdown(f"**Home Latitude:** {customer.home_latitude}")
                        cols[0].markdown(f"**Home Longitude:** {customer.home_longitude}")
                        cols[1].markdown(f"**Is ECM Boat:** {'Yes' if customer.is_ecm_boat else 'No'}")
                        
                        st.markdown("---")
                        st.subheader(f"Edit {customer.customer_name}")
                        with st.form(key=f"edit_form_{customer.customer_id}"):
                            edit_name = st.text_input("Cust. Name", value=customer.customer_name, key=f"name_{customer.customer_id}")
                            edit_phone = st.text_input("Phone", value=customer.phone, key=f"phone_{customer.customer_id}")
                            edit_email = st.text_input("Email", value=customer.email, key=f"email_{customer.customer_id}")
                            edit_address = st.text_area("Address", value=customer.address, key=f"addr_{customer.customer_id}")
                            edit_boat_type = st.text_input("Boat Type", value=customer.boat_type, key=f"btype_{customer.customer_id}")
                            edit_boat_length = st.number_input("Length", value=customer.boat_length, min_value=1.0, format="%.1f", key=f"blen_{customer.customer_id}")
                            edit_boat_draft = st.number_input("Draft", value=customer.boat_draft, min_value=0.0, format="%.1f", key=f"bdr_{customer.customer_id}")
                            edit_lat = st.number_input("Latitude", value=customer.home_latitude, format="%.6f", key=f"lat_{customer.customer_id}")
                            edit_lon = st.number_input("Longitude", value=customer.home_longitude, format="%.6f", key=f"lon_{customer.customer_id}")
                            edit_ecm = st.checkbox("ECM Boat", value=customer.is_ecm_boat, key=f"ecm_{customer.customer_id}")
                            
                            update_submitted = st.form_submit_button("Save Changes")
                            if update_submitted:
                                updated_cust_obj = Customer(
                                    customer_name=edit_name, phone=edit_phone, email=edit_email, address=edit_address,
                                    boat_type=edit_boat_type, boat_length=edit_boat_length, boat_draft=edit_boat_draft,
                                    home_latitude=edit_lat, home_longitude=edit_lon, is_ecm_boat=edit_ecm,
                                    customer_id=customer.customer_id # CRITICAL: Pass existing ID
                                )
                                if customer_manager.update_customer_by_id(customer.customer_id, updated_cust_obj):
                                    st.experimental_rerun()
                                # Error message handled by manager method

    elif menu_choice == "Import Customers from CSV":
        st.header("📥 Import Customers from CSV to Google Sheet")
        st.warning("This utility will add new customers from the CSV to your Google Sheet. It tries to avoid adding exact duplicates based on email if provided and valid in CSV, or by existing customer_id if present in CSV.")
        st.info(f"Ensure `{CSV_CUSTOMER_IMPORT_FILE}` is in the root of your GitHub repository.")

        if st.button(f"Start Import from '{CSV_CUSTOMER_IMPORT_FILE}'"):
            if not os.path.exists(CSV_CUSTOMER_IMPORT_FILE):
                st.error(f"File '{CSV_CUSTOMER_IMPORT_FILE}' not found. Please upload it to your GitHub repository.")
            elif not customer_manager.conn or not customer_manager.gs_worksheet:
                st.error("Cannot import: Google Sheets connection not available.")
            else:
                with st.spinner("Importing CSV data..."):
                    try:
                        csv_df = pd.read_csv(CSV_CUSTOMER_IMPORT_FILE)
                        st.write(f"Read {len(csv_df)} rows from CSV.")

                        # Get existing customers from Google Sheet to check for duplicates
                        gs_customers_current_data = customer_manager.gs_worksheet.get_all_records() # list of dicts
                        existing_customer_ids_in_gs = {str(row.get('customer_id','')).strip() for row in gs_customers_current_data if str(row.get('customer_id','')).strip()}
                        existing_emails_in_gs = {str(row.get('email','')).strip().lower() for row in gs_customers_current_data if str(row.get('email','')).strip()}
                        
                        customers_to_add_to_sheet = []
                        skipped_count = 0
                        updated_count = 0 # Not implementing update from CSV in this pass for simplicity

                        for index, csv_row in csv_df.iterrows():
                            # Map CSV headers (case-sensitive from CSV) to Customer object attributes
                            csv_cust_id = str(csv_row.get('customer_id', '')).strip()
                            csv_email = str(csv_row.get('Email', '')).strip().lower()

                            # Skip if customer_id from CSV already exists in Google Sheet
                            if csv_cust_id and csv_cust_id in existing_customer_ids_in_gs:
                                skipped_count += 1
                                continue
                            # Skip if email from CSV already exists in Google Sheet (and customer_id wasn't a match or was blank)
                            if csv_email and csv_email in existing_emails_in_gs:
                                skipped_count += 1
                                continue
                            
                            # If we are here, it's potentially a new customer
                            customer_obj = Customer(
                                customer_id=(csv_cust_id if csv_cust_id else None), # Let Customer class generate if blank
                                customer_name=csv_row.get('Customer Name', ''),
                                phone=csv_row.get('Phone', ''),
                                email=str(csv_row.get('Email', '')).strip(), # Keep original case for storage, but checked lowercase
                                address=csv_row.get('Address', ''),
                                boat_type=csv_row.get('Boat Type', ''),
                                boat_length=csv_row.get('Boat Length', 0.0),
                                boat_draft=csv_row.get('Boat Draft', 0.0),
                                home_latitude=csv_row.get('Home Latitude', 0.0),
                                home_longitude=csv_row.get('Home Longitude', 0.0),
                                is_ecm_boat=csv_row.get('Is ECM Boat', False)
                            )
                            
                            # One last check for the newly generated ID or email before queuing
                            if customer_obj.customer_id in existing_customer_ids_in_gs: # Handles if new UUID matches somehow (rare)
                                skipped_count +=1; continue
                            if customer_obj.email and customer_obj.email.lower() in existing_emails_in_gs:
                                skipped_count +=1; continue
                                
                            customers_to_add_to_sheet.append(customer_obj.to_sheet_row())
                            # Add to sets to avoid duplicates from within the CSV itself in this run
                            existing_customer_ids_in_gs.add(customer_obj.customer_id)
                            if customer_obj.email: existing_emails_in_gs.add(customer_obj.email.lower())


                        if customers_to_add_to_sheet:
                            customer_manager.gs_worksheet.append_rows(customers_to_add_to_sheet, value_input_option='USER_ENTERED')
                            st.success(f"Successfully added {len(customers_to_add_to_sheet)} new customers to Google Sheet. Skipped {skipped_count} rows (already exist or missing key data).")
                        else:
                            st.info(f"No new customers were added. Skipped {skipped_count} rows (likely duplicates or missing data).")

                    except FileNotFoundError:
                        st.error(f"'{CSV_CUSTOMER_IMPORT_FILE}' not found. Please ensure it's in your GitHub repo root.")
                    except Exception as e:
                        st.error(f"An error occurred during CSV import: {e}")
                        st.exception(e)

# --- Main Execution ---
if __name__ == "__main__":
    streamlit_main()
