import streamlit as st
import uuid
from datetime import datetime
import os
import pandas as pd

# --- Configuration ---
DATETIME_FORMAT = "%Y-%m-%d %H:%M"
CUSTOMER_CSV_FILE = "ECM Sample Cust.csv" # Your primary CSV data file
JOB_CSV_FILE = "boat_jobs.csv" # Define this for the Job manager
ECM_HOME_ADDRESS = "43 Mattakeeset Street, Pembroke, MA 02359"

# --- Customer Class ---
class Customer:
    def __init__(self, customer_name, boat_type, boat_length, phone, email, address,
                 boat_draft, home_latitude, home_longitude, is_ecm_boat,
                 preferred_truck="", customer_id=None):

        if pd.isna(customer_id) or str(customer_id).strip() == "":
            self.customer_id = str(uuid.uuid4())
        else:
            self.customer_id = str(customer_id)

        self.customer_name = str(customer_name if pd.notna(customer_name) else "")
        self.phone = str(phone if pd.notna(phone) else "")
        self.email = str(email if pd.notna(email) else "")
        self.address = str(address if pd.notna(address) else "")
        self.boat_type = str(boat_type if pd.notna(boat_type) else "")
        self.preferred_truck = str(preferred_truck if pd.notna(preferred_truck) else "")

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

    def to_dict(self): # To convert CUSTOMER object to a dictionary for DataFrame
        return {
            'customer_id': self.customer_id,
            'Customer Name': self.customer_name, # Matches CSV header
            'Boat Type': self.boat_type,         # Matches CSV header
            'PREFERRED TRUCK': self.preferred_truck, # Matches CSV header
            'Boat Length': self.boat_length,     # Matches CSV header
            'Phone': self.phone,                 # Matches CSV header
            'Email': self.email,                 # Matches CSV header
            'Address': self.address,             # Matches CSV header
            'Boat Draft': self.boat_draft,       # Matches CSV header
            'Home Latitude': self.home_latitude, # Matches CSV header
            'Home Longitude': self.home_longitude, # Matches CSV header
            'Is ECM Boat': self.is_ecm_boat      # Matches CSV header
        }

    @staticmethod
    def from_dict(data_dict): # Create CUSTOMER object from a dictionary (e.g., a DataFrame row)
        return Customer(
            customer_id=data_dict.get('customer_id'),
            customer_name=data_dict.get('Customer Name'),
            boat_type=data_dict.get('Boat Type'),
            preferred_truck=data_dict.get('PREFERRED TRUCK'),
            boat_length=data_dict.get('Boat Length'),
            phone=data_dict.get('Phone'),
            email=data_dict.get('Email'),
            address=data_dict.get('Address'),
            boat_draft=data_dict.get('Boat Draft'),
            home_latitude=data_dict.get('Home Latitude'),
            home_longitude=data_dict.get('Home Longitude'),
            is_ecm_boat=data_dict.get('Is ECM Boat')
        )

# --- Job Class ---
class Job:
    def __init__(self, customer_id, service_type, requested_date,
                 origin_is_ecm_storage=False, origin_address="",
                 destination_is_ecm_storage=False, destination_address="",
                 boat_details_snapshot="", job_status="Requested", notes="",
                 preferred_truck_snapshot="", scheduled_date_time="", job_id=None):

        self.job_id = str(job_id) if job_id and str(job_id).strip() else str(uuid.uuid4())
        self.customer_id = str(customer_id)
        self.service_type = str(service_type) # "Launch", "Haul-out", "Transport"
        self.requested_date = str(requested_date) # Store as string e.g., "YYYY-MM-DD"
        self.scheduled_date_time = str(scheduled_date_time) # e.g., "YYYY-MM-DD HH:MM" or blank
        self.origin_is_ecm_storage = bool(origin_is_ecm_storage)
        self.origin_address = str(origin_address)
        self.destination_is_ecm_storage = bool(destination_is_ecm_storage)
        self.destination_address = str(destination_address)
        self.boat_details_snapshot = str(boat_details_snapshot)
        self.job_status = str(job_status)
        self.notes = str(notes)
        self.preferred_truck_snapshot = str(preferred_truck_snapshot)

    def to_dict(self): # To convert JOB object to a dictionary for DataFrame
        return {
            'job_id': self.job_id,
            'customer_id': self.customer_id,
            'service_type': self.service_type,
            'requested_date': self.requested_date,
            'scheduled_date_time': self.scheduled_date_time,
            'origin_is_ecm_storage': self.origin_is_ecm_storage,
            'origin_address': self.origin_address,
            'destination_is_ecm_storage': self.destination_is_ecm_storage,
            'destination_address': self.destination_address,
            'boat_details_snapshot': self.boat_details_snapshot,
            'job_status': self.job_status,
            'notes': self.notes,
            'preferred_truck_snapshot': self.preferred_truck_snapshot
        }

    @staticmethod
    def from_dict(data_dict): # Create JOB object from a dictionary (e.g., a DataFrame row)
        return Job( # Corrected to return Job object
            job_id=data_dict.get('job_id'),
            customer_id=data_dict.get('customer_id'),
            service_type=data_dict.get('service_type'),
            requested_date=data_dict.get('requested_date'),
            scheduled_date_time=data_dict.get('scheduled_date_time', ""),
            origin_is_ecm_storage=data_dict.get('origin_is_ecm_storage', False),
            origin_address=data_dict.get('origin_address', ""),
            destination_is_ecm_storage=data_dict.get('destination_is_ecm_storage', False),
            destination_address=data_dict.get('destination_address', ""),
            boat_details_snapshot=data_dict.get('boat_details_snapshot', ""),
            job_status=data_dict.get('job_status', "Requested"),
            notes=data_dict.get('notes', ""),
            preferred_truck_snapshot=data_dict.get('preferred_truck_snapshot', "")
        )
    
# --- CSV Data Manager for Customers ---
class CustomerCsvManager:
    EXPECTED_HEADERS = [
        'customer_id', 'Customer Name', 'Boat Type', 'PREFERRED TRUCK', 'Boat Length', 'Phone',
        'Email', 'Address', 'Boat Draft', 'Home Latitude', 'Home Longitude', 'Is ECM Boat'
    ]

    def __init__(self, csv_filepath):
        self.filepath = csv_filepath
        self.df = self._load_csv()

    def _load_csv(self):
        try:
            if os.path.exists(self.filepath):
                dtype_map = {'customer_id': str, 'Phone': str} # Ensure Phone is read as string
                df = pd.read_csv(self.filepath, dtype=dtype_map)
                for col in self.EXPECTED_HEADERS:
                    if col not in df.columns:
                        if col == 'Is ECM Boat': df[col] = False
                        elif col in ['Boat Length', 'Boat Draft', 'Home Latitude', 'Home Longitude']: df[col] = 0.0
                        else: df[col] = ""
                df = df[self.EXPECTED_HEADERS] # Ensure correct order and select only expected columns
                
                if not df.empty:
                    for col_numeric in ['Boat Length', 'Boat Draft', 'Home Latitude', 'Home Longitude']:
                        if col_numeric in df.columns:
                            df[col_numeric] = pd.to_numeric(df[col_numeric], errors='coerce').fillna(0.0)
                    if 'Is ECM Boat' in df.columns:
                        df['Is ECM Boat'] = df['Is ECM Boat'].apply(
                            lambda x: str(x).strip().upper() == 'TRUE' if pd.notna(x) else False
                        ).astype(bool)
            else: 
                st.warning(f"Customer CSV file '{self.filepath}' not found. Creating a new one.")
                df = pd.DataFrame(columns=self.EXPECTED_HEADERS)
                for col in self.EXPECTED_HEADERS: # Initialize dtypes for empty DataFrame
                    if col == 'Is ECM Boat': df[col] = df[col].astype(bool)
                    elif col in ['Boat Length', 'Boat Draft', 'Home Latitude', 'Home Longitude']: df[col] = df[col].astype(float)
                    else: df[col] = df[col].astype(str)
                df.to_csv(self.filepath, index=False)
            return df
        except pd.errors.EmptyDataError:
            st.warning(f"Customer CSV file '{self.filepath}' is empty. Initializing with headers.")
            df = pd.DataFrame(columns=self.EXPECTED_HEADERS)
            for col in self.EXPECTED_HEADERS: # Initialize dtypes for empty DataFrame
                if col == 'Is ECM Boat': df[col] = df[col].astype(bool)
                elif col in ['Boat Length', 'Boat Draft', 'Home Latitude', 'Home Longitude']: df[col] = df[col].astype(float)
                else: df[col] = df[col].astype(str)
            df.to_csv(self.filepath, index=False)
            return df
        except Exception as e:
            st.error(f"Error loading/creating Customer CSV '{self.filepath}': {e}")
            df = pd.DataFrame(columns=self.EXPECTED_HEADERS) # Fallback
            for col in self.EXPECTED_HEADERS: # Initialize dtypes for fallback
                if col == 'Is ECM Boat': df[col] = df[col].astype(bool)
                elif col in ['Boat Length', 'Boat Draft', 'Home Latitude', 'Home Longitude']: df[col] = df[col].astype(float)
                else: df[col] = df[col].astype(str)
            return df


    def _save_csv(self):
        try:
            if 'Is ECM Boat' in self.df.columns: # Ensure boolean consistency before saving
                 self.df['Is ECM Boat'] = self.df['Is ECM Boat'].fillna(False).astype(bool)
            self.df.to_csv(self.filepath, index=False)
        except Exception as e:
            st.error(f"Error saving data to Customer CSV '{self.filepath}': {e}")

    def get_all_customers(self):
        self.df = self._load_csv() 
        customers = []
        for _, row in self.df.iterrows():
            customers.append(Customer.from_dict(row.to_dict())) # This should now work
        return customers

    def add_customer_obj(self, customer_obj: Customer):
        self.df['customer_id'] = self.df['customer_id'].astype(str) # Ensure consistent type for comparison
        if customer_obj.email and customer_obj.email.strip() != "":
            if 'Email' in self.df.columns and not self.df[self.df['Email'].astype(str).str.lower() == customer_obj.email.lower()].empty:
                st.warning(f"Customer with email '{customer_obj.email}' already exists. Not adding.")
                return False
        
        new_customer_dict = customer_obj.to_dict()
        new_df_row = pd.DataFrame([new_customer_dict])
        
        for col in new_df_row.columns: # Ensure dtypes match before concat
            if col in self.df.columns:
                try:
                    new_df_row[col] = new_df_row[col].astype(self.df[col].dtype)
                except Exception: # Fallback if astype fails for some complex case
                    pass 
        
        self.df = pd.concat([self.df, new_df_row], ignore_index=True)
        self._save_csv()
        st.success(f"Customer '{customer_obj.customer_name}' added to CSV.")
        return True

    def update_customer_by_id(self, customer_id_to_update, updated_customer_obj: Customer):
        self.df['customer_id'] = self.df['customer_id'].astype(str)
        customer_id_to_update = str(customer_id_to_update)

        if self.df['customer_id'].eq(customer_id_to_update).any():
            idx_series = self.df[self.df['customer_id'] == customer_id_to_update].index
            if not idx_series.empty:
                idx = idx_series[0] 
                customer_dict = updated_customer_obj.to_dict()
                for key, value in customer_dict.items():
                    if key in self.df.columns:
                        self.df.loc[idx, key] = value
                    else:
                        st.warning(f"Column '{key}' not in CSV, cannot update for CustID {customer_id_to_update}.")
                self._save_csv()
                st.success(f"Customer '{updated_customer_obj.customer_name}' (ID: {customer_id_to_update}) updated.")
                return True
        else:
            st.error(f"Customer with ID '{customer_id_to_update}' not found for update.")
            return False
            
    def get_customer_by_id(self, customer_id): # Added this method
        self.df = self._load_csv()
        self.df['customer_id'] = self.df['customer_id'].astype(str)
        customer_id = str(customer_id)
        customer_row = self.df[self.df['customer_id'] == customer_id]
        if not customer_row.empty:
            return Customer.from_dict(customer_row.iloc[0].to_dict())
        return None

# --- JobCsvManager Class ---
class JobCsvManager:
    EXPECTED_HEADERS = [
        'job_id', 'customer_id', 'service_type', 'requested_date', 'scheduled_date_time',
        'origin_is_ecm_storage', 'origin_address', 'destination_is_ecm_storage', 'destination_address',
        'boat_details_snapshot', 'job_status', 'notes', 'preferred_truck_snapshot'
    ]

    def __init__(self, csv_filepath):
        self.filepath = csv_filepath
        self.df = self._load_csv()

    def _load_csv(self):
        try:
            if os.path.exists(self.filepath):
                df = pd.read_csv(self.filepath, dtype={'job_id': str, 'customer_id': str})
                for col in self.EXPECTED_HEADERS:
                    if col not in df.columns:
                        if col.endswith('_is_ecm_storage'): df[col] = False
                        else: df[col] = ""
                df = df[self.EXPECTED_HEADERS]
                if not df.empty: # Type conversions for loaded data
                    for col_bool in ['origin_is_ecm_storage', 'destination_is_ecm_storage']:
                        if col_bool in df.columns:
                             df[col_bool] = df[col_bool].apply(
                                lambda x: str(x).strip().upper() == 'TRUE' if pd.notna(x) else False
                            ).astype(bool)
            else:
                st.warning(f"Jobs CSV file '{self.filepath}' not found. Creating a new one.")
                df = pd.DataFrame(columns=self.EXPECTED_HEADERS)
                for col in self.EXPECTED_HEADERS: # Initialize dtypes for empty DataFrame
                    if col.endswith('_is_ecm_storage'): df[col] = df[col].astype(bool)
                    else: df[col] = df[col].astype(str)
                df.to_csv(self.filepath, index=False)
            return df
        except pd.errors.EmptyDataError:
            st.warning(f"Jobs CSV file '{self.filepath}' is empty. Initializing with headers.")
            df = pd.DataFrame(columns=self.EXPECTED_HEADERS)
            for col in self.EXPECTED_HEADERS: # Initialize dtypes for empty DataFrame
                if col.endswith('_is_ecm_storage'): df[col] = df[col].astype(bool)
                else: df[col] = df[col].astype(str)
            df.to_csv(self.filepath, index=False)
            return df
        except Exception as e:
            st.error(f"Error loading or creating Jobs CSV '{self.filepath}': {e}")
            df = pd.DataFrame(columns=self.EXPECTED_HEADERS) # Fallback
            for col in self.EXPECTED_HEADERS: # Initialize dtypes for fallback
                if col.endswith('_is_ecm_storage'): df[col] = df[col].astype(bool)
                else: df[col] = df[col].astype(str)
            return df

    def _save_csv(self):
        try:
            # Ensure boolean consistency before saving
            for col_bool in ['origin_is_ecm_storage', 'destination_is_ecm_storage']:
                if col_bool in self.df.columns:
                    self.df[col_bool] = self.df[col_bool].fillna(False).astype(bool)
            self.df.to_csv(self.filepath, index=False)
        except Exception as e:
            st.error(f"Error saving data to Jobs CSV '{self.filepath}': {e}")

    def get_all_jobs(self):
        self.df = self._load_csv()
        jobs = []
        for _, row in self.df.iterrows():
            jobs.append(Job.from_dict(row.to_dict()))
        return jobs

    def add_job_obj(self, job_obj: Job):
        new_job_dict = job_obj.to_dict()
        new_df_row = pd.DataFrame([new_job_dict])
        for col in new_df_row.columns: # Ensure dtypes match before concat
            if col in self.df.columns:
                try:
                    new_df_row[col] = new_df_row[col].astype(self.df[col].dtype)
                except Exception:
                    pass
        self.df = pd.concat([self.df, new_df_row], ignore_index=True)
        self._save_csv()
        st.success(f"New job for service '{job_obj.service_type}' requested. Job ID: {job_obj.job_id}")
        return True

    def update_job_by_id(self, job_id_to_update, updated_job_obj: Job):
        self.df['job_id'] = self.df['job_id'].astype(str)
        job_id_to_update = str(job_id_to_update)

        if self.df['job_id'].eq(job_id_to_update).any():
            idx_series = self.df[self.df['job_id'] == job_id_to_update].index
            if not idx_series.empty:
                idx = idx_series[0]
                job_dict = updated_job_obj.to_dict()
                for key, value in job_dict.items():
                    if key in self.df.columns:
                        self.df.loc[idx, key] = value
                self._save_csv()
                st.success(f"Job ID '{job_id_to_update}' updated.")
                return True
        st.error(f"Job with ID '{job_id_to_update}' not found for update.")
        return False

    def get_job_by_id(self, job_id):
        self.df = self._load_csv()
        self.df['job_id'] = self.df['job_id'].astype(str)
        job_id = str(job_id)
        job_row = self.df[self.df['job_id'] == job_id]
        if not job_row.empty:
            return Job.from_dict(job_row.iloc[0].to_dict())
        return None

# --- Streamlit UI Application ---
def streamlit_main():
    st.set_page_config(layout="wide", page_title="Boat Hauling Automator")
    st.title("🚤 ECM Boat Hauling Business Automator (CSV Mode)")
    st.caption(f"Customer Data: {CUSTOMER_CSV_FILE} | Job Data: {JOB_CSV_FILE}")
    st.write(f"Current Time: {datetime.now().strftime(DATETIME_FORMAT)} (Pembroke, MA)")

    # Initialize Managers
    if 'customer_manager' not in st.session_state:
        st.session_state.customer_manager = CustomerCsvManager(CUSTOMER_CSV_FILE)
    customer_manager = st.session_state.customer_manager

    if 'job_manager' not in st.session_state:
        st.session_state.job_manager = JobCsvManager(JOB_CSV_FILE)
    job_manager = st.session_state.job_manager
    
    menu_options = [
        "Home", "Add New Customer", "List/View All Customers",
        "Schedule New Service", "View All Jobs"
    ]
    menu_choice = st.sidebar.selectbox("Navigation", menu_options)

    if menu_choice == "Home":
        st.header("Welcome!")
        st.write("Select an option from the sidebar.")
        all_customers = customer_manager.get_all_customers()
        st.metric("Total Customers", len(all_customers))
        all_jobs = job_manager.get_all_jobs()
        st.metric("Total Jobs", len(all_jobs))
        st.sidebar.info(f"""
        **Note on Data Saving (CSV Mode):**
        - When running locally, changes are saved directly to the CSV files.
        - When deployed on Streamlit Cloud, saved changes update the CSVs in the app's temporary session.
        These changes on Streamlit Cloud may be lost if the app restarts.
        To make changes permanent on Streamlit Cloud, you would typically download the updated CSVs
        and manually update them in your GitHub repository.
        """)

    elif menu_choice == "Add New Customer":
        st.header("➕ Add New Customer")
        with st.form("add_customer_csv_form", clear_on_submit=True):
            st.subheader("Customer Info")
            customer_name = st.text_input("Customer Name*", help="e.g., John Doe")
            phone = st.text_input("Phone", help="e.g., 555-123-4567")
            email = st.text_input("Email", help="e.g., john.doe@example.com")
            address = st.text_area("Address", help="e.g., 123 Main St, Anytown, USA")

            st.subheader("Boat Info")
            boat_type_options = ["Powerboat", "Sailboat DT", "Sailboat MT"]
            boat_type = st.selectbox("Boat Type*", options=boat_type_options, index=0, help="Select the type of boat")
            
            preferred_truck_options = ["S20", "S21", "S23", ""]
            preferred_truck = st.selectbox("Preferred Truck", options=preferred_truck_options, index=len(preferred_truck_options)-1, help="Select the preferred truck or leave blank")
            
            boat_length = st.number_input("Boat Length (ft)*", min_value=1.0, value=25.0, format="%.1f")
            boat_draft = st.number_input("Boat Draft (ft)", min_value=0.0, value=3.0, format="%.1f")
            
            st.subheader("Location & Other")
            home_latitude = st.number_input("Home Latitude", format="%.6f", value=42.078000)
            home_longitude = st.number_input("Home Longitude", format="%.6f", value=-70.710000)
            is_ecm_boat = st.checkbox("Is ECM Boat?", value=False)
            
            submitted = st.form_submit_button("Add Customer")

            if submitted:
                if not all([customer_name, boat_type]): 
                    st.error("Please fill in required fields: Customer Name, Boat Type.")
                else:
                    new_customer = Customer(
                        customer_name=customer_name, phone=phone, email=email, address=address,
                        boat_type=boat_type, preferred_truck=preferred_truck,
                        boat_length=boat_length, boat_draft=boat_draft,
                        home_latitude=home_latitude, home_longitude=home_longitude,
                        is_ecm_boat=is_ecm_boat
                    )
                    customer_manager.add_customer_obj(new_customer)

    elif menu_choice == "List/View All Customers":
        st.header("👥 List of Customers")
        all_customers = customer_manager.get_all_customers()
        if not all_customers:
            st.info(f"No customers found in '{CUSTOMER_CSV_FILE}'.")
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
                    cols[1].markdown(f"**Preferred Truck:** {customer.preferred_truck if customer.preferred_truck else 'N/A'}")
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
                        
                        boat_type_options = ["Powerboat", "Sailboat DT", "Sailboat MT"]
                        try: current_boat_type_index = boat_type_options.index(customer.boat_type)
                        except ValueError: current_boat_type_index = 0
                        edit_boat_type = st.selectbox("Boat Type", options=boat_type_options, index=current_boat_type_index, key=f"btype_{customer.customer_id}")

                        preferred_truck_options = ["S20", "S21", "S23", ""]
                        try: current_truck_index = preferred_truck_options.index(customer.preferred_truck)
                        except ValueError: current_truck_index = len(preferred_truck_options) - 1
                        edit_preferred_truck = st.selectbox("Preferred Truck", options=preferred_truck_options, index=current_truck_index, key=f"ptruck_{customer.customer_id}")
                        
                        edit_boat_length = st.number_input("Length", value=float(customer.boat_length), min_value=0.0, format="%.1f", key=f"blen_{customer.customer_id}")
                        edit_boat_draft = st.number_input("Draft", value=float(customer.boat_draft), min_value=0.0, format="%.1f", key=f"bdr_{customer.customer_id}")
                        edit_lat = st.number_input("Latitude", value=float(customer.home_latitude), format="%.6f", key=f"lat_{customer.customer_id}")
                        edit_lon = st.number_input("Longitude", value=float(customer.home_longitude), format="%.6f", key=f"lon_{customer.customer_id}")
                        edit_ecm = st.checkbox("ECM Boat", value=bool(customer.is_ecm_boat), key=f"ecm_{customer.customer_id}")
                        
                        update_submitted = st.form_submit_button("Save Changes")
                        if update_submitted:
                            updated_cust_obj = Customer(
                                customer_name=edit_name, phone=edit_phone, email=edit_email, address=edit_address,
                                boat_type=edit_boat_type, preferred_truck=edit_preferred_truck,
                                boat_length=edit_boat_length, boat_draft=edit_boat_draft,
                                home_latitude=edit_lat, home_longitude=edit_lon, is_ecm_boat=edit_ecm,
                                customer_id=customer.customer_id
                            )
                            if customer_manager.update_customer_by_id(customer.customer_id, updated_cust_obj):
                                st.experimental_rerun()

    elif menu_choice == "Schedule New Service":
        st.header("🗓️ Schedule New Service")
        all_customers = customer_manager.get_all_customers()
        if not all_customers:
            st.warning("No customers found. Please add a customer first.")
            return

        customer_options_dict = {f"{c.customer_name} ({c.boat_type} - {c.boat_length}ft)": c for c in sorted(all_customers, key=lambda c: c.customer_name)}
        selected_customer_display_name = st.selectbox("Select Customer*", list(customer_options_dict.keys()))
        selected_customer_obj = customer_options_dict.get(selected_customer_display_name)

        if selected_customer_obj:
            st.write(f"**Selected:** {selected_customer_obj.customer_name} | **Boat:** {selected_customer_obj.boat_length}ft {selected_customer_obj.boat_type} | **Truck:** {selected_customer_obj.preferred_truck}")

            with st.form("schedule_job_form", clear_on_submit=True):
                service_type_options = ["Launch", "Haul-out", "Transport"]
                service_type = st.selectbox("Service Type*", options=service_type_options, index=0)
                requested_date = st.date_input("Requested Service Date*", value=datetime.now().date())

                # Determine default origin/destination before creating the input fields
                default_origin = selected_customer_obj.address if selected_customer_obj.address else ""
                default_destination = selected_customer_obj.address if selected_customer_obj.address else ""

                if service_type == "Launch":
                    # Default origin might be customer address, destination likely a ramp (manual for now)
                    default_destination = "" 
                elif service_type == "Haul-out":
                    # Default origin likely a ramp (manual for now), destination might be customer address
                    default_origin = ""
                # For "Transport", both might be different, defaults remain customer address or blank

                st.subheader("Origin Details")
                origin_is_ecm = st.checkbox("Origin is ECM Storage (Pembroke)", key="origin_ecm")
                if origin_is_ecm:
                    origin_address_val_final = ECM_HOME_ADDRESS
                    st.text_input("Origin Address", value=origin_address_val_final, disabled=True, key="origin_addr_disabled")
                else:
                    origin_address_val_final = st.text_input("Origin Address*", value=default_origin, help="Customer home, ramp name, or other address", key="origin_addr_manual")
                
                st.subheader("Destination Details")
                dest_is_ecm = st.checkbox("Destination is ECM Storage (Pembroke)", key="dest_ecm")
                if dest_is_ecm:
                    destination_address_val_final = ECM_HOME_ADDRESS
                    st.text_input("Destination Address", value=destination_address_val_final, disabled=True, key="dest_addr_disabled")
                else:
                    destination_address_val_final = st.text_input("Destination Address*", value=default_destination, help="Customer home, ramp name, or other address", key="dest_addr_manual")

                boat_details_snapshot_val = f"{selected_customer_obj.customer_name} - {selected_customer_obj.boat_length}ft {selected_customer_obj.boat_type}, Truck: {selected_customer_obj.preferred_truck}"
                st.text_area("Boat Details Snapshot (auto-generated)", value=boat_details_snapshot_val, disabled=True, height=100)
                
                preferred_truck_snapshot_val = selected_customer_obj.preferred_truck
                notes = st.text_area("Job Notes/Special Instructions")
                job_status_val = "Requested"

                submit_job_button = st.form_submit_button("Request Service")

                if submit_job_button:
                    # Re-fetch values from inputs if they are not disabled, as st.form only submits current values
                    current_origin_address = ECM_HOME_ADDRESS if origin_is_ecm else origin_address_val_final
                    current_destination_address = ECM_HOME_ADDRESS if dest_is_ecm else destination_address_val_final

                    if not current_origin_address or not current_destination_address:
                        st.error("Origin and Destination addresses are required.")
                    else:
                        new_job = Job(
                            customer_id=selected_customer_obj.customer_id,
                            service_type=service_type,
                            requested_date=requested_date.strftime("%Y-%m-%d"),
                            origin_is_ecm_storage=origin_is_ecm,
                            origin_address=current_origin_address,
                            destination_is_ecm_storage=dest_is_ecm,
                            destination_address=current_destination_address,
                            boat_details_snapshot=boat_details_snapshot_val,
                            job_status=job_status_val,
                            notes=notes,
                            preferred_truck_snapshot=preferred_truck_snapshot_val
                        )
                        if job_manager.add_job_obj(new_job):
                            st.success(f"Service request for {selected_customer_obj.customer_name} submitted!")
                        else:
                            st.error("Failed to submit service request.")
        else:
            st.info("Please select a customer to schedule a service.")

    elif menu_choice == "View All Jobs":
        st.header(" JobList")
        all_jobs = job_manager.get_all_jobs()
        if not all_jobs:
            st.info(f"No jobs found in '{JOB_CSV_FILE}'.")
        else:
            st.write(f"Found {len(all_jobs)} job(s).")
            
            jobs_data_for_df = [{'job_id': j.job_id, **j.to_dict()} for j in all_jobs] # ensure job_id is part of dict if not already
            jobs_df = pd.DataFrame(jobs_data_for_df)

            display_cols = [
                'customer_id', 'customer_name', 'scheduled_date_time', 'service_type', 'requested_date', 'job_status',
                'origin_is_ecm_storage', 'origin_address', 'destination_is_ecm_storage', 'destination_address', 
                'boat_details_snapshot', 'preferred_truck_snapshot', 'notes'
            ]
            display_cols_present = [col for col in display_cols if col in jobs_df.columns]

            if not jobs_df.empty:
                st.dataframe(jobs_df[display_cols_present])
            else:
                st.info("No job data to display in table format yet.")

            for job in sorted(all_jobs, key=lambda j: (j.requested_date is None, j.requested_date, j.job_id), reverse=True):
                customer = customer_manager.get_customer_by_id(job.customer_id)
                customer_name_display = customer.customer_name if customer else "N/A (Cust. ID not found)"
                
                with st.expander(f"Job ID: ...{job.job_id[-6:]} - {job.service_type} for {customer_name_display} (Req: {job.requested_date}) - Status: {job.job_status}"):
                    st.markdown(f"**Customer:** {customer_name_display} (ID: `{job.customer_id}`)")
                    st.markdown(f"**Service Type:** {job.service_type}")
                    st.markdown(f"**Requested Date:** {job.requested_date}")
                    st.markdown(f"**Scheduled Date/Time:** {job.scheduled_date_time if job.scheduled_date_time else 'Not Scheduled'}")
                    st.markdown(f"**Origin:** {'ECM Storage' if job.origin_is_ecm_storage else job.origin_address}")
                    st.markdown(f"**Destination:** {'ECM Storage' if job.destination_is_ecm_storage else job.destination_address}")
                    st.markdown(f"**Boat Snapshot:** {job.boat_details_snapshot}")
                    st.markdown(f"**Truck Snapshot:** {job.preferred_truck_snapshot}")
                    st.markdown(f"**Notes:** {job.notes if job.notes else 'N/A'}")
                    st.markdown(f"**Status:** {job.job_status}")

# --- Main Execution ---
if __name__ == "__main__":
    streamlit_main()
