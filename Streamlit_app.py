import streamlit as st
import uuid
from datetime import datetime
import os
import pandas as pd

# --- Configuration ---
DATETIME_FORMAT = "%Y-%m-%d %H:%M"
CUSTOMER_CSV_FILE = "ECM Sample Cust.csv" # Your primary CSV data file
ECM_HOME_ADDRESS = "43 Mattakeeset Street, Pembroke, MA 02359"
JOB_CSV_FILE = "boat_jobs.csv"

# --- Customer Class ---
It looks like you've pasted the Job class definition again, but the to_dict and from_dict methods inside it are still structured to handle Customer attributes and create Customer objects, not Job objects.

This is the core issue causing the AttributeError when you call Customer.from_dict(...) because the Customer class itself is missing these methods.

You need to:

Define to_dict(self) and @staticmethod from_dict(data_dict) inside your Customer class to handle Customer attributes.
Ensure the to_dict(self) and @staticmethod from_dict(data_dict) methods inside your Job class handle Job attributes and create Job objects.
Let's correct this.

Here's what your Job class's to_dict and from_dict methods should look like (they were correct in a previous version I provided):

Python

class Job:
    # ... (your __init__ method for Job is here and looks correct) ...
    def __init__(self, customer_id, service_type, requested_date,
                 origin_is_ecm_storage=False, origin_address="",
                 destination_is_ecm_storage=False, destination_address="",
                 boat_details_snapshot="", job_status="Requested", notes="",
                 preferred_truck_snapshot="", scheduled_date_time="", job_id=None):

        self.job_id = str(job_id) if job_id and str(job_id).strip() else str(uuid.uuid4())
        self.customer_id = str(customer_id)
        self.service_type = str(service_type)
        self.requested_date = str(requested_date)
        self.scheduled_date_time = str(scheduled_date_time)
        self.origin_is_ecm_storage = bool(origin_is_ecm_storage)
        self.origin_address = str(origin_address)
        self.destination_is_ecm_storage = bool(destination_is_ecm_storage)
        self.destination_address = str(destination_address)
        self.boat_details_snapshot = str(boat_details_snapshot)
        self.job_status = str(job_status)
        self.notes = str(notes)
        self.preferred_truck_snapshot = str(preferred_truck_snapshot)

    def to_dict(self): # To convert JOB object to a dictionary
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
    def from_dict(data_dict): # Create JOB object from a dictionary
        return Job( # <--- Should return a Job object
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
And, crucially, here is what your Customer class needs for its own to_dict and from_dict methods. These must be part of the Customer class, not the Job class.

Python

# Ensure pandas and uuid are imported at the top of your file
# import pandas as pd
# import uuid

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

    # --- Methods for the Customer Class ---
    def to_dict(self): # To convert CUSTOMER object to a dictionary for DataFrame
        return {
            'customer_id': self.customer_id,
            'Customer Name': self.customer_name,
            'Boat Type': self.boat_type,
            'PREFERRED TRUCK': self.preferred_truck,
            'Boat Length': self.boat_length,
            'Phone': self.phone,
            'Email': self.email,
            'Address': self.address,
            'Boat Draft': self.boat_draft,
            'Home Latitude': self.home_latitude,
            'Home Longitude': self.home_longitude,
            'Is ECM Boat': self.is_ecm_boat
        }

    @staticmethod  # <--- THIS DECORATOR IS CRUCIAL for Customer.from_dict
    def from_dict(data_dict): # Create CUSTOMER object from a dictionary
        return Customer( # <--- Should return a Customer object
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
                df = pd.read_csv(self.filepath, dtype={'customer_id': str}) # Read customer_id as string
                # Ensure all expected columns exist, add if missing (fill with empty)
                for col in self.EXPECTED_HEADERS:
                    if col not in df.columns:
                        df[col] = "" # Or pd.NA or appropriate default
                # Reorder columns to expected order for consistency, dropping unexpected ones if any
                df = df[self.EXPECTED_HEADERS]
            else: # File doesn't exist, create an empty DataFrame with headers
                st.warning(f"CSV file '{self.filepath}' not found. Creating a new one with expected headers.")
                df = pd.DataFrame(columns=self.EXPECTED_HEADERS)
                # Save it immediately so it exists for future operations in the session
                df.to_csv(self.filepath, index=False)
            return df
        except pd.errors.EmptyDataError:
            st.warning(f"CSV file '{self.filepath}' is empty. Initializing with headers.")
            df = pd.DataFrame(columns=self.EXPECTED_HEADERS)
            df.to_csv(self.filepath, index=False)
            return df
        except Exception as e:
            st.error(f"Error loading or creating CSV file '{self.filepath}': {e}")
            # Return an empty DataFrame with headers as a fallback
            return pd.DataFrame(columns=self.EXPECTED_HEADERS)

    def _save_csv(self):
        try:
            self.df.to_csv(self.filepath, index=False)
            # st.sidebar.success("Data saved to CSV.") # Optional: feedback on every save
        except Exception as e:
            st.error(f"Error saving data to CSV '{self.filepath}': {e}")

    def get_all_customers(self):
        # Reload from CSV each time to ensure data is current for this simple model
        # Or, for more complex apps, you might manage the self.df state more carefully.
        self.df = self._load_csv()
        customers = []
        for _, row in self.df.iterrows():
            customers.append(Customer.from_dict(row.to_dict()))
        return customers

    def add_customer_obj(self, customer_obj: Customer):
        # Check for duplicates by email or name if desired
        if customer_obj.email and customer_obj.email.strip() != "":
            if not self.df[self.df['Email'].astype(str).str.lower() == customer_obj.email.lower()].empty:
                st.warning(f"Customer with email '{customer_obj.email}' already exists. Not adding.")
                return False
        # Add more sophisticated duplicate checks if needed (e.g., by name AND boat type)

        new_customer_dict = customer_obj.to_dict()
        
        # Append new customer as a new row to the DataFrame
        # self.df = self.df.append(new_customer_dict, ignore_index=True) # df.append is deprecated
        new_df_row = pd.DataFrame([new_customer_dict])
        self.df = pd.concat([self.df, new_df_row], ignore_index=True)
        
        self._save_csv()
        st.success(f"Customer '{customer_obj.customer_name}' added to CSV.")
        return True

    def update_customer_by_id(self, customer_id_to_update, updated_customer_obj: Customer):
        if self.df['customer_id'].astype(str).eq(str(customer_id_to_update)).any():
            idx = self.df[self.df['customer_id'].astype(str) == str(customer_id_to_update)].index
            if not idx.empty:
                customer_dict = updated_customer_obj.to_dict()
                for key, value in customer_dict.items():
                    self.df.loc[idx, key] = value
                self._save_csv()
                st.success(f"Customer '{updated_customer_obj.customer_name}' (ID: {customer_id_to_update}) updated in CSV.")
                return True
            else: # Should not happen if first condition is true
                st.error(f"Customer with ID '{customer_id_to_update}' found but index not retrieved (unexpected).")
                return False
        else:
            st.error(f"Customer with ID '{customer_id_to_update}' not found for update.")
            return False

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
                df = pd.read_csv(self.filepath, dtype={'job_id': str, 'customer_id': str}) # Or your relevant dtype map
                # Ensure all expected columns exist
                for col in self.EXPECTED_HEADERS:
                    if col not in df.columns:
                        if col.endswith('_is_ecm_storage') or col == 'Is ECM Boat': # Boolean columns
                            df[col] = False
                        elif col in ['Boat Length', 'Boat Draft', 'Home Latitude', 'Home Longitude']: # Numeric columns
                            df[col] = 0.0 # Or pd.NA
                        else: # String columns
                            df[col] = ""
                df = df[self.EXPECTED_HEADERS] # Ensure correct order and drop unexpected
                if not df.empty: # Only apply if df has rows, otherwise dtypes are set for empty df
                    for col in ['Boat Length', 'Boat Draft', 'Home Latitude', 'Home Longitude']:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
                    if 'Is ECM Boat' in df.columns:
                        df['Is ECM Boat'] = df['Is ECM Boat'].apply(
                            lambda x: str(x).strip().upper() == 'TRUE' if pd.notna(x) else False
                        ).astype(bool)
                return df
    
            else:
                st.warning(f"Jobs CSV file '{self.filepath}' not found. Creating a new one.")
                df = pd.DataFrame(columns=self.EXPECTED_HEADERS)
                df.to_csv(self.filepath, index=False)
            return df
        except pd.errors.EmptyDataError:
            st.warning(f"Jobs CSV file '{self.filepath}' is empty. Initializing with headers.")
            df = pd.DataFrame(columns=self.EXPECTED_HEADERS)
            df.to_csv(self.filepath, index=False)
            return df
        except Exception as e:
            st.error(f"Error loading or creating Jobs CSV '{self.filepath}': {e}")
            return pd.DataFrame(columns=self.EXPECTED_HEADERS)

    def _save_csv(self):
        try:
            self.df.to_csv(self.filepath, index=False)
        except Exception as e:
            st.error(f"Error saving data to Jobs CSV '{self.filepath}': {e}")

    def get_all_jobs(self):
        self.df = self._load_csv() # Reload data
        jobs = []
        for _, row in self.df.iterrows():
            jobs.append(Job.from_dict(row.to_dict()))
        return jobs

    def add_job_obj(self, job_obj: Job):
        new_job_dict = job_obj.to_dict()
        new_df_row = pd.DataFrame([new_job_dict])
        self.df = pd.concat([self.df, new_df_row], ignore_index=True)
        self._save_csv()
        st.success(f"New job for service '{job_obj.service_type}' requested. Job ID: {job_obj.job_id}")
        return True

    def update_job_by_id(self, job_id_to_update, updated_job_obj: Job):
        # Ensure customer_id and job_id are treated as strings for matching
        self.df['job_id'] = self.df['job_id'].astype(str)
        job_id_to_update = str(job_id_to_update)

        if self.df['job_id'].eq(job_id_to_update).any():
            idx = self.df[self.df['job_id'] == job_id_to_update].index
            if not idx.empty:
                job_dict = updated_job_obj.to_dict()
                for key, value in job_dict.items():
                    if key in self.df.columns: # Ensure key exists before trying to update
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
        "Schedule New Service", "View All Jobs" # New Job Options
    ]
    menu_choice = st.sidebar.selectbox("Navigation", menu_options)

    # ... (Home, Add New Customer, List/View All Customers sections remain the same) ...
    # Make sure the "Add New Customer" and "List/View All Customers" sections still use
    # customer_manager correctly as per our last working version.

    if menu_choice == "Schedule New Service":
        st.header("🗓️ Schedule New Service")
        
        all_customers = customer_manager.get_all_customers()
        if not all_customers:
            st.warning("No customers found. Please add a customer first.")
            return

        customer_options_dict = {f"{c.customer_name} (ID: ...{c.customer_id[-6:]})": c for c in sorted(all_customers, key=lambda c: c.customer_name)}
        selected_customer_display_name = st.selectbox("Select Customer*", list(customer_options_dict.keys()))
        
        selected_customer_obj = customer_options_dict.get(selected_customer_display_name)

        if selected_customer_obj:
            st.write(f"Selected Customer: {selected_customer_obj.customer_name}, Phone: {selected_customer_obj.phone}, Email: {selected_customer_obj.email}")
            st.write(f"Boat: {selected_customer_obj.boat_type}, Length: {selected_customer_obj.boat_length}ft, Preferred Truck: {selected_customer_obj.preferred_truck}")

            with st.form("schedule_job_form", clear_on_submit=True):
                service_type_options = ["Launch", "Haul-out", "Transport"]
                service_type = st.selectbox("Service Type*", options=service_type_options, index=0)
                
                requested_date = st.date_input("Requested Service Date*", value=datetime.now().date())

                st.subheader("Origin Details")
                origin_is_ecm = st.checkbox("Origin is ECM Storage (Pembroke)", key="origin_ecm")
                if origin_is_ecm:
                    origin_address_val = ECM_HOME_ADDRESS
                    st.text_input("Origin Address", value=origin_address_val, disabled=True)
                else:
                    origin_address_val = st.text_input("Origin Address*", help="Customer home, ramp name, or other address")

                st.subheader("Destination Details")
                dest_is_ecm = st.checkbox("Destination is ECM Storage (Pembroke)", key="dest_ecm")
                if dest_is_ecm:
                    destination_address_val = ECM_HOME_ADDRESS
                    st.text_input("Destination Address", value=destination_address_val, disabled=True)
                else:
                    destination_address_val = st.text_input("Destination Address*", help="Customer home, ramp name, or other address")

                # Auto-populate based on service type (can be refined)
                if service_type == "Launch" and not origin_is_ecm: # Assuming launch often from customer home
                     if selected_customer_obj.address:
                        origin_address_val = selected_customer_obj.address
                        st.info(f"Origin auto-filled with customer address: {origin_address_val} (editable if ECM storage not checked)")
                elif service_type == "Haul-out" and not dest_is_ecm: # Assuming haul-out often to customer home
                     if selected_customer_obj.address:
                        destination_address_val = selected_customer_obj.address
                        st.info(f"Destination auto-filled with customer address: {destination_address_val} (editable if ECM storage not checked)")


                boat_details_snapshot_val = f"{selected_customer_obj.customer_name} - {selected_customer_obj.boat_length}ft {selected_customer_obj.boat_type}, Truck: {selected_customer_obj.preferred_truck}"
                st.text_area("Boat Details Snapshot (auto-generated)", value=boat_details_snapshot_val, disabled=True, height=100)
                
                preferred_truck_snapshot_val = selected_customer_obj.preferred_truck
                # Allow override if needed, or just use customer's default
                # preferred_truck_override = st.text_input("Assign Truck for this Job (override default)", value=preferred_truck_snapshot_val)


                notes = st.text_area("Job Notes/Special Instructions")
                
                job_status_val = "Requested" # Default status for new requests

                submit_job_button = st.form_submit_button("Request Service")

                if submit_job_button:
                    if not origin_address_val or not destination_address_val:
                        st.error("Origin and Destination addresses are required.")
                    else:
                        new_job = Job(
                            customer_id=selected_customer_obj.customer_id,
                            service_type=service_type,
                            requested_date=requested_date.strftime("%Y-%m-%d"),
                            origin_is_ecm_storage=origin_is_ecm,
                            origin_address=origin_address_val,
                            destination_is_ecm_storage=dest_is_ecm,
                            destination_address=destination_address_val,
                            boat_details_snapshot=boat_details_snapshot_val,
                            job_status=job_status_val,
                            notes=notes,
                            preferred_truck_snapshot=preferred_truck_snapshot_val # Using customer's default for now
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
            
            # Convert jobs to DataFrame for better display/filtering later
            jobs_data_for_df = [j.to_dict() for j in all_jobs]
            jobs_df = pd.DataFrame(jobs_data_for_df)

            # Reorder and select columns for display
            display_cols = [
                'job_id', 'customer_id', 'service_type', 'requested_date', 'scheduled_date_time', 'job_status',
                'origin_address', 'destination_address', 'boat_details_snapshot', 'preferred_truck_snapshot', 'notes'
            ]
            # Filter out columns not present in DataFrame to avoid errors
            display_cols_present = [col for col in display_cols if col in jobs_df.columns]

            if not jobs_df.empty:
                st.dataframe(jobs_df[display_cols_present])
            else:
                st.info("No job data to display in table format yet.")

            # Detailed view in expanders
            for job in sorted(all_jobs, key=lambda j: j.requested_date, reverse=True): # Sort by requested date
                customer = customer_manager.get_customer_by_id(job.customer_id)
                customer_name_display = customer.customer_name if customer else "N/A"
                
                with st.expander(f"Job ID: ...{job.job_id[-6:]} - {job.service_type} for {customer_name_display} (Requested: {job.requested_date}) - Status: {job.job_status}"):
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
                    # Add an "Edit Job" button/form here later if needed

# --- Main Execution ---
if __name__ == "__main__":
    streamlit_main()
