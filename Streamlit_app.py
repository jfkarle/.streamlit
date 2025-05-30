import streamlit as st
import uuid
from datetime import datetime
import os
import pandas as pd

# --- Configuration ---
DATETIME_FORMAT = "%Y-%m-%d %H:%M"
CUSTOMER_CSV_FILE = "ECM Sample Cust.csv" # Your primary CSV data file

# --- Customer Class ---
class Customer:
    def __init__(self, customer_name, boat_type, boat_length, phone, email, address,
                 boat_draft, home_latitude, home_longitude, is_ecm_boat,
                 preferred_truck="", customer_id=None): # preferred_truck is a parameter here

        # --- This is where you assign the parameters to instance attributes ---

        # Handle customer_id (generate if None or empty)
        if pd.isna(customer_id) or str(customer_id).strip() == "":
            self.customer_id = str(uuid.uuid4())
        else:
            self.customer_id = str(customer_id)

        self.customer_name = str(customer_name if pd.notna(customer_name) else "")
        self.phone = str(phone if pd.notna(phone) else "")
        self.email = str(email if pd.notna(email) else "")
        self.address = str(address if pd.notna(address) else "")
        self.boat_type = str(boat_type if pd.notna(boat_type) else "")

        # ===> ADD THE LINE HERE <===
        self.preferred_truck = str(preferred_truck if pd.notna(preferred_truck) else "")

        # Continue with other attributes
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


    def to_dict(self): # To convert Customer object to a dictionary for DataFrame
        return {
            'customer_id': self.customer_id,
            'Customer Name': self.customer_name,
            'Boat Type': self.boat_type,
            'PREFERRED TRUCK': self.preferred_truck, # Added new field
            'Boat Length': self.boat_length,
            'Phone': self.phone,
            'Email': self.email,
            'Address': self.address,
            'Boat Draft': self.boat_draft,
            'Home Latitude': self.home_latitude,
            'Home Longitude': self.home_longitude,
            'Is ECM Boat': self.is_ecm_boat
        }

    @staticmethod
    def from_dict(data_dict): # Create Customer object from a dictionary (e.g., a DataFrame row)
        return Customer(
            customer_id=data_dict.get('customer_id'),
            customer_name=data_dict.get('Customer Name'),
            boat_type=data_dict.get('Boat Type'),
            preferred_truck=data_dict.get('PREFERRED TRUCK'), # Added new field
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

# --- Streamlit UI Application ---
def streamlit_main():
    st.set_page_config(layout="wide", page_title="Boat Hauling Automator")
    st.title("🚤 Boat Hauling Business Automator (CSV Mode)")
    st.caption(f"Data file: {CUSTOMER_CSV_FILE}")
    st.write(f"Current Time: {datetime.now().strftime(DATETIME_FORMAT)} (Location: Pembroke, MA)")

    # Initialize CSV manager for Customers
    if 'customer_manager' not in st.session_state:
        st.session_state.customer_manager = CustomerCsvManager(CUSTOMER_CSV_FILE)
    customer_manager = st.session_state.customer_manager
    
    menu_options = ["Home", "Add New Customer", "List/View All Customers"]
    menu_choice = st.sidebar.selectbox("Navigation", menu_options)

    if menu_choice == "Home":
        st.header("Welcome!")
        st.write("Select an option from the sidebar to manage customers using a CSV file.")
        all_customers = customer_manager.get_all_customers()
        st.metric("Total Customers (from CSV)", len(all_customers))
        st.sidebar.info(f"""
        **Note on Data Saving (CSV Mode):**
        - When running locally, changes are saved directly to `{CUSTOMER_CSV_FILE}`.
        - When deployed on Streamlit Cloud, saved changes update the CSV in the app's temporary session.
        These changes on Streamlit Cloud may be lost if the app restarts.
        To make changes permanent on Streamlit Cloud, you would typically download the updated CSV
        and manually update it in your GitHub repository.
        """)


    elif menu_choice == "Add New Customer":
        st.header("➕ Add New Customer (to CSV)")
        with st.form("add_customer_csv_form", clear_on_submit=True):
            st.subheader("Customer Info")
            # Column names from ECM Sample Cust.csv:
            # customer_id, Customer Name, Boat Type, Boat Length, Phone, Email, Address,
            # Boat Draft, Home Latitude, Home Longitude, Is ECM Boat
            customer_name = st.text_input("Customer Name*", help="e.g., John Doe")
            phone = st.text_input("Phone", help="e.g., 555-123-4567")
            email = st.text_input("Email", help="e.g., john.doe@example.com")
            address = st.text_area("Address", help="e.g., 123 Main St, Anytown, USA")

            st.subheader("Boat Info")
            boat_type = st.text_input("Boat Type*", help="e.g., Powerboat, Sailboat MD")
            preferred_truck = st.text_input("Preferred Truck", help="e.g., S20, S21, S23") # New field
            boat_length = st.number_input("Boat Length (ft)*", min_value=1.0, value=25.0, format="%.1f")
            boat_draft = st.number_input("Boat Draft (ft)", min_value=0.0, value=3.0, format="%.1f")
            
            st.subheader("Location & Other")
            home_latitude = st.number_input("Home Latitude", format="%.6f", value=42.078000)
            home_longitude = st.number_input("Home Longitude", format="%.6f", value=-70.710000)
            is_ecm_boat = st.checkbox("Is ECM Boat?", value=False)
            
            # customer_id is auto-generated by the class if not provided
            submitted = st.form_submit_button("Add Customer")

            if submitted:
                if not all([customer_name, boat_type]): # Basic validation
                    st.error("Please fill in required fields: Customer Name, Boat Type.")
                else:
                    new_customer = Customer(
                        customer_name=customer_name, phone=phone, email=email, address=address,
                        boat_type=boat_type, boat_length=boat_length, boat_draft=boat_draft,
                        preferred_truck=preferred_truck,
                        home_latitude=home_latitude, home_longitude=home_longitude,
                        is_ecm_boat=is_ecm_boat
                        # customer_id will be auto-generated
                    )
                    customer_manager.add_customer_obj(new_customer)

    elif menu_choice == "List/View All Customers":
        st.header("👥 List of Customers (from CSV)")
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
                    cols[1].markdown(f"**Preferred Truck:** {customer.preferred_truck if customer.preferred_truck else 'N/A'}") # Display new field
                    cols[1].markdown(f"**Boat Length:** {customer.boat_length} ft")
                    cols[1].markdown(f"**Boat Draft:** {customer.boat_draft} ft")
                    cols[0].markdown(f"**Home Latitude:** {customer.home_latitude}")
                    cols[0].markdown(f"**Home Longitude:** {customer.home_longitude}")
                    cols[1].markdown(f"**Is ECM Boat:** {'Yes' if customer.is_ecm_boat else 'No'}")
                    
                    st.markdown("---")
                    st.subheader(f"Edit {customer.customer_name}")
                    # Use customer_id for unique form key
                    with st.form(key=f"edit_form_{customer.customer_id}"):
                        edit_name = st.text_input("Cust. Name", value=customer.customer_name, key=f"name_{customer.customer_id}")
                        edit_phone = st.text_input("Phone", value=customer.phone, key=f"phone_{customer.customer_id}")
                        edit_email = st.text_input("Email", value=customer.email, key=f"email_{customer.customer_id}")
                        edit_address = st.text_area("Address", value=customer.address, key=f"addr_{customer.customer_id}")
                        edit_boat_type = st.text_input("Boat Type", value=customer.boat_type, key=f"btype_{customer.customer_id}")
                        edit_preferred_truck = st.text_input("Preferred Truck", value=customer.preferred_truck, key=f"ptruck_{customer.customer_id}")
                        edit_boat_length = st.number_input("Length", value=float(customer.boat_length), min_value=0.0, format="%.1f", key=f"blen_{customer.customer_id}")
                        edit_boat_draft = st.number_input("Draft", value=float(customer.boat_draft), min_value=0.0, format="%.1f", key=f"bdr_{customer.customer_id}")
                        edit_lat = st.number_input("Latitude", value=float(customer.home_latitude), format="%.6f", key=f"lat_{customer.customer_id}")
                        edit_lon = st.number_input("Longitude", value=float(customer.home_longitude), format="%.6f", key=f"lon_{customer.customer_id}")
                        edit_ecm = st.checkbox("ECM Boat", value=bool(customer.is_ecm_boat), key=f"ecm_{customer.customer_id}")
                        
                        update_submitted = st.form_submit_button("Save Changes")
                        if update_submitted:
                            updated_cust_obj = Customer(
                                customer_name=edit_name, phone=edit_phone, email=edit_email, address=edit_address,
                                boat_type=edit_boat_type, boat_length=edit_boat_length, boat_draft=edit_boat_draft,
                                preferred_truck=edit_preferred_truck,
                                home_latitude=edit_lat, home_longitude=edit_lon, is_ecm_boat=edit_ecm,
                                customer_id=customer.customer_id # CRITICAL: Pass existing ID
                            )
                            if customer_manager.update_customer_by_id(customer.customer_id, updated_cust_obj):
                                st.experimental_rerun() # Refresh the list to show updated data

# --- Main Execution ---
if __name__ == "__main__":
    streamlit_main()
