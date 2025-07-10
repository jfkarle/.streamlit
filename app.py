import streamlit as st
from st_supabase_connection import SupabaseConnection

st.set_page_config(layout="wide")
st.title("Supabase Connection Test")

try:
    # Initialize connection.
    conn = st.connection("supabase", type=SupabaseConnection)

    # Perform a test query. This will query the 'jobs' table we created.
    # If the table doesn't exist yet, this will fail, which is okay for a first test.
    st.write("Attempting to query the 'jobs' table...")
    rows = conn.query("*", table="jobs", ttl="10m").execute()

    # Print results.
    st.write("Connection successful! Data from 'jobs' table:")
    st.dataframe(rows.data)

except Exception as e:
    st.error("Could not connect to Supabase or query the table.")
    st.exception(e)
