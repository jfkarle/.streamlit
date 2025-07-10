import streamlit as st
from st_supabase_connection import SupabaseConnection

st.set_page_config(layout="wide")
st.title("Supabase Connection Test")

conn = st.connection("supabase", type=SupabaseConnection, url="postgresql://postgres:[YOUR-PASSWORD]@db.knexrzljvagiwqstapnk.supabase.co:5432/postgres", key="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtuZXhyemxqdmFnaXdxc3RhcG5rIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MjA4NjQ4MiwiZXhwIjoyMDY3NjYyNDgyfQ.KpLrXdOQFYvDKq1DQ_YjLU-3yYFFqkIjFpO20dFFsk4")


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
