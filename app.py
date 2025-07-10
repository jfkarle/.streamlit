import streamlit as st
from st_supabase_connection import SupabaseConnection

st.set_page_config(layout="wide")
st.title("Supabase Connection Test")

conn = st.connection("supabase", type=SupabaseConnection, url="postgresql://postgres:[YOUR-PASSWORD]@db.knexrzljvagiwqstapnk.supabase.co:5432/postgres", key="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImtuZXhyemxqdmFnaXdxc3RhcG5rIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTIwODY0ODIsImV4cCI6MjA2NzY2MjQ4Mn0.hgWhtefyiEmGj5CERladOe3hMBM-rVnwMGNwrt8FT6Y")


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
