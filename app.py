import streamlit as st

st.set_page_config(layout="wide")
st.title("Supabase Connection Test")

# 1️⃣ Debug your secrets first
st.write("All secrets keys:", st.secrets)
st.write("Connections keys:", list(st.secrets.get("connections", {}).keys()))
st.write("Supabase creds:", st.secrets["connections"].get("supabase"))

from st_supabase_connection import SupabaseConnection

try:
    # 2️⃣ Only now create your connection
    conn = st.connection("supabase", type=SupabaseConnection)

    st.write("Attempting to query the 'jobs' table...")
    rows = conn.query("*", table="jobs", ttl="10m").execute()
    st.write("Connection successful! Data from 'jobs' table:")
    st.dataframe(rows.data)

except Exception as e:
    st.error("Could not connect to Supabase or query the table.")
    st.exception(e)
