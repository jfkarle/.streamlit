import streamlit as st
from st_supabase_connection import SupabaseConnection, execute_query

st.set_page_config(layout="wide")
st.title("Supabase Connection Test")

# (Assuming url/key are passed or in st.secrets as before)
conn = st.connection(
    "supabase",
    type=SupabaseConnection,
    url=st.secrets["connections"]["supabase"]["url"],
    key=st.secrets["connections"]["supabase"]["key"],
)

try:
    st.write("Attempting to query the 'jobs' table...")
    # 1. Build
    builder = conn.table("jobs").select("*")
    # 2. Execute with caching
    response = execute_query(builder, ttl="10m")
    # 3. Show data
    st.write("Connection successful! Data from 'jobs' table:")
    st.dataframe(response.data)

except Exception as e:
    st.error("Could not connect to Supabase or query the table.")
    st.exception(e)
