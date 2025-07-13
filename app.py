import streamlit as st

st.set_page_config(layout="wide")
st.title("Supabase Connection Test")

# 1️⃣ Debug your secrets first
st.write("All secrets keys:", st.secrets)
st.write("Connections keys:", list(st.secrets.get("connections", {}).keys()))
st.write("Supabase creds:", st.secrets["connections"].get("supabase"))

from st_supabase_connection import SupabaseConnection


try:
    conn = st.connection(
        "supabase",
        type=SupabaseConnection,
        url=st.secrets["connections"]["supabase"]["url"],
        key=st.secrets["connections"]["supabase"]["key"],
    )

    rows = conn.query("*", table="jobs", ttl="10m").execute()
    st.dataframe(rows.data)

except Exception as e:
    st.error("Could not connect to Supabase or query the table.")
    st.exception(e)
