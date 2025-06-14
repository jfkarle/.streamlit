import streamlit as st
import requests
from datetime import datetime

def get_tide_times_scituate(date_str):
    base = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
    params = {
        "product": "predictions",
        "application": "streamlit-tide-app",
        "begin_date": date_str,
        "end_date": date_str,
        "datum": "MLLW",
        "station": "8445138",  # Scituate Harbor, MA
        "time_zone": "lst_ldt",
        "units": "english",
        "interval": "hilo",
        "format": "json",
    }
    resp = requests.get(base, params=params)
    resp.raise_for_status()
    data = resp.json().get("predictions", [])
    result = {"high": [], "low": []}
    for item in data:
        t = datetime.strptime(item["t"], "%Y-%m-%d %H:%M")
        typ = item["type"].lower()
        if typ == "h":
            result["high"].append((t, float(item["v"])))
        elif typ == "l":
            result["low"].append((t, float(item["v"])))
    return result

# --- Streamlit UI ---
st.title("NOAA High & Low Tides – Scituate, MA")

date_input = st.date_input("Choose a date", datetime.today())
date_str = date_input.strftime("%Y%m%d")

if st.button("Get Tide Data"):
    try:
        tides = get_tide_times_scituate(date_str)
        st.subheader(f"Tide Times for {date_input.strftime('%B %d, %Y')}")
        
        st.markdown("**High Tides:**")
        for ht in tides["high"]:
            st.write(f"🌊 {ht[0].strftime('%I:%M %p')} — {ht[1]} ft")

        st.markdown("**Low Tides:**")
        for lt in tides["low"]:
            st.write(f"⬇️ {lt[0].strftime('%I:%M %p')} — {lt[1]} ft")

    except Exception as e:
        st.error(f"Failed to fetch tide data: {e}")
