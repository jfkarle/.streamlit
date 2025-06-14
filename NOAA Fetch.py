import requests
from datetime import datetime

def get_tide_times_scituate(date_str):
    """
    Fetch high and low tide times for Scituate (station 8445138) on the given date (YYYYMMDD).
    Returns a dict with 'high' and 'low' tide lists.
    """
    base = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
    params = {
        "product": "predictions",
        "application": "python-script",
        "begin_date": date_str,
        "end_date": date_str,
        "datum": "MLLW",
        "station": "8445138",
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
        typ = item["type"].lower()  # "H" or "L"
        if typ == "h":
            result["high"].append((t, float(item["v"])))
        elif typ == "l":
            result["low"].append((t, float(item["v"])))
    return result

if __name__ == "__main__":
    date = input("Enter date (YYYYMMDD): ")
    tides = get_tide_times_scituate(date)
    print(f"Tide times for Scituate on {date}:")
    for ht in tides["high"]:
        print(f"  High tide at {ht[0].strftime('%I:%M %p')} ({ht[1]} ft)")
    for lt in tides["low"]:
        print(f"  Low tide at {lt[0].strftime('%I:%M %p')} ({lt[1]} ft)")
