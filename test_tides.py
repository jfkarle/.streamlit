import requests
import datetime

def fetch_noaa_tides_debug(station_id, date_to_check):
    """
    A debugging version of the function to get detailed error output.
    """
    date_str = date_to_check.strftime('%Y%m%d')
    api_url = (
        f"https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?"
        f"begin_date={date_str}&end_date={date_str}"
        f"&station={station_id}"
        f"&product=predictions&datum=MLLW&time_zone=lst_ldt&units=english&format=json"
    )

    print(f"\n--- Testing Station: {station_id} for Date: {date_to_check} ---")
    print(f"Attempting to call URL: {api_url}")

    try:
        response = requests.get(api_url, timeout=10)
        print(f"Response Status Code: {response.status_code}")
        response.raise_for_status()
        
        data = response.json()
        print("Successfully received and parsed JSON data.")
        
        if 'predictions' not in data:
            print("FAIL: 'predictions' key not found in the response.")
            return []

        tide_events = []
        for event in data.get('predictions', []):
            if event['type'] in ['H', 'L']:
                tide_dt = datetime.datetime.strptime(event['t'], '%Y-%m-%d %H:%M')
                tide_events.append({'type': event['type'], 'time': tide_dt.time()})
        
        print(f"SUCCESS: Found {len(tide_events)} High/Low tide events.")
        return tide_events

    except requests.exceptions.HTTPError as e:
        print(f"!!!!!!!!!! HTTP ERROR !!!!!!!!!!! -> Error: {e}")
    except requests.exceptions.Timeout:
        print(f"!!!!!!!!!! TIMEOUT ERROR !!!!!!!!!!!")
    except requests.exceptions.RequestException as e:
        print(f"!!!!!!!!!! REQUEST ERROR !!!!!!!!!!! -> Error: {e}")
    except Exception as e:
        print(f"!!!!!!!!!! UNEXPECTED ERROR !!!!!!!!!!! -> Error: {e}")
    
    return None # Return None to indicate failure

# --- Main test execution ---
if __name__ == "__main__":
    test_station_id = "8446493" # Plymouth Harbor

    # --- TEST CASE 1: The date that previously failed ---
    print("\n=======================================================")
    print("=== TEST CASE 1: Requesting data for a FUTURE date ===")
    print("=======================================================")
    future_date = datetime.date(2025, 6, 17)
    future_result = fetch_noaa_tides_debug(test_station_id, future_date)
    
    # --- TEST CASE 2: Today's date ---
    print("\n\n=======================================================")
    print("=== TEST CASE 2: Requesting data for the CURRENT date ===")
    print("=======================================================")
    current_date = datetime.date.today()
    current_result = fetch_noaa_tides_debug(test_station_id, current_date)
    
    print("\n\n--- ALL TESTS COMPLETE ---")
    if future_result is None and current_result is not None:
         print("\nCONCLUSION: The API is working for current dates but failing for far-future dates.")
         print("This confirms the issue is with the NOAA service itself, not our code or connection.")
    elif future_result is None and current_result is None:
         print("\nCONCLUSION: Both API calls failed. This suggests a persistent connection issue or a problem with the station ID.")
    else:
         print("\nCONCLUSION: Both tests succeeded. Please review the results above.")
