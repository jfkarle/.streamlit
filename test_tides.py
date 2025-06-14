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
        # Set a timeout to prevent the script from hanging indefinitely
        response = requests.get(api_url, timeout=10)
        
        # Print status code to see the raw response from the server
        print(f"Response Status Code: {response.status_code}")

        # This will raise an error for 4xx/5xx responses, which will be caught below
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
        print(f"\n!!!!!!!!!! HTTP ERROR !!!!!!!!!!!")
        print(f"The NOAA server returned an error code, which means it received our request but couldn't process it.")
        print(f"Error: {e}")
        print("This could be due to an invalid Station ID or a problem on the server's end.")
        print("Response Body:", response.text)
    except requests.exceptions.Timeout:
        print(f"\n!!!!!!!!!! TIMEOUT ERROR !!!!!!!!!!!")
        print("The request to the NOAA server timed out after 10 seconds.")
        print("This often indicates a network issue, a firewall blocking the connection, or a slow server response.")
    except requests.exceptions.RequestException as e:
        print(f"\n!!!!!!!!!! REQUEST ERROR !!!!!!!!!!!")
        print("A general network error occurred (e.g., DNS failure, no internet connection, connection refused).")
        print(f"Error: {e}")
    except Exception as e:
        print(f"\n!!!!!!!!!! UNEXPECTED ERROR !!!!!!!!!!!")
        print(f"An unexpected error occurred during the process: {e}")
    
    return None # Return None to indicate failure in the test

# --- Main test execution ---
if __name__ == "__main__":
    # Use the station ID and date from the failed scenario in your app
    # Plymouth Harbor's station ID is "8446493"
    test_station_id = "8446493" 
    test_date = datetime.date(2025, 6, 17)

    result = fetch_noaa_tides_debug(test_station_id, test_date)

    print("\n--- TEST COMPLETE ---")
    if result is not None:
        print("Final Result:", result)
    else:
        print("The function failed to return tide data due to an error.")