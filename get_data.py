import pandas as pd
from polygon import RESTClient
from datetime import date, timedelta
import time
import os

# --- Configuration ---
# Read API key from environment variable for security
API_KEY = os.environ.get('POLYGON_API_KEY') 
TICKER = 'SPY'
END_DATE = date.today()
START_DATE = END_DATE - timedelta(days=730)

# --- Main Script ---
if not API_KEY:
    raise ValueError("API key not found. Please set the POLYGON_API_KEY secret in GitHub Actions.")

all_bars = []
current_date = START_DATE

# --- FIX: Initialize the client directly, not with a 'with' statement ---
client = RESTClient(API_KEY)

print(f"Starting data fetch for {TICKER} from {START_DATE} to {END_DATE}.")
print("This will take approximately 100-110 minutes.")

while current_date <= END_DATE:
    date_str = current_date.strftime('%Y-%m-%d')
    
    try:
        resp = client.get_aggs(
            ticker=TICKER, multiplier=5, timespan='minute',
            from_=date_str, to=date_str, limit=50000
        )
        
        if hasattr(resp, 'results') and resp.results:
            print(f"Fetched {len(resp.results)} bars for {date_str}")
            all_bars.extend(resp.results)
        else:
            print(f"No data for {date_str}")

        time.sleep(13)

    except Exception as e:
        print(f"Error on {date_str}: {e}. Waiting 60 seconds before retrying.")
        time.sleep(60)
        continue

    current_date += timedelta(days=1)

print("\nData fetching complete. Processing...")

df = pd.DataFrame(all_bars)

if not df.empty:
    df['timestamp'] = pd.to_datetime(df['t'], unit='ms')
    df.set_index('timestamp', inplace=True)
    df.rename(columns={'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'}, inplace=True)
    df = df[['open', 'high', 'low', 'close', 'volume']]
    
    output_filename = f"{TICKER}_5min_data.csv"
    df.to_csv(output_filename)
    
    print(f"Success! Data saved to {output_filename}")
else:
    print("Could not fetch any data.")
