import pandas as pd
from polygon import RESTClient
from datetime import date, timedelta
import time
import os

# --- Configuration ---
API_KEY = os.environ.get('POLYGON_API_KEY') 
TICKER = 'SPY'
END_DATE = date.today()
START_DATE = END_DATE - timedelta(days=730)

# --- Main Script ---
if not API_KEY:
    raise ValueError("API key not found. Please set the POLYGON_API_KEY secret in GitHub Actions.")

print(f"Starting data fetch for {TICKER} from {START_DATE} to {END_DATE}.")
print("Requesting all data in a single API call...")

# Initialize the client
client = RESTClient(API_KEY)

try:
    # Request all 5-minute bars for the entire date range at once
    resp = client.get_aggs(
        ticker=TICKER,
        multiplier=5,
        timespan='minute',
        from_=START_DATE,
        to=END_DATE,
        limit=50000 # Max limit ensures we get all data
    )

    if not hasattr(resp, 'results') or not resp.results:
         raise Exception("No data was returned from the API.")

    print(f"Successfully fetched {len(resp.results)} bars.")
    print("Processing data...")
    
    # Convert the collected data to a Pandas DataFrame
    df = pd.DataFrame(resp.results)
    
    # Convert UNIX timestamp to readable datetime and set as index
    df['timestamp'] = pd.to_datetime(df['t'], unit='ms')
    df.set_index('timestamp', inplace=True)
    
    # Rename columns and keep the essentials
    df.rename(columns={'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'}, inplace=True)
    df = df[['open', 'high', 'low', 'close', 'volume']]
    
    # Save to a CSV file
    output_filename = f"{TICKER}_5min_data.csv"
    df.to_csv(output_filename)
    
    print(f"\nSuccess! Data saved to {output_filename}")

except Exception as e:
    print(f"An error occurred: {e}")
