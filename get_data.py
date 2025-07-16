import os
from datetime import datetime, timedelta
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame

# --- Get API credentials from GitHub secrets ---
API_KEY = os.environ.get('ALPACA_API_KEY')
SECRET_KEY = os.environ.get('ALPACA_SECRET_KEY')

if not API_KEY or not SECRET_KEY:
    raise ValueError("API keys not found. Please set secrets in GitHub Actions.")

# --- 1. Setup the client and request parameters ---
client = StockHistoricalDataClient(API_KEY, SECRET_KEY)

# --- Set date range for maximum available history ---
start_date = datetime(2016, 1, 1)
end_date = datetime.now() - timedelta(minutes=16) # Most recent allowed data

request_params = StockBarsRequest(
    symbol_or_symbols=["SPY"],
    timeframe=TimeFrame.Minute, # 1-minute bars
    start=start_date,
    end=end_date
)

# --- 2. Fetch the data ---
print(f"Fetching maximum available 1-minute SPY data from {start_date.year} to present...")
bars_df = client.get_stock_bars(request_params).df
print("Data fetching complete.")

# --- 3. Save the data to CSV ---
output_filename = "SPY_1min_data_max_history.csv"
bars_df.reset_index(inplace=True) 
bars_df.to_csv(output_filename, index=False)

print(f"\nSuccess! Data saved to {output_filename}")
print(f"Total bars fetched: {len(bars_df)}")
