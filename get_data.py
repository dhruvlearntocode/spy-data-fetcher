import os
from datetime import datetime
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

# --- Get API credentials from GitHub secrets ---
API_KEY = os.environ.get('ALPACA_API_KEY')
SECRET_KEY = os.environ.get('ALPACA_SECRET_KEY')

# --- Check if keys exist ---
if not API_KEY or not SECRET_KEY:
    raise ValueError("API keys not found. Please set secrets in GitHub Actions.")

# --- 1. Setup the client and request parameters ---
client = StockHistoricalDataClient(API_KEY, SECRET_KEY)

# --- ✅ FIX: Set a fixed date range for 2022 and 2023 ---
start_date = datetime(2022, 1, 1)
end_date = datetime(2023, 12, 31)

request_params = StockBarsRequest(
    symbol_or_symbols=["SPY"],
    timeframe=TimeFrame(5, TimeFrameUnit.Minute), # 5-minute bars
    start=start_date,
    end=end_date
)

# --- 2. Fetch the data ---
print(f"Fetching 5-minute SPY data from {start_date.year} to {end_date.year}...")
bars_df = client.get_stock_bars(request_params).df
print("Data fetching complete.")

# --- 3. Save the data to CSV ---
output_filename = "SPY_5min_data_2022-2023.csv"
bars_df.reset_index(inplace=True) 
bars_df.to_csv(output_filename, index=False)

print(f"\nSuccess! Data saved to {output_filename}")
print(f"Total bars fetched: {len(bars_df)}")
