import os
import json
from pathlib import Path
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed
from datetime import datetime, timezone
from uuid import uuid4
from metadata import add_ingestion_metadata
from validation import validate_stock_record, get_validation_failure_reason

# Your existing imports


def get_historical_stock_data():

    # 1. Read the stock configuration

    # 2. Create the Alpaca client

    # 3. Request historical stock data

    # 4. Add ingestion metadata

    # 5. Validate the records

    # 6. Separate valid and invalid records

    return valid_stock_data, invalid_stock_data

BASE_DIR = Path(__file__).resolve().parent.parent

config_path = BASE_DIR / "config" / "stocks.json"

with open(config_path, "r") as file:
    config = json.load(file)
symbols = config["symbols"]

load_dotenv()

api_key = os.getenv("ALPACA_API_KEY")
secret_key = os.getenv("ALPACA_SECRET_KEY")

print("API key loaded:", bool(api_key))
print("Secret key loaded:", bool(secret_key))

client = StockHistoricalDataClient(api_key, secret_key)

request = StockBarsRequest(
    symbol_or_symbols=symbols,
    timeframe=TimeFrame.Minute,
    start=datetime(2026, 10, 12, tzinfo=timezone.utc),
    end=datetime(2026, 10, 13, tzinfo=timezone.utc),
    feed=DataFeed.IEX
)

bars = client.get_stock_bars(request)
stock_data = bars.df.reset_index()
stock_data = add_ingestion_metadata(stock_data)

stock_data["is_valid"] = stock_data.apply(
    lambda row: validate_stock_record(row.to_dict()),
    axis=1
)
valid_stock_data = stock_data[stock_data["is_valid"]].copy()
invalid_stock_data = stock_data[~stock_data["is_valid"]].copy()

stock_data["validation_failure_reason"] = stock_data.apply(
    lambda row: get_validation_failure_reason(row.to_dict()),
    axis=1
)

print(
    stock_data[
        ["symbol", "is_valid", "validation_failure_reason"]
    ].head()
)
print(stock_data.head())
print(stock_data.columns.tolist())
print("Number of rows:", len(stock_data))
print("Column names:", bars.df.columns.tolist())
print(bars.df.index.get_level_values("symbol").unique())
print(bars.df.groupby(level="symbol").size())
print("Stocks loaded:", symbols)
print("Number of stocks:", len(symbols))
print("Total records:", len(stock_data))
print("Valid records:", len(valid_stock_data))
print("Invalid records:", len(invalid_stock_data))


assert len(stock_data) == (
    len(valid_stock_data) + len(invalid_stock_data)
)

if __name__ == "__main__":
    valid_data, invalid_data = get_historical_stock_data()

    print("Valid records:", len(valid_data))
    print("Invalid records:", len(invalid_data))