import os
import json
from pathlib import Path
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed
from datetime import datetime, timezone

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
print (bars.df.head())

print("Number of rows:", len(bars.df))
print("Column names:", bars.df.columns.tolist())
print(bars.df.index.get_level_values("symbol").unique())
print(bars.df.groupby(level="symbol").size())
print("Stocks loaded:", symbols)
print("Number of stocks:", len(symbols))