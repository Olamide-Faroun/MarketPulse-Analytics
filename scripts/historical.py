import os
from dotenv import load_dotenv
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import DataFeed
from datetime import datetime, timezone

load_dotenv()

api_key = os.getenv("ALPACA_API_KEY")
secret_key = os.getenv("ALPACA_SECRET_KEY")

print("API key loaded:", bool(api_key))
print("Secret key loaded:", bool(secret_key))

client = StockHistoricalDataClient(api_key, secret_key)

request = StockBarsRequest(
    symbol_or_symbols=["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA", "JPM", "WMT", "XOM"],
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