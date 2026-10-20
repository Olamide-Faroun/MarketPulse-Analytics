import os

from dotenv import load_dotenv
from alpaca.data.live import StockDataStream
from alpaca.data.enums import DataFeed

load_dotenv()

api_key = os.getenv("ALPACA_API_KEY")
secret_key = os.getenv("ALPACA_SECRET_KEY")

stream = StockDataStream(
    api_key=api_key,
    secret_key=secret_key,
    feed=DataFeed.IEX
)

async def handle_stock_bar(bar):
    print(bar)

stream.subscribe_bars(handle_stock_bar, "AAPL")
stream.run()