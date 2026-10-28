import os

from dotenv import load_dotenv
from alpaca.data.live import StockDataStream
from alpaca.data.enums import DataFeed

import json
from datetime import datetime, timezone
from uuid import uuid4
from confluent_kafka import Producer
from validation import check_stock_record

load_dotenv()

api_key = os.getenv("ALPACA_API_KEY")
secret_key = os.getenv("ALPACA_SECRET_KEY")

stream = StockDataStream(
    api_key=api_key,
    secret_key=secret_key,
    feed=DataFeed.IEX
)

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

pipeline_run_id = str(uuid4())

def delivery_report(error, message):
    if error is not None:
        print("Kafka delivery failed:", error)
    else:
        print(
            "Delivered to Kafka:",
            message.key().decode("utf-8")
        )

async def handle_stock_bar(bar):

    record = {
        "symbol": bar.symbol,
        "timestamp": bar.timestamp,
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
    }


    record["data_source"] = "Alpaca Markets"
    record["market_data_feed"] = "IEX"
    record["ingestion_timestamp_utc"] = datetime.now(timezone.utc)
    record["pipeline_run_id"] = pipeline_run_id
    record["pipeline_stage"] = "live_ingestion"


    failure_reason = check_stock_record(record)

    if failure_reason is not None:
        print("Rejected record:", record["symbol"], failure_reason)
        return


    record["timestamp"] = record["timestamp"].isoformat()
    record["ingestion_timestamp_utc"] = (
        record["ingestion_timestamp_utc"].isoformat()
    )


    producer.produce(
    topic="stock_prices_raw",
    key=record["symbol"].encode("utf-8"),
    value=json.dumps(record).encode("utf-8"),
    callback=delivery_report
)

    producer.poll(0)

    print("Queued for Kafka:", record["symbol"])


stream.subscribe_bars(handle_stock_bar, *symbols)
stream.run()