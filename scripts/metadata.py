from datetime import datetime, timezone
from uuid import uuid4


def add_ingestion_metadata(stock_data):
    pipeline_run_id = str(uuid4())
    ingestion_timestamp = datetime.now(timezone.utc)

    stock_data = stock_data.copy()

    stock_data["data_source"] = "Alpaca Markets"
    stock_data["market_data_feed"] = "IEX"
    stock_data["ingestion_timestamp_utc"] = ingestion_timestamp
    stock_data["pipeline_run_id"] = pipeline_run_id
    stock_data["pipeline_stage"] = "historical_ingestion"

    return stock_data