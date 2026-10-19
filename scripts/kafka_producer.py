import json
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

stock_record = {
    "symbol": "AAPL",
    "close": 250.50,
    "volume": 12000
}
message = json.dumps(stock_record)

producer.produce(
    topic="stock_prices_raw",
    key=stock_record["symbol"],
    value=message.encode("utf-8")
)

producer.flush()