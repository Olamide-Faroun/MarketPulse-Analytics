import json
from confluent_kafka import Producer
from historical import get_historical_stock_data


valid_data, invalid_data = get_historical_stock_data()

print("Valid records:", len(valid_data))
print("Invalid records:", len(invalid_data))

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

sample_data = valid_data.head(5)

print(sample_data[["symbol", "timestamp", "close"]])

messages = json.loads(
    sample_data.to_json(
        orient="records",
        date_format="iso"
    )
)

delivery_failures = []


def delivery_report(error, message):
    if error is not None:
        delivery_failures.append(str(error))
        print("Delivery failed:", error)
    else:
        print("Delivered:", message.key().decode("utf-8"))

for record in messages:
    producer.produce(
        topic="stock_prices_raw",
        key=record["symbol"].encode("utf-8"),
        value=json.dumps(record).encode("utf-8"),
        callback=delivery_report
    )

remaining = producer.flush(timeout=30)

if remaining > 0 or delivery_failures:
    raise RuntimeError("Some Kafka messages were not delivered")

print("All five records delivered successfully.")