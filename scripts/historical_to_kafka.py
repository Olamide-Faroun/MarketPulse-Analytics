import json
from confluent_kafka import Producer
from historical import get_historical_stock_data
from message_format import prepare_kafka_messages


valid_data, invalid_data = get_historical_stock_data()

print("Valid records:", len(valid_data))
print("Invalid records:", len(invalid_data))

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

sample_data = valid_data
print("Stocks selected:", sample_data["symbol"].unique().tolist())
print("Number of records:", len(sample_data))
assert len(sample_data) > 0
assert sample_data["symbol"].nunique() == 10

print(sample_data[["symbol", "timestamp", "close"]])

messages = prepare_kafka_messages(sample_data)

delivery_count = 0
delivery_failures = []


def delivery_report(error, message):
    global delivery_count

    if error is not None:
        delivery_failures.append(str(error))
        print("Delivery failed:", error)
    else:
        delivery_count += 1

for record in messages:
    producer.produce(
        topic="stock_prices_raw",
        key=record["symbol"].encode("utf-8"),
        value=json.dumps(record).encode("utf-8"),
        callback=delivery_report
    )
    producer.poll(0)

remaining = producer.flush(timeout=30)

if remaining > 0 or delivery_failures:
    raise RuntimeError("Some Kafka messages were not delivered")

print("Records prepared:", len(messages))
print("Records delivered:", delivery_count)
print("Delivery failures:", len(delivery_failures))

print("All selected records delivered successfully.")