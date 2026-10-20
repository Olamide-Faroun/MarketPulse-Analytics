import json


def prepare_kafka_messages(stock_data):
    messages = json.loads(
        stock_data.to_json(
            orient="records",
            date_format="iso"
        )
    )

    return messages