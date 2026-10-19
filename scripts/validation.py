def validate_stock_record(record):
        required_fields = [
        "symbol",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]

        for field in required_fields:
            if field not in record:
                return False

        if not record["symbol"]:
            return False

        if record["open"] <= 0:
            return False

        if record["high"] <= 0:
            return False

        if record["low"] <= 0:
            return False

        if record["close"] <= 0:
            return False

        if record["volume"] < 0:
            return False

        if max(record["open"], record["close"], record["low"]) > (record["high"]):
            return False

        if min(record["open"], record["close"], record["high"]) < (record["low"]):
            return False

        return True