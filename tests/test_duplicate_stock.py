import json
from pathlib import Path


def test_duplicate_stock():
    base_dir = Path(__file__).resolve().parent.parent
    config_path = base_dir / "config" / "stocks.json"

    with open(config_path, "r") as file:
        config = json.load(file)

    symbols = config["symbols"]

    assert len(symbols) == len(set(symbols))