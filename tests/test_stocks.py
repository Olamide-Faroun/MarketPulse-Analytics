import json
from pathlib import Path


def test_stock_list_not_empty():
    base_dir = Path(__file__).resolve().parent.parent
    config_path = base_dir / "config" / "stocks.json"

    with open(config_path, "r") as file:
        config = json.load(file)

    symbols = config["symbols"]

    assert len(symbols) > 0