from collections.abc import Mapping
from typing import Any


def is_valid_trade_record(record: Mapping[str, Any]) -> bool:
    symbol = str(record.get("symbol", "")).strip()
    if not symbol:
        return False

    event_time = record.get("event_time")
    event_time_ms = record.get("event_time_ms")
    if event_time is None and event_time_ms is None:
        return False

    price = record.get("price")
    quantity = record.get("quantity", record.get("qty"))
    if price is None or quantity is None:
        return False

    try:
        price_value = float(price)
        quantity_value = float(quantity)
    except (TypeError, ValueError):
        return False

    return price_value > 0 and quantity_value >= 0
