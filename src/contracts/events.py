from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict

from pydantic import BaseModel, Field


class FuturesTradeEvent(BaseModel):
    symbol: str
    event_time: datetime
    price: Decimal
    qty: Decimal
    side: str = Field(description="buy|sell inferred from buyer maker flag")
    trade_id: int
    exchange: str = "binance_futures"
    ingest_ts: datetime


def parse_binance_aggtrade(
    payload: Dict[str, Any], ingest_ts: datetime | None = None
) -> FuturesTradeEvent:
    ts = ingest_ts or datetime.now(timezone.utc)
    try:
        symbol = str(payload["s"]).upper()
        event_time = datetime.fromtimestamp(int(payload["T"]) / 1000, tz=timezone.utc)
        price = Decimal(str(payload["p"]))
        qty = Decimal(str(payload["q"]))
        trade_id = int(payload["a"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"Invalid Binance aggTrade payload: {payload}") from exc

    side = "sell" if payload.get("m", False) else "buy"
    return FuturesTradeEvent(
        symbol=symbol,
        event_time=event_time,
        price=price,
        qty=qty,
        side=side,
        trade_id=trade_id,
        ingest_ts=ts,
    )
