from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, field_validator


class TradeEventV1(BaseModel):
    model_config = ConfigDict(extra="ignore", allow_inf_nan=False)

    event_version: Literal["v1"] = "v1"
    source: Literal["binance"] = "binance"
    symbol: str
    event_time_ms: int
    trade_time_ms: int | None = None
    price: float
    quantity: float
    trade_id: int | None = None
    is_buyer_maker: bool | None = None
    raw: dict[str, Any] | None = None

    @field_validator("symbol")
    @classmethod
    def _normalize_symbol(cls, value: str) -> str:
        symbol = value.strip().upper()
        if not symbol:
            raise ValueError("symbol cannot be empty")
        return symbol


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    return None


def normalize_binance_payload_to_trade_event_v1(payload: dict[str, Any]) -> TradeEventV1:
    if not isinstance(payload, dict):
        raise ValueError("Trade payload must be a JSON object")

    candidate = payload.get("data", payload)
    if not isinstance(candidate, dict):
        raise ValueError("Trade payload.data must be a JSON object")

    symbol = candidate.get("symbol", candidate.get("s"))
    event_time_ms = _coerce_int(candidate.get("event_time_ms", candidate.get("E", candidate.get("T"))))
    trade_time_ms = _coerce_int(candidate.get("trade_time_ms", candidate.get("T")))
    price = _coerce_float(candidate.get("price", candidate.get("p")))
    quantity = _coerce_float(candidate.get("quantity", candidate.get("q")))
    trade_id = _coerce_int(candidate.get("trade_id", candidate.get("a")))
    is_buyer_maker = _coerce_bool(candidate.get("is_buyer_maker", candidate.get("m")))

    missing: list[str] = []
    if symbol in (None, ""):
        missing.append("symbol")
    if price is None:
        missing.append("price")
    if quantity is None:
        missing.append("quantity")
    if event_time_ms is None:
        missing.append("event_time_ms")
    if missing:
        raise ValueError(f"Missing required trade event fields: {', '.join(missing)}")

    return TradeEventV1(
        event_version="v1",
        source="binance",
        symbol=str(symbol),
        event_time_ms=event_time_ms,
        trade_time_ms=trade_time_ms,
        price=price,
        quantity=quantity,
        trade_id=trade_id,
        is_buyer_maker=is_buyer_maker,
    )
