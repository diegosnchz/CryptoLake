# Contracts

## TradeEventV1

Canonical event for Binance trades:

- `event_version`: `"v1"`
- `source`: `"binance"`
- `symbol`: `str`
- `event_time_ms`: `int` (epoch milliseconds)
- `trade_time_ms`: `int | null`
- `price`: `float`
- `quantity`: `float`
- `trade_id`: `int | null`
- `is_buyer_maker`: `bool | null`
- `raw`: `dict | null` (debug only; not persisted to Iceberg bronze table)

## Compatibility policy

- Only add optional fields in future versions.
- Do not rename fields.
- Do not change existing field types.
- Consumers must remain compatible with both direct payloads and `{stream,data}` wrappers.

## Example

See `samples/trade_event_v1.json`.
