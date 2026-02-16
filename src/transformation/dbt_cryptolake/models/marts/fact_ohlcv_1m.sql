select
    symbol,
    window_start,
    window_end,
    trade_date,
    open,
    high,
    low,
    close,
    volume,
    trades,
    case
        when open is null or open = 0 then null
        else ((close - open) / open) * 100
    end as return_pct_1m,
    current_timestamp() as _loaded_at
from {{ ref('stg_ohlcv_1m') }}
