select
    symbol,
    min(window_start) as first_seen_at,
    max(window_start) as last_seen_at,
    count(*) as candle_count,
    current_timestamp() as _loaded_at
from {{ ref('stg_ohlcv_1m') }}
group by symbol
