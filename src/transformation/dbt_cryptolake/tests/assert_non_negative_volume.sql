select
    symbol,
    window_start,
    volume
from {{ ref('fact_ohlcv_1m') }}
where volume < 0
