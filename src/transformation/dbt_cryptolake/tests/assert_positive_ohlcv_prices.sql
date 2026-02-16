select
    symbol,
    window_start,
    open,
    high,
    low,
    close
from {{ ref('fact_ohlcv_1m') }}
where open <= 0
   or high <= 0
   or low <= 0
   or close <= 0
