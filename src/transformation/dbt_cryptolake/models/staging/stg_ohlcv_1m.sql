with source as (
    select * from {{ source('silver', 'ohlcv_1m') }}
)

select
    upper(cast(symbol as string)) as symbol,
    cast(window_start as timestamp) as window_start,
    cast(window_end as timestamp) as window_end,
    cast(open as double) as open,
    cast(high as double) as high,
    cast(low as double) as low,
    cast(close as double) as close,
    cast(volume as double) as volume,
    cast(trades as bigint) as trades,
    coalesce(cast(trade_date as date), to_date(cast(window_start as timestamp))) as trade_date,
    cast(updated_at as timestamp) as updated_at
from source
where symbol is not null
