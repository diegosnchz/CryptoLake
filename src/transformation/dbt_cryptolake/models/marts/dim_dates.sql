with distinct_dates as (
    select distinct trade_date
    from {{ ref('stg_ohlcv_1m') }}
    where trade_date is not null
)

select
    trade_date as date_day,
    year(trade_date) as year_num,
    month(trade_date) as month_num,
    day(trade_date) as day_num,
    quarter(trade_date) as quarter_num,
    dayofweek(trade_date) as day_of_week_num,
    case when dayofweek(trade_date) in (1, 7) then true else false end as is_weekend,
    current_timestamp() as _loaded_at
from distinct_dates
