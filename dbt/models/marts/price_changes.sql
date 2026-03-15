select
    coin,
    price_eur,
    round(change::numeric, 4)  as change_24h_pct,
    fetched_at
from {{ ref('stg_raw_prices') }}
where fetched_at = (
    select max(fetched_at)
    from {{ ref('stg_raw_prices') }} as inner_t
    where inner_t.coin = stg_raw_prices.coin
)