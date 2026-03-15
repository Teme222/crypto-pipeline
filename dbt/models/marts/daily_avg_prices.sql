select
    coin,
    date,
    round(avg(price_eur)::numeric, 2)   as avg_price_eur,
    round(min(price_eur)::numeric, 2)   as min_price_eur,
    round(max(price_eur)::numeric, 2)   as max_price_eur,
    count(*)                            as num_samples
from {{ ref('stg_raw_prices') }}
group by coin, date
order by coin, date desc