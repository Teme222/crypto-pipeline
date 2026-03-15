select
    coin,
    price_eur,
    change,
    fetched_at,
    date(fetched_at) as date
from raw_prices