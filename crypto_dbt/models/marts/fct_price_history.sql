with intermediate_prices as (
    select * from {{ ref('int_hourly_prices_converted') }}
)
select
    symbol,
    coin_name,
    price_timestamp,
    open_price as open_usd,
    high_price as high_usd,
    low_price as low_usd,
    price_usd as close_usd,
    (open_price * usd_ngn_rate) as open_ngn,
    (high_price * usd_ngn_rate) as high_ngn,
    (low_price * usd_ngn_rate) as low_ngn,
    price_ngn as close_ngn,
    -- Metadata
    volume_crypto,
    usd_ngn_rate
from intermediate_prices