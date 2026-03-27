with prices as (
    select * from {{ ref('stg_crypto_prices') }}
),

fx as (
    select * from {{ ref('stg_fx_rates') }}
),

coins as (
    select * from {{ ref('stg_coins') }}
),

joined as (
    select
        p.symbol,
        c.coin_name,
        c.market_cap_rank,  
        p.price_timestamp,
        p.open_price,
        p.high_price,
        p.low_price,
        p.price_usd,
        p.volume_crypto,
        f.usd_ngn_rate,
        (p.price_usd * f.usd_ngn_rate) as price_ngn
    from prices p
    left join fx f
        on cast(p.price_timestamp as date) = f.fx_date
    left join coins c
        on p.symbol = c.symbol
)

select * from joined