with base_prices as (
    select * from {{ ref('int_hourly_prices_converted') }}
),

calculations_step_1 as (
    select 
        *,
        price_usd - lag(price_usd) over (partition by symbol order by price_timestamp) as price_change
    from base_prices
),

calculations_step_2 as (
    select
        *,
        avg(price_usd) over (
            partition by symbol 
            order by price_timestamp 
            rows between 167 preceding and current row
        ) as moving_avg_7d_usd,
        (high_price - low_price) / nullif(open_price, 0) as hourly_volatility_pct,
        case when price_change > 0 then price_change else 0 end as gain,
        case when price_change < 0 then abs(price_change) else 0 end as loss
    from calculations_step_1
),

calculations_step_3 as (
    select
        *,
        avg(gain) over (
            partition by symbol 
            order by price_timestamp 
            rows between 13 preceding and current row
        ) as avg_gain_14h,
        avg(loss) over (
            partition by symbol 
            order by price_timestamp 
            rows between 13 preceding and current row
        ) as avg_loss_14h
    from calculations_step_2
),
final_indicators as (
    select
        symbol,
        coin_name,
        price_timestamp,
        price_usd,
        price_ngn,
        usd_ngn_rate,
        volume_crypto,
        moving_avg_7d_usd,
        hourly_volatility_pct,
        case 
            when avg_loss_14h = 0 then 100
            else 100 - (100 / (1 + (avg_gain_14h / avg_loss_14h)))
        end as rsi_14h,
        case 
            when price_usd > moving_avg_7d_usd then 'Bullish'
            else 'Bearish'
        end as trend_signal

    from calculations_step_3
)

select * from final_indicators