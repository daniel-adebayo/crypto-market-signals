with source as (
    select * from {{ source('bronze', 'target_coins') }}
),
final as (
    select
        coin_id,
        name as coin_name,
        binance_symbol as symbol,
        rank as market_cap_rank
    from source
)
select * from final