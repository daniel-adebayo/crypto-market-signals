with source as (
    select * from {{ source('bronze', 'raw_klines') }}
),

renamed as (
    select
        symbol,
        open_time as price_timestamp,
        open as open_price,
        high as high_price,
        low as low_price,
        close as price_usd,
        volume as volume_crypto,
        close_time
    from source
),

final as (
    select *
    from renamed
    qualify row_number() over (
        partition by symbol, price_timestamp 
        order by close_time desc
    ) = 1
)

select * from final