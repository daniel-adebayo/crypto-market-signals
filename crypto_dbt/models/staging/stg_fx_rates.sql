with source as (
    select * from {{ source('bronze', 'raw_fx_rates') }}
),

renamed as (
    select
        date as fx_date,
        rate as usd_ngn_rate,
        target_currency
    from source
),

final as (
    select *
    from renamed
    qualify row_number() over (
        partition by fx_date 
        order by fx_date desc
    ) = 1
)

select * from final