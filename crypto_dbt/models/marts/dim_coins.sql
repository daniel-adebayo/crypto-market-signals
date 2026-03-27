with coins as (
    select * from {{ ref('stg_coins') }}
)

select 
    symbol,
    coin_name,
    market_cap_rank,
    image_url,
    case when market_cap_rank <= 5 then True else False end as is_top_5
from coins