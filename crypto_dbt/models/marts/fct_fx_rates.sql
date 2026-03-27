with fx as (
    select * from {{ ref('stg_fx_rates') }}
)

select 
    fx_date,
    usd_ngn_rate,
    usd_ngn_rate - lag(usd_ngn_rate) over (order by fx_date) as rate_change
from fx