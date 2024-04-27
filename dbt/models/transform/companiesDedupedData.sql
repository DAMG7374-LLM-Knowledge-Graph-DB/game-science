{{  config(
        materialized='incremental'
) }}

with ranked_websites as (
    select
        *,
        row_number() over (partition by website order by uid asc) as RowNum
    from 
        {{ ref('joinAndDedupe') }}
)
select
    *
from 
    ranked_websites
where 
    RowNum = 1