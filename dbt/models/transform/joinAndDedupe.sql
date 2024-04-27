with ranked_websites as (
    select
        *,
        row_number() over (partition by website order by uid asc) as RowNumCounter
    from 
        {{ ref('companiesScrappedData') }}
)
select
    *
from 
    ranked_websites
where 
    RowNumCounter = 1