select
    *
from 
    {{ ref('companiesDedupedData') }} as dd
where
    dd.country_code = 'US'