select
    c.*,
    row_number() over(order by c.website) as UID
from
    {{ source('companiesData', 'companies') }} as c
join
    {{ ref('stage_scrappedData') }} as scd
        on c.website = scd.website