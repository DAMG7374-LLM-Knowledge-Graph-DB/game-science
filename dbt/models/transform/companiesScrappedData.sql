select
    c.website,
    c.handle,
    c.name,
    c.type,
    c.industry,
    c.size,
    c.founded,
    c.country_code,
    c.city,
    c.state,
    c.UID,
    csd.CREATEDATE as scrappedDate,
    csd.SCRAPPEDDATA
from
    {{ ref('stage_scrappedData') }} as csd
right join
    {{ ref('stage_companies') }} as c
        -- on c.website = csd.website
        on TRIM(c.website) = TRIM(csd.website)
where
    c.industry not like '%automotive%'
    and csd.website IS NOT NULL
    and csd.website NOT ILIKE '%facebook%'
    and csd.website NOT ILIKE '%linkedin%'
    and csd.website NOT ILIKE '%wordpress%'
    and csd.website NOT ILIKE '%instagram%'
    and csd.SCRAPPEDDATA not like '%HTTP Error%'
    and csd.SCRAPPEDDATA not like '%Request Error%'
    and csd.SCRAPPEDDATA IS NOT NULL