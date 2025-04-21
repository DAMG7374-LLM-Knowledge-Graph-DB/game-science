select
    *
from
    {{ source('scrappedWebsiteData', 'COMPANY_SCRAPPED_WEB_DATA') }}