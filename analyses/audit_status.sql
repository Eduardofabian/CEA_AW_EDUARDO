select distinct 
    status
from {{ source('ad_works', 'sales_salesorderheader') }} 
order by 1