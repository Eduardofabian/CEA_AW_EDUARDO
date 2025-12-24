with order_header as (
    select *
    from {{ref('stg_ad_works_salesorderheader')}}
)
, order_detail as (
    select *
    from {{ref('stg_ad_works_sales_order_detail')}}



)
select * 
from order_header