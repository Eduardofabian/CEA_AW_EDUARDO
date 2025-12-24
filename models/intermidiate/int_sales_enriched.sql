with order_header as (
    select *
    from {{ref('stg_ad_works_salesorderheader')}}
)
, order_detail as (
    select *
    from {{ref('stg_ad_works_sales_order_detail')}}
)
, joined as (
    select *
    from order_header
    left join order_detail
        on order_header.sales_order_id = order_detail.sales_order_id
)