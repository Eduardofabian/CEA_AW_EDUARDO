with sales_reason_header as (
    select *
    from {{ ref('stg_ad_works_salesorderheadersalesreason') }}
)

, sales_reason as (
    select *
    from {{ ref('stg_ad_works_salesreason') }}
)

, joined as (
    select
        sales_reason_header.sales_order_header_reason_sk
        , sales_reason_header.sales_order_id
        , sales_reason_header.sales_reason_id
        , sales_reason.reason_name
        , sales_reason.reason_type

    from sales_reason_header
    left join sales_reason 
        on sales_reason_header.sales_reason_id = sales_reason.sales_reason_id
)

select * from joined