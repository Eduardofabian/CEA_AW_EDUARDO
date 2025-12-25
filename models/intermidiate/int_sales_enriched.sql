with order_header as (
    select 
        sales_order_id
        ,customer_id       -- Vai virar fk_customer na Fato
        ,salesperson_id   -- Vai virar fk_employee na Fato
        ,bill_to_address_id -- Vai virar fk_location na Fato
        ,credit_card_id,    -- Vai virar fk_credit_card na Fato
        ,order_date
        ,ship_date
        status
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