with order_header as (
    select 
        sales_order_id
        , customer_id        
        , salesperson_id     
        , ship_to_address_id 
        , credit_card_id     
        , order_date
        , ship_date
        , status
    from {{ ref('stg_ad_works_salesorderheader') }} 
),

order_detail as (
    select 
        sales_order_id
        , sales_order_detail_sk
        , product_id         
        , order_qty          
        , unit_price         
        , unit_price_discount 
    from {{ ref('stg_ad_works_order_detail') }} 
),

joined as (
    select 
        order_header.sales_order_id
        , order_header.customer_id
        , order_header.salesperson_id
        , order_header.ship_to_address_id as address_id 
        , order_header.credit_card_id
        , order_detail.product_id
        , order_detail.sales_order_detail_sk
        , order_header.order_date
        , order_header.ship_date
        , order_header.status
        , order_detail.order_qty
        , order_detail.unit_price
        , order_detail.unit_price_discount

    from order_header
    left join order_detail on order_header.sales_order_id = order_detail.sales_order_id
)

select * from joined