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
        , onlineorderflag
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

header_sales_reason as (
    select * from {{ ref('stg_ad_works_salesorderheadersalesreason') }}
),

sales_reason as (
    select * from {{ ref('stg_ad_works_salesreason') }}
),
best_reason_id as (
    select 
        h.sales_order_id,      
        h.sales_reason_id,     
        row_number() over (
            partition by h.sales_order_id 
            order by case when r.reason_type = 'Promotion' then 1 else 2 end 
        ) as rn
    from header_sales_reason h
    left join sales_reason r on h.sales_reason_id = r.sales_reason_id
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
        , case 
            when order_header.onlineorderflag = true then 'Online'
            else 'Revenda'
        end as sales_channel
        , best_reason_id.sales_reason_id
        , order_detail.order_qty
        , order_detail.unit_price
        , order_detail.unit_price_discount

    from order_header
    left join order_detail on order_header.sales_order_id = order_detail.sales_order_id
    left join best_reason_id on order_header.sales_order_id = best_reason_id.sales_order_id and best_reason_id.rn = 1
)

select * from joined