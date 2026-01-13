with orders as (
    select * from {{ ref('int_sales_enriched') }}
),
products as (
    select * from {{ ref('dim_products') }}
),
customers as (
    select * from {{ ref('dim_customers') }}
),
employees as (
    select * from {{ ref('dim_employees') }}
),
locations as (
    select * from {{ ref('dim_locations') }}
),
dates as (
    select * from {{ ref('dim_dates') }}
),
credit_card as (
    select * from {{ ref('dim_credit_card') }}
),
dim_reasons as (
    select * from {{ ref('dim_sales_reason') }}
)

, final as (
    select
        orders.sales_order_id
        , customers.sk_customer as fk_customer
        , employees.employee_sk as fk_employees
        , products.product_sk as fk_product
        , locations.address_sk as fk_locations 
        , dates.sk_date as fk_order_date
        , credit_card.sk_credit_card as fk_credit_card
        , dim_reasons.sk_sales as fk_sales 
        , orders.status as order_status
        , orders.sales_channel
        , orders.order_qty
        , orders.unit_price
        , orders.unit_price_discount
        , (orders.order_qty * orders.unit_price) as gross_amount
        , (orders.order_qty * orders.unit_price * (1 - orders.unit_price_discount)) as net_amount

    from orders
    left join products 
        on orders.product_id = products.product_id
    left join customers    
        on orders.customer_id = customers.id_customer
    left join employees
        on orders.salesperson_id = employees.employee_id
    left join locations
        on orders.address_id = locations.address_id
    left join dates 
        on orders.order_date = dates.date_day
    left join credit_card 
        on coalesce(orders.credit_card_id, -1) = credit_card.credit_card_id    
    left join dim_reasons
        on coalesce(orders.sales_reason_id, -1) = dim_reasons.sales_reason_id 
)

select * from final