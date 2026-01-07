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
sales_reasons as (
    select * from {{ ref('int_sales_reasons_joined') }}
),
dim_reasons as (
    select * from {{ ref('dim_sales_reason') }}
)

, first_reason_per_order as (
    select
        sales_order_id
        , sales_reason_id
    from sales_reasons
    qualify row_number() over (partition by sales_order_id order by sales_reason_id) = 1
)

, final as (
    select
        orders.sales_order_id
        , customers.sk_customer as fk_customer
        , employees.employee_sk as fk_employees
        , products.product_sk as fk_product
        , locations.address_sk as fk_locations 
        , credit_card.sk_credit_card as fk_credit_card
        , dates.sk_date as fk_order_date
        , dim_reasons.sk_sales as fk_sales 
        , orders.status as order_status
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
    left join credit_card 
        on orders.credit_card_id = credit_card.credit_card_id    
    left join dates 
        on orders.order_date = dates.date_day
    left join first_reason_per_order
        on orders.sales_order_id = first_reason_per_order.sales_order_id
    left join dim_reasons
        on first_reason_per_order.sales_reason_id = dim_reasons.sales_reason_id 
)

select * from final