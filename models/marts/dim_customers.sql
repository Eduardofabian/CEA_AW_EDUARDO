with source as (
    select *
    from {{ ref('int_customers_prep') }}
)

, final as (
    select
        customer_sk as sk_customer
        , customer_id as id_customer
        , customer_name
        , customer_phone as customer_contact
        , customer_city
        , customer_region
        , customer_country
    from source
)

select * from final