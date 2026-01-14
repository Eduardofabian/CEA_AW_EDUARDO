with source as (
    select
        customer_id
        , customer_name
        , customer_phone
        , customer_city
        , customer_region
        , customer_country
    from {{ ref('int_customers_prep') }}
)

, ghost_record as (
    select
        -1 as customer_id
        , 'Cliente Não Identificado' as customer_name
        , 'N/A' as customer_phone
        , 'N/A' as customer_city
        , 'N/A' as customer_region
        , 'N/A' as customer_country
)

, united as (
    select * from source
    union all
    select * from ghost_record
)

, final as (
    select
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as sk_customer
        , customer_id as id_customer
        , customer_name
        , customer_phone as customer_contact
        , customer_city
        , customer_region
        , customer_country
    from united
)

select * from final