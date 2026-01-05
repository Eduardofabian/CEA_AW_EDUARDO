with source as (
    select *
    from {{ ref('int_products_joined') }}
)

select * from source