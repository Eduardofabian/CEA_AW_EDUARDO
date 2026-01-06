with source as (
    select *
    from {{ ref('int_sales_reasons_joined') }}
)

select * from source