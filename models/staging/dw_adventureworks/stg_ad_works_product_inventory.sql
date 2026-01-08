with source as (
    select * from {{ source('ad_works', 'production_productinventory') }}
)
, renamed as (
    select
        cast(productid as int) as product_id
        , sum(cast(quantity as int)) as units_in_stock
    from source
    group by productid
)
select * from renamed