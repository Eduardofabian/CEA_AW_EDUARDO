with source as (
    select * from {{ source('ad_works', 'purchasing_productvendor') }}
)
, renamed as (
    select
        cast(productid as int) as product_id
        , cast(businessentityid as int) as vendor_id
    from source
)
select * from renamed