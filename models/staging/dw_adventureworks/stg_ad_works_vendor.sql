with source as (
    select * from {{ source('ad_works', 'purchasing_vendor') }}
)
, renamed as (
    select
        cast(businessentityid as int) as vendor_id
        , cast(name as string) as supplier_name
    from source
)
select * from renamed