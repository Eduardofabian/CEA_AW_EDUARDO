with source as (
    select * from {{ source('ad_works', 'sales_store') }}
)

, renamed as (
    select
        businessentityid as business_entity_id
        , name as store_name
    from source
)

select * from renamed