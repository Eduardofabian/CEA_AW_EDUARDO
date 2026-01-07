with source as (
    select * from {{ source('ad_works', 'person_businessentityaddress') }}
)
, renamed as (
    select
        cast(businessentityid as int) as business_entity_id
        , cast(addressid as int) as address_id
        , cast(addresstypeid as int) as address_type_id
    from source
)
select * from renamed