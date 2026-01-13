with source as (
    select * from {{ source('ad_works', 'person_personphone') }}
)
, renamed as (
    select
        cast(businessentityid as int) as business_entity_id
        , cast(phonenumber as string) as phone_number
        , cast(phonenumbertypeid as int) as phone_number_type_id
    from source
)
select * from renamed