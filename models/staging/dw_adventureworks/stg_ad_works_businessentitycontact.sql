with source as (
    select * from {{ source('ad_works', 'person_businessentitycontact') }}
)
, renamed as (
    select
        cast(businessentityid as int) as business_entity_id
        , cast(personid as int) as person_id
        , cast(contacttypeid as int) as contact_type_id 
    from source
)
select * from renamed