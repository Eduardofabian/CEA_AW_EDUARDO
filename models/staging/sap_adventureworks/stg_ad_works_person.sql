with source as (
    select * from {{source ('ad_works','person_person')}}
)
, renamed as (
    select
    -- PK
        {{ dbt_utils.generate_surrogate_key(['businessentityid']) }} as person_sk
        , cast(businessentityid as int) as business_entity_id
        , cast(persontype as string) as person_type
        , cast(firstname as string) as first_name
        , cast(middlename as string) as middle_name
        , cast(lastname as string) as last_name
        , trim(cast(firstname as string) || ' ' || coalesce(cast(middlename as string), '') || ' ' || cast(lastname as string)) as full_name
    from source
)

select * from renamed