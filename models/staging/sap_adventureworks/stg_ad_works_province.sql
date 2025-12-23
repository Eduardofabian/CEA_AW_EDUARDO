with source as (
    select * from {{ source('ad_works', 'person_stateprovince') }}
)

, renamed as (
    select
        -- PK
        {{ dbt_utils.generate_surrogate_key(['stateprovinceid']) }} as state_province_sk
        , cast(stateprovinceid as int) as state_province_id
        , cast(territoryid as int) as territory_id
        , cast(stateprovincecode as string) as state_province_code
        , cast(countryregioncode as string) as country_region_code
        , cast(name as string) as state_name
        , cast(isonlystateprovinceflag as boolean) as is_only_state_province
        , cast(modifieddate as timestamp) as modified_date
    from source
)

select * from renamed