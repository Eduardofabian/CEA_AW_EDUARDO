with source as (
    select * from {{ source('ad_works', 'person_countryregion') }}
)

, renamed as (
    select
        -- PK
        {{ dbt_utils.generate_surrogate_key(['countryregioncode']) }} as country_region_sk
        , cast(countryregioncode as string) as country_region_code
        , cast(name as string) as country_name
        , cast(modifieddate as timestamp) as modified_date
    from source
)

select * from renamed