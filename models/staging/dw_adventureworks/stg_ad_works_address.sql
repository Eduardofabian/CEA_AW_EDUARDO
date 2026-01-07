with source as (
    select * from {{ source('ad_works', 'person_address') }}
)

, renamed as (
    select
        -- PK
        {{ dbt_utils.generate_surrogate_key(['addressid']) }} as address_sk
        , cast(addressid as int) as address_id
        , cast(stateprovinceid as int) as state_province_id
        , cast(addressline1 as string) as address_line_1
        , cast(addressline2 as string) as address_line_2
        , cast(city as string) as city
        , cast(postalcode as string) as postal_code
        , cast(modifieddate as timestamp) as modified_date
    from source
)

select * from renamed