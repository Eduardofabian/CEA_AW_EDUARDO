with source as (
    select * from {{ source('ad_works', 'sales_customer') }}
)

, renamed as (
    select
        -- PK
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customer_sk
        , cast(customerid as int) as customer_id
        , cast(personid as int) as person_id
        , cast(storeid as int) as store_id
        , cast(territoryid as int) as territory_id
        , cast(modifieddate as timestamp) as modified_date
    from source
)

select * from renamed