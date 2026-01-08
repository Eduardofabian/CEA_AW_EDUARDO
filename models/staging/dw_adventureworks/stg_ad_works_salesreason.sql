with source as (
    select * from {{ source('ad_works', 'sales_salesreason') }}
)

, renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['salesreasonid']) }} as sales_reason_sk
        , cast(salesreasonid as int) as sales_reason_id
        , cast(name as string) as reason_name
        , cast(reasontype as string) as reason_type
        , cast(modifieddate as timestamp) as modified_date
    from source
)

select * from renamed