with source as (
    select * from {{ source('ad_works', 'sales_salesorderheadersalesreason') }}
)

, renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['salesorderid', 'salesreasonid']) }} as sales_order_header_reason_sk
        , cast(salesorderid as int) as sales_order_id
        , cast(salesreasonid as int) as sales_reason_id
        , cast(modifieddate as timestamp) as modified_date
    from source
)

select * from renamed