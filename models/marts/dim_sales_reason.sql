with source as (
    select *
    from {{ ref('stg_ad_works_salesreason') }}
)

, final as (
    select
        {{ dbt_utils.generate_surrogate_key(['sales_reason_id']) }} as sk_sales
        
        , sales_reason_id
        , reason_name as sales_reason_name
        , reason_type
    from source
)

select * from final