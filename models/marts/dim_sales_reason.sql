with source as (
    select
        sales_reason_id
        , reason_name
        , reason_type
    from {{ ref('stg_ad_works_salesreason') }}
)

, ghost_record as (
    select
        -1 as sales_reason_id
        , 'Não Informado' as reason_name
        , 'Outros' as reason_type
)

, united as (
    select * from source
    union all
    select * from ghost_record
)

, final as (
    select
        {{ dbt_utils.generate_surrogate_key(['sales_reason_id']) }} as sk_sales
        , sales_reason_id
        , reason_name as sales_reason_name
        , reason_type
    from united
)

select * from final