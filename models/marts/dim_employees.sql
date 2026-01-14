with source as (
    select
        business_entity_id
        , employee_name
        , job_title
        , city
        , state_name
        , country_name
        , is_current
    from {{ ref('int_employees_prep') }}
)

, ghost_record as (
    select
        -1 as business_entity_id
        , 'Venda Online / Sem Vendedor' as employee_name
        , 'System' as job_title
        , 'N/A' as city
        , 'N/A' as state_name
        , 'N/A' as country_name
        , cast(1 as boolean) as is_current
)

, united as (
    select * from source
    union all
    select * from ghost_record
)

, final as (
    select
        {{ dbt_utils.generate_surrogate_key(['business_entity_id']) }} as employee_sk
        , business_entity_id as employee_id
        , employee_name
        , job_title as job_position
        , city as employee_city
        , state_name as employee_region
        , country_name as employee_country
        , is_current
    from united
)

select * from final