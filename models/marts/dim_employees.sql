with source as (
    select *
    from {{ ref('int_employees_prep') }}
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
    from source
)

select * from final