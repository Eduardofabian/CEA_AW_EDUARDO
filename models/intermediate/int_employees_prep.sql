with employees as (
    select *
    from {{ ref('stg_ad_works_employee') }}
)

, person as (
    select *
    from {{ ref('stg_ad_works_person') }}
)

, business_entity_address as (
    select *
    from {{ ref('stg_ad_works_businessentityaddress') }}
)

, address as (
    select *
    from {{ ref('stg_ad_works_address') }}
)

, state_province as (
    select *
    from {{ ref('stg_ad_works_province') }}
)

, country_region as (
    select *
    from {{ ref('stg_ad_works_country_region') }}
)

, joined as (
    select
        employees.business_entity_id
        , employees.job_title
        , employees.national_id
        , employees.is_current
        , person.full_name as employee_name
        , address.city
        , state_province.state_name
        , country_region.country_name
        
    from employees
    left join person 
        on employees.business_entity_id = person.business_entity_id
    left join business_entity_address
        on employees.business_entity_id = business_entity_address.business_entity_id
    left join address
        on business_entity_address.address_id = address.address_id
    left join state_province
        on address.state_province_id = state_province.state_province_id
    left join country_region
        on state_province.country_region_code = country_region.country_region_code
)

select * from joined