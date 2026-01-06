with customers as (
    select *
    from {{ ref('stg_ad_works_customer') }}
)
, person as (
    select *
    from {{ ref('stg_ad_works_person') }}
)
, address as (
    select * from {{ ref('stg_ad_works_address') }}
)
, state_province as (
    select * from {{ ref('stg_ad_works_province') }}
)
, country_region as (
    select * from {{ ref('stg_ad_works_country_region') }}
)
, business_entity_address as (
    select * from {{ ref('stg_ad_works_businessentityaddress') }} -- Criamos esta na etapa anterior!
)

, joined as (
    select
        customers.customer_sk
        , customers.customer_id
        , customers.person_id
        , person.full_name as customer_name
        , address.city as customer_city
        , state_province.state_name as customer_region
        , country_region.country_name as customer_country
        
        , customers.store_id
    from customers
    left join person on customers.person_id = person.business_entity_id
    left join business_entity_address 
        on person.business_entity_id = business_entity_address.business_entity_id
    left join address 
        on business_entity_address.address_id = address.address_id
    left join state_province 
        on address.state_province_id = state_province.state_province_id
    left join country_region 
        on state_province.country_region_code = country_region.country_region_code
)

select * from joined