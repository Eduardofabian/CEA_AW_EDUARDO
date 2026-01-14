with customers as (
    select *
    from {{ ref('stg_ad_works_customer') }}
)
, person as (
    select *
    from {{ ref('stg_ad_works_person') }}
)
, store as (
    select * from {{ ref('stg_ad_works_store') }}
)
, person_phone as (
    select *
    from {{ ref('stg_ad_works_personphone') }}
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
    select * from {{ ref('stg_ad_works_businessentityaddress') }}
)

, joined as (
    select
        customers.customer_id
        , customers.person_id
        , customers.store_id
        
        -- LÓGICA DE OURO: Se não achar Pessoa, pega o nome da Loja
        , coalesce(person.full_name, store.store_name, 'Cliente Desconhecido') as customer_name
        
        , person_phone.phone_number as customer_phone
        , address.city as customer_city
        , state_province.state_name as customer_region
        , country_region.country_name as customer_country
        
        , row_number() over (
            partition by customers.customer_id 
            order by person_phone.phone_number_type_id asc
          ) as rn_phone

    from customers
    left join person on customers.person_id = person.business_entity_id
    -- Novo Join com a tabela Store
    left join store on customers.store_id = store.business_entity_id
    
    left join person_phone 
        on person.business_entity_id = person_phone.business_entity_id
    left join business_entity_address 
        on coalesce(customers.person_id, customers.store_id) = business_entity_address.business_entity_id
    left join address 
        on business_entity_address.address_id = address.address_id
    left join state_province 
        on address.state_province_id = state_province.state_province_id
    left join country_region 
        on state_province.country_region_code = country_region.country_region_code
)

select * from joined
where rn_phone = 1