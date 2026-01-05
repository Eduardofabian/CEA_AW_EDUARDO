with customers as (
    select *
    from {{ref('stg_ad_works_customer')}}
)
, person as (
    select *
    from {{ref('stg_ad_works_person')}}
)
, joined as (
    select 
        customers.customer_sk
        , customers.customer_id
        , customers.person_id
        , person.full_name as customer_name
        , customers.store_id
    from customers
    left join person
        on customers.person_id = person.business_entity_id
)
select * from joined