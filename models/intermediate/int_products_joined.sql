with products as (
    select *
    from {{ ref('stg_ad_works_product') }}
)

, subcategory as (
    select *
    from {{ ref('stg_ad_works_product_subcategory') }}
)

, category as (
    select *
    from {{ ref('stg_ad_works_product_category') }}
)

, inventory as (
    select *
    from {{ ref('stg_ad_works_product_inventory') }}
)

, product_vendor as (
    select *
    from {{ ref('stg_ad_works_product_vendor') }}
)

, vendor as (
    select *
    from {{ ref('stg_ad_works_vendor') }}
)

, business_entity_address as (
    select * from {{ ref('stg_ad_works_businessentityaddress') }}
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

, business_entity_contact as (
    select * from {{ ref('stg_ad_works_businessentitycontact') }}
)
, person as (
    select * from {{ ref('stg_ad_works_person') }}
)

, joined as (
    select
        products.product_sk
        , products.product_id
        , products.product_name
        , products.color
        , products.list_price
        , products.reorder_point
        , products.safety_stock_level
        , case 
            when products.sell_end_date is not null then true 
            else false 
          end as is_discontinued
        , subcategory.subcategory_name
        , category.category_name
        , coalesce(inventory.units_in_stock, 0) as units_in_stock 
        , vendor.supplier_name
        , address.city as supplier_city
        , country_region.country_name as supplier_country
        , person.full_name as supplier_contact

    from products
    left join subcategory 
        on products.product_subcategory_id = subcategory.product_subcategory_id
    left join category 
        on subcategory.product_category_id = category.product_category_id
    left join inventory
        on products.product_id = inventory.product_id  
    left join product_vendor
        on products.product_id = product_vendor.product_id
    left join vendor
        on product_vendor.vendor_id = vendor.vendor_id
    left join business_entity_address
        on vendor.vendor_id = business_entity_address.business_entity_id
        and business_entity_address.address_type_id = 3 -- Main Office
    left join address
        on business_entity_address.address_id = address.address_id
    left join state_province
        on address.state_province_id = state_province.state_province_id
    left join country_region
        on state_province.country_region_code = country_region.country_region_code
    left join business_entity_contact
        on vendor.vendor_id = business_entity_contact.business_entity_id
    left join person
        on business_entity_contact.person_id = person.business_entity_id
)

select *
from joined
qualify row_number() over (partition by product_id order by supplier_name) = 1