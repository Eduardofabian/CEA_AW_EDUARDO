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

, joined as (
    select
        products.product_sk
        , products.product_id
        , products.product_name
        , products.color
        , products.list_price
        , products.reorder_point
        , products.safety_stock_level
        , subcategory.subcategory_name
        , category.category_name
        , coalesce(inventory.units_in_stock, 0) as units_in_stock
        , vendor.supplier_name

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
)

select *
from joined
qualify row_number() over (partition by product_id order by supplier_name) = 1