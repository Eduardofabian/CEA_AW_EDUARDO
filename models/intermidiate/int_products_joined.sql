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

    from products
    left join subcategory 
        on products.product_subcategory_id = subcategory.product_subcategory_id
    left join category 
        on subcategory.product_category_id = category.product_category_id
)

select * from joined