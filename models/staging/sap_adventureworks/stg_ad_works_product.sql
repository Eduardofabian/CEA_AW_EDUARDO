with source as (
    select * from {{ source('ad_works', 'production_product') }}
)

, renamed as (
    select
        -- PK
        {{ dbt_utils.generate_surrogate_key(['productid']) }} as product_sk
        , cast(productid as int) as product_id
        , cast(name as string) as product_name
        , cast(productnumber as string) as product_number
        , cast(coalesce(color, 'No Color') as string) as color
        , cast(coalesce(class, 'No Class') as string) as class_name
        , cast(coalesce(style, 'No Style') as string) as style_name
        , cast(standardcost as float) as standard_cost
        , cast(listprice as float) as list_price
        , cast(reorderpoint as int) as reorder_point
        , cast(safetystocklevel as int) as safety_stock_level
        , cast(productsubcategoryid as int) as product_subcategory_id
        , cast(modifieddate as timestamp) as modified_date
    from source
)

select * from renamed