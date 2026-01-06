with source as (
    select *
    from {{ source('ad_works', 'production_product') }}
)

, renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['productid']) }} as product_sk
        , cast(productid as int) as product_id
        , cast(productsubcategoryid as int) as product_subcategory_id
        , cast(name as string) as product_name
        , cast(productnumber as string) as product_number
        , cast(color as string) as color
        , cast(safetystocklevel as int) as safety_stock_level
        , cast(reorderpoint as int) as reorder_point
        , cast(standardcost as numeric) as standard_cost
        , cast(listprice as numeric) as list_price
        , cast(size as string) as size
        , cast(weight as numeric) as weight
        , cast(daystomanufacture as int) as days_to_manufacture
        , cast(productline as string) as product_line
        , cast(class as string) as class_name
        , cast(style as string) as style_name
        , cast(sellstartdate as timestamp) as sell_start_date
        , cast(sellenddate as timestamp) as sell_end_date
        
        , cast(modifieddate as timestamp) as modified_date
    from source
)

select * from renamed