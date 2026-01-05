with source as (
    select * from {{ source('ad_works', 'production_productsubcategory') }}
)
, renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['productsubcategoryid']) }} as product_subcategory_sk
        , cast(productsubcategoryid as int) as product_subcategory_id
        , cast(productcategoryid as int) as product_category_id 
        , cast(name as string) as subcategory_name
        , cast(modifieddate as timestamp) as modified_date
    from source
)
select * from renamed