with source as (
    select * from {{ source('ad_works', 'production_productcategory') }}
)
, renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['productcategoryid']) }} as product_category_sk
        , cast(productcategoryid as int) as product_category_id
        , cast(name as string) as category_name
        , cast(modifieddate as timestamp) as modified_date
    from source
)
select * from renamed