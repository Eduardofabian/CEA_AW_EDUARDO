with source as (
    select * from {{ source('ad_works', 'sales_salesorderdetail') }}
)

, renamed as (
    select
        -- PK
        {{ dbt_utils.generate_surrogate_key(['salesorderid', 'salesorderdetailid']) }} as sales_order_detail_sk
        , cast(salesorderid as int) as sales_order_id
        , cast(salesorderdetailid as int) as order_detail_id
        , cast(orderqty as int) as order_qty
        , cast(productid as int) as product_id
        , cast(specialofferid as int) as specialoffer_id
        , cast(unitprice as float) as unit_price
        , cast(unitpricediscount as float) as unit_price_discount
        , cast(modifieddate as timestamp) as modified_date
    from source
)

select * from renamed