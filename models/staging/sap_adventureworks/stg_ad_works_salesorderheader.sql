with source as (
    select * from {{ source('ad_works', 'salesorderheader') }}
)

, renamed as (
    select
        cast(salesorderid as int) as sales_order_id
        , cast(customerid as int) as customer_id
        , cast(salespersonid as int) as salesperson_id
        , cast(territoryid as int) as territory_id
        , cast(billtoaddressid as int) as bill_to_address_id
        , cast(shiptoaddressid as int) as ship_to_address_id
        , cast(creditcardid as int) as credit_card_id
        , cast(orderdate as timestamp) as order_date
        , cast(duedate as timestamp) as due_date
        , cast(shipdate as timestamp) as ship_date
        , cast(status as int) as status
        , cast(onlineorderflag as boolean) as is_online_order
        , cast(purchaseordernumber as string) as purchase_order_number
        , cast(accountnumber as string) as account_number
        , cast(subtotal as float) as subtotal
        , cast(taxamt as float) as tax_amount
        , cast(freight as float) as freight
        , cast(totaldue as float) as total_due
        , cast(modifieddate as timestamp) as modified_date
    from source
)

select * from renamed