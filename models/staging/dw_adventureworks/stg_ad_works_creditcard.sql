with source as (
    select * from {{ source('ad_works', 'sales_creditcard') }}
)
, renamed as (
    select
        cast(creditcardid as int) as credit_card_id
        , cast(cardtype as string) as card_type
    from source
)
select * from renamed