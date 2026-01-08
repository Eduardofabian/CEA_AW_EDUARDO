with source as (
    select *
    from {{ ref('stg_ad_works_creditcard') }}
)

, final as (
    select
        {{ dbt_utils.generate_surrogate_key(['credit_card_id']) }} as sk_credit_card
        , credit_card_id
        , card_type
    from source
)

select * from final