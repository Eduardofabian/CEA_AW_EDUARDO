with source as (
    select *
    from {{ ref('stg_ad_works_creditcard') }}
)

, ghost_record as (
    select
        -1 as credit_card_id
        , 'Outros Métodos' as card_type
)

, united as (
    select * from source
    union all
    select * from ghost_record
)

, final as (
    select
        {{ dbt_utils.generate_surrogate_key(['credit_card_id']) }} as sk_credit_card
        , credit_card_id
        , card_type
    from united
)

select * from final