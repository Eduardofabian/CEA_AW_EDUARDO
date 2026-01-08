with address as (
    select *
    from {{ ref('stg_ad_works_address') }}
)

, state_province as (
    select *
    from {{ ref('stg_ad_works_province') }}
)

, country_region as (
    select *
    from {{ ref('stg_ad_works_country_region') }}
)

, joined as (
    select
        address.address_id
        , address.address_sk
        , address.city
        , state_province.state_name
        , state_province.state_province_code
        , country_region.country_name
        , country_region.country_region_code
        , cast(address.city || ' - ' || state_province.state_province_code as string) as city_state_name

    from address
    left join state_province
        on address.state_province_id = state_province.state_province_id
    left join country_region 
        on state_province.country_region_code = country_region.country_region_code
)

select * from joined