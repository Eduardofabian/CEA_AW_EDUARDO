with source as (
    select *
    from {{ ref('int_locations_joined') }}
)

select * from source