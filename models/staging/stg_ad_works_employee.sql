with source as (
    select * from {{ source('ad_works', 'humanresources_employee') }}
)
, renamed as (
    select
        cast(businessentityid as int) as business_entity_id
        , cast(jobtitle as string) as job_title
        , cast(nationalidnumber as string) as national_id
        , cast(currentflag as boolean) as is_current
        , cast(hiredate as string) as hire_date
        , cast(vacationhours as int) as vacantion_hours
    from source
)
select * from renamed