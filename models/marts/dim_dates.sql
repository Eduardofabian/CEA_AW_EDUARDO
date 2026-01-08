with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2011-01-01' as date)",
        end_date="cast('2015-01-01' as date)"
    )
    }}
)

, calculated as (
    select
        date_day
        , year(date_day) as year_num
        , month(date_day) as month_num
        , day(date_day) as day_num
        , date_format(date_day, 'MMMM') as month_name
        , date_format(date_day, 'EEEE') as day_name
    from date_spine
)

select 
    date_day as sk_date
    , *
from calculated