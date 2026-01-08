with sales as (
    select 
        gross_amount
        , fk_order_date 
    from {{ ref('fct_sales') }}
)

, sum_2011 as (
    select 
        sum(gross_amount) as total_2011
    from sales
    where fk_order_date >= '2011-01-01' 
      and fk_order_date <= '2011-12-31'
)

select *
from sum_2011
where not (total_2011 between 12646112.00 and 12646112.20)