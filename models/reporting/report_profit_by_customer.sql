select
    customerid,
    segment,
    Country,
    sum(orderprofit) as profit
from {{ ref('stg_orders') }}
group by
    customerid,
    segment,
    Country