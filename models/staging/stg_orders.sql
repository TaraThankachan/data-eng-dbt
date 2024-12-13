select 
o.orderid,
o.orderdate,
o.shipdate,
o.shipmode,
o.ordersellingprice - o.ordercostprice as orderprofit,
o.ordercostprice,
o.ordersellingprice,
--from raw customer
c.C1 as customerid,
c.c2 as CustomerName,  -- Use exact case
c.C3 as Segment,
c.C4 as Country,
-- from raw product
p.C2 as productid,
p.C1 as category,
p.C3 as productname,
p.C4 as subcategory
from {{ ref('raw_orders') }} as o
left join {{ ref('raw_customer') }} as c
on o.customerid = c.C1
left join {{ ref('raw_product') }} as p
on o.productid = p.c2