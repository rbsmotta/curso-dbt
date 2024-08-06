-- select
--     *,
--     od.unit_price * od.quantity as total,
--     ( pr.unit_price * od.quantity ) - total as discount
-- from 
--     {{ source('sources', 'order_details') }} as od
-- left join
--     {{ source('sources', 'products') }} as pr
-- on
--     od.product_id = pr.product_id

with total as (
  select
    order_id,
    product_id,
    unit_price,
    quantity,
    unit_price * quantity as total
  from 
      {{ source('sources', 'order_details') }}
)
select
  t.order_id,
  t.product_id,
  t.unit_price,
  t.quantity,
  pr.product_name,
  pr.supplier_id,
  pr.category_id,
  t.total,
  ( pr.unit_price * t.quantity ) - total as discount
from 
    total as t
left join
    {{ source('sources', 'products') }} as pr
on
    (t.product_id = pr.product_id)
    