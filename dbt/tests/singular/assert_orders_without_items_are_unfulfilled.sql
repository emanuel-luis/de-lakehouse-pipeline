{#
    An order with no item lines can only be one that never reached fulfilment.
    Returns the offending orders, so the test fails when any exist.
#}
select
    order_id,
    order_status

from {{ ref('fct_orders') }}
where total_items = 0
  and order_status not in ('canceled', 'unavailable', 'created')
