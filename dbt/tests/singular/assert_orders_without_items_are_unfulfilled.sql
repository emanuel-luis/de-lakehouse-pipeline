{#
    An order with no item lines should be one that never reached fulfilment.
    Returns the offending orders. The synthetic source never has any; the published Olist data
    is not guaranteed to follow the rule exactly, so a handful of exceptions only warn and more
    than 10 fail the build.
#}
{{ config(warn_if='>0', error_if='>10') }}

select
    order_id,
    order_status

from {{ ref('fct_orders') }}
where total_items = 0
  and order_status not in ('canceled', 'unavailable', 'created')
