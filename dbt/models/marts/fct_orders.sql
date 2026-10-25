with

orders as (

    select * from {{ ref('stg_olist__orders') }}

),

customers as (

    select
        customer_id,
        customer_unique_id

    from {{ ref('stg_olist__customers') }}

),

items as (

    select
        order_id,
        count(*) as total_items,
        sum(price) as total_amount,
        sum(freight_value) as total_freight

    from {{ ref('stg_olist__order_items') }}
    group by order_id

),

final as (

    select
        orders.order_id::varchar as order_id,
        orders.customer_id::varchar as customer_id,
        customers.customer_unique_id::varchar as customer_unique_id,
        orders.order_status::varchar as order_status,
        orders.order_purchase_timestamp::timestamp as order_purchase_timestamp,
        orders.order_approved_at::timestamp as order_approved_at,
        orders.order_delivered_carrier_date::timestamp as order_delivered_carrier_date,
        orders.order_delivered_customer_date::timestamp as order_delivered_customer_date,
        orders.order_estimated_delivery_date::timestamp as order_estimated_delivery_date,
        coalesce(items.total_items, 0)::bigint as total_items,
        coalesce(items.total_amount, 0)::decimal(18, 2) as total_amount,
        coalesce(items.total_freight, 0)::decimal(18, 2) as total_freight,
        date_sub(
            'day',
            orders.order_purchase_timestamp,
            orders.order_delivered_customer_date
        )::integer as delivery_days,
        -- The estimated date is a calendar day (Olist stores it at midnight): compare dates, so
        -- a delivery on the promised day is on time.
        coalesce(
            orders.order_delivered_customer_date::date
                > orders.order_estimated_delivery_date::date,
            false
        )::boolean as is_late_delivery

    from orders
    left join customers on orders.customer_id = customers.customer_id
    left join items on orders.order_id = items.order_id

)

select * from final
