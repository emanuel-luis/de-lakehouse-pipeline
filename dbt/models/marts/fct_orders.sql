with

orders as (

    select * from {{ ref('bronze_orders') }}

),

customers as (

    select
        customer_id,
        customer_unique_id

    from {{ ref('bronze_customers') }}

),

items as (

    select
        order_id,
        count(*) as total_items,
        sum(price) as total_amount,
        sum(freight_value) as total_freight,
        count(distinct product_id) as distinct_products,
        count(distinct seller_id) as distinct_sellers

    from {{ ref('int_order_items_enriched') }}
    group by order_id

),

payments as (

    select
        order_id,
        sum(payment_value) as total_payment_value

    from {{ ref('bronze_payments') }}
    group by order_id

),

reviews as (

    select
        order_id,
        avg(review_score) as avg_review_score

    from {{ ref('int_review_enriched') }}
    group by order_id

),

final as (

    select
        orders.order_id::varchar as order_id,
        orders.customer_id::varchar as customer_id,
        customers.customer_unique_id::varchar as customer_unique_id,
        strftime(orders.order_purchase_timestamp, '%Y%m%d')::integer as order_date_key,
        orders.order_status::varchar as order_status,
        orders.order_purchase_timestamp::timestamp as order_purchase_timestamp,
        orders.order_approved_at::timestamp as order_approved_at,
        orders.order_delivered_carrier_date::timestamp as order_delivered_carrier_date,
        orders.order_delivered_customer_date::timestamp as order_delivered_customer_date,
        orders.order_estimated_delivery_date::timestamp as order_estimated_delivery_date,
        coalesce(items.total_items, 0)::bigint as total_items,
        coalesce(items.total_amount, 0)::decimal(18, 2) as total_amount,
        coalesce(items.total_freight, 0)::decimal(18, 2) as total_freight,
        coalesce(items.distinct_products, 0)::bigint as distinct_products,
        coalesce(items.distinct_sellers, 0)::bigint as distinct_sellers,
        coalesce(payments.total_payment_value, 0)::decimal(18, 2) as total_payment_value,
        round(reviews.avg_review_score, 2)::decimal(3, 2) as avg_review_score,
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
    left join payments on orders.order_id = payments.order_id
    left join reviews on orders.order_id = reviews.order_id

)

select * from final
