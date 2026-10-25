with

customers as (

    select * from {{ ref('bronze_customers') }}

),

orders as (

    select
        customer_id,
        order_id,
        order_purchase_timestamp

    from {{ ref('bronze_orders') }}

),

payments as (

    select
        order_id,
        sum(payment_value) as order_payment_value

    from {{ ref('bronze_payments') }}
    group by order_id

),

customer_orders as (

    select
        customers.customer_unique_id,
        customers.customer_zip_code_prefix,
        customers.customer_city,
        customers.customer_state,
        orders.order_id,
        orders.order_purchase_timestamp,
        payments.order_payment_value,
        row_number() over (
            partition by customers.customer_unique_id
            order by
                orders.order_purchase_timestamp desc nulls last,
                customers.customer_id desc
        ) as recency_rank

    from customers
    left join orders on customers.customer_id = orders.customer_id
    left join payments on orders.order_id = payments.order_id

),

final as (

    select
        customer_unique_id::varchar as customer_unique_id,
        max(customer_zip_code_prefix) filter (where recency_rank = 1)::varchar
            as customer_zip_code_prefix,
        max(customer_city) filter (where recency_rank = 1)::varchar as customer_city,
        max(customer_state) filter (where recency_rank = 1)::varchar as customer_state,
        min(order_purchase_timestamp)::timestamp as first_order_at,
        max(order_purchase_timestamp)::timestamp as last_order_at,
        count(order_id)::bigint as total_orders,
        coalesce(sum(order_payment_value), 0)::decimal(18, 2) as total_spent

    from customer_orders
    group by customer_unique_id

)

select * from final
