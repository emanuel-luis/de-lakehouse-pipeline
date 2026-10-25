with

reviews as (

    select * from {{ ref('bronze_reviews') }}

),

orders as (

    select * from {{ ref('bronze_orders') }}

),

customers as (

    select
        customer_id,
        customer_unique_id

    from {{ ref('bronze_customers') }}

),

order_products as (

    -- one row per order, so joining it to reviews cannot change the review grain
    select
        order_id,
        count(distinct product_id) as product_count,
        arg_min(product_id, order_item_id) as primary_product_id,
        arg_min(product_category, order_item_id) as primary_product_category

    from {{ ref('int_order_items_enriched') }}
    group by order_id

),

enriched as (

    select
        reviews.review_id,
        reviews.order_id,
        reviews.review_score,
        reviews.review_comment_title,
        reviews.review_comment_message,
        reviews.review_creation_date,
        reviews.review_answer_timestamp,
        orders.customer_id,
        customers.customer_unique_id,
        orders.order_status,
        orders.order_purchase_timestamp,
        orders.order_delivered_customer_date,
        coalesce(order_products.product_count, 0) as product_count,
        order_products.primary_product_id,
        order_products.primary_product_category

    from reviews
    left join orders on reviews.order_id = orders.order_id
    left join customers on orders.customer_id = customers.customer_id
    left join order_products on reviews.order_id = order_products.order_id

)

select * from enriched
