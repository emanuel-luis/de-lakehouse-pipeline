with

items as (

    select * from {{ ref('bronze_order_items') }}

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

products as (

    select * from {{ ref('bronze_products') }}

),

translations as (

    select * from {{ ref('bronze_category_translation') }}

),

sellers as (

    select * from {{ ref('bronze_sellers') }}

),

enriched as (

    select
        items.order_id,
        items.order_item_id,
        items.product_id,
        items.seller_id,
        orders.customer_id,
        customers.customer_unique_id,
        orders.order_status,
        orders.order_purchase_timestamp,
        orders.order_approved_at,
        orders.order_delivered_carrier_date,
        orders.order_delivered_customer_date,
        orders.order_estimated_delivery_date,
        items.shipping_limit_date,
        items.price,
        items.freight_value,
        products.product_category_name,
        -- a null category and an untranslated category both end up as 'unknown'
        coalesce(translations.product_category_name_english, 'unknown') as product_category,
        products.product_weight_g,
        sellers.seller_city,
        sellers.seller_state

    from items
    left join orders on items.order_id = orders.order_id
    left join customers on orders.customer_id = customers.customer_id
    left join products on items.product_id = products.product_id
    left join translations on products.product_category_name = translations.product_category_name
    left join sellers on items.seller_id = sellers.seller_id

)

select * from enriched
