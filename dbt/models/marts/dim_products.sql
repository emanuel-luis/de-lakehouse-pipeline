with

products as (

    select * from {{ ref('bronze_products') }}

),

items as (

    select * from {{ ref('int_order_items_enriched') }}

),

reviews as (

    select
        order_id,
        review_score

    from {{ ref('int_review_enriched') }}

),

product_sales as (

    select
        product_id,
        count(distinct order_id) as total_orders,
        sum(price) as total_revenue

    from items
    group by product_id

),

product_orders as (

    -- each product counts once per order, however many lines it has there
    select distinct
        product_id,
        order_id

    from items

),

product_reviews as (

    select
        product_orders.product_id,
        avg(reviews.review_score) as avg_review_score

    from product_orders
    inner join reviews on product_orders.order_id = reviews.order_id
    group by product_orders.product_id

),

final as (

    select
        products.product_id::varchar as product_id,
        products.product_category_name::varchar as product_category_name,
        -- same rule as int_order_items_enriched: null or untranslated becomes 'unknown'
        coalesce(translations.product_category_name_english, 'unknown')::varchar
            as product_category,
        products.product_name_length::bigint as product_name_length,
        products.product_description_length::bigint as product_description_length,
        products.product_photos_qty::bigint as product_photos_qty,
        products.product_weight_g::bigint as product_weight_g,
        products.product_length_cm::bigint as product_length_cm,
        products.product_height_cm::bigint as product_height_cm,
        products.product_width_cm::bigint as product_width_cm,
        coalesce(product_sales.total_orders, 0)::bigint as total_orders,
        coalesce(product_sales.total_revenue, 0)::decimal(18, 2) as total_revenue,
        round(product_reviews.avg_review_score, 2)::decimal(3, 2) as avg_review_score

    from products
    left join {{ ref('bronze_category_translation') }} as translations
        on products.product_category_name = translations.product_category_name
    left join product_sales on products.product_id = product_sales.product_id
    left join product_reviews on products.product_id = product_reviews.product_id

)

select * from final
