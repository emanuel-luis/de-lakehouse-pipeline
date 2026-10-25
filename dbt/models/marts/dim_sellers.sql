with

sellers as (

    select * from {{ ref('bronze_sellers') }}

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

seller_sales as (

    select
        seller_id,
        count(distinct order_id) as total_orders,
        count(*) as total_items_sold,
        sum(price) as total_revenue

    from items
    group by seller_id

),

seller_orders as (

    select distinct
        seller_id,
        order_id

    from items

),

seller_reviews as (

    select
        seller_orders.seller_id,
        avg(reviews.review_score) as avg_review_score

    from seller_orders
    inner join reviews on seller_orders.order_id = reviews.order_id
    group by seller_orders.seller_id

),

final as (

    select
        sellers.seller_id::varchar as seller_id,
        sellers.seller_zip_code_prefix::varchar as seller_zip_code_prefix,
        sellers.seller_city::varchar as seller_city,
        sellers.seller_state::varchar as seller_state,
        coalesce(seller_sales.total_orders, 0)::bigint as total_orders,
        coalesce(seller_sales.total_items_sold, 0)::bigint as total_items_sold,
        coalesce(seller_sales.total_revenue, 0)::decimal(18, 2) as total_revenue,
        round(seller_reviews.avg_review_score, 2)::decimal(3, 2) as avg_review_score

    from sellers
    left join seller_sales on sellers.seller_id = seller_sales.seller_id
    left join seller_reviews on sellers.seller_id = seller_reviews.seller_id

)

select * from final
