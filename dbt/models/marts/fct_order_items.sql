with

items as (

    select * from {{ ref('int_order_items_enriched') }}

),

final as (

    select
        order_id::varchar as order_id,
        order_item_id::bigint as order_item_id,
        product_id::varchar as product_id,
        seller_id::varchar as seller_id,
        customer_unique_id::varchar as customer_unique_id,
        strftime(order_purchase_timestamp, '%Y%m%d')::integer as order_date_key,
        order_status::varchar as order_status,
        order_purchase_timestamp::timestamp as order_purchase_timestamp,
        shipping_limit_date::timestamp as shipping_limit_date,
        product_category::varchar as product_category,
        price::decimal(18, 2) as price,
        freight_value::decimal(18, 2) as freight_value,
        (price + freight_value)::decimal(18, 2) as item_total

    from items

)

select * from final
