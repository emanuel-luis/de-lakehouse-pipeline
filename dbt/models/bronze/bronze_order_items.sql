with

new_rows as (

    select * from {{ ref('stg_olist__order_items') }}

),

ranked as (

    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value,
        _batch_id,
        _extracted_at,
        _source_file,
        row_number() over (
            partition by order_id, order_item_id
            -- the other columns break ties, so one batch repeating a key still picks one row
            order by
                _batch_id desc,
                product_id,
                seller_id,
                shipping_limit_date,
                price,
                freight_value
        ) as _row_rank

    from new_rows

),

deduped as (

    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value,
        _batch_id,
        _extracted_at,
        _source_file

    from ranked
    where _row_rank = 1

)

select * from deduped
