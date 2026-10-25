with

source as (

    select * from {{ source('olist', 'order_items') }}

),

renamed as (

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

    from source

)

select * from renamed
