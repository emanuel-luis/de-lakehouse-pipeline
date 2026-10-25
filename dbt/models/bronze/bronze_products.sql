with

new_rows as (

    select * from {{ ref('stg_olist__products') }}

),

ranked as (

    select
        product_id,
        product_category_name,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        _batch_id,
        _extracted_at,
        _source_file,
        row_number() over (
            partition by product_id
            -- the other columns break ties, so one batch repeating a key still picks one row
            order by
                _batch_id desc,
                product_category_name,
                product_name_length,
                product_description_length,
                product_photos_qty,
                product_weight_g,
                product_length_cm,
                product_height_cm,
                product_width_cm
        ) as _row_rank

    from new_rows

),

deduped as (

    select
        product_id,
        product_category_name,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        _batch_id,
        _extracted_at,
        _source_file

    from ranked
    where _row_rank = 1

)

select * from deduped
