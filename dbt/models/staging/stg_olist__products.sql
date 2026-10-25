with

source as (

    select * from {{ source('olist', 'products') }}

),

renamed as (

    select
        product_id,
        lower(trim(product_category_name)) as product_category_name,
        -- the Olist header misspells length as "lenght"; fixed here, once
        product_name_lenght as product_name_length,
        product_description_lenght as product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        _batch_id,
        _extracted_at,
        _source_file

    from source

)

select * from renamed
