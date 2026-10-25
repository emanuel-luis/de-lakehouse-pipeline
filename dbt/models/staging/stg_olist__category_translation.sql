with

source as (

    select * from {{ source('olist', 'category_translation') }}

),

cleaned as (

    select
        lower(trim(product_category_name)) as product_category_name,
        lower(trim(product_category_name_english)) as product_category_name_english,
        _batch_id,
        _extracted_at,
        _source_file

    from source

)

select * from cleaned
