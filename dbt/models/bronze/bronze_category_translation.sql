with

new_rows as (

    select * from {{ ref('stg_olist__category_translation') }}

),

ranked as (

    select
        product_category_name,
        product_category_name_english,
        _batch_id,
        _extracted_at,
        _source_file,
        row_number() over (
            partition by product_category_name
            -- the other columns break ties, so one batch repeating a key still picks one row
            order by _batch_id desc, product_category_name_english
        ) as _row_rank

    from new_rows

),

deduped as (

    select
        product_category_name,
        product_category_name_english,
        _batch_id,
        _extracted_at,
        _source_file

    from ranked
    where _row_rank = 1

)

select * from deduped
