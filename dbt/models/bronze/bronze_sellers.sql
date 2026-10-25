with

new_rows as (

    select * from {{ ref('stg_olist__sellers') }}

),

ranked as (

    select
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        _batch_id,
        _extracted_at,
        _source_file,
        row_number() over (
            partition by seller_id
            -- the other columns break ties, so one batch repeating a key still picks one row
            order by
                _batch_id desc,
                seller_zip_code_prefix,
                seller_city,
                seller_state
        ) as _row_rank

    from new_rows

),

deduped as (

    select
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        _batch_id,
        _extracted_at,
        _source_file

    from ranked
    where _row_rank = 1

)

select * from deduped
