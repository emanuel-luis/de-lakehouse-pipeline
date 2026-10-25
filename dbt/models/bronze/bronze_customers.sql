with

new_rows as (

    select * from {{ ref('stg_olist__customers') }}

),

ranked as (

    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        _batch_id,
        _extracted_at,
        _source_file,
        row_number() over (
            partition by customer_id
            -- the other columns break ties, so one batch repeating a key still picks one row
            order by
                _batch_id desc,
                customer_unique_id,
                customer_zip_code_prefix,
                customer_city,
                customer_state
        ) as _row_rank

    from new_rows

),

deduped as (

    select
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        _batch_id,
        _extracted_at,
        _source_file

    from ranked
    where _row_rank = 1

)

select * from deduped
