with

new_rows as (

    select * from {{ ref('stg_olist__payments') }}

),

ranked as (

    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value,
        _batch_id,
        _extracted_at,
        _source_file,
        row_number() over (
            partition by order_id, payment_sequential
            -- the other columns break ties, so one batch repeating a key still picks one row
            order by
                _batch_id desc,
                payment_type,
                payment_installments,
                payment_value
        ) as _row_rank

    from new_rows

),

deduped as (

    select
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value,
        _batch_id,
        _extracted_at,
        _source_file

    from ranked
    where _row_rank = 1

)

select * from deduped
