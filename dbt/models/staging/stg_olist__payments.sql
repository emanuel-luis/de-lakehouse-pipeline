with

source as (

    select * from {{ source('olist', 'payments') }}

),

cleaned as (

    select
        order_id,
        payment_sequential,
        lower(trim(payment_type)) as payment_type,
        payment_installments,
        payment_value,
        _batch_id,
        _extracted_at,
        _source_file

    from source

)

select * from cleaned
