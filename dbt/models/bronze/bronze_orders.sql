with

new_rows as (

    -- The only place that changes when this model becomes incremental: add the batch filter here.
    select * from {{ ref('stg_olist__orders') }}

),

ranked as (

    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        order_updated_at,
        _batch_id,
        _extracted_at,
        _source_file,
        row_number() over (
            partition by order_id
            -- the other columns break ties, so one batch repeating a key still picks one row
            order by
                _batch_id desc,
                order_updated_at desc,
                order_status,
                customer_id,
                order_purchase_timestamp,
                order_approved_at,
                order_delivered_carrier_date,
                order_delivered_customer_date,
                order_estimated_delivery_date
        ) as _row_rank

    from new_rows

),

deduped as (

    select
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        order_updated_at,
        _batch_id,
        _extracted_at,
        _source_file

    from ranked
    where _row_rank = 1

)

select * from deduped
