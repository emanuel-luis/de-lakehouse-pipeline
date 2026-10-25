with

source as (

    select * from {{ source('olist', 'orders') }}

),

renamed as (

    select
        order_id,
        customer_id,
        lower(trim(order_status)) as order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        order_updated_at,
        _batch_id,
        _extracted_at,
        _source_file

    from source

)

select * from renamed
