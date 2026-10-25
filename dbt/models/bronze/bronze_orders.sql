{#
    Incremental: each run merges only the landing batches newer than the table (delete+insert on
    order_id). A successful full-mode ingest of orders newer than the table replaces the landing
    directory, so the pre-hook empties the table first and the run reloads everything; without it,
    orders from an earlier source would linger after the source is regenerated.
#}
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='order_id',
        on_schema_change='fail',
        pre_hook="
            {% if is_incremental() %}
            delete from {{ this }}
            where exists (
                select 1
                from {{ source('ingest', 'ingest_log') }}
                where table_name = 'orders'
                  and status = 'success'
                  and mode = 'full'
                  and run_id > (select coalesce(max(_batch_id), '') from {{ this }})
            )
            {% endif %}
        "
    )
}}

with

new_rows as (

    select * from {{ ref('stg_olist__orders') }}
    {% if is_incremental() %}
    where _batch_id > (select coalesce(max(_batch_id), '') from {{ this }})
    {% endif %}

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
