with

source as (

    select * from {{ source('olist', 'sellers') }}

),

cleaned as (

    select
        seller_id,
        lpad(trim(seller_zip_code_prefix), 5, '0') as seller_zip_code_prefix,
        lower(trim(seller_city)) as seller_city,
        upper(trim(seller_state)) as seller_state,
        _batch_id,
        _extracted_at,
        _source_file

    from source

)

select * from cleaned
