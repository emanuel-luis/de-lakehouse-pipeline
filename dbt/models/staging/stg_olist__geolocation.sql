with

source as (

    select * from {{ source('olist', 'geolocation') }}

),

cleaned as (

    select
        lpad(trim(geolocation_zip_code_prefix), 5, '0') as geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        lower(trim(geolocation_city)) as geolocation_city,
        upper(trim(geolocation_state)) as geolocation_state,
        _batch_id,
        _extracted_at,
        _source_file

    from source

)

select * from cleaned
