with

source as (

    select * from {{ ref('stg_olist__geolocation') }}

),

coordinates as (

    -- rounded to 6 decimals (about 0.1 m): a parallel float sum may differ in the last bit
    -- between runs, the rounded value does not
    select
        geolocation_zip_code_prefix,
        round(avg(geolocation_lat), 6) as geolocation_lat,
        round(avg(geolocation_lng), 6) as geolocation_lng,
        max(_batch_id) as _batch_id,
        max(_extracted_at) as _extracted_at,
        max(_source_file) as _source_file

    from source
    group by geolocation_zip_code_prefix

),

place_counts as (

    select
        geolocation_zip_code_prefix,
        geolocation_city,
        geolocation_state,
        count(*) as row_count

    from source
    group by geolocation_zip_code_prefix, geolocation_city, geolocation_state

),

top_place as (

    -- most frequent (city, state) per zip; ties go to the alphabetically first pair
    select
        geolocation_zip_code_prefix,
        geolocation_city,
        geolocation_state

    from place_counts
    qualify row_number() over (
        partition by geolocation_zip_code_prefix
        order by row_count desc, geolocation_city asc nulls last, geolocation_state asc nulls last
    ) = 1

),

final as (

    select
        coordinates.geolocation_zip_code_prefix,
        coordinates.geolocation_lat,
        coordinates.geolocation_lng,
        top_place.geolocation_city,
        top_place.geolocation_state,
        coordinates._batch_id,
        coordinates._extracted_at,
        coordinates._source_file

    from coordinates
    inner join top_place
        on coordinates.geolocation_zip_code_prefix = top_place.geolocation_zip_code_prefix

)

select * from final
