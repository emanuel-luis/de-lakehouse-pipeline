with

source as (

    select * from {{ source('olist', 'reviews') }}

),

renamed as (

    select
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp,
        _batch_id,
        _extracted_at,
        _source_file

    from source

)

select * from renamed
