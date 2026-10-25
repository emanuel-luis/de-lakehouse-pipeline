with

new_rows as (

    select * from {{ ref('stg_olist__reviews') }}

),

ranked as (

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
        _source_file,
        row_number() over (
            partition by review_id, order_id
            -- the other columns break ties, so one batch repeating a key still picks one row
            order by
                _batch_id desc,
                review_answer_timestamp desc nulls last,
                review_score,
                review_creation_date,
                review_comment_title,
                review_comment_message
        ) as _row_rank

    from new_rows

),

deduped as (

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

    from ranked
    where _row_rank = 1

)

select * from deduped
