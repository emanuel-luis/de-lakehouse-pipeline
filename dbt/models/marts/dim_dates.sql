with

bounds as (

    select
        min(order_purchase_timestamp)::date as first_day,
        max(order_purchase_timestamp)::date as last_day

    from {{ ref('bronze_orders') }}

),

days as (

    select unnest(generate_series(first_day, last_day, interval 1 day))::date as date_day
    from bounds

),

final as (

    select
        strftime(date_day, '%Y%m%d')::integer as date_key,
        date_day::date as date_day,
        isodow(date_day)::integer as day_of_week,
        dayname(date_day)::varchar as day_name,
        day(date_day)::integer as day_of_month,
        weekofyear(date_day)::integer as week_of_year,
        month(date_day)::integer as month_number,
        monthname(date_day)::varchar as month_name,
        quarter(date_day)::integer as quarter,
        year(date_day)::integer as year,
        (isodow(date_day) >= 6)::boolean as is_weekend

    from days

)

select * from final
