{#
    The landing row count must equal what the ingest log says was committed:
    sum(rows_out) over successful log rows for this table since the latest
    successful full-mode run (a full load replaces the landing directory).
    The table name in the log is the source table's identifier.
    Returns one row, and so fails, when the two counts differ.
#}
{% test reconciles_with_ingest_log(model) %}

with

log as (

    select run_id, mode, rows_out
    from {{ source('ingest', 'ingest_log') }}
    where table_name = '{{ model.identifier }}'
      and status = 'success'

),

latest_full as (

    select max(run_id) as run_id
    from log
    where mode = 'full'

),

expected as (

    select coalesce(sum(log.rows_out), 0) as expected_rows
    from log
    inner join latest_full on log.run_id >= latest_full.run_id

),

actual as (

    select count(*) as actual_rows
    from {{ model }}

)

select
    actual.actual_rows,
    expected.expected_rows
from actual
cross join expected
where actual.actual_rows <> expected.expected_rows

{% endtest %}
