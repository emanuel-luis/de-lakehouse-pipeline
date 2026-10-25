{#
    Elementary's test materialization, enabled from the project (required from dbt 1.8).
    Non-Snowflake adapters use the default variant; see the Elementary quickstart.
#}
{% materialization test, default %}
    {{ return(elementary.materialization_test_default()) }}
{% endmaterialization %}
