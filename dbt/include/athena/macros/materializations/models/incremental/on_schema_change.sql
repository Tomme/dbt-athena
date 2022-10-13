{% macro incremental_validate_on_schema_change(on_schema_change, default='ignore') %}
    -- sync_all_columns is not supported
    {% if on_schema_change not in ['append_new_columns', 'fail', 'ignore'] %}
        {% set log_message = 'Invalid value for on_schema_change (%s) specified. Setting default value of %s.' % (on_schema_change, default) %}
        {% do log(log_message) %}

        {{ return(default) }}

    {% else %}

        {{ return(on_schema_change) }}

   {% endif %}

{% endmacro %}
