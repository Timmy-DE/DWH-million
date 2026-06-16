{% macro incremental_filter(column_name) %}
    {% if is_incremental() %}
        {{ column_name }} > (SELECT max({{ column_name }}) FROM {{ this }})
    {% endif %}
{% endmacro %}
