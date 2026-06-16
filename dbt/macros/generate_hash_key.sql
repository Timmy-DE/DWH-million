{% macro generate_hash_key(column_name) %}
    lower(hex(MD5(coalesce(cast({{ column_name }} as String), 'UNKNOWN'))))
{% endmacro %}
