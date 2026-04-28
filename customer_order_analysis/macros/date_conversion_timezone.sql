{% macro date_conversion_timezone(column_name, timezone) %}
        CONVERT_TIMEZONE('UTC', '{{ timezone }}', {{ column_name }}::TIMESTAMP_NTZ), 
        
{% endmacro %}