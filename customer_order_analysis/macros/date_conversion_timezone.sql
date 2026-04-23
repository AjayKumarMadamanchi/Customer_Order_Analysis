 {% macro date_conversion_timezone(created_at,time_zone) %}

TO_CHAR(convert_timezone('{{timezone}}',{{created_at}}::TIMESTAMP_NTZ), 'YYYY-MM-DD HH24:MI:SS TZH:TZM')
{% endmacro %}