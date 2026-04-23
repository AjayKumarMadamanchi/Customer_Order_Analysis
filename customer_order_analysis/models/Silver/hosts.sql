{{
  config(
    materialized='incremental',
    unique_key='updated_at',
    incremental_strategy='merge'
  )
}}

SELECT 
HOST_ID,
HOST_NAME,
HOST_SINCE,
IS_SUPERHOST,
RESPONSE_RATE,
{{date_conversion_timezone('updated_at','Asia/Kolkata')}} as updated_at
FROM {{source('Bronze','hosts_stream')}}

{% if is_incremental() %}
  WHERE updated_at>(
    select coalesce(max(created_at), '1900-01-01') 
    from {{this}}
  )
{% endif %}