{{
  config(
    materialized='incremental',
    unique_key='HOST_ID',
    incremental_strategy='merge'
  )
}}

-- 1. Pre-calculate the watermark (max date) using a set block
{% if is_incremental() %}
    {% set max_created_query %}
        select coalesce(max(updated_at), '1900-01-01') from {{ this }}
    {% endset %}
    
    {% set max_created = run_query(max_created_query).columns[0][0] %}
{% endif %}

SELECT 
    HOST_ID,
    HOST_NAME,
    HOST_SINCE,
    IS_SUPERHOST,
    RESPONSE_RATE,
    {{ date_conversion_timezone('updated_at', 'Asia/Kolkata') }} as updated_at
FROM {{ source('Bronze', 'hosts') }}

{% if is_incremental() %}
    -- 2. Use the literal value calculated above
    WHERE updated_at > '{{ max_created }}'
{% endif %}