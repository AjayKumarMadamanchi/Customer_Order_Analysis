{{
  config(
    materialized='incremental',
    unique_key='updated_at',
    incremental_strategy='merge'
  )
}}

SELECT 
    HOST_ID, -- Natural Key
    HOST_NAME,
    HOST_SINCE,
    IS_SUPERHOST,
    RESPONSE_RATE,
    updated_at
FROM {{ ref('snapshot_hosts') }}

{% if is_incremental() %}
  -- Only pull records updated since the last run
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }}) 
{% endif %}