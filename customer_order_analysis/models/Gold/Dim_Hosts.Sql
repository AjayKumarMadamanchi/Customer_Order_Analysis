{{
  config(
    materialized='incremental',
    unique_key='HOST_ID',
    incremental_strategy='merge'
  )
}}

SELECT 
    HOST_ID,
    HOST_NAME,
    HOST_SINCE,
    IS_SUPERHOST,
    RESPONSE_RATE,
    DBT_VALID_FROM,
    DBT_VALID_TO,
    UPDATED_AT
FROM {{ ref('snapshot_hosts') }}

{% if is_incremental() %}
  WHERE "UPDATED_AT" > (
    select coalesce(max("UPDATED_AT")::timestamp, '1900-01-01'::timestamp) 
    from {{ this }}
  )
{% endif %}