{{
  config(
    materialized='incremental',
    unique_key='HOST_ID',
    incremental_strategy='merge'
  )
}}

WITH hosts_transformed as (
    SELECT 
        "HOST_ID",
        "HOST_NAME",
        "HOST_SINCE",
        "IS_SUPERHOST",
        "RESPONSE_RATE",
        {{ date_conversion_timezone('updated_at','Asia/Kolkata') }}  UPDATED_AT
    FROM {{ source('Bronze', 'hosts') }}
)

SELECT * FROM hosts_transformed

{% if is_incremental() %}
  WHERE UPDATED_AT > (
    SELECT COALESCE(MAX(UPDATED_AT), '1900-01-01'::timestamp) 
    FROM {{ this }}
  )
{% endif %}