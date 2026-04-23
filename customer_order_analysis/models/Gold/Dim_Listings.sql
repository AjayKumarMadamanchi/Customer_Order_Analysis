
{{
  config(
    materialized='incremental',
    unique_key='updated_at',
    incremental_strategy='merge'
  )
}}

SELECT 
    LISTING_ID, -- Natural Key
    HOST_ID,    -- Foreign Key to dim_hosts
    PROPERTY_TYPE,
    ROOM_TYPE,
    CITY,
    COUNTRY,
    ACCOMMODATES,
    BEDROOMS,
    BATHROOMS,
    PRICE_PER_NIGHT,
    updated_at
FROM {{ ref('listings') }}

{% if is_incremental() %}
  -- Only pull records updated since the last run
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}