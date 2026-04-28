{{
    config(
    materialized='incremental',
    unique_key='listing_id',
    incremental_strategy='merge'
  )
}}


WITH listings_transformed AS (
    SELECT 
        LISTING_ID,
        HOST_ID,
        PROPERTY_TYPE,
        ROOM_TYPE,
        CITY,
        COUNTRY,
        ACCOMMODATES,
        BEDROOMS,
        BATHROOMS,
        PRICE_PER_NIGHT,
        {{ date_conversion_timezone('updated_at','Asia/Kolkata') }}  UPDATED_AT
    FROM {{ source('Bronze','listings') }}
)

SELECT * FROM listings_transformed

{% if is_incremental() %}
  WHERE UPDATED_AT > (
    SELECT COALESCE(MAX(UPDATED_AT), '1900-01-01'::timestamp) 
    FROM {{ this }} 
  )
{% endif %}