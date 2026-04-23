{{
    config(
    materialized='incremental',
    unique_key='listing_id',
    incremental_strategy='merge'
  )
}}

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
    {{date_conversion_timezone('updated_at','Asia/Kolkata')}} as updated_at

FROM {{source('Bronze','listings_stream')}}

 {% if is_incremental() %}
 WHERE
 updated_at>(
    select coalsec(max(updated_at,'1900-01-01'::timestamp))
 )
 from this

{% endif %}