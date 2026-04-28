{{
  config(
    materialized='incremental',
    unique_key='booking_id',
    incremental_strategy='merge'
  )
}}

{{
  config(
    materialized='incremental',
    unique_key='BOOKING_ID',
    incremental_strategy='merge'
  )
}}

WITH TRANSFORMED_DATA AS (
    SELECT 
        BOOKING_ID,
        LISTING_ID,
        BOOKING_DATE,
        NIGHTS_BOOKED,
        BOOKING_AMOUNT,
        CLEANING_FEE,
        SERVICE_FEE,
        {{ total_booking_cost('NIGHTS_BOOKED','BOOKING_AMOUNT','CLEANING_FEE','SERVICE_FEE') }}  TOTAL_BOOKING_COST,
        BOOKING_STATUS,
        {{ date_conversion_timezone('UPDATED_AT', 'Asia/Kolkata') }}  UPDATED_AT
    FROM {{ source('Bronze', 'bookings') }}
)

SELECT * FROM TRANSFORMED_DATA

{% if is_incremental() %}
  WHERE UPDATED_AT > (
    SELECT COALESCE(MAX(UPDATED_AT), '1900-01-01'::TIMESTAMP) 
    FROM {{ this }}
  )
{% endif %}
