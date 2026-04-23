{{
  config(
    materialized='incremental',
    unique_key='booking_id',
    incremental_strategy='merge'
  )
}}

SELECT 
    booking_id,
    listing_id,     -- Foreign Key to dim_listings
    booking_date,
    nights_booked,
    booking_amount,
    cleaning_fee,
    service_fee,
    total_booking_cost,
    booking_status,
    updated_at
FROM {{ ref('bookings') }}

{% if is_incremental() %}
  -- Only pull records updated since the last run
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}