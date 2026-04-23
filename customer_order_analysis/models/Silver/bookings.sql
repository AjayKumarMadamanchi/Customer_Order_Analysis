{{
  config(
    materialized='incremental',
    unique_key='updated_at',
    incremental_strategy='merge'
  )
}}

SELECT booking_id,
        listing_id,
        booking_date,
        nights_booked,
        booking_amount,
        cleaning_fee,
        service_fee,
        {{total_booking_cost('nights_booked','booking_amount','cleaning_fee','service_fee')}} as total_booking_cost,
        booking_status,
        {{date_conversion_timezone('updated_at','Asia/Kolkata')}} as updated_at
FROM  {{ source('Bronze', 'bookings_stream') }}

{% if is_incremental() %}
  WHERE updated_at>(
    select coalesce(max(created_at), '1900-01-01') 
    from {{this}}
    )
{% endif %}
