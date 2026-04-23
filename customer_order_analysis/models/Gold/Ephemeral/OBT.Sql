{{
  config(
    materialized='incremental',
    unique_key='booking_id',
    incremental_strategy='merge'
  )
}}

{% set configs = [
    {
        "table" : "DEV.SILVER.BOOKINGS",
        "columns" : "bookings.*",
        "alias" : "bookings"
    },
    { 
        "table" : "DEV.SNAPSHOTS.LISTINGS",
        "columns" : "listings.HOST_ID, listings.PROPERTY_TYPE, listings.ROOM_TYPE, listings.CITY, listings.COUNTRY, listings.ACCOMMODATES, listings.BEDROOMS, listings.BATHROOMS, listings.PRICE_PER_NIGHT",
        "alias" : "listings",
        "join_condition" : "bookings.listing_id = listings.listing_id"
    },
    {
        "table" : "DEV.SNAPSHOTS.HOSTS",
        "columns" : "hosts.HOST_NAME, hosts.HOST_SINCE, hosts.IS_SUPERHOST, hosts.RESPONSE_RATE",
        "alias" : "hosts",
        "join_condition" : "listings.host_id = hosts.host_id"
    }
] %}

SELECT 
    {% for config in configs %}
        {{config["columns"]}} {% if not loop.last %},{% endif %}
    {% endfor %}
FROM 
    {% for config in configs %}
        {% if loop.first %} 
            {{config["table"]}} AS {{config["alias"]}}
        {% else %}
            LEFT JOIN {{config["table"]}} AS {{config["alias"]}}
            ON {{config["join_condition"]}}
        {% endif %}
    {% endfor %}

{% if is_incremental() %}
    WHERE bookings.updated_at > (
        select coalesce(max(updated_at), '1900-01-01'::timestamp)
        from {{ this }}
    )
{% endif %}

ORDER BY booking_date