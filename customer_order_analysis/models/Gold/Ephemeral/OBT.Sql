{{
  config(
    materialized='incremental',
    unique_key='booking_id',
    incremental_strategy='merge'
  )
}}

{% set configs = [
    {
        "table" : ref('bookings'),
        "columns" : "BOOKINGS.*",
        "alias" : "BOOKINGS"
    },
    { 
        "table" : ref('listings'),
        "columns" : "LISTINGS.HOST_ID, LISTINGS.PROPERTY_TYPE, LISTINGS.ROOM_TYPE, LISTINGS.CITY, LISTINGS.COUNTRY, LISTINGS.ACCOMMODATES, LISTINGS.BEDROOMS, LISTINGS.BATHROOMS, LISTINGS.PRICE_PER_NIGHT",
        "alias" : "LISTINGS",
        "join_condition" : "BOOKINGS.LISTING_ID = LISTINGS.LISTING_ID"
    },
    {
        "table" : ref('snapshot_hosts'),
        "columns" : "HOSTS.HOST_NAME, HOSTS.HOST_SINCE, HOSTS.IS_SUPERHOST, HOSTS.RESPONSE_RATE",
        "alias" : "HOSTS",
        "join_condition" : "LISTINGS.HOST_ID = HOSTS.HOST_ID"
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