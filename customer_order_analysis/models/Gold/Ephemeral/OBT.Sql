{{
  config(
    materialized='incremental',
    unique_key='updated_at',
    incremental_strategy='merge'
  )
}}

{% set congigs = [
    {
        "table" : "DEV.SILVER.SILVER_BOOKINGS",
        "columns" : "SILVER_bookings.*",
        "alias" : "SILVER_bookings"
    },
    { 
        "table" : "DEV.SNAPSHOTS.SILVER_LISTINGS",
        "columns" : "SILVER_listings.HOST_ID, SILVER_listings.PROPERTY_TYPE, SILVER_listings.ROOM_TYPE, SILVER_listings.CITY, SILVER_listings.COUNTRY, SILVER_listings.ACCOMMODATES, SILVER_listings.BEDROOMS, SILVER_listings.BATHROOMS, SILVER_listings.PRICE_PER_NIGHT",
        "alias" : "SILVER_listings",
        "join_condition" : "SILVER_bookings.listing_id = SILVER_listings.listing_id and updated_at is NOT NULL"
    },
    {
        "table" : "DEV.SNAPSHOTS.SILVER_HOSTS",
        "columns" : "SILVER_hosts.HOST_NAME, SILVER_hosts.HOST_SINCE, SILVER_hosts.IS_SUPERHOST, SILVER_hosts.RESPONSE_RATE",
        "alias" : "SILVER_hosts",
        "join_condition" : "SILVER_listings.host_id = SILVER_hosts.host_id and updated_at is NOT NULL"
    }
] %}

SELECT 
    {% for congigs in congigs%}
       {{config["columns"]}} {%if loop.last %} {% else %},{% endif %}{% endfor%}
FROM 
    {% for config in congigs %}
    {% if loop.first %} 
        {{config["table"]}} 
    {% else %}
        LEFT JOIN {{config["table"]}}
        ON {{config["join_condition"]}} 
    {% endif %}
    {% endfor %}


{% if is_incremental() %}
    where SILVER_bookings.updated_at > (
        select coalesce(max(s.updated_at), '1900-01-01'::timestamp)
        from {{ this }} s
    )
    {% endif %}
ORDER BY booking_date