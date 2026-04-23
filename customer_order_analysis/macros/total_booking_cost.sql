{% macro total_booking_cost(nights_booked, booking_amount, cleaning_fee, service_fee) %}
(
    ({{ nights_booked }} * {{ booking_amount }})
    + {{ cleaning_fee }}
    + {{ service_fee }}
)
{% endmacro %}