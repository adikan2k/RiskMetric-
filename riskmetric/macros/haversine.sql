{% macro haversine(lat1, lon1, lat2, lon2) %}
    -- Returns distance in miles between two lat/lon points using the Haversine formula
    (
        3959.0 * 2 * ASIN(
            SQRT(
                POWER(SIN(RADIANS(({{ lat2 }} - {{ lat1 }}) / 2)), 2)
                + COS(RADIANS({{ lat1 }}))
                * COS(RADIANS({{ lat2 }}))
                * POWER(SIN(RADIANS(({{ lon2 }} - {{ lon1 }}) / 2)), 2)
            )
        )
    )
{% endmacro %}
