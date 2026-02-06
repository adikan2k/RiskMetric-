-- Silver Layer: Impossible Travel Detection
-- Calculates Haversine distance and ground speed between consecutive
-- transactions per user. Flags pairs exceeding 500 mph.
-- Incremental: processes only new transactions since last run.

{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='append'
    )
}}

WITH source_txns AS (
    SELECT *
    FROM {{ ref('stg_transactions') }}
    {% if is_incremental() %}
    WHERE transaction_timestamp > (SELECT MAX(transaction_timestamp) FROM {{ this }})
    {% endif %}
),

ordered_txns AS (
    SELECT
        transaction_id,
        user_id,
        transaction_timestamp,
        amount,
        merchant_category,
        city,
        country,
        latitude,
        longitude,
        is_fraud,
        fraud_type,
        LAG(transaction_id) OVER (PARTITION BY user_id ORDER BY transaction_timestamp) AS prev_transaction_id,
        LAG(transaction_timestamp) OVER (PARTITION BY user_id ORDER BY transaction_timestamp) AS prev_timestamp,
        LAG(latitude) OVER (PARTITION BY user_id ORDER BY transaction_timestamp) AS prev_latitude,
        LAG(longitude) OVER (PARTITION BY user_id ORDER BY transaction_timestamp) AS prev_longitude,
        LAG(city) OVER (PARTITION BY user_id ORDER BY transaction_timestamp) AS prev_city
    FROM source_txns
),

with_distance AS (
    SELECT
        *,
        -- Time gap in hours
        CASE
            WHEN prev_timestamp IS NOT NULL
            THEN EXTRACT(EPOCH FROM (transaction_timestamp - prev_timestamp)) / 3600.0
            ELSE NULL
        END AS time_gap_hours,

        -- Haversine distance in miles
        CASE
            WHEN prev_latitude IS NOT NULL AND prev_longitude IS NOT NULL
            THEN {{ haversine('prev_latitude', 'prev_longitude', 'latitude', 'longitude') }}
            ELSE NULL
        END AS distance_miles
    FROM ordered_txns
)

SELECT
    transaction_id,
    user_id,
    transaction_timestamp,
    amount,
    merchant_category,
    city,
    country,
    latitude,
    longitude,
    is_fraud,
    fraud_type,
    prev_transaction_id,
    prev_timestamp,
    prev_city,
    prev_latitude,
    prev_longitude,
    time_gap_hours,
    distance_miles,
    -- Ground speed in mph
    CASE
        WHEN time_gap_hours IS NOT NULL AND time_gap_hours > 0
        THEN distance_miles / time_gap_hours
        ELSE NULL
    END AS ground_speed_mph,
    -- Flag: impossible travel if speed > 500 mph
    CASE
        WHEN time_gap_hours IS NOT NULL
             AND time_gap_hours > 0
             AND (distance_miles / time_gap_hours) > 500
        THEN TRUE
        ELSE FALSE
    END AS flag_impossible_travel
FROM with_distance
