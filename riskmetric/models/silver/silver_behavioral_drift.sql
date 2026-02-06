-- Silver Layer: Behavioral Drift Detection
-- Calculates Z-Score of each transaction amount against the user's
-- 30-day rolling average. Flags transactions > 3 standard deviations.
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

user_rolling_stats AS (
    SELECT
        transaction_id,
        user_id,
        transaction_timestamp,
        amount,
        merchant_category,
        city,
        country,
        is_fraud,
        fraud_type,
        -- 30-day rolling average
        AVG(amount) OVER (
            PARTITION BY user_id
            ORDER BY transaction_timestamp
            RANGE BETWEEN INTERVAL '30 DAYS' PRECEDING AND INTERVAL '1 SECOND' PRECEDING
        ) AS rolling_avg_30d,
        -- 30-day rolling standard deviation
        STDDEV_POP(amount) OVER (
            PARTITION BY user_id
            ORDER BY transaction_timestamp
            RANGE BETWEEN INTERVAL '30 DAYS' PRECEDING AND INTERVAL '1 SECOND' PRECEDING
        ) AS rolling_std_30d,
        -- Count of transactions in 30-day window (for reliability)
        COUNT(*) OVER (
            PARTITION BY user_id
            ORDER BY transaction_timestamp
            RANGE BETWEEN INTERVAL '30 DAYS' PRECEDING AND INTERVAL '1 SECOND' PRECEDING
        ) AS txn_count_30d
    FROM source_txns
)

SELECT
    transaction_id,
    user_id,
    transaction_timestamp,
    amount,
    merchant_category,
    city,
    country,
    is_fraud,
    fraud_type,
    ROUND(rolling_avg_30d, 2) AS rolling_avg_30d,
    ROUND(rolling_std_30d, 2) AS rolling_std_30d,
    txn_count_30d,
    -- Z-Score calculation
    CASE
        WHEN rolling_std_30d IS NOT NULL AND rolling_std_30d > 0 AND txn_count_30d >= 5
        THEN ROUND((amount - rolling_avg_30d) / rolling_std_30d, 4)
        ELSE NULL
    END AS z_score,
    -- Flag: behavioral drift if |z_score| > 3
    CASE
        WHEN rolling_std_30d IS NOT NULL
             AND rolling_std_30d > 0
             AND txn_count_30d >= 5
             AND ABS((amount - rolling_avg_30d) / rolling_std_30d) > 3
        THEN TRUE
        ELSE FALSE
    END AS flag_behavioral_drift
FROM user_rolling_stats
