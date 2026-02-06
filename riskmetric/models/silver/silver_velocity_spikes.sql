-- Silver Layer: Velocity Spike Detection
-- Identifies "Card Testing" behavior: 10+ transactions within a 60-second window per user.
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

txn_windows AS (
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
        -- Count transactions in the 60-second window ending at this transaction
        COUNT(*) OVER (
            PARTITION BY user_id
            ORDER BY transaction_timestamp
            RANGE BETWEEN INTERVAL '60 SECONDS' PRECEDING AND CURRENT ROW
        ) AS txn_count_60s,
        -- Sum of amounts in 60-second window
        SUM(amount) OVER (
            PARTITION BY user_id
            ORDER BY transaction_timestamp
            RANGE BETWEEN INTERVAL '60 SECONDS' PRECEDING AND CURRENT ROW
        ) AS amount_sum_60s,
        -- Average amount in 60-second window
        AVG(amount) OVER (
            PARTITION BY user_id
            ORDER BY transaction_timestamp
            RANGE BETWEEN INTERVAL '60 SECONDS' PRECEDING AND CURRENT ROW
        ) AS amount_avg_60s
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
    txn_count_60s,
    amount_sum_60s,
    ROUND(amount_avg_60s, 2) AS amount_avg_60s,
    -- Flag: velocity spike if 10+ transactions in 60 seconds
    CASE
        WHEN txn_count_60s >= 10 THEN TRUE
        ELSE FALSE
    END AS flag_velocity_spike
FROM txn_windows
