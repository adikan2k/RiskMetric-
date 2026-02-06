-- Gold Layer: Unified Risk Scoring
-- Joins all three fraud detection signals into a single risk score per transaction.
-- Risk Score = weighted combination of all flags.

WITH impossible_travel AS (
    SELECT
        transaction_id,
        flag_impossible_travel,
        ground_speed_mph,
        distance_miles,
        time_gap_hours,
        prev_city
    FROM {{ ref('silver_impossible_travel') }}
),

velocity AS (
    SELECT
        transaction_id,
        flag_velocity_spike,
        txn_count_60s,
        amount_sum_60s
    FROM {{ ref('silver_velocity_spikes') }}
),

drift AS (
    SELECT
        transaction_id,
        flag_behavioral_drift,
        z_score,
        rolling_avg_30d,
        rolling_std_30d
    FROM {{ ref('silver_behavioral_drift') }}
),

base_txns AS (
    SELECT
        transaction_id,
        user_id,
        transaction_timestamp,
        amount,
        merchant_name,
        merchant_category,
        city,
        country,
        latitude,
        longitude,
        is_fraud,
        fraud_type
    FROM {{ ref('stg_transactions') }}
)

SELECT
    b.transaction_id,
    b.user_id,
    b.transaction_timestamp,
    b.amount,
    b.merchant_name,
    b.merchant_category,
    b.city,
    b.country,
    b.latitude,
    b.longitude,
    b.is_fraud AS labeled_fraud,
    b.fraud_type AS labeled_fraud_type,

    -- Individual flags
    COALESCE(it.flag_impossible_travel, FALSE) AS flag_impossible_travel,
    COALESCE(v.flag_velocity_spike, FALSE) AS flag_velocity_spike,
    COALESCE(d.flag_behavioral_drift, FALSE) AS flag_behavioral_drift,

    -- Supporting metrics
    it.ground_speed_mph,
    it.distance_miles,
    it.time_gap_hours,
    it.prev_city,
    v.txn_count_60s,
    v.amount_sum_60s,
    d.z_score,
    d.rolling_avg_30d,
    d.rolling_std_30d,

    -- Composite Risk Score (0-100) — weights from dbt vars for calibration
    LEAST(100, (
        (CASE WHEN COALESCE(it.flag_impossible_travel, FALSE) THEN {{ var('weight_impossible_travel') }} ELSE 0 END)
        + (CASE WHEN COALESCE(v.flag_velocity_spike, FALSE) THEN {{ var('weight_velocity_spike') }} ELSE 0 END)
        + (CASE WHEN COALESCE(d.flag_behavioral_drift, FALSE) THEN {{ var('weight_behavioral_drift') }} ELSE 0 END)
    )) AS risk_score,

    -- Risk tier classification — cutoffs from dbt vars for calibration
    CASE
        WHEN (CASE WHEN COALESCE(it.flag_impossible_travel, FALSE) THEN {{ var('weight_impossible_travel') }} ELSE 0 END)
           + (CASE WHEN COALESCE(v.flag_velocity_spike, FALSE) THEN {{ var('weight_velocity_spike') }} ELSE 0 END)
           + (CASE WHEN COALESCE(d.flag_behavioral_drift, FALSE) THEN {{ var('weight_behavioral_drift') }} ELSE 0 END) >= {{ var('tier_critical') }}
        THEN 'CRITICAL'
        WHEN (CASE WHEN COALESCE(it.flag_impossible_travel, FALSE) THEN {{ var('weight_impossible_travel') }} ELSE 0 END)
           + (CASE WHEN COALESCE(v.flag_velocity_spike, FALSE) THEN {{ var('weight_velocity_spike') }} ELSE 0 END)
           + (CASE WHEN COALESCE(d.flag_behavioral_drift, FALSE) THEN {{ var('weight_behavioral_drift') }} ELSE 0 END) >= {{ var('tier_high') }}
        THEN 'HIGH'
        WHEN (CASE WHEN COALESCE(it.flag_impossible_travel, FALSE) THEN {{ var('weight_impossible_travel') }} ELSE 0 END)
           + (CASE WHEN COALESCE(v.flag_velocity_spike, FALSE) THEN {{ var('weight_velocity_spike') }} ELSE 0 END)
           + (CASE WHEN COALESCE(d.flag_behavioral_drift, FALSE) THEN {{ var('weight_behavioral_drift') }} ELSE 0 END) >= {{ var('tier_medium') }}
        THEN 'MEDIUM'
        ELSE 'LOW'
    END AS risk_tier,

    -- Detected (predicted) fraud
    CASE
        WHEN COALESCE(it.flag_impossible_travel, FALSE)
          OR COALESCE(v.flag_velocity_spike, FALSE)
          OR COALESCE(d.flag_behavioral_drift, FALSE)
        THEN TRUE
        ELSE FALSE
    END AS detected_fraud

FROM base_txns b
LEFT JOIN impossible_travel it ON b.transaction_id = it.transaction_id
LEFT JOIN velocity v ON b.transaction_id = v.transaction_id
LEFT JOIN drift d ON b.transaction_id = d.transaction_id
