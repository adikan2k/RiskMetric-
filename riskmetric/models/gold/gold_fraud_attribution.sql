-- Gold Layer: Fraud Attribution Report
-- Executive-level summary of detected fraud by archetype, time, and geography.

WITH risk_data AS (
    SELECT * FROM {{ ref('gold_risk_scores') }}
    WHERE detected_fraud = TRUE
)

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
    risk_score,
    risk_tier,

    -- Attribution: which archetype(s) triggered the flag
    flag_impossible_travel,
    flag_velocity_spike,
    flag_behavioral_drift,

    -- Primary fraud type attribution
    CASE
        WHEN flag_impossible_travel AND flag_velocity_spike THEN 'MULTI: Travel + Velocity'
        WHEN flag_impossible_travel AND flag_behavioral_drift THEN 'MULTI: Travel + Drift'
        WHEN flag_velocity_spike AND flag_behavioral_drift THEN 'MULTI: Velocity + Drift'
        WHEN flag_impossible_travel THEN 'Impossible Travel'
        WHEN flag_velocity_spike THEN 'Velocity Spike'
        WHEN flag_behavioral_drift THEN 'Behavioral Drift'
        ELSE 'Unknown'
    END AS primary_fraud_attribution,

    -- Supporting evidence
    ground_speed_mph,
    distance_miles,
    time_gap_hours,
    prev_city,
    txn_count_60s,
    z_score,
    rolling_avg_30d,

    -- Labeled ground truth (for model evaluation)
    labeled_fraud,
    labeled_fraud_type,

    -- Detection accuracy
    CASE
        WHEN labeled_fraud = TRUE AND detected_fraud = TRUE THEN 'TRUE_POSITIVE'
        WHEN labeled_fraud = FALSE AND detected_fraud = TRUE THEN 'FALSE_POSITIVE'
        ELSE 'UNKNOWN'
    END AS detection_accuracy

FROM risk_data
