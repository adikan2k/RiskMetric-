-- Gold Layer: Threshold Calibration Analysis
-- Evaluates detection performance across different threshold values
-- to support data-driven calibration of risk parameters.

-- Impossible Travel: Test speed thresholds from 200-800 mph
WITH speed_thresholds AS (
    SELECT
        threshold_mph,
        SUM(CASE WHEN ground_speed_mph > threshold_mph AND is_fraud AND fraud_type = 'impossible_travel' THEN 1 ELSE 0 END) AS tp,
        SUM(CASE WHEN ground_speed_mph > threshold_mph AND NOT (is_fraud AND fraud_type = 'impossible_travel') THEN 1 ELSE 0 END) AS fp,
        SUM(CASE WHEN (ground_speed_mph <= threshold_mph OR ground_speed_mph IS NULL) AND is_fraud AND fraud_type = 'impossible_travel' THEN 1 ELSE 0 END) AS fn
    FROM {{ ref('silver_impossible_travel') }}
    CROSS JOIN (
        SELECT UNNEST([200, 300, 400, 500, 600, 700, 800]) AS threshold_mph
    ) thresholds
    GROUP BY threshold_mph
),

speed_metrics AS (
    SELECT
        'IMPOSSIBLE_TRAVEL' AS archetype,
        threshold_mph AS threshold_value,
        'mph' AS threshold_unit,
        tp, fp, fn,
        CASE WHEN tp + fp > 0 THEN ROUND(tp * 1.0 / (tp + fp), 4) ELSE 0 END AS precision_score,
        CASE WHEN tp + fn > 0 THEN ROUND(tp * 1.0 / (tp + fn), 4) ELSE 0 END AS recall_score,
        CASE
            WHEN tp + fp > 0 AND tp + fn > 0
                 AND (tp * 1.0 / (tp + fp) + tp * 1.0 / (tp + fn)) > 0
            THEN ROUND(
                2.0 * (tp * 1.0 / (tp + fp)) * (tp * 1.0 / (tp + fn))
                / (tp * 1.0 / (tp + fp) + tp * 1.0 / (tp + fn)), 4)
            ELSE 0
        END AS f1_score
    FROM speed_thresholds
),

-- Velocity Spike: Test count thresholds from 3-20 txns
velocity_thresholds AS (
    SELECT
        threshold_count,
        SUM(CASE WHEN txn_count_60s >= threshold_count AND is_fraud AND fraud_type = 'velocity_spike' THEN 1 ELSE 0 END) AS tp,
        SUM(CASE WHEN txn_count_60s >= threshold_count AND NOT (is_fraud AND fraud_type = 'velocity_spike') THEN 1 ELSE 0 END) AS fp,
        SUM(CASE WHEN txn_count_60s < threshold_count AND is_fraud AND fraud_type = 'velocity_spike' THEN 1 ELSE 0 END) AS fn
    FROM {{ ref('silver_velocity_spikes') }}
    CROSS JOIN (
        SELECT UNNEST([3, 5, 7, 8, 10, 12, 15, 20]) AS threshold_count
    ) thresholds
    GROUP BY threshold_count
),

velocity_metrics AS (
    SELECT
        'VELOCITY_SPIKE' AS archetype,
        threshold_count AS threshold_value,
        'txn_count_60s' AS threshold_unit,
        tp, fp, fn,
        CASE WHEN tp + fp > 0 THEN ROUND(tp * 1.0 / (tp + fp), 4) ELSE 0 END AS precision_score,
        CASE WHEN tp + fn > 0 THEN ROUND(tp * 1.0 / (tp + fn), 4) ELSE 0 END AS recall_score,
        CASE
            WHEN tp + fp > 0 AND tp + fn > 0
                 AND (tp * 1.0 / (tp + fp) + tp * 1.0 / (tp + fn)) > 0
            THEN ROUND(
                2.0 * (tp * 1.0 / (tp + fp)) * (tp * 1.0 / (tp + fn))
                / (tp * 1.0 / (tp + fp) + tp * 1.0 / (tp + fn)), 4)
            ELSE 0
        END AS f1_score
    FROM velocity_thresholds
),

-- Behavioral Drift: Test Z-Score thresholds from 1.5-5.0
drift_thresholds AS (
    SELECT
        threshold_z,
        SUM(CASE WHEN z_score IS NOT NULL AND ABS(z_score) > threshold_z AND is_fraud AND fraud_type = 'behavioral_drift' THEN 1 ELSE 0 END) AS tp,
        SUM(CASE WHEN z_score IS NOT NULL AND ABS(z_score) > threshold_z AND NOT (is_fraud AND fraud_type = 'behavioral_drift') THEN 1 ELSE 0 END) AS fp,
        SUM(CASE WHEN (z_score IS NULL OR ABS(z_score) <= threshold_z) AND is_fraud AND fraud_type = 'behavioral_drift' THEN 1 ELSE 0 END) AS fn
    FROM {{ ref('silver_behavioral_drift') }}
    CROSS JOIN (
        SELECT UNNEST([1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 5.0]) AS threshold_z
    ) thresholds
    GROUP BY threshold_z
),

drift_metrics AS (
    SELECT
        'BEHAVIORAL_DRIFT' AS archetype,
        threshold_z AS threshold_value,
        'z_score_sigma' AS threshold_unit,
        tp, fp, fn,
        CASE WHEN tp + fp > 0 THEN ROUND(tp * 1.0 / (tp + fp), 4) ELSE 0 END AS precision_score,
        CASE WHEN tp + fn > 0 THEN ROUND(tp * 1.0 / (tp + fn), 4) ELSE 0 END AS recall_score,
        CASE
            WHEN tp + fp > 0 AND tp + fn > 0
                 AND (tp * 1.0 / (tp + fp) + tp * 1.0 / (tp + fn)) > 0
            THEN ROUND(
                2.0 * (tp * 1.0 / (tp + fp)) * (tp * 1.0 / (tp + fn))
                / (tp * 1.0 / (tp + fp) + tp * 1.0 / (tp + fn)), 4)
            ELSE 0
        END AS f1_score
    FROM drift_thresholds
)

SELECT * FROM speed_metrics
UNION ALL
SELECT * FROM velocity_metrics
UNION ALL
SELECT * FROM drift_metrics
ORDER BY archetype, threshold_value
