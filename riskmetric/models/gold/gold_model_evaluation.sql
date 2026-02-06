-- Gold Layer: Model Evaluation Metrics
-- Computes precision, recall, F1-score, and confusion matrix
-- against ground-truth labels for each fraud archetype and overall.

WITH confusion_base AS (
    SELECT
        transaction_id,
        labeled_fraud,
        detected_fraud,

        -- Per-archetype ground truth vs detection
        COALESCE(labeled_fraud_type = 'impossible_travel', FALSE) AS label_impossible_travel,
        COALESCE(labeled_fraud_type = 'velocity_spike', FALSE) AS label_velocity_spike,
        COALESCE(labeled_fraud_type = 'behavioral_drift', FALSE) AS label_behavioral_drift,

        flag_impossible_travel,
        flag_velocity_spike,
        flag_behavioral_drift

    FROM {{ ref('gold_risk_scores') }}
),

-- Overall confusion matrix
overall_matrix AS (
    SELECT
        'OVERALL' AS archetype,
        SUM(CASE WHEN labeled_fraud AND detected_fraud THEN 1 ELSE 0 END) AS true_positives,
        SUM(CASE WHEN NOT labeled_fraud AND detected_fraud THEN 1 ELSE 0 END) AS false_positives,
        SUM(CASE WHEN labeled_fraud AND NOT detected_fraud THEN 1 ELSE 0 END) AS false_negatives,
        SUM(CASE WHEN NOT labeled_fraud AND NOT detected_fraud THEN 1 ELSE 0 END) AS true_negatives
    FROM confusion_base
),

-- Impossible Travel confusion matrix
it_matrix AS (
    SELECT
        'IMPOSSIBLE_TRAVEL' AS archetype,
        SUM(CASE WHEN label_impossible_travel AND flag_impossible_travel THEN 1 ELSE 0 END) AS true_positives,
        SUM(CASE WHEN NOT label_impossible_travel AND flag_impossible_travel THEN 1 ELSE 0 END) AS false_positives,
        SUM(CASE WHEN label_impossible_travel AND NOT flag_impossible_travel THEN 1 ELSE 0 END) AS false_negatives,
        SUM(CASE WHEN NOT label_impossible_travel AND NOT flag_impossible_travel THEN 1 ELSE 0 END) AS true_negatives
    FROM confusion_base
),

-- Velocity Spike confusion matrix
vs_matrix AS (
    SELECT
        'VELOCITY_SPIKE' AS archetype,
        SUM(CASE WHEN label_velocity_spike AND flag_velocity_spike THEN 1 ELSE 0 END) AS true_positives,
        SUM(CASE WHEN NOT label_velocity_spike AND flag_velocity_spike THEN 1 ELSE 0 END) AS false_positives,
        SUM(CASE WHEN label_velocity_spike AND NOT flag_velocity_spike THEN 1 ELSE 0 END) AS false_negatives,
        SUM(CASE WHEN NOT label_velocity_spike AND NOT flag_velocity_spike THEN 1 ELSE 0 END) AS true_negatives
    FROM confusion_base
),

-- Behavioral Drift confusion matrix
bd_matrix AS (
    SELECT
        'BEHAVIORAL_DRIFT' AS archetype,
        SUM(CASE WHEN label_behavioral_drift AND flag_behavioral_drift THEN 1 ELSE 0 END) AS true_positives,
        SUM(CASE WHEN NOT label_behavioral_drift AND flag_behavioral_drift THEN 1 ELSE 0 END) AS false_positives,
        SUM(CASE WHEN label_behavioral_drift AND NOT flag_behavioral_drift THEN 1 ELSE 0 END) AS false_negatives,
        SUM(CASE WHEN NOT label_behavioral_drift AND NOT flag_behavioral_drift THEN 1 ELSE 0 END) AS true_negatives
    FROM confusion_base
),

combined AS (
    SELECT * FROM overall_matrix
    UNION ALL SELECT * FROM it_matrix
    UNION ALL SELECT * FROM vs_matrix
    UNION ALL SELECT * FROM bd_matrix
)

SELECT
    archetype,
    true_positives,
    false_positives,
    false_negatives,
    true_negatives,
    true_positives + false_positives + false_negatives + true_negatives AS total_samples,

    -- Precision = TP / (TP + FP)
    CASE
        WHEN (true_positives + false_positives) > 0
        THEN ROUND(true_positives * 1.0 / (true_positives + false_positives), 6)
        ELSE 0
    END AS precision_score,

    -- Recall = TP / (TP + FN)
    CASE
        WHEN (true_positives + false_negatives) > 0
        THEN ROUND(true_positives * 1.0 / (true_positives + false_negatives), 6)
        ELSE 0
    END AS recall_score,

    -- F1 = 2 * (Precision * Recall) / (Precision + Recall)
    CASE
        WHEN (true_positives + false_positives) > 0 AND (true_positives + false_negatives) > 0
             AND (
                 (true_positives * 1.0 / (true_positives + false_positives))
                 + (true_positives * 1.0 / (true_positives + false_negatives))
             ) > 0
        THEN ROUND(
            2.0 * (true_positives * 1.0 / (true_positives + false_positives))
                * (true_positives * 1.0 / (true_positives + false_negatives))
            / (
                (true_positives * 1.0 / (true_positives + false_positives))
                + (true_positives * 1.0 / (true_positives + false_negatives))
            ), 6)
        ELSE 0
    END AS f1_score,

    -- Accuracy = (TP + TN) / Total
    ROUND(
        (true_positives + true_negatives) * 1.0
        / (true_positives + false_positives + false_negatives + true_negatives),
        6
    ) AS accuracy,

    -- False Positive Rate = FP / (FP + TN)
    CASE
        WHEN (false_positives + true_negatives) > 0
        THEN ROUND(false_positives * 1.0 / (false_positives + true_negatives), 6)
        ELSE 0
    END AS false_positive_rate

FROM combined
