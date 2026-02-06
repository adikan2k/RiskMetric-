-- Staging model: reads raw Bronze-layer Parquet into DuckDB
-- Casts types and standardizes column names

WITH raw AS (
    SELECT *
    FROM read_parquet('{{ var("raw_transactions_path", "../data/raw_transactions.parquet") }}')
)

SELECT
    transaction_id,
    user_id,
    CAST(timestamp AS TIMESTAMP) AS transaction_timestamp,
    CAST(amount AS DOUBLE) AS amount,
    merchant_name,
    merchant_category,
    city,
    country,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,
    CAST(is_fraud AS BOOLEAN) AS is_fraud,
    fraud_type
FROM raw
