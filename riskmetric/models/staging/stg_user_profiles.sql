-- Staging model: reads raw user profiles Parquet

WITH raw AS (
    SELECT *
    FROM read_parquet('{{ var("user_profiles_path", "../data/user_profiles.parquet") }}')
)

SELECT
    user_id,
    home_city,
    home_country,
    CAST(home_lat AS DOUBLE) AS home_lat,
    CAST(home_lon AS DOUBLE) AS home_lon,
    CAST(avg_amount AS DOUBLE) AS avg_amount,
    CAST(std_amount AS DOUBLE) AS std_amount
FROM raw
