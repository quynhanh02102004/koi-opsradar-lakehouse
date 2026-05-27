{{ config(
    materialized='external',
    location='s3://' ~ env_var("AWS_S3_BUCKET") ~ '/gold/dim_date/data.parquet',
    format='parquet'
) }}

SELECT
    CAST(d AS DATE) AS date_key,
    CAST(d AS DATE) AS date_value, 
    EXTRACT(year FROM d) AS year,
    EXTRACT(month FROM d) AS month,
    EXTRACT(quarter FROM d) AS quarter,
    DAYNAME(d) AS day_of_week
FROM (
    SELECT range AS d 
    FROM range(DATE '2000-01-01', DATE '2026-12-31', INTERVAL '1' DAY)
)