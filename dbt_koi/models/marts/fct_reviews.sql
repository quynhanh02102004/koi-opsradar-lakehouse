{{ config(
    materialized='external',
    location='s3://' ~ env_var("AWS_S3_BUCKET") ~ '/gold/fct_reviews/data.parquet',
    format='parquet'
) }}

SELECT
    review_content_hash,
    user_store_key,
    store_id,                
    review_date AS date_key, 
    customer_name,
    review_star AS rating,
    review_content,
    sentiment,
    sentiment_score
FROM {{ ref('stg_reviews') }}
