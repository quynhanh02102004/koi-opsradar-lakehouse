{{ config(materialized='view') }}

SELECT
    review_content_hash,
    user_store_key,
    store_id,
    store_name,
    store_category,
    store_address,
    latitude,
    longitude,
    total_store_reviews,
    store_overall_rating,
    customer_name,
    review_star,
    review_content,
    sentiment,
    sentiment_score,
    CAST(time_posted AS DATE) AS review_date
FROM read_parquet('s3://{{ env_var("AWS_S3_BUCKET") }}/silver/**/*.parquet')