{{ config(
    materialized='external',
    location='s3://' ~ env_var("AWS_S3_BUCKET") ~ '/gold/dim_store/data.parquet',
    format='parquet'
) }}

SELECT DISTINCT
    store_id,
    store_name,
    store_category,
    store_address,
    latitude,
    longitude,
    regexp_replace(
        CASE 
            WHEN len(str_split(store_address, ',')) >= 2 THEN trim(str_split(store_address, ',')[-2])
            ELSE trim(store_address)
        END,
        '\s*[0-9]+$', 
        ''
    ) AS city
FROM {{ ref('stg_reviews') }}