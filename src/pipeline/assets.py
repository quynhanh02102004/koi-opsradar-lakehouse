import os
import json
import subprocess
import duckdb
import pandas as pd
import time
import boto3
from datetime import datetime, timedelta
from dagster import asset, AssetExecutionContext, DailyPartitionsDefinition
from underthesea import sentiment as ut_sentiment
from src.scraper.run_scraper import execute_scraper

daily_partition = DailyPartitionsDefinition(start_date="2026-01-01")
S3_BUCKET = os.getenv("AWS_S3_BUCKET")


# HELPER FUNCTION

def execute_athena_query_sync(client, query, output_loc):
    """
    forces boto3 to wait until Athena finishes executing a query before running the next command.
    resolves asynchronous race condition issues.

    """
    response = client.start_query_execution(
        QueryString=query,
        ResultConfiguration={'OutputLocation': output_loc}
    )
    exec_id = response['QueryExecutionId']
    
    while True:
        status = client.get_query_execution(QueryExecutionId=exec_id)['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            if status != 'SUCCEEDED':
                reason = client.get_query_execution(QueryExecutionId=exec_id)['QueryExecution']['Status'].get('StateChangeReason')
                raise Exception(f"❌ Athena query thất bại: {reason} | Query: {query}")
            break
        time.sleep(2) # WAIT 2S



# ASSET 1: FROM SCRAPED RAW DATA TO BRONZE LAYER

@asset(partitions_def=daily_partition)
def bronze_raw_reviews(context: AssetExecutionContext) -> str:
    yesterday_str = context.partition_key 
    year, month, day = yesterday_str.split("-")
    
    queries_path = "src/scraper/queries.txt"
    raw_output_filename = "reviews_data_temp.json"
    local_raw_path = f"/shared_data/{raw_output_filename}"
    bronze_s3_dir = f"s3://{S3_BUCKET}/bronze/year{year}/month{month}/day{day}/"
    output_file = f"{bronze_s3_dir}data.parquet"
    
    if os.path.exists(local_raw_path):
        os.remove(local_raw_path)
        
    # CALL Docker Scraper
    execute_scraper(queries_path, raw_output_filename)

    # DuckDB & AWS Credentials
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("CREATE SECRET aws_s3_secret (TYPE S3, PROVIDER CREDENTIAL_CHAIN);")

    try:
        con.execute(f"SELECT 1 FROM read_parquet('s3://{S3_BUCKET}/bronze/**/*.parquet') LIMIT 1")
        is_first_run = False
    except Exception:
        is_first_run = True

    if is_first_run:
        context.log.info("Initial Load")
        final_sql = f"""
            SELECT 
                md5(coalesce(title, '') || coalesce(review.Name, '') || coalesce(review.Description, '')) AS review_content_hash,
                md5(coalesce(title, '') || coalesce(review.Name, '')) AS user_store_key,
                place_id AS store_id,
                title AS store_name,
                category AS store_category,
                address AS store_address,
                latitude,
                longtitude AS longitude,
                review_count AS total_store_reviews,
                review_rating AS store_overall_rating,
                review.Name AS customer_name,
                review.Rating AS review_star,
                review.Description AS review_content,
                review.When AS time_posted
            FROM (
                SELECT *, UNNEST(user_reviews_extended) AS review 
                FROM read_json_auto('{local_raw_path}')
            )
        """
    else:
        context.log.info("Incremental Load , Left anti join")
        final_sql = f"""
            WITH incoming_data AS (
                SELECT 
                    md5(coalesce(title, '') || coalesce(review.Name, '') || coalesce(review.Description, '')) AS review_content_hash,
                    md5(coalesce(title, '') || coalesce(review.Name, '')) AS user_store_key,
                    place_id AS store_id,
                    title AS store_name,
                    category AS store_category,
                    address AS store_address,
                    latitude,
                    longtitude AS longitude,
                    review_count AS total_store_reviews,
                    review_rating AS store_overall_rating,
                    review.Name AS customer_name,
                    review.Rating AS review_star,
                    review.Description AS review_content,
                    review.When AS time_posted
                FROM (
                    SELECT *, UNNEST(user_reviews_extended) AS review 
                    FROM read_json_auto('{local_raw_path}')
                )
            )
            SELECT A.* 
            FROM incoming_data A
            ANTI JOIN read_parquet('s3://{S3_BUCKET}/bronze/**/*.parquet') B 
            ON A.review_content_hash = B.review_content_hash
        """

    con.execute(f"COPY ({final_sql}) TO '{output_file}' (FORMAT PARQUET)")
    row_count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_file}')").fetchone()[0]
    context.log.info(f"Đã lưu {row_count} dòng mới vào Bronze tại: {output_file}")
    return output_file



# NLP - Sentiment scores

def predict_sentiment_vietnamese(text: str, rating: int) -> tuple[str, float]:
    if not text or pd.isna(text) or str(text).strip() == "" or str(text).strip().lower() == "nan":
        if rating >= 4: return "positive", 1.0
        elif rating <= 2: return "negative", 1.0
        else: return "neutral", 1.0
    try:
        pred = ut_sentiment(str(text))
        return pred if pred else "neutral", 1.0
    except Exception:
        return "neutral", 0.5



# ASSET 2: FROM BRONZE TO SILVER LAYER

@asset(partitions_def=daily_partition, deps=[bronze_raw_reviews])
def silver_clean_reviews(context: AssetExecutionContext) -> str:
    yesterday_str = context.partition_key
    year, month, day = yesterday_str.split("-")
    
    bronze_file = f"s3://{S3_BUCKET}/bronze/year{year}/month{month}/day{day}/data.parquet"
    silver_s3_path = f"s3://{S3_BUCKET}/silver/year{year}/month{month}/day{day}/clean_data.parquet"

    columns_schema = [
        'review_content_hash', 'user_store_key', 'store_id', 'store_name',
        'store_category', 'store_address', 'latitude', 'longitude',
        'total_store_reviews', 'store_overall_rating', 'customer_name',
        'review_star', 'review_content', 'sentiment', 'sentiment_score', 'time_posted'
    ]

    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("CREATE SECRET aws_s3_secret (TYPE S3, PROVIDER CREDENTIAL_CHAIN);")

    try:
        df = con.execute(f"SELECT * FROM read_parquet('{bronze_file}')").df()
    except Exception as e:
        if "HTTP" in str(e) or "No files match" in str(e):
            context.log.warn(f"⚠️ Không có file Bronze cho ngày {yesterday_str}. Tạo Silver rỗng.")
            df = pd.DataFrame()
        else:
            raise e

    if df.empty:
        empty_df = pd.DataFrame(columns=columns_schema)
        con.register('df_empty_temp', empty_df)
        con.execute(f"COPY (SELECT * FROM df_empty_temp) TO '{silver_s3_path}' (FORMAT PARQUET)")
        return silver_s3_path

    sentiments, scores, clean_dates = [], [], []
    for _, row in df.iterrows():
        label, score = predict_sentiment_vietnamese(row['review_content'], row['review_star'])
        sentiments.append(label)
        scores.append(score)
        
        date_val = row['time_posted']
        if pd.isna(date_val) or str(date_val).strip() == "" or str(date_val).strip().lower() == "nan":
            clean_dates.append(yesterday_str)
        else:
            clean_dates.append(str(date_val))

    df['sentiment'] = sentiments
    df['sentiment_score'] = scores
    df['time_posted'] = clean_dates
    df = df[columns_schema]

    con.register('df_silver_enriched', df)
    con.execute(f"COPY (SELECT * FROM df_silver_enriched) TO '{silver_s3_path}' (FORMAT PARQUET)")
    row_count = len(df)
    context.log.info(f"Prossed NLP and saved {row_count} rows cleaned data in silver: {silver_s3_path}")
    return silver_s3_path



# ASSET 3: FROM SILVER TO GOLD LAYER (DBT RUN)

@asset(partitions_def=daily_partition, deps=[silver_clean_reviews])
def gold_marts_dbt(context: AssetExecutionContext):
    """
    Asset 3: dbt run will automatically extract city name by DuckDB SQL and write file Gold Parquet on S3.
    """
    context.log.info("Trigger dbt build Star Schema on S3")
    subprocess.run(["dbt", "run", "--project-dir", "dbt_koi", "--profiles-dir", "dbt_koi"], check=True)



# ASSET 4: ATHENA CATALOG SYNCHRONIZER

@asset(partitions_def=daily_partition, deps=[gold_marts_dbt])
def update_athena_catalog(context: AssetExecutionContext):
    
    context.log.info("Connecting to AWS Athena to update Catalog")

    aws_region = os.getenv("AWS_REGION", "ap-southeast-1")
    client = boto3.client('athena', region_name=aws_region)
    
    db_name = "koi_opsradar_db"
    out_loc = f"s3://{S3_BUCKET}/athena_results/"
    
    
    # create database
    context.log.info(f"check and create database {db_name}...")
    execute_athena_query_sync(client, f"CREATE DATABASE IF NOT EXISTS {db_name};", out_loc)

    #  DDL of Star Schema tables 
    tables_ddl = {
         "fct_reviews": f"""
            CREATE EXTERNAL TABLE {db_name}.fct_reviews (
                review_content_hash STRING,
                user_store_key STRING,
                store_id STRING,
                date_key DATE,
                customer_name STRING,
                rating BIGINT,
                review_content STRING,
                sentiment STRING,
                sentiment_score DOUBLE
            )
            STORED AS PARQUET
            LOCATION 's3://{S3_BUCKET}/gold/fct_reviews/';
        """,
        
        # dim_store 
        "dim_store": f"""
            CREATE EXTERNAL TABLE {db_name}.dim_store (
                store_id STRING,
                store_name STRING,
                store_category STRING,
                store_address STRING,
                latitude DOUBLE,
                longitude DOUBLE,
                city STRING
            )
            STORED AS PARQUET
            LOCATION 's3://{S3_BUCKET}/gold/dim_store/';
        """,
        
        "dim_date": f"""
            CREATE EXTERNAL TABLE {db_name}.dim_date (
                date_key DATE,
                date_value DATE,
                year BIGINT,
                month BIGINT,
                quarter BIGINT,
                day_of_week STRING
            )
            STORED AS PARQUET
            LOCATION 's3://{S3_BUCKET}/gold/dim_date/';
        """
    }

    
    for table_name, create_query in tables_ddl.items():
        context.log.info(f"update schema for : {table_name}...")
        
        
        drop_query = f"DROP TABLE IF EXISTS {db_name}.{table_name};"
        execute_athena_query_sync(client, drop_query, out_loc)
        
        
        execute_athena_query_sync(client, create_query, out_loc)
    
    context.log.info("done process")