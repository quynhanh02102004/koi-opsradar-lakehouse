# KOI Thé Operational Radar: End-to-End Sentiment Lakehouse

An enterprise-grade, Data Lakehouse system designed to monitor and analyze **customer sentiment** across **over 40 KOI Thé branches from 4 different cities** in Vietnam by scraping, processing, and modeling **Google Maps reviews**

<img width="835" height="970" alt="{7C10F3F0-37C9-4D26-9FF3-875B90C28C9C}" src="https://github.com/user-attachments/assets/80bbd674-6f26-4a8c-a5b8-052c0188021a" />


---

## Table of Contents
1. Introduction
2. System Architecture
3. Tools & Technologies
4. Loading Strategy 
5. ELT Process 
6. Orchestration
7. Dashboard Insights
8. Project Structure

---

## 1. Introduction

### Project Overview
The **KOI Thé Operational Radar** is an **end-to-end Sentiment Lakehouse** designed to automate the **ingestion, processing, and analysis** of customer feedback. The system crawls **real reviews data from Google Maps**, processes them through a multi-stage pipeline **(Ingest → Clean ➔ Store ➔ Data Model ➔ Query)**, and serves structured analytical insights to stakeholders.

### Business Demand & Stakeholders
In the highly competitive F&B industry, modern consumers are becoming increasingly skeptical of **over-commercialized marketing campaigns, excessive influencer bookings, & seeded reviews** across social media platforms. As a result, customers **no longer fully trust feedback** that appears overly promotional or lacks authenticity. Instead, platforms such as **Google Maps** have become one of the primary sources customers rely on when researching products and services, as the reviews are perceived to come from **genuine users with real experiences**.


The key stakeholders include:
*   **Chief Operating Officer & Regional Managers:** To monitor regional performance, identify operational bottlenecks, and track branch-level Customer Satisfaction trends.
*   **Store Managers:** To receive direct feedback on daily store operations, product quality, and staff attitude.
*   **Customer Service Team:** To intercept negative reviews in real-time, enabling rapid intervention and crisis mitigation.

---

## 2. System Architecture

The project implements a **Data Lakehouse architecture**, separating **storage** and **computation**

<img width="1920" height="1080" alt="Collect Data (7)" src="https://github.com/user-attachments/assets/8bb79fa6-b899-4b29-bd0c-373f467dd5af" />


*   **Data Source:** Customer reviews and store metadata are scraped directly from Google Maps using a playwright-based scraper container.
*   **Storage Layer** AWS S3 serves as the scalable, single source of truth, structured **Medallion Paradigm** into three layers
*   **ETL process:** DuckDB is the high-performance, embedded, in-memory query engine. Running natively inside the Dagster pipeline container, **DuckDB handles all heavy-duty ETL tasks**. DuckDB processes large datasets, writing Parquet outputs to S3, then terminating.
    Scraped data is **deduplicated and stored** in the **Bronze** layer. The data is then **transformed, cleaned, processed with NLP techniques**, and loaded into the **Silver layer**. The refined dataset is subsequently **modeled into a Star Schema** and stored in the **Gold** to support **OLAP** analytics.
*   **Data Modeling:** Compiles and orchestrates the transformation logic. **dbt manages the SQL compilation** that converts Silver's flat tables into the Gold layer (Fact and Dimension tables)
*   **Serving Engine** Serves as the query engine for **end-user business intelligence and dashboard visualization on Metabase**. **AWS Athena reads the metadata** catalog registered in the AWS Glue Data Catalog and **queries the S3 Gold Parquet** files directly. 
    This clean separation of concerns decouples heavy ETL computations (executed locally by DuckDB) from interactive analytical queries (executed on the cloud by Athena), preventing concurrent Metabase BI queries from causing performance bottlenecks on the data pipeline.
*   **Orchestrator:** **Dagster coordinates the entire execution lifecycle**. It manages the workflow using Software-Defined Assets (SDAs), tracking physical data dependencies, passing partition keys (daily run dates), managing execution state, and triggering subsequent stages (dbt compiles or AWS Athena catalog updates) only when their upstream dependencies have successfully materialized.

<img width="1519" height="554" alt="{0E27594A-68AB-4FE4-BE4D-9BB41758DEDF}" src="https://github.com/user-attachments/assets/ec369bda-31cf-4ed5-8515-0e094107dfc9" />

---

## 3. Tools & Technologies

| Component | Technology Used
| :--- | :--- 
| **Data Source** | Google Maps Reviews | Source of unstructured customer feedback. |
| **Orchestrator** | Dagster | Manages the workflow using Software-Defined Assets. |
| **Compute Engine** | DuckDB | High-performance, embedded in-memory SQL database. |
| **Transformation** | dbt | Coordinates SQL-based modeling and schema building. |
| **Storage Layer (Data Lake)** | AWS S3 | Serverless object storage partitioned by ingestion date. |
| **Serving Engine** | AWS Athena | Serverless, interactive SQL query engine. |
| **Visualization BI** | Metabase | Frontend BI tool connected to AWS Athena. |
| **DevOps** | Docker & Docker Compose | Containerizes the runner and scraper environments. |

---

## 4. Loading Strategy 

### Ingestion 
The Lakehouse separates its ingestion workflow into two distinct strategies: **Initial Load** and **Daily Incremental Ingestion**
*   **Bronze & Silver Layers:** Store the daily incremental delta (only reviews that were newly scraped or modified on that specific day).
*   **Gold Layer:** Aggregates all historical partitions recursively to present a complete, unified view.

#### 1. Initial Load (Bulk Backfill)
*   Establish the **historical database** baseline and write them on S3
*   The scraper is configured to execute a deep crawl (up to 300+ reviews per store across all over 40 branches).

#### 2. Daily Incremental Ingestion
*   Runs automatically **every day at 06:00 AM** ICT and just **scrape reviews during the day before**
*   Capture only the daily delta (newly created or modified reviews)

### Partitioning Scheme

The storage paths on AWS S3 are strictly **partitioned by the pipeline execution date**
The physical layout on AWS S3 is structured hierarchically as follows:

```text
s3://koi-opsradar-lakehouse-bucket/
├── bronze/
│   ├── yearYYYY/
│   │   └── monthMM/
│   │       ├── dayYY/
│   │          ....
├── silver/
│   ├── yearYYYY/
│   │   └── monthMM/
│   │       ├── dayYY/
│   │           ....
└── gold/
    ├── fct_reviews/
    ├── dim_store/
    └── dim_date/
                ....
```
---

## 5. ELT Process 

The data pipeline progresses through three logical **Medallion layers to transform raw text into structured business intelligence**:
### 1. Bronze Layer (Raw Ingestion)
*   **Extraction:** The scraper fetches reviews and writes a raw JSONL file.
*   **Flattening:** DuckDB reads the JSONL, unnests the nested array `user_reviews_extended` to flatten the attributes.
*   **Filtering:** Deduplicated against S3 history using an anti-join, and written as raw Parquet partition files

### 2. Silver Layer (Cleaning & NLP Enrichment)
*   **Isolated Processing:** Silver only reads and processes the specific daily Bronze partition file.
*   **Date Imputation:** Resolves missing or relative Google Maps timestamps by imputing nulls with the execution date.
*   **Hybrid Sentiment Analysis:** 
    *   *Rule-Based (Star-only reviews):* If the review text is empty or null, it bypasses the NLP model to save computation. It maps rating $\ge 4$ to `positive`, $3$ to `neutral`, and $\le 2$ to `negative` (confidence score `1.0`).
    *   *NLP-Based (Text reviews):* If text is present, it invokes the `underthesea` model to perform Vietnamese sentiment analysis (`positive/negative/neutral`). 

### 3. Gold Layer (Dimensional Star Schema)
dbt orchestrates the modeling of Silver data into a highly optimized Star Schema consisting of **1 Fact Table** and **2 Dimension Tables**:
*   **dim_date:** A dynamically generated date dimension 
*   **dim_store:** A dimension table containing store attributes and geographical metadata.
*   **fact_reviews:** The central fact table mapping metrics and foreign keys 
*  Every run completely overwrites the S3 Gold targets. if the Gold layer is deleted, a single dbt run reconstructs the entire timeline from the raw S3 Silver history in seconds.

<img width="1024" height="559" alt="image" src="https://github.com/user-attachments/assets/b7834452-d5a2-454a-9e6c-8288df22f085" />

---

## 6. Orchestration

The pipeline is orchestrated in Dagster, emphasizing data dependencies over task execution.

<img width="1804" height="694" alt="{2B723DAC-E58B-4DC5-B47F-EA738873053A}" src="https://github.com/user-attachments/assets/1b67933a-258f-4f76-b4f1-c1c60c187f2d" />


*   **`bronze_raw_reviews`:** Invokes the Google Maps scraper containe and saves raw Parquet to S3.
*   **`silver_clean_reviews`:** Automatically triggers after Bronze finishes, executes NLP sentiment scoring, and saves enriched Parquet.
*   **`gold_marts_dbt`:** Invokes dbt as a subprocess, prompting DuckDB to rebuild the consolidated Star Schema on S3.
*   **`update_athena_catalog`:** A crucial synchronous asset. Since AWS Athena is asynchronous, this asset uses a `while True` polling loop via `boto3` to drop old tables and create new ones. 

---

## 7. Dashboard Insights

Metabase connects directly to AWS Athena using the catalog `koi_opsradar_db`. Because of Athena's serverless pay-per-query model, stakeholders can query dashboards simultaneously without impacting ELT pipelines.
<img width="1204" height="919" alt="{0D026149-7F5D-4EB9-90BA-484D465142A9}" src="https://github.com/user-attachments/assets/fd65d879-d837-4fad-b004-0c08f36dc1e8" />



---

## 8. Project Structure

```text
koi-opsradar-lakehouse/
├── .env                       # Environment variables (AWS Keys, S3 Bucket)
├── docker-compose.yml         # Local container orchestration (Postgres, Dagster, Metabase)
├── Dockerfile.dagster         # Custom Dagster image with Docker CLI & dbt
├── pyproject.toml             # Python dependencies managed by `uv`
├── src/
│   ├── scraper/
│   │   ├── queries.txt        # Target Google Maps stores
│   │   └── run_scraper.py     # Scraper execution wrapper 
│   └── pipeline/
│       ├── __init__.py        # Dagster Definitions & Schedules
│       └── assets.py          # Bronze, Silver, Gold, and Athena assets
├── dbt_koi/
│   ├── dbt_project.yml        # dbt configuration (Materialized: External)
│   ├── profiles.yml           # DuckDB-S3 connector settings
│   └── models/
│       ├── staging/
│       │   └── stg_reviews.sql
│       └── marts/
│           ├── dim_store.sql  
│           ├── dim_date.sql   
│           └── fct_reviews.sql 
