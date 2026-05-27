from dagster import Definitions, ScheduleDefinition, define_asset_job
from src.pipeline.assets import bronze_raw_reviews, silver_clean_reviews, gold_marts_dbt, update_athena_catalog

opsradar_job = define_asset_job(
    name="opsradar_daily_job",
    selection=[bronze_raw_reviews, silver_clean_reviews, gold_marts_dbt, update_athena_catalog]
)

# Run on 06:00 AM daily
opsradar_schedule = ScheduleDefinition(
    job=opsradar_job,
    cron_schedule="0 6 * * *",
    execution_timezone="Asia/Ho_Chi_Minh"
)

defs = Definitions(
    assets=[bronze_raw_reviews, silver_clean_reviews, gold_marts_dbt, update_athena_catalog],
    schedules=[opsradar_schedule]
)