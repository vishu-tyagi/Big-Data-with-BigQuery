import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@dag(
    # start_date=datetime(2018, 12, 31),
    start_date=datetime(2021, 10, 31),
    end_date=datetime(2021, 12, 31),
    schedule_interval="0 6 2 * *"   # 6 am on 2nd of every month
)
def pipeline():
    from nyc_taxi.data_pipeline import BigQueryPipeline


    AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
    data = BigQueryPipeline()
    data.make_dirs()

    @task()
    def download_data(dataset: str):
        data.download_data(dataset=dataset)
        return dataset

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def upload_to_gcs(dataset: str):
        from google.cloud import storage

        client = storage.Client()
        bucket = client.get_bucket(os.getenv("GCP_GCS_BUCKET"))

        data.upload_to_gcs(dataset=dataset, bucket=bucket)
        return dataset

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def create_bigquery_table(dataset: str):
        from google.cloud import storage

        client = storage.Client()
        bucket = client.get_bucket(os.getenv("GCP_GCS_BUCKET"))
        project_id = client.project

        data.create_bq_table(
            dataset=dataset,
            bucket=bucket,
            project_id=project_id,
            bq_schema=os.getenv("BIGQUERY_DATASET", "staging")
        )
        return

    success_task = BashOperator(
        task_id="sucess_id",
        bash_command=f"echo HELLO WORLD!"
    )

    dbt_debug = BashOperator(
        task_id = "dbt_debug",
        bash_command = f"cd {AIRFLOW_HOME}/dbt && dbt deps && dbt debug --profiles-dir ."
    )

    dbt_seed = BashOperator(
        task_id = "dbt_seed",
        bash_command = f"cd {AIRFLOW_HOME}/dbt && dbt deps && dbt seed --profiles-dir ."
    )

    dbt_run = BashOperator(
        task_id = "dbt_run",
        bash_command = f"cd {AIRFLOW_HOME}/dbt && dbt deps && dbt run --profiles-dir ."
    )

    yello_taxi_dataset = \
        'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
    green_taxi_dataset = \
        'green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'

    [
        create_bigquery_table(upload_to_gcs(download_data(green_taxi_dataset))),
        create_bigquery_table(upload_to_gcs(download_data(yello_taxi_dataset)))
    ] \
    >> success_task >> dbt_debug >> dbt_seed >> dbt_run

pipeline()