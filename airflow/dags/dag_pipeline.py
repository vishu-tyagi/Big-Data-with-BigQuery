from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@dag(
    start_date=datetime(2018, 12, 31),
    end_date=datetime(2021, 12, 31),
    schedule_interval="0 6 2 * *"
)
def pipeline():
    from nyc_taxi.data_pipeline import BigQueryPipeline

    data = BigQueryPipeline()
    data.make_dirs()

    @task()
    def download_data(dataset: str):
        data.download_data(dataset=dataset)
        return dataset

    @task(retries=2, retry_delay=timedelta(minutes=2))
    def upload_to_gcs(dataset: str):
        import os
        from google.cloud import storage

        client = storage.Client()
        bucket = client.get_bucket(os.getenv("GCP_GCS_BUCKET"))

        data.upload_to_gcs(dataset=dataset, bucket=bucket)
        return

    yello_taxi_dataset = \
        'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
    green_taxi_dataset = \
        'green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'

    upload_to_gcs(download_data(green_taxi_dataset))
    upload_to_gcs(download_data(yello_taxi_dataset))

pipeline()