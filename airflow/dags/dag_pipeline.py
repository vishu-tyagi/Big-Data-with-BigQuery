from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


# AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# YELLOW_DATA = 'green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
# GREEN_DATA = 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
# YEAR = f'{{ execution_date.strftime(\'%Y\') }}'
# MONTH = f'{{ execution_date.strftime(\'%M\') }}'

@dag(
    start_date=datetime(2021, 1, 1),
    schedule_interval="0 6 2 * *",
    catchup=False
)
def pipeline():
    from nyc_taxi.data_pipeline import BigQueryPipeline

    data = BigQueryPipeline()
    data.make_dirs()

    @task()
    def download_data(dataset: str):
        data.download_data(dataset)
        return dataset

    @task()
    def upload_to_gcs(dataset: str):
        data.upload_to_gcs(dataset)
        return

    dataset = f"green_tripdata_2021-01.parquet"
    upload_to_gcs(download_data(dataset))

pipeline()