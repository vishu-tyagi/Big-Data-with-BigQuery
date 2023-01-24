import os
from google.cloud import storage
from nyc_taxi.data_pipeline import BigQueryPipeline
from nyc_taxi.utils import timing


@timing
def download_data(dataset: str):
    data = BigQueryPipeline()
    data.make_dirs()
    data.download_data(dataset=dataset)
    return dataset


@timing
def upload_to_gcs(dataset: str):
    data = BigQueryPipeline()
    client = storage.Client()
    bucket = client.get_bucket(os.getenv("GCP_GCS_BUCKET"))
    data.upload_to_gcs(dataset=dataset, bucket=bucket)
    return dataset


def create_bigquery_table(dataset: str):
    data = BigQueryPipeline()
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