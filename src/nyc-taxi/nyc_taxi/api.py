import os
import logging

# from sqlalchemy import create_engine
# from sqlalchemy.schema import CreateSchema
# from google.cloud import storage

import pandas as pd

from nyc_taxi.config import NYCTaxiConfig
from nyc_taxi.data_pipeline import BigQueryPipeline
from nyc_taxi.utils import timing

logger = logging.getLogger(__name__)



@timing
def download_data(
    taxi: str,
    year: int,
    month: int
) -> None:
    data = BigQueryPipeline()
    data.make_dirs()
    dataset = f"{taxi}_tripdata_{year}-{month:02d}.parquet"
    logger.info(f"Downloading {taxi} trips data for {year}-{month:02d} ...")
    data.download_data(dataset)
    return


@timing
def read_data(
    taxi: str,
    year: int,
    month: int
) -> None:
    data = BigQueryPipeline()
    dataset = f"{taxi}_tripdata_{year}-{month:02d}.parquet"
    df = pd.read_parquet(os.path.join(data.data_path, dataset))
    print('Number of rows: ')
    print(df.shape)
    return


# @timing
# def download_data(
#     config: NYCTaxiConfig = NYCTaxiConfig
# ) -> None:
#     print(config.CURRENT_PATH)
#     return

# @timing
# def extract_load_postgres(
#     connection_string: str,
#     schema: str,
#     config: NYCTaxiConfig = NYCTaxiConfig
# ) -> None:
#     engine = create_engine(connection_string)
#     if not engine.dialect.has_schema(engine, schema):
#         logger.info(f"Created new schema {schema}")
#         engine.execute(CreateSchema(schema))
#     data = PostgresPipeline(config)
#     data.make_dirs()
#     for dataset in data.data_url:
#         data.extract(dataset=dataset)
#     for dataset in config.DATA_URL:
#         data.load(
#             dataset=dataset,
#             engine=engine,
#             schema=schema
#         )
#     return

# @timing
# def extract_load_bigquery(
#     bucket_name: str,
#     schema: str,
#     config: NYCTaxiConfig = NYCTaxiConfig
# ) -> None:
#     client = storage.Client()
#     bucket = client.get_bucket(bucket_name)
#     project_id = client.project
#     data = BigQueryPipeline(config)
#     data.make_dirs()
#     for dataset in data.data_url:
#         data.extract(dataset=dataset)
#     for dataset in config.DATA_URL:
#         data.upload_to_datalake(
#             dataset=dataset,
#             bucket=bucket
#         )
#     for dataset in config.DATA_URL:
#         data.upload(
#             dataset=dataset,
#             bucket=bucket,
#             project_id=project_id,
#             schema=schema
#         )
#     return

# @timing
# def extract_load_bigquery(
#     config: NYCTaxiConfig = NYCTaxiConfig
# ) -> None:
#     print(config.CURRENT_PATH)
#     return
