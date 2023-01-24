import os
from pathlib import Path
import logging

import pandas as pd
from google.cloud.storage.bucket import Bucket

from nyc_taxi.config import NYCTaxiConfig
from nyc_taxi.data_pipeline.base_pipeline import DataPipeline
from nyc_taxi.utils import timing

logger = logging.getLogger(__name__)


class BigQueryPipeline(DataPipeline):
    def __init__(self, config: NYCTaxiConfig = NYCTaxiConfig):
        super(BigQueryPipeline, self).__init__(config)

    @timing
    def upload_to_gcs(self, dataset: str, bucket: Bucket):
        df = pd.read_parquet(os.path.join(self.data_path, dataset))
        logger.info(f"Read {df.shape[0]} rows ...")
        logger.info(f"Uploading {dataset} to gs://{bucket.name}/{dataset} ...")
        columns = [c for c in df.columns if c not in self.config.EXCLUDE_COLUMNS]
        df = df[columns].copy()
        df.columns = [c.lower() for c in df.columns]
        bucket \
            .blob(dataset) \
            .upload_from_string(df.to_parquet(index=False), "parquet")
        logger.info(f"Finished uploading {dataset}")
        return dataset

    @timing
    def create_bq_table(
        self,
        dataset: str,
        bucket: Bucket,
        project_id: str,
        bq_schema: str
    ):
        external_table = "_".join(dataset.split("_")[:2])
        logger.info(f"Ingesting table {bq_schema}.{external_table} ...")
        df = pd.read_parquet(f"gs://{bucket.name}/{dataset}")
        df.to_gbq(
            project_id=project_id,
            destination_table=f"{bq_schema}.{external_table}",
            if_exists="append",
            chunksize=10000
        )
        logger.info(f"Finished ingesting {dataset} with {df.shape[0]} rows")
