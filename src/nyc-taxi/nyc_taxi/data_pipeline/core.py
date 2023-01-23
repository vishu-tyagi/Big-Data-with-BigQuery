import os
from pathlib import Path
import logging

import pandas as pd
from google.cloud.storage.bucket import Bucket

from nyc_taxi.config import NYCTaxiConfig
from nyc_taxi.data_pipeline.base_pipeline import DataPipeline
from nyc_taxi.data_pipeline.helpers import (
    upload_to_bigquery
)
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
    def upload(
        self,
        dataset: str,
        bucket: Bucket,
        project_id: str,
        schema: str
    ):
        logger.info(
            f"Ingesting {dataset} into table {schema}.{dataset} ..."
        )
        if_table_exists = "replace"
        nrows = 0
        for fname in self.data_url[dataset]:
            df = pd.read_parquet(f"gs://{bucket.name}/{dataset}/{fname}")
            columns = [c for c in df.columns if c not in self.config.EXCLUDE_COLUMNS]
            df = df[columns].copy()
            df.columns = [c.lower() for c in df.columns]
            upload_to_bigquery(
                df=df,
                project_id=project_id,
                schema=schema,
                table=dataset,
                if_table_exists=if_table_exists,
                chunksize=10000,
                file_name=fname
            )
            if_table_exists = "append"
            nrows += df.shape[0]
        logger.info(f"Completed ingesting {nrows} rows")
