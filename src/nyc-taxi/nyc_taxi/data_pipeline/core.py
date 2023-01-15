import os
from pathlib import Path
import logging

import pandas as pd

from nyc_taxi.config import NYCTaxiConfig
from nyc_taxi.data_pipeline.helpers import download_data, upload_data
from nyc_taxi.utils import timing
from nyc_taxi.utils.constants import (
    DATA_DIR
)

logger = logging.getLogger(__name__)


class DataPipeline():
    def __init__(self, config: NYCTaxiConfig = NYCTaxiConfig):
        self.config = config
        self.data_url = config.DATA_URL
        self.current_path = Path(os.getcwd()) if not config.CURRENT_PATH else config.CURRENT_PATH
        self.data_path = Path(os.path.join(self.current_path, DATA_DIR))

    def make_dirs(self):
        dirs = [self.data_path]
        for dir in dirs:
            dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created data directory {self.data_path}")

    @timing
    def extract(self, dataset: str):
        logger.info(f"Fetching raw data for {dataset}...")
        for fname in self.data_url[dataset]:
            save_to = Path(os.path.join(self.data_path, fname))
            download_data(url=self.data_url[dataset][fname], save_to=save_to)
            logger.info(f"Downloaded {fname} to {self.data_path}")

    @timing
    def load(
        self,
        dataset: str,
        connection_string: str,
        engine,
        table: str,
        schema: str
    ):
        logger.info(
            f"Ingesting {dataset} into table {schema}.{table} on {connection_string} ..."
        )
        if_table_exists = "replace"
        nrows = 0
        for fname in self.data_url[dataset]:
            df = pd.read_parquet(os.path.join(self.data_path, fname))
            df.columns = [c.lower() for c in df.columns]
            upload_data(
                df=df,
                connection=engine,
                table=table,
                if_table_exists=if_table_exists,
                chunksize=10000,
                schema=schema
            )
            if_table_exists = "append"
            nrows += df.shape[0]
        logger.info(f"Completed ingesting {nrows} rows")