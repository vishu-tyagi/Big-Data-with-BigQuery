import os
from pathlib import Path
import logging

import pandas as pd
from sqlalchemy import create_engine

from nyc_taxi.config import NYCTaxiConfig
from nyc_taxi.data_access.helpers import download_data, upload_data
from nyc_taxi.utils import timing
from nyc_taxi.utils.constants import (
    DATA_DIR
)

logger = logging.getLogger(__name__)


class DataClass():
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

    def fetch(self):
        logger.info(f"Fetching raw data ...")
        for fname in self.data_url:
            save_to = Path(os.path.join(self.data_path, fname))
            download_data(url=self.data_url[fname], save_to=save_to)
            logger.info(f"Downloaded {fname} to {self.data_path}")

    def ingest(self, connection_string: str, table: str):
        logger.info(f"Ingesting data into table {table} on {connection_string} ...")
        engine = create_engine(connection_string)
        if_exists = "replace"
        nrows = 0
        for fname in self.data_url:
            df = pd.read_parquet(os.path.join(self.data_path, fname))
            df.columns = [c.lower() for c in df.columns]
            upload_data(
                df=df,
                connection=engine,
                name=table,
                if_exists=if_exists,
                chunksize=10000
            )
            if_exists = "append"
            nrows += df.shape[0]
        logger.info(f"Completed ingesting {nrows} rows")