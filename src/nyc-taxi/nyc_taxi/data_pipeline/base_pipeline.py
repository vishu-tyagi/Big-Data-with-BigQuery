import os
from pathlib import Path
import logging

import pandas as pd

from nyc_taxi.config import NYCTaxiConfig
from nyc_taxi.data_pipeline.helpers import download_data
from nyc_taxi.utils import timing
from nyc_taxi.utils.constants import (
    DATA_DIR
)

logger = logging.getLogger(__name__)


class DataPipeline():
    def __init__(self, config: NYCTaxiConfig):
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
