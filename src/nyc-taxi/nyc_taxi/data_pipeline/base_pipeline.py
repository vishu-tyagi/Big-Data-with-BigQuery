import os
from pathlib import Path
import logging

import requests
import pandas as pd

from nyc_taxi.config import NYCTaxiConfig
from nyc_taxi.utils import timing
from nyc_taxi.utils.constants import (
    DATA_DIR
)

logger = logging.getLogger(__name__)


class DataPipeline():
    def __init__(self, config: NYCTaxiConfig):
        self.config = config
        self.current_path = \
            Path(os.getcwd()) if not config.CURRENT_PATH else config.CURRENT_PATH
        self.data_path = Path(os.path.join(self.current_path, DATA_DIR))

    def make_dirs(self):
        dirs = [self.data_path]
        for dir in dirs:
            dir.mkdir(parents=True, exist_ok=True)

    def download_data(self, dataset):
        url = os.path.join(self.config.URL_PREFIX, dataset)
        target_path = Path(os.path.join(self.data_path, dataset))
        logger.info(f"Downloading {dataset} from {url} ...")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(str(target_path), 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        logger.info(f"Downloaded to {target_path}")
        return str(target_path)