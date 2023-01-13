import logging
import logging.config
from pathlib import Path

import requests
import pandas as pd

from nyc_taxi.utils import timing

logger = logging.getLogger(__name__)


@timing
def download_data(url: str, save_to: Path) -> None:
    """
    Download data from URL
    Args:
        data_url (_type_): URL to download from
        to_ (Path): Destination for downloaded data
    """
    logger.info(f"Downloading {url}")
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(str(save_to), 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


@timing
def upload_data(
    df: pd.DataFrame,
    connection,
    name: str,
    if_exists: str,
    chunksize: int
):
    logger.info(f"Uploading {df.shape[0]} rows ...")
    df.to_sql(
        name=name,
        con=connection,
        if_exists=if_exists,
        index=False,
        chunksize=chunksize
    )
