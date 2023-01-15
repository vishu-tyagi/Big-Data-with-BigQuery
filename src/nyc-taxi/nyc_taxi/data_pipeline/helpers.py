import logging
import logging.config
from pathlib import Path

import requests
import pandas as pd
from sqlalchemy.engine.base import Engine
from google.cloud.storage.bucket import Bucket

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
    return


@timing
def upload_to_postgres(
    df: pd.DataFrame,
    engine: Engine,
    schema: str,
    table: str,
    if_table_exists: str,
    chunksize: int
):
    logger.info(f"Uploading {df.shape[0]} rows ...")
    df.to_sql(
        name=table,
        con=engine,
        if_exists=if_table_exists,
        index=False,
        chunksize=chunksize,
        schema=schema
    )
    return


@timing
def upload_to_gcs_bucket(
    df: pd.DataFrame,
    bucket: Bucket,
    file_path_in_bucket: str,
    file_type: str
):
    logger.info(f"Uploading {file_path_in_bucket}")
    bucket.blob(file_path_in_bucket) \
        .upload_from_string(df.to_parquet(index=False), file_type)
    return


@timing
def upload_to_bigquery(
    df: pd.DataFrame,
    project_id: str,
    schema: str,
    table: str,
    if_table_exists: str,
    chunksize: int
):
    logger.info(f"Uploading {df.shape[0]} rows ...")
    df.to_gbq(
        project_id=project_id,
        destination_table=f"{schema}.{table}",
        if_exists=if_table_exists,
        chunksize=chunksize
    )
    return