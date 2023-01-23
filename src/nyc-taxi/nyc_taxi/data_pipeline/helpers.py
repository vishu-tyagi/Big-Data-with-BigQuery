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
def upload_to_postgres(
    df: pd.DataFrame,
    engine: Engine,
    schema: str,
    table: str,
    if_table_exists: str,
    chunksize: int,
    file_name: str
):
    logger.info(f"Ingesting {file_name} with {df.shape[0]} rows ...")
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
def upload_to_bigquery(
    df: pd.DataFrame,
    project_id: str,
    schema: str,
    table: str,
    if_table_exists: str,
    chunksize: int,
    file_name: str
):
    logger.info(f"Ingesting {file_name} with {df.shape[0]} rows ...")
    df.to_gbq(
        project_id=project_id,
        destination_table=f"{schema}.{table}",
        if_exists=if_table_exists,
        chunksize=chunksize
    )
    return