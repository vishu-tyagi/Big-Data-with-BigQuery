import logging
from multiprocessing import connection

from sqlalchemy import create_engine
from sqlalchemy.schema import CreateSchema

from nyc_taxi.config import NYCTaxiConfig
from nyc_taxi.data_pipeline import DataPipeline
from nyc_taxi.utils import timing

logger = logging.getLogger(__name__)


@timing
def extract_load(
    connection_string: str,
    schema: str,
    config: NYCTaxiConfig = NYCTaxiConfig
) -> None:
    engine = create_engine(connection_string)
    if not engine.dialect.has_schema(engine, schema):
        logger.info(f"Created new schema {schema}")
        engine.execute(CreateSchema(schema))
    data = DataPipeline(config)
    data.make_dirs()
    for dataset in data.data_url:
        data.extract(dataset=dataset)
    for dataset in config.DATA_URL:
        data.load(
            dataset=dataset,
            connection_string=connection_string,
            engine=engine,
            table=dataset,
            schema=schema
        )
    return