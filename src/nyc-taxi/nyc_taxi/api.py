import logging

from nyc_taxi.config import NYCTaxiConfig
from nyc_taxi.data_access import DataClass
from nyc_taxi.utils import timing

logger = logging.getLogger(__name__)


@timing
def fetch(config: NYCTaxiConfig = NYCTaxiConfig) -> None:
    data = DataClass(config)
    data.make_dirs()
    data.fetch()
    return


@timing
def ingest(
    connection_string: str,
    table: str,
    config: NYCTaxiConfig = NYCTaxiConfig
) -> None:
    data = DataClass(config)
    data.ingest(connection_string=connection_string, table=table)
    return