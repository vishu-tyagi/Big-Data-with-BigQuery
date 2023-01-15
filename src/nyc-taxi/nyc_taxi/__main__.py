import sys
import logging
import argparse

from nyc_taxi.api import extract_load

FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)


def main(args):
    try:
        if sys.argv[1] == "extract-load":
            user = args.user
            password = args.password
            host = args.host
            port = args.port
            db = args.db
            schema = args.schema
    except IndexError:
        raise IndexError("Call to API requires an endpoint")
    except AttributeError:
        raise AttributeError(
            "Required username, password, host, port, database," + \
            " and schema name to establish connection"
        )
    connection_string = \
        f"postgresql://{user}:{password}@{host}:{port}/{db}"
    extract_load(
        connection_string=connection_string,
        schema=schema
    )
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Data Ingestion Pipelien for Postgres"
    )
    parser.add_argument(
        "--user",
        default=argparse.SUPPRESS,
        help="username for postgres"
    )
    parser.add_argument(
        "--password",
        default=argparse.SUPPRESS,
        help="password for postgres"
    )
    parser.add_argument(
        "--host",
        default=argparse.SUPPRESS,
        help="host for postgres"
    )
    parser.add_argument(
        "--port",
        default=argparse.SUPPRESS,
        help="port for postgres"
    )
    parser.add_argument(
        "--db",
        default=argparse.SUPPRESS,
        help="database name for postgres"
    )
    parser.add_argument(
        "--schema",
        default=argparse.SUPPRESS,
        help="name of schema the table will be saved to"
    )
    args = parser.parse_args(sys.argv[2:])
    main(args)