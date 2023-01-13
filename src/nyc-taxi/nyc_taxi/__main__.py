import sys
import logging
import argparse

from nyc_taxi.api import fetch, ingest

FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)


def main(args):
    try:
        if sys.argv[1] == "fetch":
            fetch()
        if sys.argv[1] == "ingest":
            try:
                user = args.user
                password = args.password
                host = args.host
                port = args.port
                db = args.db
                table = args.table
            except AttributeError:
                raise AttributeError(
                    "Required username, password, host, port, database, and table name"
                )
            connection_string = \
                f"postgresql://{user}:{password}@{host}:{port}/{db}"
            ingest(connection_string=connection_string, table=table)
    except IndexError:
        raise IndexError("Call to API requires an endpoint")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest data into Postgres"
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
        "--table",
        default=argparse.SUPPRESS,
        help="name of table the results will be written to"
    )
    args = parser.parse_args(sys.argv[2:])
    main(args)