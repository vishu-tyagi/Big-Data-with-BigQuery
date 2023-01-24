import sys
import logging
import argparse

from nyc_taxi.api import download_data, upload_to_gcs, create_bigquery_table

FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)


def main(args):
    try:
        taxi = args.taxi
        year = int(args.year)
        month = int(args.month)
        dataset = f"{taxi}_tripdata_{year}-{month:02d}.parquet"
        if sys.argv[1] == "extract-load":
            download_data(dataset=dataset)
            upload_to_gcs(dataset=dataset)
            create_bigquery_table(dataset=dataset)
    except IndexError:
        raise IndexError("Call to API requires an endpoint")
    except AttributeError:
        raise AttributeError("Required taxi color, year and month of dataset")
    return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Data Ingestion Pipeline"
    )
    parser.add_argument(
        "--taxi",
        default=argparse.SUPPRESS,
        help="Dataset taxi color"
    )
    parser.add_argument(
        "--year",
        default=argparse.SUPPRESS,
        help="Dataset year"
    )
    parser.add_argument(
        "--month",
        default=argparse.SUPPRESS,
        help="Dataset month"
    )
    args = parser.parse_args(sys.argv[2:])
    main(args)