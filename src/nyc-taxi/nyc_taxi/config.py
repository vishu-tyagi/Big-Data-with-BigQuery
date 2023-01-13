class NYCTaxiConfig():
    # URLs for downloading data
    DATA_URL = {
        f"green_tripdata_2021-{i:02}": \
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2021-{i:02}.parquet" \
        for i in range(1, 13)
    }

    CURRENT_PATH = None