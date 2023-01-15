class NYCTaxiConfig():
    # URLs for downloading data
    DATA_URL = {
        "green_tripdata": {
            f"green_tripdata_{year}-{i:02}.parquet": \
            f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{i:02}.parquet" \
            for i in range(1, 3) \
            for year in [2021]
        },
        "yellow_tripdata": {
            f"yellow_tripdata_{year}-{i:02}.parquet": \
            f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{i:02}.parquet" \
            for i in range(1, 2) \
            for year in [2021]
        }
    }

    CURRENT_PATH = None