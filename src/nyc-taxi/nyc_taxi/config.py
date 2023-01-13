class NYCTaxiConfig():
    # URL for downloading data
    DATA_URL = {
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2021-{i:02}.parquet"
        for i in range(1, 13)
    }
