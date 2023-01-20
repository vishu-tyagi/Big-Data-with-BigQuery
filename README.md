# Big-Data-with-BigQuery

## Instructions

#### Move into top-level directory
```
cd Big-Data-with-BigQuery

```

#### Build the container
```
make build

```

#### Extract raw data and ingest into BigQuery
```
make extract-load-bigquery bucket=bq_bucket schema=bq_dataset

```

Example usage
```
make extract-load-bigquery bucket=nyc_taxi_bucket schema=staging

```
will first dump raw data into google cloud bucket `nyc_taxi_bucket`, then it will read the raw data from this bucket and ingest it into BigQuery dataset `staging`.

#### Extract raw data and ingest into Postgres

Note: This will take very long. It's recommended to reduce files in `src/nyc-taxi/nyc_taxi/config.py` before ingesting into a postgres database.

```
make extract-load-postgres \
    network=pg_network \
    user=pg_user \
    password=pg_password \
    host=pg_host \
    port=pg_port \
    db=pg_db \
    schema=pg_schema

```

Example usage
```
make extract-load-postgres \
    network=host \
    user=root \
    password=root \
    host=127.0.0.1 \
    port=5432 \
    db=nyc_taxi \
    schema=staging

```
will extract raw data and, create (if it does not exist) the schema `staging` in the Postgres database `nyc_taxi` running on `host` network on port `5432` and ingest the tables into it.

## Dashboard

The dashboard is now live and can be accessed on [Tableau Public](https://public.tableau.com/views/NYCTaxiDashboard_16740928210530/Dashboard?:language=en-US&:display_count=n&:origin=viz_share_link). The cleaning steps and SQL queries are present in `Big-Data-with-BigQuery/src/dbt/models`

## Instructions for local development

#### Move into top-level directory
```
cd Big-Data-with-BigQuery

```

#### Install environment
```
conda env create -f environment.yml

```

#### Activate environment
```
conda activate nyc-taxi

```

#### Install package
```
pip install -e src/nyc-taxi

```

Including the optional -e flag will install the package in "editable" mode, meaning that instead of copying the files into your virtual environment, a symlink will be created to the files where they are.

#### Extract raw data and ingest into BigQuery
```
python -m nyc_taxi extract-load-bigquery \
    --bucket=bq_bucket \
    --schema=bq_dataset

```

Requires environment variable `GOOGLE_APPLICATION_CREDENTIALS`.

Example usage
```
python -m nyc_taxi extract-load-bigquery \
    --bucket=nyc_taxi_bucket \
    --schema=staging

```
will first dump raw data into google cloud bucket `nyc_taxi_bucket`, then it will read the raw data from this bucket and ingest it into BigQuery dataset `staging`.

#### Extract raw data and ingest into Postgres
```
python -m nyc_taxi extract-load-postgres \
    --user=pg_user \
    --password=pg_password \
    --host=pg_host \
    --port=pg_port \
    --db=pg_db \
    --schema=pd_schema

```

Example usage
```
python -m nyc_taxi extract-load-postgres \
    --user=root \
    --password=root \
    --host=127.0.0.1 \
    --port=5432 \
    --db=nyc_taxi \
    --schema=staging

```
will extract raw data and, create (if it does not exist) the schema `staging` in the Postgres database `nyc_taxi` running on host network on port `5432` and ingest the tables into it.

#### Run jupyter server
```
jupyter notebook notebooks/

```

You can now use the jupyter kernel to run notebooks.
