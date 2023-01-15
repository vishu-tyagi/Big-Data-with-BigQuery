# Data-Engineering-Project

## Instructions

#### Move into top-level directory
```
cd Data-Engineering-Project
```

#### Build the container
```
make build
```

#### Extract raw data and ingest into Postgres
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

#### Extract raw data and ingest into BigQuery
```
make extract-load-postgres \
    bucket=bq_bucket \
    schema=bq_schema

```

Example usage
```
make extract-load-postgres \
    bucket=nyc_taxi_bucket \
    schema=staging

```
will first dump raw data into google cloud bucket `nyc_taxi_bucket`, then it will read the raw data from this bucket and ingest the table into BigQuery schema `staging`.

## Instructions for local development

#### Move into top-level directory
```
cd Data-Engineering-Project

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

#### Extract raw data and ingest into BigQuery
```
python -m nyc_taxi extract-load-bigquery \
    --bucket=bq_bucket

```

Example usage
```
python -m nyc_taxi extract-load-bigquery \
    --bucket=nyc_taxi_bucket \
    --schema=staging

```
will first dump raw data into google cloud bucket `nyc_taxi_bucket`, then it will read the raw data from this bucket and ingest the table into BigQuery schema `staging`.

#### Run jupyter server
```
jupyter notebook notebooks/

```

You can now use the jupyter kernel to run notebooks.