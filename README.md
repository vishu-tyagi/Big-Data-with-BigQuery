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

#### Fetch raw data
```
make fetch
```

#### Ingest raw data into Postgres
```
make ingest user=pguser password=pgpassword host=pghost port=pgport db=pgdb table=pgtable
```

Example usage
```
make ingest user=root password=root host=127.0.0.1 port=5432 db=nyc_taxi table=raw
```
will create (and replace, if it already exists) the table `raw` in the Postgres database `nyc_taxi` and upload raw data into it.

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

#### Fetch raw data
```
python -m nyc_taxi fetch
```
#### Ingest raw data into Postgres
```
python -m nyc_taxi ingest \
    --user=pguser \
    --password=pgroot \
    --host=pghost \
    --port=pgport \
    --db=pgdb \
    --table=pgtable
```

Example usage
```
python -m nyc_taxi ingest \
    --user=root \
    --password=root \
    --host=127.0.0.1 \
    --port=5432 \
    --db=nyc_taxi \
    --table=raw
```
will create (and replace, if it already exists) the table raw in the Postgres database nyc_taxi and upload raw data into it.

#### Run jupyter server
```
jupyter notebook notebooks/
```

You can now use the jupyter kernel to run notebooks.