# Data-Engineering-Project

## Instructions

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

#### Run jupyter server
```
jupyter notebook notebooks/
```

You can now use the jupyter kernel to run notebooks.