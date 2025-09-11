# ARTPARK DataIO Documentation

:::{caution}
ARTPARK's dataio is in alpha (v0.3.0-alpha) and is neither a release candidate nor ready for production use.
:::

:::{note}
This documentation is a work in progress. Feedback on how to improve it is welcome.
:::

## Overview

ARTPARK's DataIO is a platform for managing and sharing data. It consists of our internal API server which manages the catalogue, and a python SDK
and CLI for users. This documentation is for the SDK, which you can use to access our data. Please contact us for getting API keys.

## Installation

`dataio` is not yet available on PyPI. You can install it from the source code.

Using uv:

```bash
uv add git+https://github.com/dsih-artpark/dataio.git@staging
```

or using pip:

```bash
pip install git+https://github.com/dsih-artpark/dataio.git@staging
```

It is always recommended to use a virtual environment to install the package, regardless of the installation method. ```uv``` provides a seamless way to create and manage virtual environments within the same command.

## Terminology

DataIO uses the following terminology:

| Term | Description | Example |
|------|-------------|---------|
| **Table** | A table is usually a csv file, but can also be a parquet file. This is a collection of records for a specific topic. | Karnataka livestock census district level data |
| **Dataset** | A dataset is a collection of tables, usually related to a specific overarching topic. | State Livestock Census Data, containing tables for Karnataka and Maharashtra |
| **Bucket Type** | A bucket type can be either `STANDARDISED` or `PREPROCESSED`: <br> **Standardised**: The data is in a standardised format, ready to be used. This is the default bucket type and the data made available to analysts.<br> **Preprocessed**: The data has been preprocessed by the team and stripped of PII/sensitive information. Not generally made available to analysts. | |

## Configuration

The client relies on two variables to authenticate with the API Server:

1. `DATAIO_API_BASE_URL`: The base URL of the API. The current staging environment is at http://staging.dataio.artpark.ai/api/v1
2. `DATAIO_API_KEY`: The API key for the API.

You can set these variables in a .env file or pass them as arguments to the `DataIOAPI` constructor.

## Usage

The package builds an API client for interacting with the API and the S3 filestore. The simplest way to use it is to create an instance of the `DataIOAPI` client class.

```python
from dataio import DataIOAPI

client = DataIOAPI() # Use when environment variables are set in a .env file

# OR
client = DataIOAPI(base_url="api_base_url", api_key="your_api_key") # Use when environment variables are not set
```

The major functionalities currently supported are:

1. Listing datasets and their tables
2. Downloading a complete dataset
3. Listing tables in a dataset
4. Downloading shapefiles


### Listing All Datasets
You can list all the datasets by passing the `list_datasets` method. This will return a list of dictionaries, each containing the metadata of a dataset. You will only get the information of the datasets you have access to.

```python
dataset_list = client.list_datasets()
```
:::{tip}
By default, the dataset list will be a paginated list limited to 100 datasets per page. You can change this by passing the `limit` argument to the `list_datasets` method. This cannot be set to more than 100 datasets.
```python
dataset_list = client.list_datasets(limit=10)
```
:::

When you list the datasets, you will get a list of dictionaries, each containing the metadata of a dataset. This will include the dataset's unique identifier, which is the `ds_id` field in the dataset metadata, and will also include the dataset's title, description, and other metadata.


### Downloading a Complete Dataset

Downloading a complete dataset will download all the tables in the dataset to the specified directory. You can do this by passing the dataset's unique identifier to the `download_dataset` method. The client will download the dataset to the default directory `.data` within the current working directory. By default, the metadata will be downloaded to the root of the dataset directory as a YAML file. This can be controlled by passing the `get_metadata` and `metadata_format` arguments to the `download_dataset` method.

In a future release, the README file of the dataset will be added to the dataset directory.

```python
download_dir = client.download_dataset("TS0001DS9999") # Downloads the dataset to the default directory `.data` within the current working directory
```


```bash
current_working_directory/
├── .data/
│   └── TS0001DS9999-Test_Dataset/
│       ├── table_containing_information_abc.csv
│       ├── table_containing_information_def.csv
│       ├── metadata.yaml
```

:::{tip}
You can also specify a different directory to download the dataset to by passing the `root_dir` argument to the `download_dataset` method.

```python
client.download_dataset("TS0001DS9999", root_dir="custom_directory") # Downloads the dataset to the specified directory
```

```bash
custom_directory/
└── TS0001DS9999-Test_Dataset/
    ├── table_containing_information_abc.csv
    ├── table_containing_information_def.csv
    ├── metadata.yaml
    └── README.md
```
:::


### Listing Tables in a Dataset

You can list the tables in a dataset by passing the dataset's unique identifier to the `list_dataset_tables` method.

```python
client.list_dataset_tables("TS0001DS9999")
```

When you list the tables in a dataset, you will get a list of dictionaries, each containing the metadata of a table. This will include the table's name, which is the `table_name` field in the table metadata, and will also include the table's download link, which is the `download_link` field in the table metadata. The download link is a signed link that will expire in 1 hour.

:::{warning}
The download link is a signed link that will expire in 1 hour.
:::

## Endpoints

The API endpoints are documented in the [Endpoints](https://staging.dataio.artpark.ai/endpoints) page.

```{toctree}
:maxdepth: 2
:caption: Contents:

Introduction <self>
examples
```
