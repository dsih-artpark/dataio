# dataio

:::{important}
ARTPARK's dataio is in beta (v{{version}})! You can now use it and report your issues and feedback to (sneha) / (akhil) (AT) artpark (dot) in.
:::

## Overview

ARTPARK's DataIO is a platform for managing and sharing data. It consists of our internal API server which manages the catalogue, and a python SDK
and CLI for users. This documentation is for the SDK, which you can use to access our data. You can find us on PyPI [here](https://pypi.org/project/dataio-artpark/).

Please contact us for getting API keys.

## Quickstart

You can start by reading the [Quick Start](/sdk/index.md) guide for the SDK, or the [CLI Guide](/cli/index.md) for the CLI. The package is available on PyPI, and you can install it using pip or uv.

```bash
venv .venv
source .venv/bin/activate

pip install dataio-artpark
```

or using uv:

```bash
uv init
uv add dataio-artpark
```

## Key Features

DataIO provides a Python SDK and a CLI for accessing and managing datasets with these core capabilities:

1. **Dataset Discovery** - List and search available datasets
2. **Data Download** - Download complete datasets or individual tables
3. **Tag-based Filtering** - Find datasets by categories like "Livestock"
4. **Shapefile Support** - Download geographic boundary data
5. **Metadata Access** - Get comprehensive dataset information

We are currently working on a front end to view and interact with the data. For now, you can view the list of datasets using the CLI or SDK.

CLI:

```bash
uv run dataio init
uv run dataio list-datasets
```

SDK:

```python
from dataio import DataIOAPI

client = DataIOAPI()
datasets = client.list_datasets()
print(datasets)
```

## Terminology

DataIO uses the following terminology:

| Term            | Description                                                                                                                                                                                                                                                                                                                                                                 | Example                                                                      |
| --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| **Table**       | A table is usually a csv file, but can also be a parquet file. This is a collection of records for a specific topic.                                                                                                                                                                                                                                                        | Karnataka livestock census district level data                               |
| **Dataset**     | A dataset is a collection of tables, usually related to a specific overarching topic.                                                                                                                                                                                                                                                                                       | State Livestock Census Data, containing tables for Karnataka and Maharashtra |
| **Bucket Type** | A bucket type can be either `STANDARDISED` or `PREPROCESSED`: <br> **Standardised**: The data is in a standardised format, ready to be used. This is the default bucket type and the data made available to analysts.<br> **Preprocessed**: The data has been preprocessed by the team and stripped of PII/sensitive information. Not generally made available to analysts. |                                                                              |

## Endpoints

The API endpoints are documented in the {{ '[Endpoints](url)'.replace('url', api_url) }} page.

:::{todo}

1. Add support for additional file types
2. Comply with NDAP/NITI Aayog's Data Standardisation Protocols for all geospatial data
3. Add additional advanced datasets
4. Build a front end to view and interact with the data.
   :::

```{toctree}
:hidden:
:maxdepth: 2
Introduction <self>
installation
sdk/index
cli/index
developer-guide
apidocs/index
CHANGELOG <CHANGELOG>
CONTRIBUTING <CONTRIBUTING>
```
