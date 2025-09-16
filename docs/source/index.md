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

## Key Features

DataIO provides a Python SDK for accessing and managing datasets with these core capabilities:

1. **Dataset Discovery** - List and search available datasets
2. **Data Download** - Download complete datasets or individual tables
3. **Tag-based Filtering** - Find datasets by categories like "Livestock" 
4. **Shapefile Support** - Download geographic boundary data
5. **Metadata Access** - Get comprehensive dataset information

## Terminology

DataIO uses the following terminology:

| Term | Description | Example |
|------|-------------|---------|
| **Table** | A table is usually a csv file, but can also be a parquet file. This is a collection of records for a specific topic. | Karnataka livestock census district level data |
| **Dataset** | A dataset is a collection of tables, usually related to a specific overarching topic. | State Livestock Census Data, containing tables for Karnataka and Maharashtra |
| **Bucket Type** | A bucket type can be either `STANDARDISED` or `PREPROCESSED`: <br> **Standardised**: The data is in a standardised format, ready to be used. This is the default bucket type and the data made available to analysts.<br> **Preprocessed**: The data has been preprocessed by the team and stripped of PII/sensitive information. Not generally made available to analysts. | |


## Endpoints

The API endpoints are documented in the [Endpoints](https://staging.dataio.artpark.ai/endpoints) page.

```{toctree}
:maxdepth: 2
:caption: Overview:

Introduction <self>
installation
```

```{toctree}
:maxdepth: 2
:caption: SDK Documentation:

getting-started
examples
api-reference
```

```{toctree}
:maxdepth: 2
:caption: CLI Documentation:

cli-reference
```
