# dataio

dataio is a Python package designed to simplify data access within the Data Science Innovation Hub at ARTPARK.

## Installation

You can install dataio using [Poetry](https://python-poetry.org/) and Git SSH by running the following command:

```bash
poetry add git+ssh://git@github.com:dsih-artpark/dataio.git
```

## Usage
```python
from dataio.download import fetch_data_documentation, download_dataset_v2

# Example usage of fetch_data_documentation to get metadata
metadata, datadict = fetch_data_documentation(dsid="DSID01")

# Example usage of download_dataset_v2 to download your dataset with ID "DSID01" only for files for the year "2023"
# This assumes the files for that year contain the year in the name, or are in a folder with the same name
download_dataset_v2(dsid="DSID01", data_state="standardised", contains = "2023")
```
The ```download_dataset_v2``` has more advanced functionality, to download specific files based on your need. Read about it [here][download_dataset_v2].

## Version
Current version: v0.1.0
dataio is in limited alpha release.





[download_dataset_v2]: src/dataio/download/__init__.py#L212