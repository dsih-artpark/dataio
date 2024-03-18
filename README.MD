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
metadata, datadict = fetch_data_documentation(dsid="your_dataset_id")

# Example usage of download_dataset_v2 to download the dataset
download_dataset_v2(dsid="your_dataset_id", data_state="standardised")
```
The ```download_dataset_v2``` has more advanced functionality, to download specific files based on your need. Read about it [here][download_dataset_v2].

## Version
Current version: v0.1.0
dataio is in limited alpha release.





[download_dataset_v2]: src/dataio/download/__init__.py#L212