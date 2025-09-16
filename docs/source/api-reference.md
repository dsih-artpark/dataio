# API Reference

Complete reference for the `DataIOAPI` client class.

## DataIOAPI Class

```python
from dataio import DataIOAPI
```

The main client class for interacting with the DataIO API.

### Constructor

#### `DataIOAPI(base_url=None, api_key=None)`

Initialize a new DataIO API client.

**Parameters:**
- `base_url` (str, optional): The base URL of the DataIO API. If not provided, uses the `DATAIO_API_BASE_URL` environment variable.
- `api_key` (str, optional): The API key for authentication. If not provided, uses the `DATAIO_API_KEY` environment variable.

**Raises:**
- `ValueError`: If neither environment variables nor parameters are provided for base_url or api_key.

**Example:**
```python
# Using environment variables
client = DataIOAPI()

# Passing credentials directly  
client = DataIOAPI(
    base_url="https://staging.dataio.artpark.ai/api/v1",
    api_key="your_api_key"
)
```

---

## Dataset Methods

### `list_datasets(limit=None)`

Get a list of all datasets available to the authenticated user.

**Parameters:**
- `limit` (int, optional): Maximum number of datasets to return. Defaults to 100 if not specified.

**Returns:**
- `list`: List of dataset dictionaries containing metadata for each dataset.

**Example:**
```python
# Get all datasets (up to 100)
datasets = client.list_datasets()

# Get first 10 datasets
datasets = client.list_datasets(limit=10)

# Each dataset contains:
# - ds_id: Unique dataset identifier
# - title: Dataset title
# - description: Dataset description  
# - tags: List of tag dictionaries with 'id' and 'tag_name'
# - collection: Collection information
```

### `get_dataset_details(dataset_id)`

Get detailed metadata for a specific dataset.

**Parameters:**
- `dataset_id` (str or int): The dataset ID. Can be the full ds_id or just the numeric part.

**Returns:**
- `dict`: Complete dataset metadata including title, description, collection, and other fields.

**Raises:**
- `ValueError`: If the dataset with the specified ID is not found.

**Example:**
```python
# Using full dataset ID
details = client.get_dataset_details("TS0001DS9999")

# Using just the numeric part (will be zero-padded)
details = client.get_dataset_details("9999")
details = client.get_dataset_details(9999)
```

### `list_dataset_tables(dataset_id, bucket_type="STANDARDISED")`

Get a list of tables within a dataset, including download links.

**Parameters:**
- `dataset_id` (str): The dataset ID to get tables for.
- `bucket_type` (str, optional): Type of bucket. Either "STANDARDISED" or "PREPROCESSED". Defaults to "STANDARDISED".

:::{note}
Currently, only "STANDARDISED" datasets are available. "PREPROCESSED" datasets are not yet accessible through the API.
:::

**Returns:**
- `list`: List of table dictionaries, each containing:
  - `table_name`: Name of the table
  - `download_link`: Signed URL for downloading (expires in 1 hour)
  - `metadata`: Table-level metadata

**Example:**
```python
# Get tables for a dataset
tables = client.list_dataset_tables("TS0001DS9999")

# Get preprocessed tables
tables = client.list_dataset_tables("TS0001DS9999", bucket_type="PREPROCESSED")

for table in tables:
    print(f"Table: {table['table_name']}")
    print(f"Download: {table['download_link']}")
```

### `download_dataset(dataset_id, **kwargs)`

Download a complete dataset with all its tables and metadata.

**Parameters:**
- `dataset_id` (str): The dataset ID to download.
- `bucket_type` (str, optional): Bucket type to download. Defaults to "STANDARDISED".
- `root_dir` (str, optional): Root directory for downloads. Defaults to "data".
- `get_metadata` (bool, optional): Whether to download metadata file. Defaults to True.
- `metadata_format` (str, optional): Format for metadata ("yaml" or "json"). Defaults to "yaml".
- `update_sync_history` (bool, optional): Whether to update sync history. Defaults to True.
- `sync_history_file` (str, optional): Name of sync history file. Defaults to "sync-history.yaml".

**Returns:**
- `str`: Path to the downloaded dataset directory.

**Example:**
```python
# Basic download
path = client.download_dataset("TS0001DS9999")

# Download to custom directory with JSON metadata
path = client.download_dataset(
    "TS0001DS9999",
    root_dir="my_datasets",
    metadata_format="json"
)

# Download without metadata
path = client.download_dataset(
    "TS0001DS9999", 
    get_metadata=False
)
```

**Directory Structure:**
```
root_dir/
├── sync-history.yaml  (if update_sync_history=True)
└── TS0001DS9999-Dataset_Title/
    ├── table1.csv
    ├── table2.csv
    ├── table3.csv
    └── metadata.yaml  (if get_metadata=True)
```

### `construct_dataset_metadata(dataset_details, bucket_type="STANDARDISED")`

Build comprehensive metadata combining dataset and table-level information.

**Parameters:**
- `dataset_details` (dict): Dataset details from `get_dataset_details()`.
- `bucket_type` (str, optional): Bucket type for table metadata. Defaults to "STANDARDISED".

**Returns:**
- `dict`: Combined metadata with dataset and table information.

**Required fields in dataset_details:**
- `title`: Dataset title
- `description`: Dataset description  
- `collection`: Collection object with `category_name` and `collection_name`

**Example:**
```python
dataset_details = client.get_dataset_details("TS0001DS9999")
metadata = client.construct_dataset_metadata(dataset_details)

# Metadata structure:
# - dataset_title: Title of the dataset
# - dataset_description: Description
# - category: Category name
# - collection: Collection name  
# - dataset_tables: Dict of table metadata keyed by table name
```

---

## Shapefile Methods

### `get_shapefile_list()`

Get a list of all available shapefiles.

**Returns:**
- `list`: List of shapefile dictionaries containing metadata for each shapefile.

**Example:**
```python
shapefiles = client.get_shapefile_list()

for shapefile in shapefiles:
    print(f"Region ID: {shapefile['region_id']}")
    print(f"Name: {shapefile['name']}")
```

### `download_shapefile(region_id, shp_folder="data/GS0012DS0051-Shapefiles_India")`

Download a shapefile for a specific region.

**Parameters:**
- `region_id` (str): ID of the region to download shapefile for.
- `shp_folder` (str, optional): Directory to save the shapefile. Defaults to "data/GS0012DS0051-Shapefiles_India".

**Returns:**
- `str`: Path to the downloaded GeoJSON file.

**Raises:**
- `ValueError`: If shapefile for the specified region is not found.

**Example:**
```python
# Download shapefile for a state
path = client.download_shapefile("state_29")

# Download to custom folder
path = client.download_shapefile(
    "state_29", 
    shp_folder="my_shapefiles"
)
```

**Note:** Shapefiles are downloaded as GeoJSON format, not traditional .shp files.

---

## Error Handling

The DataIO API client raises standard Python exceptions:

- `ValueError`: For invalid parameters or missing data
- `requests.HTTPError`: For HTTP-related errors (authentication, not found, etc.)
- `requests.ConnectionError`: For network connectivity issues

**Example:**
```python
try:
    datasets = client.list_datasets()
except requests.HTTPError as e:
    if e.response.status_code == 401:
        print("Authentication failed - check your API key")
    elif e.response.status_code == 403:
        print("Access forbidden - insufficient permissions")
    else:
        print(f"HTTP error: {e}")
except ValueError as e:
    print(f"Invalid parameter: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

---

## Environment Variables

The client uses these environment variables:

- `DATAIO_API_BASE_URL`: Base URL for the DataIO API
- `DATAIO_API_KEY`: API key for authentication  

Set these in a `.env` file:
```bash
DATAIO_API_BASE_URL=https://staging.dataio.artpark.ai/api/v1
DATAIO_API_KEY=your_api_key_here
```

---

## Related Documentation

### Overview
- See [Installation](installation.md) for setup instructions
- Return to the [Introduction](index.md) to access all documentation sections

### SDK Documentation
- Start with [Getting Started](getting-started.md) for your first API calls
- Learn about [downloading datasets by tags](examples.md)

### CLI Documentation
- Use the [CLI Reference](cli-reference.md) for command-line operations