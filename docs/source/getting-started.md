# Getting Started

This guide will help you get up and running with ARTPARK DataIO quickly.

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

## Configuration

The client relies on two variables to authenticate with the API Server:

1. `DATAIO_API_BASE_URL`: The base URL of the API. The current staging environment is at http://staging.dataio.artpark.ai/api/v1
2. `DATAIO_API_KEY`: The API key for the API.

You can set these variables in a .env file or pass them as arguments to the `DataIOAPI` constructor.

### Setting up Environment Variables

Create a `.env` file in your project root:

```bash
DATAIO_API_BASE_URL=https://staging.dataio.artpark.ai/api/v1
DATAIO_API_KEY=your_api_key_here
```

## Creating Your First Client

The package builds an API client for interacting with the API and the S3 filestore. The simplest way to use it is to create an instance of the `DataIOAPI` client class.

```python
from dataio import DataIOAPI

# Method 1: Using environment variables (recommended)
client = DataIOAPI()

# Method 2: Passing credentials directly
client = DataIOAPI(
    base_url="https://staging.dataio.artpark.ai/api/v1", 
    api_key="your_api_key"
)
```

## Your First API Call

Let's start by listing the datasets you have access to:

```python
from dataio import DataIOAPI

# Create client
client = DataIOAPI()

# Get all available datasets
datasets = client.list_datasets()

# Print basic information
print(f"You have access to {len(datasets)} datasets")

# Show first dataset details
if datasets:
    first_dataset = datasets[0]
    print(f"First dataset: {first_dataset['title']}")
    print(f"Dataset ID: {first_dataset['ds_id']}")
```

## What's Next?

Now that you have DataIO set up, you can:

- Browse the [Examples](examples.md) for common usage patterns
- Learn about [downloading datasets by tags](examples.md)
- Explore the full API capabilities in the main documentation

:::{tip}
Contact the DataIO administrators to get your API key if you haven't already. The API endpoint is currently in staging and not publicly available.
:::