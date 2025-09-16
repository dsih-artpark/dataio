# Getting Started

This guide will help you make your first API calls with ARTPARK DataIO.

:::{note}
Make sure you have completed the [Installation](installation.md) steps before proceeding.
:::

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

### SDK Documentation
- Learn about [downloading datasets by tags](examples.md)
- Explore the [API Reference](api-reference.md) for complete method documentation

### CLI Documentation
- Use the [CLI Reference](cli-reference.md) for command-line operations

### Overview
- Return to [Installation](installation.md) if you need to set up DataIO
- Return to the [Introduction](index.md) to access all documentation sections

:::{tip}
Contact the DataIO administrators to get your API key if you haven't already. The API endpoint is currently in staging and not publicly available.
:::