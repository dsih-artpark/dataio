# Downloading Dataset By Tags

This page contains a example of using dataio to download all datasets with a particular tag, for eg "Livestock".

## Setting up the Client

```python
from dataio import DataIOAPI

# Method 1: Using environment variables
client = DataIOAPI()

# Method 2: Passing credentials directly
client = DataIOAPI(
    base_url="https://staging.dataio.artpark.ai/api/v1",
    api_key="your_api_key_here"
)
```

For Method 1, create a `.env` file in your project root:

```bash
DATAIO_API_BASE_URL=https://staging.dataio.artpark.ai/api/v1
DATAIO_API_KEY=your_api_key_here
```

## Tags

View available tags in the system to understand what categories of datasets are available:

```python
# Get all datasets to extract available tags
datasets = client.list_datasets()

# Collect all unique tags
all_tags = {}
for dataset in datasets:
    if 'tags' in dataset and dataset['tags']:
        for tag in dataset['tags']:
            tag_name = tag['tag_name']
            if tag_name not in all_tags:
                all_tags[tag_name] = {'id': tag['id'], 'count': 0}
            all_tags[tag_name]['count'] += 1

# Print available tags
print("Available tags in the system:")
for tag_name in sorted(all_tags.keys()):
    tag_info = all_tags[tag_name]
    print(f"Tag: {tag_name} (ID: {tag_info['id']}, {tag_info['count']} datasets)")
```

## Downloading Livestock Datasets

Find and download datasets that contain the "Livestock" tag:

```python
# Get all datasets
datasets = client.list_datasets()

# Filter datasets with the Livestock tag
livestock_datasets = []
for dataset in datasets:
    if 'tags' in dataset and dataset['tags']:
        for tag in dataset['tags']:
            if tag['tag_name'] == 'Livestock':
                livestock_datasets.append(dataset)
                break

print(f"Found {len(livestock_datasets)} livestock datasets:")

# Download all livestock datasets
for dataset in livestock_datasets:
    print(f"Downloading: {dataset['title']}")
    
    # Show all tags for this dataset
    tag_names = [tag['tag_name'] for tag in dataset.get('tags', [])]
    print(f"Tags: {tag_names}")
    
    try:
        download_path = client.download_dataset(
            dataset['ds_id'],
            root_dir="livestock_data"
        )
        print(f"✓ Downloaded to: {download_path}")
    except Exception as e:
        print(f"✗ Failed to download {dataset['ds_id']}: {e}")
    
    print("-" * 50)
```

You can modify the example code for other tags.

---

## Related Documentation

### Overview
- See [Installation](installation.md) for setup instructions
- Return to the [Introduction](index.md) to access all documentation sections

### SDK Documentation
- See [Getting Started](getting-started.md) for your first API calls
- Explore the [API Reference](api-reference.md) for complete method documentation

### CLI Documentation
- Use the [CLI Reference](cli-reference.md) for command-line operations