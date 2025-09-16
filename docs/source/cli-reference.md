# CLI Reference

DataIO includes a command-line interface (CLI) built with Typer for convenient dataset management from the terminal. The CLI provides rich, formatted output with tables and colors.

## Installation

The CLI is included when you install DataIO:

```bash
uv add git+https://github.com/dsih-artpark/dataio.git@staging
```

## Running CLI Commands

You can run CLI commands in two ways:

**Option 1: Using uv run (recommended)**
```bash
uv run dataio list-datasets
uv run dataio download-dataset TS0001DS9999
```

**Option 2: Activate virtual environment**
```bash
# Activate your virtual environment first
source .venv/bin/activate  # or your venv activation command
dataio list-datasets
dataio download-dataset TS0001DS9999
```

## Configuration

The CLI uses the same environment variables as the Python SDK:

```bash
DATAIO_API_BASE_URL=https://staging.dataio.artpark.ai/api/v1
DATAIO_API_KEY=your_api_key_here
```

Set these in a `.env` file in your current working directory or as environment variables.

## Usage Patterns

The CLI can be used in two ways:

```bash
# Direct commands (recommended)
uv run dataio list-datasets
uv run dataio download-dataset TS0001DS9999

# Explicit user subcommands
uv run dataio user list-datasets
uv run dataio user download-dataset TS0001DS9999
```

Both approaches are equivalent. The direct commands are recommended for simplicity.

:::{note}
All examples below assume you're using `uv run`. If you've activated your virtual environment, you can omit `uv run` from the commands.
:::

---

## Commands

### `list-datasets`

List all available datasets with filtering options. Displays results in a formatted table.

**Usage:**
```bash
uv run dataio list-datasets [OPTIONS]
```

**Options:**
- `--limit INTEGER`: Number of datasets to list (default: 100)
- `-cl, --collection TEXT`: Filter by collection name or ID
- `-cg, --category TEXT`: Filter by category name or ID
- `--help`: Show help message

**Examples:**
```bash
# List all datasets (up to 100)
uv run dataio list-datasets

# List first 50 datasets
uv run dataio list-datasets --limit 50

# Filter by collection
uv run dataio list-datasets --collection "Census Data"

# Filter by category
uv run dataio list-datasets --category "Livestock"

# Combine filters
uv run dataio list-datasets --collection "Census Data" --category "Livestock" --limit 25
```

**Output:**
The command displays a formatted table with:
- **ID**: Dataset identifier (ds_id)
- **Title**: Dataset title
- **Description**: Dataset description
- **Data Owner**: Name of the data owner

If no datasets match the criteria, a helpful message is displayed.

---

### `download-dataset`

Download a complete dataset with all its tables and metadata.

**Usage:**
```bash
uv run dataio download-dataset DATASET_ID [OPTIONS]
```

**Arguments:**
- `DATASET_ID`: The dataset ID to download. Can be:
  - Full ID: `TS0001DS9999`
  - Last 4 digits: `9999`

**Options:**
- `-b, --bucket-type TEXT`: Bucket type to download (default: "STANDARDISED")
- `-r, --root-dir TEXT`: Root directory for download (default: "data")
- `-m, --get-metadata / --no-get-metadata`: Include metadata file (default: True)
- `-f, --metadata-format TEXT`: Metadata format - "yaml" or "json" (default: "yaml")
- `--help`: Show help message

**Examples:**
```bash
# Basic download
dataio download-dataset TS0001DS9999

# Download to custom directory
dataio download-dataset TS0001DS9999 --root-dir "my_datasets"

# Download without metadata
dataio download-dataset TS0001DS9999 --no-get-metadata

# Download with JSON metadata
dataio download-dataset TS0001DS9999 --metadata-format json

# Download using short ID
dataio download-dataset 9999

# Download preprocessed data (if available)
dataio download-dataset TS0001DS9999 --bucket-type PREPROCESSED
```

**Output:**
The command shows the download progress and final destination:
```
Dataset TS0001DS9999 downloaded to data/TS0001DS9999-Dataset_Title
```

:::{note}
Currently, only "STANDARDISED" datasets are available. "PREPROCESSED" datasets are not yet accessible through the API.
:::

---

### `download-shapefile`

Download geographic boundary data as GeoJSON files.

**Usage:**
```bash
uv run dataio download-shapefile REGION_ID [OPTIONS]
```

**Arguments:**
- `REGION_ID`: The region ID to download shapefile for (e.g., "state_29")

**Options:**
- `-f, --shp-folder TEXT`: Folder to download shapefile to (default: "data/GS0012DS0051-Shapefiles_India")
- `--help`: Show help message

**Examples:**
```bash
# Download state shapefile
dataio download-shapefile state_29

# Download to custom folder
dataio download-shapefile state_29 --shp-folder "my_shapefiles"

# Download district shapefile
dataio download-shapefile district_560
```

**Output:**
The command shows the download destination:
```
Shapefile state_29 downloaded to data/GS0012DS0051-Shapefiles_India/state_29.geojson
```

---

## Common Workflows

### Explore and Download Workflow

```bash
# 1. List available datasets
dataio list-datasets --limit 10

# 2. Filter by topic of interest
dataio list-datasets --category "Livestock"

# 3. Download a specific dataset
dataio download-dataset TS0001DS9999

# 4. Download related shapefiles
dataio download-shapefile state_29
```

## Output Formatting

The CLI uses Rich library for enhanced terminal output:

- **Colored tables** for dataset listings
- **Progress indicators** for downloads
- **Formatted error messages** with helpful suggestions
- **Consistent styling** across all commands

---

## Help System

Get help for any command:

```bash
# General help
dataio --help

# Command-specific help
dataio list-datasets --help
dataio download-dataset --help
dataio download-shapefile --help

# User subcommand help
dataio user --help
```

---

## Related Documentation

### Overview
- See [Installation](installation.md) for setup instructions
- Return to the [Introduction](index.md) to access all documentation sections

### SDK Documentation
- Start with [Getting Started](getting-started.md) for your first API calls
- Learn about [downloading datasets by tags](examples.md)
- Explore the [API Reference](api-reference.md) for complete method documentation