# Installation

This guide covers how to install and configure DataIO for both SDK and CLI usage.

## Installing DataIO

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

## Verifying Installation

### Testing SDK Installation

```python
from dataio import DataIOAPI

# Create client
client = DataIOAPI()
print("DataIO SDK installed successfully!")
```

### Testing CLI Installation

You can run CLI commands in two ways:

**Option 1: Using uv run (recommended)**
```bash
uv run dataio --help
```

**Option 2: Activate virtual environment**
```bash
# Activate your virtual environment first
source .venv/bin/activate  # or your venv activation command
dataio --help
```

## Getting API Access

:::{tip}
Contact the DataIO administrators to get your API key if you haven't already. The API endpoint is currently in staging and not publicly available.
:::

## What's Next?

Now that you have DataIO installed and configured, you can:

### SDK Documentation
- Continue with [Getting Started](getting-started.md) for your first API calls
- Learn about [downloading datasets by tags](examples.md)
- Explore the [API Reference](api-reference.md) for complete method documentation

### CLI Documentation
- Use the [CLI Reference](cli-reference.md) for command-line operations

### Navigation
Return to the [Introduction](index.md) to access all documentation sections.