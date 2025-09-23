# Installation

This guide covers how to install and configure DataIO for both SDK and CLI usage.

## Installing DataIO

`dataio` is available on [PyPI](https://pypi.org/project/dataio-artpark/). The latest version is {{version}}.

Using uv:

```bash
uv add dataio-artpark
```

or using pip:

```bash
venv .venv
source .venv/bin/activate
pip install dataio-artpark
```

It is always recommended to use a virtual environment to install the package, regardless of the installation method. `uv` provides a seamless way to create and manage virtual environments within the same command.

You can also install it from source:

```bash
uv install https://github.com/dsih-artpark/dataio.git
```

OR clone from source and install:

```bash
git clone https://github.com/dsih-artpark/dataio.git
cd dataio
uv sync
```

Installing from source will give you the latest version of the code, which may not be stable. It's recommended that for specificity, you specify the version you want to install.

(configuration)=

## Configuration

The client relies on two variables to authenticate with the API Server:

1. `DATAIO_API_BASE_URL`: The base URL of the API. Generally, this is https://dataio.artpark.ai/api/v1/
2. `DATAIO_API_KEY`: The API key for the API.

You can set these variables in a .env file or pass them as arguments to the `DataIOAPI` constructor.

You can also run

```bash
uv run dataio init
# OR using venv
source .venv/bin/activate
dataio init
```

This will prompt you to enter your API key and set the default base URL and create an `.env` file in your project root:

```bash
DATAIO_API_BASE_URL=https://dataio.artpark.ai/api/v1
DATAIO_API_KEY=your_api_key_here
DATAIO_DATA_DIR=data
```

You can also di this manually.

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
Contact the DataIO administrators to get your API key if you haven't already.
:::

## What's Next?

Now that you have DataIO installed and configured, you can continue with [Quick Start](/sdk/index.md) for your first API calls through the SDK, or use the [CLI Guide](/cli/index.md) for command-line operations
