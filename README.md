# README - Dataio

Dataio is a Postgres and FASTAPI based Dataset Management System (DMS).

## Installation

Install the project using pip:

```bash
pip install git+https://github.com/dsih-artpark/dataio.git
```

or using uv:

```bash
uv add git+https://github.com/dsih-artpark/dataio.git
```

## Development

We use uv to manage the project. Clone the repository and run:

```bash
uv sync
```

## Starting the API
```
uv run fastapi dev src/dataio/api/api
```

To start with logging enabled
```
uvicorn src.dataio.api.api.main:app --log-config log_config.yml
```

