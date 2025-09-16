# README - Dataio

[![Netlify Status](https://api.netlify.com/api/v1/badges/83e85c01-20b5-40fb-a972-65f95b1646d4/deploy-status)](https://app.netlify.com/projects/dataio/deploys)

Dataio is a Postgres and FASTAPI based Dataset Management System (DMS) for users to access and manage datasets distributed by the Data Science Innovation Hub, ARTPARK. The scaffolding can be used to build a similar system for your own datasets.

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

## How to set up the local dev environment.

Run below command to set up the DB. API keys for users will be generated in the db/init/data_inserts folder

```
bash ./src/dataio/db/init/recreate_full.sh
```

Starting the API Server

```
uv run fastapi dev src/dataio/api
```

To start with logging & autoreload enabled

```
uvicorn src.dataio.api.main:app --log-config log_config.yml --reload
```
