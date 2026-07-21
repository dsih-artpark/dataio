# dataio-artpark

Dataio is a Postgres + FastAPI based Dataset Management System (DMS) built by the Data Science Innovation Hub, ARTPARK, for cataloguing, validating, and distributing datasets. It ships as three things from one repo:

- A **Python package** (`dataio-artpark` on PyPI) with a CLI, an SDK, and a standalone manifest-validation library (`dataio.validate`) that can be reused outside this project.
- A **FastAPI backend** (`src/dataio/api/`) serving both the programmatic API and a session-based web application API.
- An **Astro + Preact web app** (`web/`) — the admin panel and public dataset catalogue/browser.

You can find the documentation for the project [here](https://dataio.artpark.ai), and we're also on PyPI [here](https://pypi.org/project/dataio-artpark/).

For contributor/agent workflow conventions (git workflow, environment variables, code style), see [AGENTS.md](AGENTS.md).

## Features

- **Dataset catalogue** — browse, search, and download versioned datasets with per-table data dictionaries, manifests, and README documentation, gated by access level.
- **Admin panel** (`web/src/components/admin/`) — import/curate datasets and raw datasets, manage manifests, run manifest validation, and manage users/groups/roles.
- **Dataset & raw dataset ID generation** — `ds_id` is assigned from a single catalogue-wide counter and `rds_id` from a per-category counter, matching the numbering used in the master Excel catalogue and S3. See `suggest_next_dataset_id` / `suggest_next_raw_dataset_id_for_category` in `src/dataio/api/database/functions.py`, and the "Next Available IDs" tool in the admin panel.
- **Manifest validation** (`dataio.validate`) — a standalone Pydantic-based library that validates a dataset's `metadata.yaml` + CSV/GeoJSON data against a versioned schema contract (tabular and geojson dataset kinds), independent of the API.
- **Auth** — passwordless web login via Email OTP or Passkey (WebAuthn), plus API-key auth for programmatic/SDK access.
- **AI chat assistant** (`/chat`) — an MCP-backed assistant that helps users discover datasets, backed by AWS Bedrock or OpenRouter.

## Installation

Install the Python package using pip:

```bash
pip install dataio-artpark
```

or using uv:

```bash
uv add dataio-artpark
```

## Development

We use uv to manage the Python project. Clone the repository and run:

```bash
uv sync --group api
```

### Backend

Set up the local database (API keys for seeded users are generated in `db/init/data_inserts`):

```bash
bash ./src/dataio/db/init/recreate_full.sh
```

Apply any new migrations:

```bash
uv run all-migrations
```

Start the API server:

```bash
uv run fastapi dev src/dataio/api
```

or, with logging and autoreload:

```bash
uvicorn src.dataio.api.main:app --log-config log_config.yml --reload
```

### Frontend

```bash
cd web
npm install
npm run dev      # dev server
npm run build    # astro check + production build
```

See [AGENTS.md](AGENTS.md) for required environment variables (backend `.env` and `web/.env`), the full project structure, and git/PR conventions.
