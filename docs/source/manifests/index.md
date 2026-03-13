# Dataset Manifests

DataIO uses a **dataset manifest** as the canonical document for dataset structure, metadata, and validation.

A manifest is broader than a schema. It can include:

- dataset-level metadata
- table or file inventory
- field definitions and constraints
- shared enums
- dataset kind and spec hints
- validation-relevant context such as temporal and region semantics

## Core Vocabulary

- **Manifest**: the canonical YAML document for a dataset
- **Validation**: checking a manifest and data files against the metadata contract and type rules
- **Filestore**: the persisted object storage copy of dataset files and dataset manifests
- **DB cache**: the platform copy used for fast reads in APIs and UI

## Validation Entry Points

CLI:

```bash
dataio validate tabular --manifest manifest.yaml --table livestock=livestock.csv
dataio validate geojson --manifest geojson-manifest.yaml --data districts.geojson
```

Python SDK:

```python
from dataio.sdk.validate import DataIOValidator

validator = DataIOValidator()
result = validator.validate_tabular(
    manifest="manifest.yaml",
    data_files={"livestock": "livestock.csv"},
)
print(result.status)
```

API:

- `POST /api/v1/validate`
- `POST /api/v1/validate/tabular`
- `POST /api/v1/validate/geojson`

```{toctree}
:hidden:

validation
management
storage-and-sync
```
