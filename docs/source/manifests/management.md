# Manifest Management

Dataset manifests are now a first-class write path.

## Canonical Files

Use these files for dataset manifests:

- `manifest.yaml`: canonical authored document
- `manifest.json`: normalized machine cache derived from the manifest

Legacy `metadata.json` table metadata is deprecated and should not be used for new manifest updates.

## Admin API

Write manifest:

```text
PUT /api/v1/admin/datasets/{dataset_id}/{bucket_type}/manifest
```

Read manifest:

```text
GET /api/v1/admin/datasets/{dataset_id}/{bucket_type}/manifest
```

The write path:

1. validates the uploaded manifest
2. writes `manifest.yaml` and `manifest.json` to filestore
3. updates the dataset manifest cache in the database in the same request

## Python SDK

Upload a manifest:

```python
from pathlib import Path
from dataio.sdk.admin import DataIOAdminAPI

client = DataIOAdminAPI()
client.upload_manifest(
    dataset_id="TS0001DS0001",
    bucket_type="STANDARDISED",
    manifest_path=Path("manifest.yaml"),
)
```

Get the stored manifest:

```python
manifest = client.get_manifest("TS0001DS0001", "STANDARDISED")
```

## Dataset Folder Uploads

If a dataset folder contains `manifest.yaml`, the admin SDK upload flow will upload it automatically after dataset creation and before table uploads.
