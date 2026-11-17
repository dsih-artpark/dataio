# Manifest Management

Dataset manifests are now a first-class write path.

## Canonical Files

Use these files for dataset manifests:

- `manifest.yaml`: canonical authored document
- `manifest.json`: normalized machine cache derived from the manifest

Legacy `metadata.json` table metadata is deprecated and should not be used for new manifest updates.

## Admin API

Session-authenticated web admin routes are also available for the platform UI:

```text
GET /api/v1/web/admin/datasets/{dataset_id}/{bucket_type}/manifest
PUT /api/v1/web/admin/datasets/{dataset_id}/{bucket_type}/manifest
POST /api/v1/web/admin/validate/tabular
POST /api/v1/web/admin/validate/geojson
```

These routes power the admin manifest management UI, where administrators can:

- view the canonical manifest stored for a dataset
- upload a replacement `manifest.yaml`
- validate a candidate manifest with optional tabular or GeoJSON data before promotion

Write manifest:

```text
PUT /api/v1/admin/datasets/{dataset_id}/{bucket_type}/manifest
```

Read manifest:

```text
GET /api/v1/admin/datasets/{dataset_id}/{bucket_type}/manifest
```

The upload can be either a platform manifest (`datasetTables` / `dataDictionary`) or a curator-authored `metadata.yaml` (`tables` / `data_dictionary`). An authored `metadata.yaml` is converted to the platform shape first, the same way the package import does it: `datasetKind: tabular` is added, each table gets `path: <table>.csv`, `year` columns become `date` with `"%Y"`, and a null `enumDefinitions` is dropped.

The write path:

1. validates the uploaded manifest
2. validates the dataset's existing stored data against that manifest
3. writes `manifest.yaml` and `manifest.json` to filestore only if both checks pass
4. updates the dataset manifest cache in the database in the same request

A canonical manifest upload is rejected when the already stored dataset files do not meet the manifest standard.

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

## Replacing an Existing Dataset

To replace both the CSV files and the metadata of a dataset that already exists, use **Import Dataset Package** in the admin UI with **Replace an existing dataset** ticked. The routes take a `replace_existing=true` form field:

```text
POST /api/v1/web/admin/datasets/import/preview
POST /api/v1/web/admin/datasets/import/apply
```

The preview lists the files that will be archived and the tables that will be replaced, added and removed. The apply step, for a dataset whose ID and collection match the package:

1. copies the dataset's current files to `archive/filestore/<bucket_type>/<dataset_id>/<UTC timestamp>/`, outside the dataset folder
2. updates the dataset's details (title, owner, description, coverage, resolution, tags, additional metadata) from `info.yml`, and adds the package's raw dataset to its raw-dataset links
3. removes the dataset's old files and uploads the package CSVs and the converted manifest

The dataset ID, collection, access level, user permissions and group memberships are not changed. If a step fails, DataIO tries to restore the old files from the archive and put the dataset's details back. If part of that rollback also fails, the error lists the steps still to do by hand and the archive location; use the archive copy to finish recovery. The archive copy is always kept; remove it once the replacement has been checked.

### Recording what changed

A replace also needs a short changelog entry: what changed (summary), why, whether it is a data or metadata change, and whether it is breaking (users may need to change their code). The apply route takes it as a `changelog_json` form field, and the admin UI asks for it in the **What changed** section; rows for added and removed tables are filled in from the preview. When the dataset already has a stored manifest, rows for added and removed columns, unit changes and changed allowed values are also suggested for tables in both versions; hidden columns (`access: false`) are never suggested. A renamed column shows as one removed and one added column: replace the pair with a single **Column renamed** row. Without a summary and a reason the replace is refused before anything is archived.

The entry is saved as a draft under **Datasets > Dataset changes**. An admin publishes it there, and it then shows in the dataset's **Version history** on its public page and on the public **Dataset changes** page (`/changes`). Old and new values and notes are hidden for Restricted datasets. Entries for metadata-only edits can be added on the same admin page.

## Dataset Folder Uploads

If a dataset folder contains `manifest.yaml`, the admin SDK upload flow will try to upload it after dataset creation and before table uploads. The upload is validated against the dataset's stored CSVs, which don't exist yet at that point, so it fails. `metadata.yaml` itself is not sent as a manifest. Use Import Dataset Package in the admin UI instead, or upload the manifest after the tables.
