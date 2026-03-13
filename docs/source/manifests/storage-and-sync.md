# Manifest Storage And Sync

## Current State

The legacy upload path stores per-table metadata in `metadata.json` during table upload and later syncs that JSON into the database.

That flow has three problems:

- JSON is treated as the canonical format even though users author YAML
- table upload and manifest storage are incorrectly coupled
- database manifest state is updated asynchronously instead of at write time

## Deprecation

Legacy `metadata.json` table metadata is **deprecated**.

It remains available as a fallback for older datasets, but new manifest updates should use:

- `manifest.yaml` as the canonical authored document
- `manifest.json` as the normalized machine cache

## Recommended Direction

Make `manifest.yaml` the canonical document in filestore.

Recommended filestore layout:

- `filestore/{bucket_type}/{dataset_id}/manifest.yaml`
- optional derived cache: `manifest.json`
- table and file payloads stay alongside it

Recommended database cache fields:

- `manifest_yaml`
- `manifest_json`
- `manifest_updated_at`
- `manifest_updated_by`

## Write Path

The preferred manifest update flow is:

1. User uploads `manifest.yaml`
2. API validates the manifest using `DataIO Validate`
3. API writes the manifest to filestore
4. API updates the DB cache in the same request

The sync script should remain a reconciliation or backfill tool, not the primary write path for manifest changes.
