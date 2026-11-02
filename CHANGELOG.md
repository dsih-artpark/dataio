# CHANGELOG

## 0.4.0b17

- Shapefiles: `list-shapefiles`, `list-shapefile-versions`, `download-shapefile`;
  versioned shapefiles in the ARTPARK GeoJSON format.
- Admin: `create-shapefile-version`, `upload-shapefile`, `upload-shapefiles`;
  safer dataset import (failed imports undone, reserved-ID check, Excel CSVs).
- `validate`: check manifests and data files locally; every column needs
  `description` and `nullable`.
- `draft`: internal tool for LLM-drafted metadata.yaml (needs the server code).

## 0.4.0b13

- Ensure uv.lock is updated.
- Update docs and changelog.

## 0.4.0b12

- Added a `data_dir` parameter to the `DataIOAPI` constructor.
- Added a `DATAIO_DATA_DIR` environment variable.
- Update docs

## 0.4.0b11

- Stable Beta Release
- Fully functional API, CLI, and SDK
- Start of utilising SemVer
- PyPI release: [dataio-artpark](https://pypi.org/project/dataio-artpark/)
- Starting to maintain a changelog

## 0.2.0

- Rewrites dataio from the ground up.
- This prerelease is not backwards compatible with the previous version.
- The database schema is currently on version 002.
