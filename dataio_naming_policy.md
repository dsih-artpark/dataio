# DataIO Policy & Dataset Naming Conventions

This document outlines the strict naming conventions, prefix categorisation, and folder validation rules required for onboarding datasets into DataIO. All onboarding guard code must enforce these rules to successfully upload datasets to the Production/Staging buckets via the DataIO SDK.

## 1. Prefix Categorisation (Category IDs)
Every dataset belongs to a specific Category, denoted by a strict **2-letter uppercase prefix**. 

Currently known and supported prefixes include:
* **`CS`** : Census and Surveys (e.g., Livestock Census, BAHS)
* **`EP`** : Epidemiology (e.g., Disease Surveillance, NIFMD)
* **`GS`** : Geospatial / Geographic Surveys (e.g., Shapefiles)

*Note: In the `metadata.yaml` file, this 2-letter prefix must be mapped under the `category.ID` field.*

## 2. Identifier Architecture
DataIO uses a hierarchical ID structure mapping Categories → Collections → Datasets → Raw Datasets.

1. **Collection ID**: Follows the pattern `[Category Prefix][Digits]`.
   - *Example*: `CS0026`
2. **Dataset ID**: Follows the pattern `[Collection ID]DS[4 Digits]`.
   - *Example*: `CS0026DS0111`
3. **Raw Dataset ID (RDS)**: Extracted automatically by replacing the leading zeros of the collection/dataset ID with `RDS`.
   - *Example*: `CS0026` → `CSRDS26`

## 3. Folder Naming Conventions
The DataIO upload CLI/SDK enforces strict regex validation on the dataset folder name (`^([A-Z]{2}\d+[A-Z]{2}\d{4})`). 

The folder name **must begin exactly with the Dataset ID**, optionally followed by a dash and the dataset slug/title.

**✅ Valid Examples:**
* `CS0026DS0111-bahs-milk-production-statistics`
* `EP0006DS0114-nifmd-fmd-surveillance-statistics-2008-2024`

**❌ Invalid Examples:**
* `bahs-milk-production` *(Missing Dataset ID prefix)*
* `cs0026ds0111-bahs` *(Dataset ID is not fully uppercase)*
* `CS0026DS111-bahs` *(Dataset ID does not have exactly 4 digits after DS)*

## 4. Required Folder Contents
To pass the onboarding guard code, the dataset folder must contain the following minimum required files:

1. **`info.yml`** (or `info.yaml`): Dataset-level metadata.
   - *Validation Rule*: Must contain `title` and `data_owner_name` keys.
2. **`metadata.yaml`** (or `metadata.yml`): Table-level schemas.
   - *Validation Rule*: Must contain table schemas defined under the `tables` key.
3. **Data Files**: At least one or more `.csv` files containing the tabular data.

## 5. Upload Workflow for Production Bucket
Datasets are shared and uploaded through the Production bucket. The upload sequence executed by the onboarding code is:
1. Validates the folder name against the regex pattern.
2. Checks for `info.yml` and `metadata.yaml`.
3. Creates the Raw Dataset (RDS) and assigns permissions to the Data Owner.
4. Creates the Dataset record in the database.
5. Uploads table metadata and CSVs to the appropriate S3 bucket (`STANDARDISED` or `PREPROCESSED`).
