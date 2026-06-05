# Complete DataIO Dataset Upload Workflow

This guide outlines the streamlined steps required to successfully prepare and upload a dataset to the DataIO Production environment. 

> [!NOTE]
> Generating a `manifest.yaml` is **not required**. We rely entirely on the `metadata.yaml` and `info.yml` files for the upload process.

## 1. Clean the Metadata
LLM-generated datasets often contain formatting bugs, such as folded block scalars (`>`), which cause S3 upload truncation issues.

Run the reformat script to clean `metadata.yaml` and convert all folded blocks into safely wrapped strings:
```powershell
uv run python reformat_yaml.py "data\Your-Dataset-Folder\metadata.yaml"
```

## 2. Generate `info.yml`
Ensure that the dataset-level information is correctly defined. If you haven't already, generate or verify the `info.yml` file in your dataset directory.
```powershell
uv run python generate_all_info_yml.py "data\Your-Dataset-Folder"
```

## 3. Dry Run Data Upload
Before making any API calls or pushing data to the server, validate your folder structure, schemas, and files using the `--dry-run` flag:
```powershell
uv run dataio user upload-dataset "data\Your-Dataset-Folder" --dry-run
```
*Check the output to ensure your CSV files and Table metadata entries are correctly detected.*

## 4. Direct Upload
Once the dry run passes, execute the actual upload. This will parse your folder, create the dataset, and upload your tables and schemas to Production:
```powershell
uv run dataio user upload-dataset "data\Your-Dataset-Folder"
```

## 5. Sync Dry Run (SSH Tunnel Required)
Because the Production Database is on a private AWS network, you must establish an SSH tunnel to connect to it locally before running sync scripts.

1. **Start the SSH Tunnel** in the background:
   ```powershell
   Start-Process ssh -ArgumentList "-N -L 15432:127.0.0.1:5432 target" -WindowStyle Hidden
   ```
2. **Update your `.env` file** so SQLAlchemy connects through your local tunnel:
   ```bash
   DB_HOST=127.0.0.1
   DB_PORT=15432
   DB_USER=postgres
   DB_PASSWORD=<production_password>
   DB_NAME=catalogue
   ```
3. **Run a Dry Run Sync**:
   This will test pulling the documentation from S3 without making actual database modifications:
   ```powershell
   uv run python -m dataio.scripts.sync_dataset_documentation --dataset YOUR_DATASET_ID --dry-run
   ```

## 6. Final Sync
If the sync dry run is successful, execute the final sync to inject your uploaded documentation into the Production Database so the frontend UI can read it:
```powershell
uv run python -m dataio.scripts.sync_dataset_documentation --dataset YOUR_DATASET_ID
```
