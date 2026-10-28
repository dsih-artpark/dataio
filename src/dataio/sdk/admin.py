"""Admin SDK for DataIO - Dataset upload and management operations."""

import json
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional, Dict, Any, List

import dotenv
import requests
import yaml
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()


class DataIOAdminAPI:
    """Admin API Client for uploading datasets to DataIO.

    :param base_url: The base URL of the DataIO API. Defaults to DATAIO_API_BASE_URL env var.
    :param api_key: The API key for admin access. Defaults to DATAIO_API_KEY env var.
    :param data_dir: The directory containing datasets. Defaults to DATAIO_DATA_DIR env var.
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        data_dir: Optional[str] = None,
    ):
        dotenv.load_dotenv(override=True)

        if base_url is None:
            base_url = os.getenv("DATAIO_API_BASE_URL")
        if base_url is None:
            raise ValueError("DATAIO_API_BASE_URL is not set")
        self.base_url = base_url

        if api_key is None:
            api_key = os.getenv("DATAIO_API_KEY")
        if api_key is None:
            raise ValueError("DATAIO_API_KEY is not set")

        self.session = requests.Session()
        self.session.headers.update({"X-API-Key": api_key})

        if data_dir is None:
            data_dir = os.getenv("DATAIO_DATA_DIR", "data")
        self.data_dir = data_dir

    def _request(self, method: str, endpoint: str, **kwargs) -> Any:
        """Make a request to the DataIO API."""
        url = f"{self.base_url}{endpoint}"
        response = self.session.request(method, url, **kwargs)
        response.raise_for_status()
        if response.content:
            return response.json()
        return None

    def _parse_dataset_folder(self, folder_path: str) -> Dict[str, Any]:
        """Parse a dataset folder and extract all required information.

        Expected folder structure:
        - {ds_id}-{title}/
          - info.yml (dataset-level metadata)
          - metadata.yaml (table-level metadata)
          - *.csv (data files)
        """
        folder = Path(folder_path)
        folder_name = folder.name

        # Extract ds_id from folder name (format: {collection_id}{DS}{4-digits})
        # Collection ID is typically 6 chars but can vary (e.g., CS0026 or CS007)
        match = re.match(r'^([A-Z]{2}\d+[A-Z]{2}\d{4})', folder_name)
        if not match:
            raise ValueError(
                f"Folder name '{folder_name}' does not start with a valid ds_id pattern "
                "(expected format: CS0026DS0111-... or similar)"
            )
        ds_id = match.group(1)

        # Extract collection_id (everything before 'DS')
        collection_match = re.match(r'^([A-Z]{2}\d+)', ds_id)
        if not collection_match:
            raise ValueError(f"Could not extract collection_id from ds_id '{ds_id}'")
        collection_id = collection_match.group(1)

        # Load info.yml for dataset-level metadata
        info_path = folder / "info.yml"
        if not info_path.exists():
            info_path = folder / "info.yaml"
        if not info_path.exists():
            raise FileNotFoundError(
                f"Missing info.yml in {folder_path}. "
                "Create one with required fields: title, data_owner_name"
            )

        with open(info_path, "r", encoding="utf-8") as f:
            info = yaml.safe_load(f)

        # Validate required fields
        if "title" not in info:
            raise ValueError(f"info.yml missing required field: title")
        if "data_owner_name" not in info:
            raise ValueError(f"info.yml missing required field: data_owner_name")

        # Allow overriding ds_id and collection_id from info.yml
        if "ds_id" in info:
            ds_id = info["ds_id"]
        if "collection_id" in info:
            collection_id = info["collection_id"]

        # Load metadata.yaml for table-level metadata
        metadata_path = folder / "metadata.yaml"
        if not metadata_path.exists():
            metadata_path = folder / "metadata.yml"
        if not metadata_path.exists():
            raise FileNotFoundError(f"Missing metadata.yaml in {folder_path}")

        with open(metadata_path, "r", encoding="utf-8") as f:
            metadata = yaml.safe_load(f)

        manifest_path = folder / "manifest.yaml"
        if not manifest_path.exists():
            manifest_path = folder / "manifest.yml"
        if not manifest_path.exists():
            manifest_path = None

        # Get table definitions
        tables = metadata.get("tables", {})

        # Find all CSV files
        csv_files = list(folder.glob("*.csv"))

        return {
            "ds_id": ds_id,
            "collection_id": collection_id,
            "folder_path": folder,
            "info": info,
            "manifest_path": manifest_path,
            "tables": tables,
            "csv_files": csv_files,
        }

    def create_data_owner(
        self,
        name: str,
        contact_person: Optional[str] = None,
        contact_person_email: Optional[str] = None,
    ) -> Dict:
        """Create a data owner entry.

        :param name: Name of the data owner (must be unique)
        :param contact_person: Optional contact person name
        :param contact_person_email: Optional contact person email
        :returns: Created data owner object
        """
        payload = {"name": name}
        if contact_person:
            payload["contact_person"] = contact_person
        if contact_person_email:
            payload["contact_person_email"] = contact_person_email
        return self._request("POST", "/admin/data-owners", json=payload)

    def ensure_data_owner_exists(self, name: str) -> bool:
        """Ensure a data owner exists, creating it if necessary.

        :param name: Name of the data owner
        :returns: True if created, False if already existed
        """
        try:
            self.create_data_owner(name=name)
            return True
        except requests.HTTPError as e:
            # 500 errors often mean "already exists" but with generic error message
            if e.response.status_code == 500:
                # Assume it already exists and continue
                return False
            # Check for unique constraint violation
            if "unique" in e.response.text.lower() or "duplicate" in e.response.text.lower():
                return False
            raise

    def create_raw_dataset(self, rds_id: str, title: str, source: str) -> Dict:
        """Create a raw dataset entry.

        :param rds_id: Raw dataset identifier
        :param title: Raw dataset title
        :param source: Raw dataset source (URL or description)
        :returns: Created raw dataset object
        """
        payload = {
            "rds_id": rds_id,
            "title": title,
            "source": source,
        }
        return self._request("POST", "/admin/raw-datasets", json=payload)

    def create_dataset(
        self,
        ds_id: str,
        title: str,
        collection_id: str,
        data_owner_name: str,
        raw_dataset_ids: List[str],
        description: Optional[str] = None,
        spatial_coverage_region_id: Optional[str] = None,
        spatial_resolution: Optional[str] = None,
        temporal_coverage_start_date: Optional[str] = None,
        temporal_coverage_end_date: Optional[str] = None,
        temporal_resolution: Optional[str] = None,
        access_level: str = "NONE",
        tags: Optional[List[str]] = None,
        additional_metadata: Optional[Dict] = None,
    ) -> Dict:
        """Create a dataset.

        :param ds_id: Dataset identifier (12 chars, must start with collection_id)
        :param title: Dataset title
        :param collection_id: Collection identifier (first 6 chars of ds_id)
        :param data_owner_name: Name of data owner (must exist in database)
        :param raw_dataset_ids: List of raw dataset IDs to link
        :param description: Optional description
        :param spatial_coverage_region_id: Optional region ID
        :param spatial_resolution: Optional spatial resolution enum
        :param temporal_coverage_start_date: Optional start date (YYYY or YYYY-MM-DD)
        :param temporal_coverage_end_date: Optional end date
        :param temporal_resolution: Optional temporal resolution enum
        :param access_level: Access level (NONE|VIEW|DOWNLOAD)
        :param tags: Optional list of tags
        :param additional_metadata: Optional additional metadata dict
        :returns: Created dataset object
        """
        payload = {
            "ds_id": ds_id,
            "title": title,
            "collection_id": collection_id,
            "data_owner_name": data_owner_name,
            "raw_dataset_ids": raw_dataset_ids,
        }

        if description:
            payload["description"] = description
        if spatial_coverage_region_id:
            payload["spatial_coverage_region_id"] = spatial_coverage_region_id
        if spatial_resolution:
            payload["spatial_resolution"] = spatial_resolution
        if temporal_coverage_start_date:
            payload["temporal_coverage_start_date"] = temporal_coverage_start_date
        if temporal_coverage_end_date:
            payload["temporal_coverage_end_date"] = temporal_coverage_end_date
        if temporal_resolution:
            payload["temporal_resolution"] = temporal_resolution
        if access_level:
            payload["access_level"] = access_level
        if tags:
            payload["tags"] = tags
        if additional_metadata:
            payload["additional_metadata"] = additional_metadata

        return self._request("POST", "/admin/datasets", json=payload)

    def upload_table(
        self,
        dataset_id: str,
        bucket_type: str,
        csv_path: Path,
        table_metadata: Dict,
    ) -> Dict:
        """Upload a table (CSV file) to a dataset.

        :param dataset_id: Dataset ID to upload to
        :param bucket_type: Bucket type (PREPROCESSED or STANDARDISED)
        :param csv_path: Path to the CSV file
        :param table_metadata: Table metadata dict with table_name, description, data_dictionary
        :returns: Upload response
        """
        # Prepare table metadata file
        metadata_json = json.dumps(table_metadata)

        with open(csv_path, "rb") as csv_file:
            files = {
                "file": (csv_path.name, csv_file, "text/csv"),
                "table_metadata_file": (
                    "metadata.json",
                    metadata_json,
                    "application/json"
                ),
            }

            url = f"{self.base_url}/admin/datasets/{dataset_id}/{bucket_type}/tables"
            response = self.session.post(url, files=files)
            response.raise_for_status()
            return response.json()

    def upload_manifest(
        self,
        dataset_id: str,
        bucket_type: str,
        manifest_path: Path,
    ) -> Dict:
        with open(manifest_path, "rb") as manifest_file:
            files = {
                "manifest_file": (
                    manifest_path.name,
                    manifest_file,
                    "application/x-yaml",
                )
            }
            url = f"{self.base_url}/admin/datasets/{dataset_id}/{bucket_type}/manifest"
            response = self.session.put(url, files=files)
            response.raise_for_status()
            return response.json()

    def get_manifest(self, dataset_id: str, bucket_type: str) -> Dict:
        return self._request("GET", f"/admin/datasets/{dataset_id}/{bucket_type}/manifest")

    def upload_dataset_folder(
        self,
        folder_path: str,
        bucket_type: str = "STANDARDISED",
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Upload an entire dataset folder (create dataset + upload all tables).

        :param folder_path: Path to the dataset folder
        :param bucket_type: Bucket type (PREPROCESSED or STANDARDISED)
        :param dry_run: If True, only validate without making API calls
        :returns: Summary of upload operation
        """
        console.print(f"\n[bold blue]Processing folder:[/] {folder_path}")

        # Parse folder
        parsed = self._parse_dataset_folder(folder_path)
        ds_id = parsed["ds_id"]
        collection_id = parsed["collection_id"]
        info = parsed["info"]
        manifest_path = parsed["manifest_path"]
        tables = parsed["tables"]
        csv_files = parsed["csv_files"]

        console.print(f"  Dataset ID: [cyan]{ds_id}[/]")
        console.print(f"  Collection ID: [cyan]{collection_id}[/]")
        console.print(f"  Title: [cyan]{info['title']}[/]")
        console.print(f"  Data Owner: [cyan]{info['data_owner_name']}[/]")
        console.print(f"  CSV files found: [cyan]{len(csv_files)}[/]")
        console.print(f"  Table metadata entries: [cyan]{len(tables)}[/]")

        if dry_run:
            console.print("\n[yellow]DRY RUN - No API calls will be made[/]")
            return {"dry_run": True, "ds_id": ds_id, "tables": len(csv_files)}

        results = {
            "ds_id": ds_id,
            "data_owner_created": False,
            "raw_dataset": None,
            "dataset": None,
            "manifest_uploaded": False,
            "tables_uploaded": [],
            "tables_failed": [],
        }

        # Step 1: Ensure data owner exists
        data_owner_name = info["data_owner_name"]
        console.print(f"\n[bold]Step 1:[/] Ensuring data owner [cyan]{data_owner_name}[/] exists...")
        try:
            created = self.ensure_data_owner_exists(data_owner_name)
            results["data_owner_created"] = created
            if created:
                console.print(f"  [green]✓[/] Data owner created")
            else:
                console.print(f"  [yellow]![/] Data owner already exists")
        except requests.HTTPError as e:
            console.print(f"  [red]✗[/] Failed to create data owner: {e.response.text}")
            raise

        # Step 2: Create raw dataset
        raw_dataset_info = info.get("raw_dataset", {})
        rds_id = raw_dataset_info.get("rds_id", f"{ds_id}-raw-001")
        raw_source = raw_dataset_info.get("source", "Manual upload")

        console.print(f"\n[bold]Step 2:[/] Creating raw dataset [cyan]{rds_id}[/]...")
        try:
            raw_result = self.create_raw_dataset(
                rds_id=rds_id,
                title=f"Raw data for {info['title']}",
                source=raw_source,
            )
            results["raw_dataset"] = raw_result
            console.print(f"  [green]✓[/] Raw dataset created")
        except requests.HTTPError as e:
            # 500 errors often mean "already exists" but with generic error message
            if e.response.status_code == 500:
                console.print(f"  [yellow]![/] Raw dataset may already exist, continuing...")
            else:
                raise

        # Step 3: Create dataset
        console.print(f"\n[bold]Step 3:[/] Creating dataset [cyan]{ds_id}[/]...")
        try:
            # First try with all optional fields
            dataset_result = self.create_dataset(
                ds_id=ds_id,
                title=info["title"],
                collection_id=collection_id,
                data_owner_name=info["data_owner_name"],
                raw_dataset_ids=[rds_id],
                description=info.get("description"),
                spatial_coverage_region_id=info.get("spatial_coverage_region_id"),
                spatial_resolution=info.get("spatial_resolution"),
                temporal_coverage_start_date=info.get("temporal_coverage_start_date"),
                temporal_coverage_end_date=info.get("temporal_coverage_end_date"),
                temporal_resolution=info.get("temporal_resolution"),
                access_level=info.get("access_level", "NONE"),
                tags=info.get("tags"),
                additional_metadata=info.get("additional_metadata"),
            )
            results["dataset"] = dataset_result
            console.print(f"  [green]✓[/] Dataset created")
        except requests.HTTPError as e:
            if e.response.status_code == 500:
                # Retry with minimal payload (required fields only)
                console.print(f"  [yellow]![/] Retrying with minimal payload...")
                try:
                    dataset_result = self.create_dataset(
                        ds_id=ds_id,
                        title=info["title"],
                        collection_id=collection_id,
                        data_owner_name=info["data_owner_name"],
                        raw_dataset_ids=[rds_id],
                    )
                    results["dataset"] = dataset_result
                    console.print(f"  [green]✓[/] Dataset created (with minimal fields)")
                except requests.HTTPError as e2:
                    if e2.response.status_code == 500:
                        console.print(f"  [yellow]![/] Dataset may already exist, continuing to upload tables...")
                    else:
                        console.print(f"  [red]✗[/] Failed to create dataset: {e2.response.text}")
                        raise
            else:
                console.print(f"  [red]✗[/] Failed to create dataset: {e.response.text}")
                raise

        if manifest_path:
            console.print(f"\n[bold]Step 4:[/] Uploading manifest [cyan]{manifest_path.name}[/]...")
            try:
                self.upload_manifest(
                    dataset_id=ds_id,
                    bucket_type=bucket_type,
                    manifest_path=manifest_path,
                )
                results["manifest_uploaded"] = True
                console.print("  [green]✓[/] Manifest uploaded")
            except requests.HTTPError as e:
                console.print(f"  [red]✗[/] Failed to upload manifest: {e.response.text}")
                raise

        # Step 5: Upload tables
        console.print(f"\n[bold]Step 5:[/] Uploading {len(csv_files)} tables...")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            console=console,
        ) as progress:
            for csv_file in csv_files:
                table_name = csv_file.stem
                task = progress.add_task(f"Uploading {table_name}...", total=None)

                # Get table metadata
                table_meta = tables.get(table_name, {})
                data_dict = table_meta.get("data_dictionary", {})
                table_info = table_meta.get("info", {})

                # Build table metadata for upload
                upload_metadata = {
                    "table_name": table_name,
                    "description": table_info.get("about", ""),
                    "source": table_info.get("source", ""),
                    "data_dictionary": {
                        col_name: {
                            "description": col_info.get("description", ""),
                            "comments": col_info.get("comments", ""),
                            "access": col_info.get("access", True),
                        }
                        for col_name, col_info in data_dict.items()
                        if isinstance(col_info, dict)
                    }
                }

                try:
                    self.upload_table(
                        dataset_id=ds_id,
                        bucket_type=bucket_type,
                        csv_path=csv_file,
                        table_metadata=upload_metadata,
                    )
                    results["tables_uploaded"].append(table_name)
                    progress.remove_task(task)
                    console.print(f"  [green]✓[/] {table_name}")
                except requests.HTTPError as e:
                    results["tables_failed"].append({
                        "table_name": table_name,
                        "error": str(e),
                    })
                    progress.remove_task(task)
                    console.print(f"  [red]✗[/] {table_name}: {e.response.text[:100]}")

        # Summary
        console.print(f"\n[bold green]Upload complete![/]")
        console.print(f"  Uploaded: {len(results['tables_uploaded'])} tables")
        if results["tables_failed"]:
            console.print(f"  Failed: {len(results['tables_failed'])} tables")

        return results

    def upload_all_datasets(
        self,
        data_dir: Optional[str] = None,
        bucket_type: str = "STANDARDISED",
        dry_run: bool = False,
    ) -> List[Dict[str, Any]]:
        """Upload all dataset folders in the data directory.

        :param data_dir: Directory containing dataset folders
        :param bucket_type: Bucket type for all uploads
        :param dry_run: If True, only validate without making API calls
        :returns: List of upload results for each dataset
        """
        if data_dir is None:
            data_dir = self.data_dir

        data_path = Path(data_dir)
        if not data_path.exists():
            raise FileNotFoundError(f"Data directory not found: {data_dir}")

        # Find all dataset folders (folders with ds_id pattern)
        dataset_folders = []
        for item in data_path.iterdir():
            if item.is_dir() and re.match(r'^[A-Z]{2}\d+[A-Z]{2}\d{4}', item.name):
                dataset_folders.append(item)

        if not dataset_folders:
            console.print("[yellow]No dataset folders found in {data_dir}[/]")
            return []

        console.print(f"[bold]Found {len(dataset_folders)} dataset folders[/]")

        results = []
        for folder in sorted(dataset_folders):
            try:
                result = self.upload_dataset_folder(
                    folder_path=str(folder),
                    bucket_type=bucket_type,
                    dry_run=dry_run,
                )
                results.append(result)
            except Exception as e:
                console.print(f"[red]Error processing {folder.name}: {e}[/]")
                results.append({"folder": folder.name, "error": str(e)})

        return results

    # Shapefiles

    def create_shapefile_version(
        self,
        version: str,
        source: str,
        dataset_id: Optional[str] = None,
        description: Optional[str] = None,
        released_at: Optional[str] = None,
    ) -> Dict:
        """Create a shapefile version (one upload batch, e.g. "kgis-2026-08").

        :param version: Version name: lowercase letters, digits, "." or "-"
        :param source: Where the boundaries come from
        :param dataset_id: Catalogue ds_id the boundaries are published as
        :param description: Free-text notes
        :param released_at: ISO timestamp that orders versions (default: now).
            Downloads without a version get the newest version.
        :returns: The created version
        """
        payload = {"version": version, "source": source, "dataset_id": dataset_id,
                   "description": description, "released_at": released_at}
        response = self.session.post(
            f"{self.base_url}/admin/shapefile-versions",
            json={k: v for k, v in payload.items() if v is not None},
        )
        _raise_for_status_with_detail(response)
        return response.json()

    def upload_shapefile(
        self,
        region_id: str,
        geojson_path: str,
        version: str,
        overwrite: bool = False,
    ) -> Dict:
        """Upload one region's boundary GeoJSON (Feature or FeatureCollection).

        The server checks the region exists and the GeoJSON is a lon/lat
        Polygon/MultiPolygon, and stores it as a FeatureCollection whose
        properties start with region_id/name/parent/parent_name.

        :param region_id: Region id, e.g. "subdistrict_5433"
        :param geojson_path: Path to the .geojson file
        :param version: Shapefile version to upload into (create it first)
        :param overwrite: Replace the region's file if the version already has one
        :returns: Upload response
        """
        return self._post_shapefile(self.session, region_id, Path(geojson_path), version, overwrite)

    def _post_shapefile(self, session, region_id: str, path: Path, version: str, overwrite: bool) -> Dict:
        with open(path, "rb") as f:
            response = session.post(
                f"{self.base_url}/admin/shapefiles/{region_id}",
                params={"version": version, "overwrite": str(overwrite).lower()},
                files={"shapefile": (path.name, f, "application/geo+json")},
            )
        _raise_for_status_with_detail(response)
        return response.json()

    def upload_shapefile_folder(
        self,
        folder_path: str,
        version: str,
        overwrite: bool = False,
        dry_run: bool = False,
        workers: int = 4,
        id_map: Optional[Dict[str, str]] = None,
    ) -> List[Dict[str, Any]]:
        """Upload every *.geojson under a folder, one region per file.

        Each file's region id is "<regionType>_<lgdCode>", read from the
        feature's properties (the standardised boundary schema), so file names
        don't matter. Files without regionType/lgdCode, with more than one
        feature, or mapping to the same region id as another file are skipped.
        Regions the version already has are skipped unless overwrite is set,
        so an interrupted upload can simply be re-run.

        :param folder_path: Folder to scan recursively
        :param version: Shapefile version to upload into (create it first)
        :param overwrite: Replace files the version already has
        :param dry_run: Only map files to region ids; upload nothing
        :param workers: Parallel uploads
        :param id_map: For levels whose region ids aren't "<regionType>_<lgdCode>"
            (e.g. wards: ward_<ULB code>-<ward number>), maps that derived id to
            the region id. Files whose derived id isn't in the map are skipped.
        :returns: One result per file: file, region_id, status
            (uploaded / exists / skipped / failed / planned) and detail
        """
        folder = Path(folder_path)
        if not folder.is_dir():
            raise FileNotFoundError(f"Folder not found: {folder_path}")

        results: List[Dict[str, Any]] = []
        planned: Dict[str, List[Path]] = {}
        for path in sorted(folder.rglob("*.geojson")):
            region_id, reason = _region_id_from_geojson_file(path)
            if region_id is not None and id_map is not None:
                if region_id not in id_map:
                    reason = f"{region_id} not in the id map"
                region_id = id_map.get(region_id)
            if region_id is None:
                results.append({"file": str(path), "region_id": None, "status": "skipped", "detail": reason})
            else:
                planned.setdefault(region_id, []).append(path)

        to_upload = []
        for region_id, paths in planned.items():
            if len(paths) > 1:
                for path in paths:
                    results.append({"file": str(path), "region_id": region_id, "status": "skipped",
                                    "detail": f"{len(paths)} files map to this region id"})
            else:
                to_upload.append((region_id, paths[0]))

        if dry_run:
            results.extend({"file": str(p), "region_id": r, "status": "planned", "detail": ""}
                           for r, p in to_upload)
            return results

        if not overwrite:
            existing = {s["region_id"] for s in self._request("GET", "/shapefiles", params={"version": version})}
            remaining = []
            for region_id, path in to_upload:
                if region_id in existing:
                    results.append({"file": str(path), "region_id": region_id, "status": "exists",
                                    "detail": "already in this version"})
                else:
                    remaining.append((region_id, path))
            to_upload = remaining

        local = threading.local()

        def upload(item):
            region_id, path = item
            if not hasattr(local, "session"):
                local.session = requests.Session()
                local.session.headers.update(self.session.headers)
            try:
                self._post_shapefile(local.session, region_id, path, version, overwrite)
                return {"file": str(path), "region_id": region_id, "status": "uploaded", "detail": ""}
            except Exception as e:
                return {"file": str(path), "region_id": region_id, "status": "failed", "detail": str(e)}

        with Progress(SpinnerColumn(), TextColumn("{task.description}"), console=console) as progress:
            task = progress.add_task(f"Uploading 0/{len(to_upload)}", total=len(to_upload))
            with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
                for done, result in enumerate(pool.map(upload, to_upload), start=1):
                    results.append(result)
                    progress.update(task, advance=1, description=f"Uploading {done}/{len(to_upload)}")

        return results


def _region_id_from_geojson_file(path: Path):
    """Returns (region_id, None), or (None, why it can't be mapped)."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as e:
        return None, f"not readable JSON ({e})"
    if not isinstance(data, dict):
        return None, "not a GeoJSON object"
    if data.get("type") == "Feature":
        feature = data
    elif data.get("type") == "FeatureCollection" and len(data.get("features") or []) == 1:
        feature = data["features"][0]
    else:
        return None, "expected one GeoJSON feature per file"
    properties = (feature or {}).get("properties") or {}
    region_type = properties.get("regionType")
    lgd_code = properties.get("lgdCode")
    if not region_type:
        return None, "no regionType property"
    if lgd_code in (None, "", 0):
        return None, "no lgdCode property"
    try:
        lgd_code = int(lgd_code)
    except (TypeError, ValueError):
        return None, f"lgdCode {lgd_code!r} isn't a number"
    return f"{region_type}_{lgd_code}", None


def _raise_for_status_with_detail(response: requests.Response) -> None:
    """raise_for_status, with the API's error detail in the message."""
    if response.ok:
        return
    try:
        detail = response.json().get("detail")
    except ValueError:
        detail = response.text[:300]
    raise requests.HTTPError(f"{response.status_code}: {detail}", response=response)
