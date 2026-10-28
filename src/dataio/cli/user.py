import os
from typing import Annotated

import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

from dataio.sdk.user import DataIOAPI

app = typer.Typer()


@app.command("init")
def init():
    """Initialize the DataIO CLI."""
    console = Console()
    console.print("Welcome to the DataIO CLI! Initializing...", style="bold green")
    console.print(
        "Checking if .env file with API Key and API Base URL exists...",
        style="bold yellow",
    )
    if os.path.exists(".env"):
        load_dotenv()
    if os.getenv("DATAIO_API_KEY") and os.getenv("DATAIO_API_BASE_URL"):
        try:
            _ = DataIOAPI()
        except Exception as e:
            console.print(f"Error initializing DataIO API: {e}", style="bold red")
            console.print(
                "Please check your API Key and API Base URL in your .env file and try again.",
                style="bold red",
            )
            return
        console.print("DataIO CLI initialized successfully!", style="bold green")
        return
    # Get the user's API Key
    api_key = typer.prompt("Enter your API Key", hide_input=True)
    # Get the user's API Base URL
    api_base_url = typer.prompt(
        "Enter your API Base URL",
        default="https://dataio.artpark.ai/api/v1",
        show_default=True,
    )
    # Get the user's Data Directory
    data_dir = typer.prompt(
        "Enter your Data Directory",
        default="data",
        show_default=True,
    )
    # Create a .env file with the API Key and API Base URL
    with open(".env", "w") as f:
        f.write(f"DATAIO_API_KEY={api_key}\n")
        f.write(f"DATAIO_API_BASE_URL={api_base_url}\n")
        f.write(f"DATAIO_DATA_DIR={data_dir}")
    # Initialize the DataIO API
    try:
        _ = DataIOAPI(data_dir=data_dir)
    except Exception as e:
        console.print(f"Error initializing DataIO CLI: {e}", style="bold red")
        console.print(
            "Please check your API Key and API Base URL in your .env file and try again.",
            style="bold red",
        )
        return
    console.print("DataIO CLI initialized successfully!", style="bold green")
    return


@app.command("list-datasets")
def list_datasets(
    limit: Annotated[
        int,
        typer.Option(
            ...,
            help="The number of datasets to list.",
        ),
    ] = 100,
    collection: Annotated[
        str,
        typer.Option(
            "-cl",
            "--collection",
            help="The collection to list datasets from.",
        ),
    ] = None,
    category: Annotated[
        str,
        typer.Option(
            "-cg",
            "--category",
            help="The category to list datasets from.",
        ),
    ] = None,
):
    """List all datasets."""
    console = Console()
    if not os.path.exists(".env"):
        init
    client = DataIOAPI()
    datasets = client.list_datasets(limit=limit)

    table = Table(title="Datasets", show_lines=True)
    table.add_column("ID", justify="right", style="cyan", no_wrap=True)
    table.add_column("Title", style="magenta")
    table.add_column("Description", style="green")
    table.add_column("Data Owner", style="red")

    for dataset in datasets:
        return_dataset = True
        if collection and (
            dataset["collection"]["collection_name"] != collection
            and dataset["collection"]["collection_id"] != collection
        ):
            return_dataset = False
        if category and (
            dataset["collection"]["category_name"] != category
            and dataset["collection"]["category_id"] != category
        ):
            return_dataset = False
        if return_dataset:
            table.add_row(
                dataset["ds_id"],
                dataset["title"],
                dataset["description"],
                dataset["data_owner"]["name"],
            )

    console = Console()
    if not table.rows:
        console.print(
            "No datasets found matching the given criteria Collection: "
            f"{collection} Category: {category}"
        )
    else:
        console.print(table)


@app.command("download-dataset")
def download_dataset(
    dataset_id: Annotated[
        str,
        typer.Argument(
            ...,
            help="The ID of the dataset to download. This can be an integer or a string."
            "String IDs can be either the full dataset ID or the last 4 digits of the dataset ID.",
        ),
    ],
    bucket_type: Annotated[
        str,
        typer.Option(
            "-b",
            "--bucket-type",
            help="The type of bucket to download the dataset from.",
        ),
    ] = "STANDARDISED",
    root_dir: Annotated[
        str,
        typer.Option(
            "-r", "--root-dir", help="The root directory to download the dataset to."
        ),
    ] = None,
    get_metadata: Annotated[
        bool,
        typer.Option(
            "-m",
            "--get-metadata",
            help="Whether to get the metadata for the dataset.",
        ),
    ] = True,
    metadata_format: Annotated[
        str,
        typer.Option(
            "-f", "--metadata-format", help="The format to download the metadata in."
        ),
    ] = "yaml",
):
    """Download a dataset."""
    client = DataIOAPI(data_dir=root_dir)

    args = {k: v for k, v in locals().items() if k != "client"}

    dataset_dir = client.download_dataset(**args)
    console = Console()
    console.print(f"Dataset {dataset_id} downloaded to {dataset_dir}")


@app.command("download-shapefile")
def download_shapefile(
    region_id: Annotated[
        str,
        typer.Argument(..., help="The ID of the region to download the shapefile for."),
    ],
    shp_folder: Annotated[
        str,
        typer.Option(
            "-f",
            "--shp-folder",
            help="Folder inside the data directory to download the shapefile to "
            "(default: GS0012DS0051-Shapefiles_India).",
        ),
    ] = None,
    version: Annotated[
        str,
        typer.Option(
            "-v",
            "--version",
            help="Shapefile version (default: the newest version that has the region). "
            "See list-shapefile-versions.",
        ),
    ] = None,
):
    """Download a shapefile."""
    client = DataIOAPI()
    console = Console()
    try:
        # The SDK puts shp_folder inside the data directory itself.
        shp_path = client.download_shapefile(region_id, shp_folder=shp_folder, version=version)
    except ValueError as e:
        console.print(f"[red]{e}[/]")
        raise typer.Exit(1)
    console.print(f"Shapefile {region_id} downloaded to {shp_path}")


@app.command("list-shapefiles")
def list_shapefiles(
    version: Annotated[
        str,
        typer.Option("-v", "--version", help="Only this version (default: newest version of each region)."),
    ] = None,
    parent_id: Annotated[
        str,
        typer.Option("-p", "--parent-id", help="Only regions with this parent, e.g. state_29."),
    ] = None,
):
    """List available shapefiles."""
    client = DataIOAPI()
    shapefiles = client.get_shapefile_list(version=version, parent_id=parent_id)
    table = Table("Region ID", "Name", "Parent", "Version", "Last modified")
    for s in shapefiles:
        table.add_row(s["region_id"], s["region_name"], s["parent_id"] or "", s["version"],
                      (s["last_modified"] or "")[:10])
    console = Console()
    console.print(table)
    console.print(f"{len(shapefiles)} shapefile(s)")


@app.command("list-shapefile-versions")
def list_shapefile_versions():
    """List shapefile versions, newest first."""
    client = DataIOAPI()
    table = Table("Version", "Released", "Regions", "Dataset", "Source")
    for v in client.get_shapefile_versions():
        table.add_row(v["version"], (v["released_at"] or "")[:10], str(v["regions"]),
                      v["dataset_id"] or "", v["source"])
    Console().print(table)


@app.command("create-shapefile-version")
def create_shapefile_version(
    version: Annotated[str, typer.Argument(..., help="Version name, e.g. kgis-2026-08.")],
    source: Annotated[str, typer.Option("-s", "--source", help="Where the boundaries come from.")],
    dataset_id: Annotated[
        str, typer.Option("-d", "--dataset-id", help="Catalogue ds_id the boundaries are published as.")
    ] = None,
    description: Annotated[str, typer.Option("--description", help="Free-text notes.")] = None,
    released_at: Annotated[
        str,
        typer.Option("--released-at", help="ISO timestamp that orders versions (default: now)."),
    ] = None,
):
    """Create a shapefile version (admin)."""
    from dataio.sdk.admin import DataIOAdminAPI

    console = Console()
    try:
        created = DataIOAdminAPI().create_shapefile_version(
            version, source, dataset_id=dataset_id, description=description, released_at=released_at
        )
    except Exception as e:
        console.print(f"[red]Failed: {e}[/]")
        raise typer.Exit(1)
    console.print(f"[green]Created shapefile version {created['version']}[/] (released {created['released_at']})")


@app.command("upload-shapefile")
def upload_shapefile(
    region_id: Annotated[str, typer.Argument(..., help="Region id, e.g. state_29.")],
    geojson_path: Annotated[str, typer.Argument(..., help="Path to the .geojson file.")],
    version: Annotated[str, typer.Option("-v", "--version", help="Shapefile version to upload into.")],
    overwrite: Annotated[
        bool, typer.Option("--overwrite", help="Replace the region's file if the version already has one.")
    ] = False,
):
    """Upload one region's boundary GeoJSON (admin)."""
    from dataio.sdk.admin import DataIOAdminAPI

    console = Console()
    try:
        result = DataIOAdminAPI().upload_shapefile(region_id, geojson_path, version, overwrite=overwrite)
    except Exception as e:
        console.print(f"[red]Upload failed: {e}[/]")
        raise typer.Exit(1)
    action = "Replaced" if result.get("overwritten") else "Uploaded"
    console.print(f"[green]{action} {region_id}[/] in version {version}")


@app.command("upload-shapefiles")
def upload_shapefiles(
    folder_path: Annotated[str, typer.Argument(..., help="Folder of .geojson files, one region per file.")],
    version: Annotated[str, typer.Option("-v", "--version", help="Shapefile version to upload into.")],
    overwrite: Annotated[
        bool, typer.Option("--overwrite", help="Replace files the version already has.")
    ] = False,
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Only map files to region ids; upload nothing.")
    ] = False,
    workers: Annotated[int, typer.Option("-w", "--workers", help="Parallel uploads.")] = 4,
    report: Annotated[
        str,
        typer.Option("-r", "--report", help="CSV report path (default: shapefile-upload-<version>.csv)."),
    ] = None,
    id_map: Annotated[
        str,
        typer.Option(
            "--id-map",
            help="CSV with source_id,region_id columns mapping <regionType>_<lgdCode> to the region id "
            "(e.g. for wards). Files not in the map are skipped.",
        ),
    ] = None,
):
    """Upload a folder of boundary GeoJSONs into a version (admin).

    Each file's region id is <regionType>_<lgdCode> from its properties, or
    looked up in --id-map. Already-uploaded regions are skipped, so an
    interrupted run can be repeated.
    """
    import csv
    from collections import Counter

    from dataio.sdk.admin import DataIOAdminAPI

    console = Console()
    mapping = None
    if id_map:
        with open(id_map, newline="", encoding="utf-8-sig") as f:
            mapping = {row["source_id"]: row["region_id"] for row in csv.DictReader(f)}
    try:
        results = DataIOAdminAPI().upload_shapefile_folder(
            folder_path, version, overwrite=overwrite, dry_run=dry_run, workers=workers, id_map=mapping
        )
    except Exception as e:
        console.print(f"[red]Upload failed: {e}[/]")
        raise typer.Exit(1)

    report = report or f"shapefile-upload-{version}.csv"
    with open(report, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["file", "region_id", "status", "detail"])
        writer.writeheader()
        writer.writerows(results)

    counts = Counter(r["status"] for r in results)
    table = Table("Status", "Files")
    for status in ("uploaded", "planned", "exists", "skipped", "failed"):
        if counts[status]:
            table.add_row(status, str(counts[status]))
    console.print(table)
    console.print(f"Report: {report}")
    if counts["failed"]:
        raise typer.Exit(1)


@app.command("upload-dataset")
def upload_dataset(
    folder_path: Annotated[
        str,
        typer.Argument(
            ...,
            help="Path to the dataset folder containing info.yml, metadata.yaml, and CSV files.",
        ),
    ],
    bucket_type: Annotated[
        str,
        typer.Option(
            "-b",
            "--bucket-type",
            help="The bucket type to upload to (STANDARDISED or PREPROCESSED).",
        ),
    ] = "STANDARDISED",
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Validate without making API calls.",
        ),
    ] = False,
):
    """Upload a dataset folder to DataIO.

    The folder must contain:
    - info.yml: Dataset-level metadata (title, data_owner_name, etc.)
    - metadata.yaml: Table-level metadata with data_dictionary
    - *.csv: Data files to upload
    """
    from dataio.sdk.admin import DataIOAdminAPI

    console = Console()
    try:
        client = DataIOAdminAPI()
        result = client.upload_dataset_folder(
            folder_path=folder_path,
            bucket_type=bucket_type.upper(),
            dry_run=dry_run,
        )
        if not dry_run:
            console.print(f"\n[bold green]Dataset uploaded successfully![/]")
    except FileNotFoundError as e:
        console.print(f"[red]Error: {e}[/]")
        raise typer.Exit(1)
    except ValueError as e:
        console.print(f"[red]Validation error: {e}[/]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Upload failed: {e}[/]")
        raise typer.Exit(1)


@app.command("upload-all-datasets")
def upload_all_datasets(
    data_dir: Annotated[
        str,
        typer.Option(
            "-d",
            "--data-dir",
            help="Directory containing dataset folders. Defaults to DATAIO_DATA_DIR.",
        ),
    ] = None,
    bucket_type: Annotated[
        str,
        typer.Option(
            "-b",
            "--bucket-type",
            help="The bucket type to upload to (STANDARDISED or PREPROCESSED).",
        ),
    ] = "STANDARDISED",
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Validate without making API calls.",
        ),
    ] = False,
):
    """Upload all dataset folders in a directory to DataIO.

    Scans the data directory for folders matching the ds_id pattern
    (e.g., CS0026DS0111-*) and uploads each one.
    """
    from dataio.sdk.admin import DataIOAdminAPI

    console = Console()
    try:
        client = DataIOAdminAPI()
        results = client.upload_all_datasets(
            data_dir=data_dir,
            bucket_type=bucket_type.upper(),
            dry_run=dry_run,
        )

        # Summary
        successful = sum(1 for r in results if "error" not in r)
        failed = len(results) - successful

        console.print(f"\n[bold]Summary:[/]")
        console.print(f"  Total datasets: {len(results)}")
        console.print(f"  Successful: [green]{successful}[/]")
        if failed:
            console.print(f"  Failed: [red]{failed}[/]")

    except Exception as e:
        console.print(f"[red]Error: {e}[/]")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
