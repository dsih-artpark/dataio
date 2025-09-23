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
        f.write(f"DATAIO_API_BASE_URL={api_base_url}")
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
            "-f", "--shp-folder", help="The folder to download the shapefile to."
        ),
    ] = None,
):
    """Download a shapefile."""
    client = DataIOAPI()
    if shp_folder is None:
        shp_folder = f"{client.data_dir}/GS0012DS0051-Shapefiles_India"
    else:
        shp_folder = f"{client.data_dir}/{shp_folder}"
    args = {k: v for k, v in locals().items() if k != "client"}
    shp_path = client.download_shapefile(**args)
    console = Console()
    console.print(f"Shapefile {region_id} downloaded to {shp_path}")


if __name__ == "__main__":
    app()
