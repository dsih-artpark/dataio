import gzip
import json
import os
import re
from datetime import datetime, timezone
from typing import Optional, Union

import dotenv
import requests
import yaml
from tabulate import tabulate


class DatasetList(list):
    def __str__(self):
        return tabulate(self, headers="keys")


class DataIOAPI:
    """API Client for interacting with the DataIO API.

    :param base_url: The base URL of the DataIO API. Defaults to the value of the
        ``DATAIO_API_BASE_URL`` environment variable.
    :type base_url: str
    :param api_key: The API key for the DataIO API. Defaults to the value of the
        ``DATAIO_API_KEY`` environment variable.
    :param data_dir: The directory to download the data to. Defaults to the value of the
        ``DATAIO_DATA_DIR`` environment variable.
    :type data_dir: str
    :type api_key: str

    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        data_dir: Optional[str] = None,
    ):
        dotenv.load_dotenv(override=True)
        if base_url is None:
            base_url = os.getenv("DATAIO_API_BASE_URL", None)
        if base_url is None:
            raise ValueError(
                "DATAIO_API_BASE_URL is neither set in environment variables nor provided as positional argument"
            )
        self.base_url = base_url
        self.session = requests.Session()
        if api_key is None:
            api_key = os.getenv("DATAIO_API_KEY", api_key)
        if api_key is None:
            raise ValueError(
                "DATAIO_API_KEY is neither set in environment variables nor provided as positional argument"
            )
        if api_key:
            self.session.headers.update({"X-API-Key": f"{api_key}"})
        if data_dir is None:
            data_dir = os.getenv("DATAIO_DATA_DIR", "data")
        self.data_dir = data_dir

    def _request(self, method, endpoint, **kwargs):
        """Make a request to the DataIO API.

        :param method: The HTTP method to use.
        :param endpoint: The endpoint to request.
        :param kwargs: Additional keyword arguments to pass to the request.
        """
        url = f"{self.base_url}{endpoint}"
        response = self.session.request(method, url, **kwargs)
        response.raise_for_status()
        return response.json()

    def list_datasets(self, limit=None):
        """
        Get a list of all datasets.

        :param limit: The maximum number of datasets to return. Defaults to None, which returns 100 datasets by default.
        :type limit: int
        :returns: A list of datasets.
        :rtype: list
        """
        if limit is None or limit == 100:
            return DatasetList(self._request("GET", "/datasets"))
        else:
            return DatasetList(self._request("GET", f"/datasets?limit={limit}"))

    def list_dataset_tables(self, dataset_id, bucket_type="STANDARDISED"):
        """Get a list of tables for a given dataset, with download links for each table

        :param dataset_id: The ID of the dataset to get tables for. This is the ``ds_id`` field in the dataset metadata.
        :type dataset_id: str
        :param bucket_type: The type of bucket to get tables for. Defaults to "STANDARDISED". Other option is "PREPROCESSED".
        :type bucket_type: str
        :returns: A list of tables.
        :rtype: list
        """
        bucket_type = bucket_type.upper()
        return self._request("GET", f"/datasets/{dataset_id}/{bucket_type}/tables")

    def _get_file(self, url):
        """Get a file from a URL

        :param url: The URL to get the file from.
        :type url: str
        :returns: The file content.
        :rtype: bytes
        """
        response = requests.get(url)
        response.raise_for_status()
        return response.content

    def get_dataset_details(self, dataset_id: Union[str, int]):
        """Get the details of a dataset - this is the dataset level metadata.

        :param dataset_id: The ID of the dataset to get details for. This is the ``ds_id`` field in the dataset metadata.
        :type dataset_id: str
        :returns: The dataset details.
        """
        dataset_list = self.list_datasets()

        assert isinstance(dataset_id, (str, int)), (
            "dataset_id must be a string or integer"
        )

        if isinstance(dataset_id, int):
            dataset_id = str(dataset_id).zfill(4)
        elif isinstance(dataset_id, str) and len(dataset_id) < 4:
            dataset_id = dataset_id.zfill(4)

        if len(dataset_id) == 4:
            dataset_details = [
                each_ds
                for each_ds in dataset_list
                if each_ds["ds_id"].endswith(dataset_id)
            ]
        else:
            dataset_details = [
                each_ds for each_ds in dataset_list if each_ds["ds_id"] == dataset_id
            ]

        if len(dataset_details) == 0:
            raise ValueError(f"Dataset with ID {dataset_id} not found")
        dataset_details = dataset_details[0]
        return dataset_details

    def _get_download_links(self, dataset_id, bucket_type="STANDARDISED"):
        """Get download links for a dataset.

        :param dataset_id: The ID of the dataset to get download links for. This is the ``ds_id`` field in the dataset metadata.
        :type dataset_id: str
        :param bucket_type: The type of bucket to get download links for. Defaults to "STANDARDISED". Other option is "PREPROCESSED".
        :type bucket_type: str
        :returns: A dictionary of download links.
        :rtype: dict
        """
        bucket_type = bucket_type.upper()
        table_list = self.list_dataset_tables(dataset_id, bucket_type)
        table_links = {}

        for each_table in table_list:
            table_links[each_table["table_name"]] = each_table["download_link"]

        return table_links

    def construct_dataset_metadata(
        self,
        dataset_details: Optional[dict] = None,
        bucket_type="STANDARDISED",
    ):
        """Get the metadata for a dataset. This combines dataset level metadata with table level metadata.

        :param dataset_details: The dataset details. This will be validated for the presence of the following fields: title, description, collection, category_name, collection_name.
        :type dataset_details: dict
        :param bucket_type: The type of bucket to get the table metadata for. Defaults to "STANDARDISED". Other option is "PREPROCESSED".
        :type bucket_type: str
        :returns: The dataset metadata. This includes the dataset title, description, category, collection, and tables with their table-level metadata.
        :rtype: dict
        """
        bucket_type = bucket_type.upper()
        assert dataset_details is not None, "dataset_details must be provided"
        assert isinstance(dataset_details, dict), "dataset_details must be a dictionary"
        assert "title" in dataset_details, "dataset_details must contain a title"
        assert "description" in dataset_details, (
            "dataset_details must contain a description"
        )
        assert "collection" in dataset_details, (
            "dataset_details must contain a collection"
        )
        assert "category_name" in dataset_details["collection"], (
            "dataset_details must contain a category_name"
        )
        assert "collection_name" in dataset_details["collection"], (
            "dataset_details must contain a collection_name"
        )

        table_list = self.list_dataset_tables(dataset_details["ds_id"], bucket_type)
        table_metadata = {
            each_table["table_name"]: each_table["metadata"]
            for each_table in table_list
        }

        metadata = {}
        metadata["dataset_title"] = dataset_details["title"]
        metadata["dataset_description"] = dataset_details["description"]
        metadata["category"] = dataset_details["collection"]["category_name"]
        metadata["collection"] = dataset_details["collection"]["collection_name"]
        metadata["dataset_tables"] = table_metadata

        return metadata

    def download_dataset(
        self,
        dataset_id,
        bucket_type="STANDARDISED",
        root_dir=None,
        get_metadata=True,
        metadata_format="yaml",
        update_sync_history=True,
        sync_history_file="sync-history.yaml",
    ):
        """Download a dataset, along with its metadata.

        :param dataset_id: The unique identifier of the dataset to download. This is the ``ds_id`` field in the dataset metadata.
        :type dataset_id: str
        :param bucket_type: The type of bucket to download. Defaults to "STANDARDISED". Other option is "PREPROCESSED".
        :type bucket_type: str (default: "STANDARDISED")
        :param root_dir: The directory to download the dataset to. Defaults to "data".
        :type root_dir: str (default: "data")
        :param get_metadata: Whether to include metadata in the download links. Defaults to True.
        :type get_metadata: bool (default: True)
        :param metadata_format: The format to download the metadata in. Defaults to "yaml". Other option is "json".
        :type metadata_format: str (default: "yaml")
        :returns: The directory the dataset was downloaded to.
        :rtype: str
        """
        # Set up the dataset directory

        if root_dir is None:
            root_dir = self.data_dir
        bucket_type = bucket_type.upper()
        dataset_details = self.get_dataset_details(dataset_id)
        dataset_id = dataset_details["ds_id"]
        dataset_title = re.sub(
            r"_+", "_", re.sub(r"[^a-zA-Z0-9]", "_", dataset_details["title"])
        )
        dataset_dir = f"{root_dir}/{dataset_id}-{dataset_title}"
        os.makedirs(dataset_dir, exist_ok=True)

        # Get the download links for the dataset
        download_links = self._get_download_links(dataset_id, bucket_type)

        for table_name, table_link in download_links.items():
            file_content = self._get_file(table_link)
            with open(f"{dataset_dir}/{table_name}.csv", "wb") as f:
                f.write(file_content)

        if get_metadata:
            metadata = self.construct_dataset_metadata(dataset_details, bucket_type)
            if metadata_format.lower() == "yaml":
                with open(f"{dataset_dir}/metadata.yaml", "w") as f:
                    yaml.dump(metadata, f, indent=4)
            elif metadata_format.lower() == "json":
                with open(f"{dataset_dir}/metadata.json", "w") as f:
                    json.dump(metadata, f, indent=4)
            else:
                raise ValueError(
                    f"Invalid metadata format: {metadata_format.lower()}. Valid options are 'yaml' and 'json'."
                )

        if update_sync_history:
            sync_history_file = f"{root_dir}/{sync_history_file}"
            if not os.path.exists(sync_history_file):
                sync_history = {}
            else:
                sync_history = yaml.safe_load(open(sync_history_file, "r"))
            sync_history[dataset_id] = {
                "dataset_title": dataset_details["title"],
                "downloaded_at": datetime.now(timezone.utc).strftime(
                    "%Y-%m-%d %H:%M:%S UTC"
                ),
            }
            with open(sync_history_file, "w") as f:
                yaml.dump(sync_history, f, indent=4)

        return dataset_dir

    def get_children_regions(self, region_id: str):
        """Get all direct children regions for a given parent region.

        :param region_id: The ID of the parent region to get children for.
        :type region_id: str
        :returns: A list of child regions with their metadata.
        :rtype: list
        """
        return self._request("GET", f"/regions/{region_id}/children")

    def get_shapefile_list(self):
        """Get a list of all shapefiles.

        :returns: A list of shapefiles.
        :rtype: list
        """
        return self._request("GET", "/shapefiles")

    def download_shapefile(self, region_id: str, shp_folder: str = None):
        """Download a shapefile.

        :param region_id: The ID of the region to download the shapefile for.
        :type region_id: str
        :param shp_folder: The folder with the data directory to download the shapefile to. Defaults to "{data_dir}/GS0012DS0051-Shapefiles_India", where data_dir is derived from the API client.
        :type shp_folder: str
        :param compress: Whether to compress the shapefile. Defaults to True.
        :type compress: bool
        :returns: The shapefile.
        :rtype: bytes
        """

        if shp_folder is None:
            shp_folder = f"{self.data_dir}/GS0012DS0051-Shapefiles_India"
        else:
            shp_folder = f"{self.data_dir}/{shp_folder}"
        shapefile_list = self.get_shapefile_list()
        shapefile_exists = any(
            [
                True
                for each_shapefile in shapefile_list
                if each_shapefile["region_id"] == region_id
            ]
        )
        if not shapefile_exists:
            raise ValueError(f"Shapefile for region {region_id} not found")

        url = f"{self.base_url}/shapefiles/{region_id}"
        response = self.session.request("GET", url)
        response.raise_for_status()
        shapefile = response.content

        shapefile = json.loads(gzip.decompress(shapefile).decode("utf-8"))

        shp_path = f"{shp_folder}/{region_id}.geojson"
        os.makedirs(shp_folder, exist_ok=True)
        with open(shp_path, "w", encoding="utf-8") as f:
            json.dump(shapefile, f, indent=4)

        return shp_path
