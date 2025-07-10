import json
import os
import re
from typing import Optional

import dotenv
import requests
import yaml


class DataIOAPI:
    """API Client for interacting with the DataIO API.

    :param base_url: The base URL of the DataIO API. Defaults to the value of the
        ``DATAIO_API_BASE_URL`` environment variable.
    :type base_url: str
    :param api_key: The API key for the DataIO API. Defaults to the value of the
        ``DATAIO_API_KEY`` environment variable.
    :type api_key: str

    """

    def __init__(self, base_url: Optional[str] = None, api_key: Optional[str] = None):
        dotenv.load_dotenv()
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

    def list_datasets(self, limit=100):
        """
        Get a list of all datasets.

        :param limit: The maximum number of datasets to return. Defaults to 100.
        :type limit: int
        :returns: A list of datasets.
        :rtype: list
        """
        return self._request("GET", f"/datasets?limit={limit}")

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

    def _get_download_links(
        self, dataset_id, bucket_type="STANDARDISED", get_metadata=True
    ):
        """Get download links for a dataset.

        :param dataset_id: The ID of the dataset to get download links for. This is the ``ds_id`` field in the dataset metadata.
        :type dataset_id: str
        :param bucket_type: The type of bucket to get download links for. Defaults to "STANDARDISED". Other option is "PREPROCESSED".
        :type bucket_type: str
        :param get_metadata: Whether to return metadata for each table as a separate dictionary. Defaults to True.
        :type get_metadata: bool
        :returns: A dictionary of download links.
        :rtype: dict
        """
        bucket_type = bucket_type.upper()
        table_list = self.list_dataset_tables(dataset_id, bucket_type)
        table_links = {}
        table_metadata = {}

        for each_table in table_list:
            table_links[each_table["table_name"]] = each_table["download_link"]
            if get_metadata:
                table_metadata[each_table["table_name"]] = each_table["metadata"]

        if get_metadata:
            return table_links, table_metadata
        else:
            return table_links

    def download_dataset(
        self,
        dataset_id,
        bucket_type="STANDARDISED",
        data_dir=".data",
        get_metadata=True,
        metadata_as_yaml=True,
    ):
        """Download a dataset.

        :param dataset_id: The unique identifier of the dataset to download. This is the ``ds_id`` field in the dataset metadata.
        :type dataset_id: str
        :param bucket_type: The type of bucket to download. Defaults to "STANDARDISED". Other option is "PREPROCESSED".
        :type bucket_type: str
        :param data_dir: The directory to download the dataset to. Defaults to ".data".
        :type data_dir: str
        :param get_metadata: Whether to include metadata in the download links. Defaults to True.
        :type get_metadata: bool
        :param metadata_as_yaml: Whether to download the metadata as a YAML file. Defaults to True.
        :type metadata_as_yaml: bool
        :returns: The directory the dataset was downloaded to.
        :rtype: str
        """
        bucket_type = bucket_type.upper()
        download_links, table_metadata = self._get_download_links(
            dataset_id, bucket_type, get_metadata=get_metadata
        )
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)

        ds_details = self.list_datasets()
        ds_details = [
            each_ds for each_ds in ds_details if each_ds["ds_id"] == dataset_id
        ][0]

        ds_title = re.sub(r"_+", "_", re.sub(r"[^a-zA-Z0-9]", "_", ds_details["title"]))
        ds_dir = f"{data_dir}/{dataset_id}-{ds_title}"
        if not os.path.exists(ds_dir):
            os.makedirs(ds_dir)

        for table_name, table_link in download_links.items():
            file_content = self._get_file(table_link)
            with open(f"{ds_dir}/{table_name.replace('-', '_')}.csv", "wb") as f:
                f.write(file_content)

        metadata = {}  # TODO: Use OrderedDict and dump in correct order
        if get_metadata:
            metadata["dataset_title"] = ds_title
            metadata["dataset_description"] = ds_details["description"]
            metadata["category"] = ds_details["collection"]["category_name"]
            metadata["collection"] = ds_details["collection"]["collection_name"]
            metadata["dataset_tables"] = table_metadata

        if get_metadata and metadata_as_yaml:
            with open(f"{ds_dir}/metadata.yaml", "w") as f:
                yaml.dump(metadata, f, indent=4)
            if os.path.exists(f"{ds_dir}/metadata.json"):
                os.remove(f"{ds_dir}/metadata.json")
        elif get_metadata and not metadata_as_yaml:
            with open(f"{ds_dir}/metadata.json", "w") as f:
                json.dump(metadata, f, indent=4)
            if os.path.exists(f"{ds_dir}/metadata.yaml"):
                os.remove(f"{ds_dir}/metadata.yaml")
        elif not get_metadata:
            pass

        return ds_dir
