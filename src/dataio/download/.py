
import os
import warnings
import boto3
from tempfile import NamedTemporaryFile
import requests
import yaml
from typing import Tuple, Dict, Optional, Union, Set, List
import pkg_resources
import platform
import logging
# Set up logging
# Set up logging
logger = logging.getLogger("dataio.download")
import base64

# Capture warnings and redirect them to the logging system
logging.captureWarnings(True)


def fetch_data_documentation(*, dsid: str,
                             gh_urls: Optional[Dict[str, str]] = None,
                             repo_info: Optional[Dict[str, str]] = None,
                             default: bool = False,
                             binary: bool = False) -> Tuple[Dict, Dict]:
    """
    Fetches metadata and data dictionary for a given dataset ID (DSID) from a GitHub repository.

    Parameters:
        dsid (str): Dataset ID.
        gh_urls (dict, optional): Dictionary containing custom GitHub URLs.
        repo_info (dict, optional): Dictionary containing owner, repo, branch, etc. information.
        default (bool): If True, suppress warnings about missing keys or no input provided.
        binary (bool): If True, returns binary content of metadata and data dictionary.

    Returns:
        Tuple[Dict, Dict]: A tuple containing metadata and data dictionary.

    Raises:
        ValueError: If metadata or data dictionary files are not found for the specified dataset ID.
        TypeError: If dsid is not a string.
    """
    # logger.info(
    #     f"Fetching metadata and data documentation for dataset ID '{dsid}'...")

    # # Validate repo_info dictionary
    # if repo_info is not None:
    #     # Check for unexpected keys in repo_info
    #     unexpected_keys = set(repo_info.keys(
    #     )) - {'owner', 'repo', 'branch', 'catalogue_path', 'datadict_fname', 'metadata_fname'}
    #     if unexpected_keys:
    #         warnings.warn(
    #             f"Ignoring unexpected keys in repo_info: {unexpected_keys}", UserWarning)

    #     # Check for missing keys in repo_info
    #     missing_keys = {'owner', 'repo', 'branch', 'catalogue_path',
    #                     'datadict_fname', 'metadata_fname'} - set(repo_info.keys())
    #     if missing_keys and not default:
    #         warnings.warn(
    #             f"Missing keys in repo_info, using default values for: {missing_keys}", UserWarning)
    #     elif missing_keys and default:
    #         warnings.warn(
    #             f"Missing keys in repo_info, using default values for: {missing_keys}", UserWarning, stacklevel=2)

    # # Issue warning if custom values not provided for repo_info and default values are used.
    # elif not default:
    #     warnings.warn(
    #         "No custom values provided for repo_info, using default values", UserWarning)

    # # Validate gh_urls dictionary
    # if gh_urls is not None:
    #     # Check for unexpected keys in gh_urls
    #     unexpected_keys = set(gh_urls.keys()) - \
    #         {'api_base_url', 'raw_base_url'}
    #     if unexpected_keys:
    #         warnings.warn(
    #             f"Ignoring unexpected keys in gh_urls: {unexpected_keys}", UserWarning)

    #     # Check for missing keys in gh_urls
    #     missing_keys = {'api_base_url', 'raw_base_url'} - set(gh_urls.keys())
    #     if missing_keys and not default:
    #         warnings.warn(
    #             f"Missing keys in gh_urls, using default values for: {missing_keys}", UserWarning)
    #     elif missing_keys and default:
    #         warnings.warn(
    #             f"Missing keys in gh_urls, using default values for: {missing_keys}", UserWarning, stacklevel=2)

    # # Issue warning if custom values not provided for gh_urls and default values are used.
    # elif not default:
    #     warnings.warn(
    #         "No custom values provided for gh_urls, using default values", UserWarning)

    # # Set default GitHub URLs if not provided
    gh_urls = gh_urls or {}
    gh_api_base_url = gh_urls.get('api_base_url', "https://api.github.com/repos/")
    gh_raw_base_url = gh_urls.get('raw_base_url', "https://raw.githubusercontent.com/")

    # Set default repository information if not provided
    repo_info = repo_info or {}
    owner = repo_info.get('owner', "dsih-artpark")
    repo = repo_info.get('repo', "data-documentation")
    branch = repo_info.get('branch', "production")
    catalogue_path = repo_info.get('catalogue_path', "info")
    metadata_fname = repo_info.get('metadata_fname', "metadata.yaml")

    # Set default nesting
    nesting = [catalogue_path, dsid[0:2], dsid, metadata_fname]

    # Construct URL to fetch the tree of files
    url = f"{gh_api_base_url}{owner}/{repo}/git/trees/{branch}"

    # Iterative requests to get first-level nesting
    for nest in nesting:
        response = requests.get(url)
        if response.status_code == 200:
            tree = response.json().get('tree', [])
            for subtree in tree:
                if subtree["path"].startswith(nest):
                    url = subtree["url"]
                    break
            if not url:
                raise ValueError("Key not found in dictionary. Please check the nesting of folders in the catalogue repository")
        elif response.status_code == 404:
            raise ValueError("Resource not found. Please check if the repository or branch exists.")
        elif response.status_code == 422:
            raise ValueError("Validation failed or the endpoint has been spammed.")
        else:
            raise ValueError(f"Error {response.status_code} occurred while fetching tree data from GitHub")
        
    # Retrieve and parse metadata
    raw_metadata_response = requests.get(url)
    if raw_metadata_response.status_code == 404:
        raise ValueError(f"Metadata file not found for dataset ID '{dsid}'.")
    elif raw_metadata_response.status_code != 200:
        raise ValueError(f"Failed to retrieve metadata for dataset ID '{dsid}'. Request failed.")

    if binary:
        return raw_metadata_response.content
    else:
        metadata = yaml.safe_load(raw_metadata_response.content.decode('utf-8'))
        return metadata