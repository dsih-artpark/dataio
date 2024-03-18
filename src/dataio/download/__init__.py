import os
import warnings
import boto3
from tempfile import NamedTemporaryFile
import requests
import yaml
from typing import Tuple, Dict, Optional, Union
import pkg_resources
import platform


def download_file_from_URI(URI: str, path: str = None, temp: bool = False):
    """Downloads a file from a URI.

    Parameters:
        URI (str): The URI from which to download the file.
        path (str, optional): The path to save the file. If None and temp is False, an error is raised.
        temp (bool, optional): If True, the file will be downloaded temporarily. Default is False.

    Raises:
        ValueError: If path is None and temp is False, or if no extension is present in the URI,
            or if the provided path does not exist, or if the URI is invalid.
    Returns:
        tuple: A tuple containing the full path if successful, or None if not, a boolean indicating success or failure,
            and an Exception object if the download fails. If the download is successful, the third element of the tuple
            will be None.
    """
    # Check if URI is valid
    if not URI.startswith("s3://"):
        raise ValueError("Invalid URI. URI should start with 's3://'.")

    # Find the index of the first "/" after "s3://"
    first_slash_index = URI.find("/", 5)

    # Check if there are characters between "s3://" and the next "/"
    if first_slash_index == -1 or first_slash_index == 5:
        raise ValueError("Invalid URI. URI should contain characters between 's3://' and the subsequent '/'.")

    # Check if there are characters after the third "/"
    if first_slash_index == len(URI) - 1:
        raise ValueError("Invalid URI. URI should contain characters after the subsequent '/'.")

    if path is None:
        if not temp:
            raise ValueError("Either temp must be True or a path must be provided.")
    else:
        if temp:
            warnings.warn("Since path is provided, a temporary directory will not be created.")

        if not os.path.exists(path):
            raise ValueError("The provided path does not exist.")

    # Infer file extension from URI
    parts = URI.split('.')
    if len(parts) < 2 or not parts[-1]:
        raise ValueError("No extension found in the URI.")
    ext = parts[-1]

    # If path is provided and temp is False, append filename to path
    if path is not None:
        filename = URI.split("/")[-1]
        path = os.path.join(path, filename)

    # Create a named temporary file if needed
    if path is None and temp:
        with NamedTemporaryFile(suffix='.' + ext, delete=False) as temp_file:
            path = temp_file.name

    client = boto3.client('s3')
    bucket = URI.split("/")[2]
    key = '/'.join(URI.split("/")[3:])

    try:
        client.download_file(Bucket=bucket, Key=key, Filename=path)
        return path, True, None
    except Exception as e:
        return None, False, e


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
        Tuple[bytes, bytes]: A tuple containing binary content of metadata and data dictionary.

    Raises:
        ValueError: If metadata or data dictionary files are not found for the specified dataset ID.
        TypeError: If dsid is not a string.
    """

    # Validate repo_info dictionary
    if repo_info is not None:
        # Check for unexpected keys in repo_info
        unexpected_keys = set(repo_info.keys()) - {'owner', 'repo', 'branch', 'catalogue_path', 'datadict_fname', 'metadata_fname'}
        if unexpected_keys:
            warnings.warn(f"Ignoring unexpected keys in repo_info: {unexpected_keys}", UserWarning)

        # Check for missing keys in repo_info
        missing_keys = {'owner', 'repo', 'branch', 'catalogue_path', 'datadict_fname', 'metadata_fname'} - set(repo_info.keys())
        if missing_keys and not default:
            warnings.warn(f"Missing keys in repo_info, using default values for: {missing_keys}", UserWarning)
        elif missing_keys and default:
            warnings.warn(f"Missing keys in repo_info, using default values for: {missing_keys}", UserWarning, stacklevel=2)
    # Issue warning if custom values not provided for repo_info and default values are used.
    elif not default:
        warnings.warn("No custom values provided for repo_info, using default values", UserWarning)

    # Validate gh_urls dictionary
    if gh_urls is not None:
        # Check for unexpected keys in gh_urls
        unexpected_keys = set(gh_urls.keys()) - {'api_base_url', 'raw_base_url'}
        if unexpected_keys:
            warnings.warn(f"Ignoring unexpected keys in gh_urls: {unexpected_keys}", UserWarning)

        # Check for missing keys in gh_urls
        missing_keys = {'api_base_url', 'raw_base_url'} - set(gh_urls.keys())
        if missing_keys and not default:
            warnings.warn(f"Missing keys in gh_urls, using default values for: {missing_keys}", UserWarning)
        elif missing_keys and default:
            warnings.warn(f"Missing keys in gh_urls, using default values for: {missing_keys}", UserWarning, stacklevel=2)

    # Issue warning if custom values not provided for gh_urls and default values are used.
    elif not default:
        warnings.warn("No custom values provided for gh_urls, using default values", UserWarning)

    # Set default GitHub URLs if not provided
    gh_urls = gh_urls or {}
    gh_api_base_url = gh_urls.get('api_base_url', "https://api.github.com/repos/")
    gh_raw_base_url = gh_urls.get('raw_base_url', "https://raw.githubusercontent.com/")

    # Set default repository information if not provided
    repo_info = repo_info or {}
    owner = repo_info.get('owner', "dsih-artpark")
    repo = repo_info.get('repo', "data-documentation")
    branch = repo_info.get('branch', "production")
    catalogue_path = repo_info.get('catalogue_path', "info")
    datadict_fname = repo_info.get('datadict_fname', "datadictionary.yaml")
    metadata_fname = repo_info.get('metadata_fname', "metadata.yaml")

    # Construct URL to fetch the tree of files
    tree_url = f"{gh_api_base_url}{owner}/{repo}/git/trees/{branch}?recursive=1"

    # Make request to GitHub tree API endpoint
    response = requests.get(tree_url)

    # Check status code of the response
    if response.status_code == 200:
        tree = response.json().get('tree', [])
    elif response.status_code == 404:
        raise ValueError("Resource not found. Please check if the repository or branch exists.")
    elif response.status_code == 422:
        raise ValueError("Validation failed or the endpoint has been spammed.")
    else:
        raise ValueError("Unknown error occurred while fetching tree data from GitHub.")

    # Construct path prefix based on dataset ID
    dsid_path_prefix = f"{catalogue_path}/{dsid[0:2]}/{dsid}-"

    # Find data dictionary file in the tree
    gh_datadict_path = None
    for file_info in tree:
        if file_info['path'].startswith(dsid_path_prefix) and file_info['path'].endswith(datadict_fname):
            gh_datadict_path = file_info['path']
            break

    # Raise error if data dictionary file not found
    if not gh_datadict_path:
        raise ValueError(f"Data dictionary file not found for dataset ID '{dsid}'.")

    # Construct paths for metadata files
    gh_metadata_path = gh_datadict_path.replace(datadict_fname, metadata_fname)

    # Construct URLs to fetch raw content of metadata and data dictionary files
    gh_raw_metadata_url = f"{gh_raw_base_url}{owner}/{repo}/{branch}/{gh_metadata_path}"
    gh_raw_datadict_url = f"{gh_raw_base_url}{owner}/{repo}/{branch}/{gh_datadict_path}"

    # Retrieve and parse metadata
    raw_metadata_response = requests.get(gh_raw_metadata_url)
    if raw_metadata_response.status_code == 404:
        raise ValueError(f"Metadata file not found for dataset ID '{dsid}'.")
    elif raw_metadata_response.status_code != 200:
        raise ValueError(f"Failed to retrieve metadata for dataset ID '{dsid}'. Request failed.")

    # Retrieve and parse data dictionary
    raw_datadict_response = requests.get(gh_raw_datadict_url)
    if raw_datadict_response.status_code == 404:
        raise ValueError(f"Data dictionary file not found for dataset ID '{dsid}'.")
    elif raw_datadict_response.status_code != 200:
        raise ValueError(f"Failed to retrieve data dictionary for dataset ID '{dsid}'. Request failed.")

    if binary:
        return raw_metadata_response.content, raw_datadict_response.content
    else:
        metadata = yaml.safe_load(raw_metadata_response.content.decode('utf-8'))
        datadict = yaml.safe_load(raw_datadict_response.content.decode('utf-8'))

        return metadata, datadict


def download_dataset_v2(*,
                        dsid: str,
                        data_state: str = "standardised",
                        contains_all: Union[str, list, None] = None,
                        contains_any: Union[str, list, None] = None,
                        suffixes: Union[str, list, None] = None,
                        datadir: str = "data",
                        update=True,
                        clean=False,
                        fetch_docs=False,
                        check_for_expected_files=False,
                        expected_file_list=[None],
                        verbose=False
                        ):
    """
    Downloads files associated with a dataset from an S3 bucket and optionally fetches metadata and data dictionary.

    Parameters:
        dsid (str): Dataset ID.
        data_state (str, optional): State of the dataset. Defaults to "standardised".
        contains_all (str, list, optional): List of substrings that must be present in the file names.
        contains_any (str, list, optional): List of substrings of which at least one must be present in the file names.
        suffixes (str, list, optional): List of file suffixes.
        datadir (str, optional): Directory to download files to. Defaults to "data".
        update (bool, optional): If True, checks for local file modifications and updates them if necessary.
        clean (bool, optional): If True, deletes extraneous files in the datadir.
        fetch_docs (bool, optional): If True, fetches metadata and data dictionary.
        check_for_expected_files (bool, optional): If True, checks for expected files in the datadir.
        expected_file_list (list, optional): List of expected files to check for.
        verbose (bool, optional): If True, prints verbose output.

    Raises:
        ValueError: If no files meet the specified criteria or if the dataset is not found in the S3 bucket.
        TypeError: If dsid or data_state is not a string.

    Returns:
        None
    """

    with open(pkg_resources.resource_filename(__name__, 'settings.yaml'), 'r') as f:
        settings = yaml.safe_load(f)

    if not isinstance(dsid, str):
        raise TypeError("dsid must be a string.")
    if not isinstance(data_state, str):
        raise TypeError("data_state must be a string.")

    Bucket = settings["data_state_buckets"].get(data_state)
    if Bucket is None:
        raise ValueError(f"{data_state} is not a valid data state. Must be one of {str(settings['data_state_buckets'].keys())}")

    # Initialize the S3 client
    client = boto3.client('s3')
    listobjv2_paginator = client.get_paginator('list_objects_v2')

    # Get the common prefixes (folders) from the bucket
    dsid_names = {}
    for prefix in listobjv2_paginator.paginate(Bucket=Bucket, Delimiter='/').search('CommonPrefixes'):
        folder = prefix.get('Prefix')
        dsid_names[folder.split("-")[0]] = folder

    # Determine the prefix for the specified dsid
    dsid_name = dsid_names.get(dsid)
    if dsid_name is None:
        raise ValueError(f"Dataset {dsid} not found in specified state {data_state} on Bucket.")

    # List objects in the dsid prefix
    listobjv2_files = listobjv2_paginator.paginate(Bucket=Bucket, Prefix=dsid_name)

    # Collect files found by iterating through all tranches
    files_found = []
    for tranch in listobjv2_files:
        files_found += [item['Key'] for item in tranch['Contents'] if not item['Key'].endswith("/")]

    # Filter files based on contains_any criteria and build the dictionary
    if contains_any is not None:
        if isinstance(contains_any, str):
            contains_any = [contains_any]
        elif not isinstance(contains_any, list):
            raise TypeError("contains_any must be a string, list, or None.")

        # Initialize dictionary to store files containing each item from contains_any
        files_containing_any = set()
        firstLoop = True
        for item in contains_any:
            files_containing_this_item = [file for file in files_found if item in file]
            if firstLoop:
                files_containing_any = set(files_containing_this_item)
            else:
                if files_containing_any.isdisjoint(files_containing_this_item):
                    files_containing_any.update(files_containing_this_item)
                else:
                    repeats = set.intersection(files_containing_any, files_containing_this_item)
                    raise ValueError(f"A file cannot contain more than one item from the contains_any list: {repeats}")
    else:
        files_containing_any = set(files_found)

    # Filter files based on contains_all criteria
    if contains_all is not None:
        if isinstance(contains_all, str):
            contains_all = [contains_all]
        elif not isinstance(contains_all, list):
            raise TypeError("contains_all must be a string, list, or None.")

        # Initialising set to store files containing all items from contains_all
        files_containing_all = set()
        firstLoop = True
        for item in contains_all:
            files_containing_this_item = set([file for file in files_found if item in file])
            # Set needs to be initialised if
            if firstLoop:
                files_containing_all = files_containing_this_item
                firstLoop = False
            else:
                files_containing_all = files_containing_all.intersection(files_containing_this_item)
    else:
        files_containing_all = set(files_found)

    # Filter files based on suffixes criteria
    if suffixes is not None:
        if isinstance(suffixes, str):
            suffixes = [suffixes]
        elif not isinstance(suffixes, list):
            raise TypeError("suffixes must be a string, list, or None.")

        files_with_suffixes = set()
        first_loop = True
        for suffix in suffixes:
            files_with_this_suffix = set([file for file in files_found if file.endswith(suffix)])
            if first_loop:
                files_with_suffixes = files_with_this_suffix
                first_loop = False
            else:
                files_with_suffixes = files_with_suffixes.intersection(files_with_this_suffix)
    else:
        files_with_suffixes = set(files_found)

    # Get the intersection of files_containing_any, files_containing_all, and files_with_suffixes
    files_to_download = files_containing_any.intersection(files_containing_all, files_with_suffixes)

    # Check if the intersection is empty
    if not files_to_download:
        raise ValueError("No files meet specified criteria.")

    # Check if datadir is a string
    if not isinstance(datadir, str):
        raise ValueError(f"{datadir} is not a string.")

    # Download all files
    for file_path in files_to_download:
        # Construct the full destination path
        destination_path = os.path.join(datadir, file_path)

        if platform.system() != 'Windows' and update:
            warnings.warn("Due to limitations in UNIX systems, update will not check to ensure that you've not" +
                          f"changed files locally. Prune local dir '{datadir}'" +
                          "if you have made changes, or set 'update' to False.", Warning)

        # Check if the file exists locally and if update is enabled
        if update and os.path.exists(destination_path):
            # Get the last modified time of the local file
            local_last_modified_time = os.path.getmtime(destination_path)

            # Get the creation time of the local file
            local_creation_time = os.path.getctime(destination_path)

            # Get the last modified time of the file on S3
            response = client.head_object(Bucket=Bucket, Key=file_path)
            s3_last_modified_time = response['LastModified'].timestamp()

            # Compare the last modified time with the creation time
            if local_last_modified_time > local_creation_time:
                # Attempt to update the file from S3 if it has been modified locally
                client.download_file(Bucket=Bucket, Key=file_path, Filename=destination_path)
                if verbose:
                    print(f"Local file '{destination_path}' has been modified since last download. Redownloading...")

            elif s3_last_modified_time > local_creation_time:
                # Download the file from S3 if it has been updated since download
                client.download_file(Bucket=Bucket, Key=file_path, Filename=destination_path)
                if verbose:
                    print(f"File '{file_path}' has been updated on S3. Redownloading...")
            elif verbose:
                print(f"File '{file_path}' is up to date with S3. Ignoring...")
        else:
            # Create the directory structure if it doesn't exist
            directory = os.path.dirname(destination_path)
            if not os.path.exists(directory):
                os.makedirs(directory)
            # Download the file from S3
            client.download_file(Bucket=Bucket, Key=file_path, Filename=destination_path)
            if verbose:
                print(f"File '{file_path}' has been downloaded from S3.")

    # Prune the folder to remove extraneous elements
    if clean:
        exception_fnames = ["datadictionary.yaml", "metadata.yaml"]
        for i in range(len(exception_fnames)):
            exception_fnames[i] = os.path.join(dsid_name, exception_fnames[i])
        print(exception_fnames)
        datadir_prefix_length = len(os.path.join(datadir)) + 1
        for root, dirs, files in os.walk(os.path.join(datadir, dsid_name)):
            for file in files:
                file_path = os.path.join(root, file)
                file_path_relative = file_path[datadir_prefix_length:]
                if file_path_relative not in files_found and file_path_relative not in exception_fnames:
                    if verbose:
                        warnings.warn(f"Deleting extraneous file: {file_path}")
                    os.remove(file_path)

    # If Requested, fetch all relevant documentation
    if fetch_docs:
        metadata, datadict = fetch_data_documentation(dsid=dsid, default=True, binary=True)

        if metadata is not None:
            metadata_file_path = os.path.join(datadir, dsid_name, "metadata.yaml")

            # Ensure that the directory exists
            os.makedirs(os.path.dirname(metadata_file_path), exist_ok=True)

            # Dump the dictionary to the metadata YAML file
            with open(metadata_file_path, 'wb') as file:
                file.write(metadata)
        if datadict is not None:
            datadict_file_path = os.path.join(datadir, dsid_name, "datadictionary.yaml")

            # Ensure that the directory exists
            os.makedirs(os.path.dirname(datadict_file_path), exist_ok=True)

            # Dump the dictionary to the metadata YAML file
            with open(datadict_file_path, 'wb') as file:
                file.write(datadict)
