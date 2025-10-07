# {py:mod}`dataio.sdk.user`

```{py:module} dataio.sdk.user
```

```{autodoc2-docstring} dataio.sdk.user
:allowtitles:
```

## Module Contents

### Classes

````{list-table}
:class: autosummary longtable
:align: left

* - {py:obj}`DatasetList <dataio.sdk.user.DatasetList>`
  -
* - {py:obj}`DataIOAPI <dataio.sdk.user.DataIOAPI>`
  - ```{autodoc2-docstring} dataio.sdk.user.DataIOAPI
    :summary:
    ```
````

### API

`````{py:class} DatasetList()
:canonical: dataio.sdk.user.DatasetList

Bases: {py:obj}`list`

````{py:method} __str__()
:canonical: dataio.sdk.user.DatasetList.__str__

````

`````

`````{py:class} DataIOAPI(base_url: typing.Optional[str] = None, api_key: typing.Optional[str] = None, data_dir: typing.Optional[str] = None)
:canonical: dataio.sdk.user.DataIOAPI

```{autodoc2-docstring} dataio.sdk.user.DataIOAPI
```

```{rubric} Initialization
```

```{autodoc2-docstring} dataio.sdk.user.DataIOAPI.__init__
```

````{py:method} _request(method, endpoint, **kwargs)
:canonical: dataio.sdk.user.DataIOAPI._request

```{autodoc2-docstring} dataio.sdk.user.DataIOAPI._request
```

````

````{py:method} list_datasets(limit=None)
:canonical: dataio.sdk.user.DataIOAPI.list_datasets

```{autodoc2-docstring} dataio.sdk.user.DataIOAPI.list_datasets
```

````

````{py:method} list_dataset_tables(dataset_id, bucket_type='STANDARDISED')
:canonical: dataio.sdk.user.DataIOAPI.list_dataset_tables

```{autodoc2-docstring} dataio.sdk.user.DataIOAPI.list_dataset_tables
```

````

````{py:method} _get_file(url)
:canonical: dataio.sdk.user.DataIOAPI._get_file

```{autodoc2-docstring} dataio.sdk.user.DataIOAPI._get_file
```

````

````{py:method} get_dataset_details(dataset_id: typing.Union[str, int])
:canonical: dataio.sdk.user.DataIOAPI.get_dataset_details

```{autodoc2-docstring} dataio.sdk.user.DataIOAPI.get_dataset_details
```

````

````{py:method} _get_download_links(dataset_id, bucket_type='STANDARDISED')
:canonical: dataio.sdk.user.DataIOAPI._get_download_links

```{autodoc2-docstring} dataio.sdk.user.DataIOAPI._get_download_links
```

````

````{py:method} construct_dataset_metadata(dataset_details: typing.Optional[dict] = None, bucket_type='STANDARDISED')
:canonical: dataio.sdk.user.DataIOAPI.construct_dataset_metadata

```{autodoc2-docstring} dataio.sdk.user.DataIOAPI.construct_dataset_metadata
```

````

````{py:method} download_dataset(dataset_id, bucket_type='STANDARDISED', root_dir=None, get_metadata=True, metadata_format='yaml', update_sync_history=True, sync_history_file='sync-history.yaml')
:canonical: dataio.sdk.user.DataIOAPI.download_dataset

```{autodoc2-docstring} dataio.sdk.user.DataIOAPI.download_dataset
```

````

````{py:method} get_children_regions(region_id: str)
:canonical: dataio.sdk.user.DataIOAPI.get_children_regions

```{autodoc2-docstring} dataio.sdk.user.DataIOAPI.get_children_regions
```

````

````{py:method} get_shapefile_list()
:canonical: dataio.sdk.user.DataIOAPI.get_shapefile_list

```{autodoc2-docstring} dataio.sdk.user.DataIOAPI.get_shapefile_list
```

````

````{py:method} download_shapefile(region_id: str, shp_folder: str = None)
:canonical: dataio.sdk.user.DataIOAPI.download_shapefile

```{autodoc2-docstring} dataio.sdk.user.DataIOAPI.download_shapefile
```

````

`````
