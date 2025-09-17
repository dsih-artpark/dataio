# {py:mod}`dataio.cli.user`

```{py:module} dataio.cli.user
```

```{autodoc2-docstring} dataio.cli.user
:allowtitles:
```

## Module Contents

### Functions

````{list-table}
:class: autosummary longtable
:align: left

* - {py:obj}`init <dataio.cli.user.init>`
  - ```{autodoc2-docstring} dataio.cli.user.init
    :summary:
    ```
* - {py:obj}`list_datasets <dataio.cli.user.list_datasets>`
  - ```{autodoc2-docstring} dataio.cli.user.list_datasets
    :summary:
    ```
* - {py:obj}`download_dataset <dataio.cli.user.download_dataset>`
  - ```{autodoc2-docstring} dataio.cli.user.download_dataset
    :summary:
    ```
* - {py:obj}`download_shapefile <dataio.cli.user.download_shapefile>`
  - ```{autodoc2-docstring} dataio.cli.user.download_shapefile
    :summary:
    ```
````

### Data

````{list-table}
:class: autosummary longtable
:align: left

* - {py:obj}`app <dataio.cli.user.app>`
  - ```{autodoc2-docstring} dataio.cli.user.app
    :summary:
    ```
````

### API

````{py:data} app
:canonical: dataio.cli.user.app
:value: >
   'Typer(...)'

```{autodoc2-docstring} dataio.cli.user.app
```

````

````{py:function} init()
:canonical: dataio.cli.user.init

```{autodoc2-docstring} dataio.cli.user.init
```
````

````{py:function} list_datasets(limit: typing.Annotated[int, typer.Option(..., help='The number of datasets to list.')] = 100, collection: typing.Annotated[str, typer.Option('-cl', '--collection', help='The collection to list datasets from.')] = None, category: typing.Annotated[str, typer.Option('-cg', '--category', help='The category to list datasets from.')] = None)
:canonical: dataio.cli.user.list_datasets

```{autodoc2-docstring} dataio.cli.user.list_datasets
```
````

````{py:function} download_dataset(dataset_id: typing.Annotated[str, typer.Argument(..., help='The ID of the dataset to download. This can be an integer or a string.String IDs can be either the full dataset ID or the last 4 digits of the dataset ID.')], bucket_type: typing.Annotated[str, typer.Option('-b', '--bucket-type', help='The type of bucket to download the dataset from.')] = 'STANDARDISED', root_dir: typing.Annotated[str, typer.Option('-r', '--root-dir', help='The root directory to download the dataset to.')] = 'data', get_metadata: typing.Annotated[bool, typer.Option('-m', '--get-metadata', help='Whether to get the metadata for the dataset.')] = True, metadata_format: typing.Annotated[str, typer.Option('-f', '--metadata-format', help='The format to download the metadata in.')] = 'yaml')
:canonical: dataio.cli.user.download_dataset

```{autodoc2-docstring} dataio.cli.user.download_dataset
```
````

````{py:function} download_shapefile(region_id: typing.Annotated[str, typer.Argument(..., help='The ID of the region to download the shapefile for.')], shp_folder: typing.Annotated[str, typer.Option('-f', '--shp-folder', help='The folder to download the shapefile to.')] = 'data/GS0012DS0051-Shapefiles_India')
:canonical: dataio.cli.user.download_shapefile

```{autodoc2-docstring} dataio.cli.user.download_shapefile
```
````
