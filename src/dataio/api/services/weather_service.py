import os
import json
from typing import Dict, List, Tuple
from io import BytesIO
import s3fs
import xarray as xr
from shapely.geometry import shape
from fastapi import HTTPException

from dataio.api.services.base_service import BaseService
from dataio.api.models import WeatherDataRequest, WeatherDatasetMetadata, WeatherVariableMetadata


class WeatherService(BaseService):
    """Service for weather data operations."""

    def __init__(self):
        super().__init__()
        self.bucket_name = os.getenv("AWS_BUCKET_NAME")
        self.weather_prefix = "weather"

        # Set up S3 filesystem with credentials from environment
        self.s3 = s3fs.S3FileSystem(
            key=os.getenv("AWS_ACCESS_KEY"),
            secret=os.getenv("AWS_SECRET_ACCESS_KEY"),
        )

    def list_weather_datasets(self) -> List[WeatherDatasetMetadata]:
        """
        List all available weather datasets with metadata.

        Returns:
            List of WeatherDatasetMetadata objects
        """
        try:
            # List all directories under weather/ prefix
            weather_path = f"{self.bucket_name}/{self.weather_prefix}"

            # Get all subdirectories (datasets) under weather/
            try:
                items = self.s3.ls(weather_path)
            except FileNotFoundError:
                self.logger.warning(f"Weather path not found: {weather_path}")
                return []

            datasets = []
            for item in items:
                # Extract dataset name from path
                dataset_name = item.split('/')[-1]

                # Skip empty names
                if not dataset_name:
                    continue

                try:
                    # Open the Zarr store to get metadata
                    s3_zarr_path = f"s3://{item}"
                    metadata = self._get_dataset_metadata(s3_zarr_path, dataset_name)
                    datasets.append(metadata)
                except Exception as e:
                    self.logger.error(f"Failed to get metadata for {dataset_name}: {str(e)}")
                    continue

            return datasets
        except Exception as e:
            self.logger.error(f"Failed to list weather datasets: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Failed to list weather datasets. Contact support."
            )

    def _get_dataset_metadata(
        self, s3_zarr_path: str, dataset_name: str
    ) -> WeatherDatasetMetadata:
        """
        Get metadata for a single weather dataset.

        Args:
            s3_zarr_path: S3 path to Zarr store
            dataset_name: Name of the dataset

        Returns:
            WeatherDatasetMetadata object
        """
        store = s3fs.S3Map(root=s3_zarr_path, s3=self.s3, check=False)

        try:
            ds = xr.open_zarr(store, consolidated=True)
        except Exception:
            ds = xr.open_zarr(store, consolidated=False)

        # Extract variable metadata
        variables = []
        for var_name in ds.data_vars:
            var = ds[var_name]

            # Calculate spatial resolution from coordinates
            spatial_res = None
            if 'latitude' in ds.coords and 'longitude' in ds.coords:
                lat_diff = abs(float(ds.latitude[1] - ds.latitude[0]))
                lon_diff = abs(float(ds.longitude[1] - ds.longitude[0]))
                # Use the average if they differ
                avg_res = (lat_diff + lon_diff) / 2
                spatial_res = f"{avg_res:.4f} degrees"

            # Calculate temporal resolution
            temporal_res = None
            if 'valid_time' in ds.coords and len(ds.valid_time) > 1:
                time_diff = ds.valid_time[1] - ds.valid_time[0]
                # Convert to hours
                hours = float(time_diff) / 1e9 / 3600  # nanoseconds to hours
                if hours == 1:
                    temporal_res = "hourly"
                elif hours == 24:
                    temporal_res = "daily"
                elif hours == 168:
                    temporal_res = "weekly"
                else:
                    temporal_res = f"{hours:.2f} hours"

            variables.append(
                WeatherVariableMetadata(
                    name=var_name,
                    long_name=var.attrs.get('long_name'),
                    units=var.attrs.get('units'),
                    spatial_resolution=spatial_res,
                    temporal_resolution=temporal_res,
                )
            )

        # Extract spatial bounds
        spatial_bounds = {
            "min_lat": float(ds.latitude.min()),
            "max_lat": float(ds.latitude.max()),
            "min_lon": float(ds.longitude.min()),
            "max_lon": float(ds.longitude.max()),
        }

        # Extract temporal coverage
        temporal_coverage_start = str(ds.valid_time.values[0])
        temporal_coverage_end = str(ds.valid_time.values[-1])

        return WeatherDatasetMetadata(
            dataset_name=dataset_name,
            variables=variables,
            temporal_coverage_start=temporal_coverage_start,
            temporal_coverage_end=temporal_coverage_end,
            spatial_bounds=spatial_bounds,
        )

    def _get_bbox_from_geojson(self, geojson_data: Dict) -> Tuple[float, float, float, float]:
        """
        Extract bounding box from a GeoJSON object.

        Args:
            geojson_data: GeoJSON dictionary (Feature or FeatureCollection)

        Returns:
            Tuple of (min_lon, min_lat, max_lon, max_lat)
        """
        # Handle both FeatureCollection and single Feature
        if geojson_data["type"] == "FeatureCollection":
            geometries = [feature["geometry"] for feature in geojson_data["features"]]
        else:
            geometries = [geojson_data["geometry"]]

        # Get bounding box from all geometries
        all_bounds = []
        for geom in geometries:
            shapely_geom = shape(geom)
            all_bounds.append(shapely_geom.bounds)

        # Combine all bounds
        min_lon = min(b[0] for b in all_bounds)
        min_lat = min(b[1] for b in all_bounds)
        max_lon = max(b[2] for b in all_bounds)
        max_lat = max(b[3] for b in all_bounds)

        return min_lon, min_lat, max_lon, max_lat

    def get_weather_data(
        self, dataset_name: str, request: WeatherDataRequest
    ) -> bytes:
        """
        Extract and filter weather data from Zarr store.

        Args:
            dataset_name: Name of the weather dataset
            request: WeatherDataRequest with filtering parameters

        Returns:
            NetCDF file as bytes
        """
        try:
            # Construct S3 path
            s3_zarr_path = f"s3://{self.bucket_name}/{self.weather_prefix}/{dataset_name}"

            # Set up S3 filesystem and open Zarr store
            store = s3fs.S3Map(root=s3_zarr_path, s3=self.s3, check=False)
            ds = xr.open_zarr(store, consolidated=True)

            self.logger.info(f"Opened Zarr store: {s3_zarr_path}")
            self.logger.info(f"Available variables: {list(ds.data_vars)}")

            # Validate variables
            for variable in request.variables:
                if variable not in ds.data_vars:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Variable '{variable}' not found. Available: {list(ds.data_vars)}"
                    )

            # Parse spatial bounds from GeoJSON
            min_lon, min_lat, max_lon, max_lat = self._get_bbox_from_geojson(request.geojson)

            self.logger.info(f"Filtering by bbox: [{min_lon:.2f}, {min_lat:.2f}] to [{max_lon:.2f}, {max_lat:.2f}]")
            self.logger.info(f"Filtering by time: {request.start_date} to {request.end_date}")

            # Filter by time
            ds_filtered = ds.sel(valid_time=slice(request.start_date, request.end_date))

            if len(ds_filtered.valid_time) == 0:
                raise HTTPException(
                    status_code=400,
                    detail=f"No data found for time range {request.start_date} to {request.end_date}"
                )

            # Filter by spatial bounds (note: latitude is descending in ERA5)
            ds_filtered = ds_filtered.sel(
                latitude=slice(max_lat, min_lat),
                longitude=slice(min_lon, max_lon),
            )

            if len(ds_filtered.latitude) == 0 or len(ds_filtered.longitude) == 0:
                raise HTTPException(
                    status_code=400,
                    detail="No data found for the specified spatial bounds"
                )

            # Select only requested variables
            ds_filtered = ds_filtered[request.variables]

            # Calculate estimated size
            total_points = (
                len(ds_filtered.valid_time)
                * len(ds_filtered.latitude)
                * len(ds_filtered.longitude)
                * len(request.variables)
            )
            estimated_size_mb = (total_points * 4) / (1024 * 1024)  # 4 bytes per float32

            self.logger.info(f"Total data points: {total_points:,}")
            self.logger.info(f"Estimated size: {estimated_size_mb:.2f} MB")

            # Check if data is too large (optional limit)
            if estimated_size_mb > 500:  # 500 MB limit
                raise HTTPException(
                    status_code=400,
                    detail=f"Requested data size ({estimated_size_mb:.2f} MB) exceeds limit of 500 MB. Please reduce temporal or spatial range."
                )

            # Load the filtered data
            self.logger.info("Loading filtered data into memory...")
            ds_filtered = ds_filtered.compute()

            # Convert to NetCDF bytes
            buffer = BytesIO()
            ds_filtered.to_netcdf(buffer)
            buffer.seek(0)

            self.logger.info("Data extraction complete!")

            return buffer.getvalue()

        except HTTPException:
            raise
        except Exception as e:
            self.logger.error(f"Failed to get weather data: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to get weather data: {str(e)}"
            )
