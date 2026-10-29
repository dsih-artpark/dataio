"""
Versioned region shapefiles (GeoJSON boundaries, gzipped in S3).

Every file is indexed in the shapefiles table (migration 026), one row per
(region, version). Each upload batch, e.g. a new KGIS release, is a version;
a download without a version gets the newest version that has the region.
The files from before versioning are version 'legacy' and are read-only.
"""

from __future__ import annotations

import gzip
import json
import re

from botocore.exceptions import ClientError
from fastapi import HTTPException, UploadFile

from dataio.api.database import functions as database
from dataio.api.models import ShapefileVersionCreate, User
from dataio.api.services.base_service import BaseService
from dataio.api.services.filestore_service import FilestoreService

LEGACY_VERSION = "legacy"
# Lowercase so a version folder can't be mistaken for a region id
# (shapefiles/<version>/... next to the legacy shapefiles/<parent_id>/...).
VERSION_PATTERN = re.compile(r"^[a-z0-9][a-z0-9.-]{0,63}$")
# Region types can't be version names: the staging bucket already holds files
# at shapefiles/<type>/<region_id>/..., which a version folder would mix with.
RESERVED_VERSIONS = {
    "country", "state", "ut", "district", "subdistrict", "taluk", "village",
    "ward", "ulb", "prabhag", "citycorporation", "zone", "gulb",
}
DATASET_ID_PATTERN = re.compile(r"^[A-Z]{2}\d{4}DS\d{4}$")
GEOMETRY_TYPES = {"Polygon", "MultiPolygon"}
# A date, optionally followed by an RFC 3339 time.
LAST_UPDATED_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}(T.*)?$")


def _first_position(coordinates):
    while isinstance(coordinates, list) and coordinates and isinstance(coordinates[0], list):
        coordinates = coordinates[0]
    return coordinates


def normalise_geojson(content: bytes, region, parent) -> dict:
    """Parses an uploaded GeoJSON Feature or FeatureCollection and returns it as
    a FeatureCollection following the ARTPARK GeoJSON spec: every feature's id
    is the region id, and its properties start with regionName/regionType/
    parentID/parentName from the regions table (names in capitals), followed by
    the source's other properties unchanged. lastUpdated is required, since
    the spec needs every feature to carry a year.

    Raises ValueError when the content isn't usable boundary GeoJSON."""
    try:
        data = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ValueError(f"not valid JSON ({e})") from None
    if not isinstance(data, dict):
        raise ValueError("expected a GeoJSON object")
    if data.get("type") == "Feature":
        features = [data]
    elif data.get("type") == "FeatureCollection" and isinstance(data.get("features"), list):
        features = data["features"]
    else:
        raise ValueError("expected a GeoJSON Feature or FeatureCollection")
    if not features:
        raise ValueError("the FeatureCollection has no features")

    standard = {
        "regionName": region.region_name.upper(),
        "regionType": region.region_id.split("_", 1)[0],
        "parentID": region.parent_region_id,
        "parentName": parent.region_name.upper() if parent is not None else None,
    }
    normalised = []
    for i, feature in enumerate(features):
        geometry = feature.get("geometry") if isinstance(feature, dict) else None
        if not isinstance(geometry, dict) or geometry.get("type") not in GEOMETRY_TYPES:
            raise ValueError(f"feature {i} needs a Polygon or MultiPolygon geometry")
        position = _first_position(geometry.get("coordinates"))
        if not (isinstance(position, list) and len(position) >= 2
                and all(isinstance(c, (int, float)) for c in position[:2])):
            raise ValueError(f"feature {i} has no coordinates")
        # GeoJSON is longitude/latitude (EPSG:4326); projected metres fail this.
        if not (-180 <= position[0] <= 180 and -90 <= position[1] <= 90):
            raise ValueError(
                f"feature {i} coordinates {position[:2]} aren't longitude/latitude "
                "(GeoJSON must be EPSG:4326)"
            )
        properties = feature.get("properties")
        if properties is None:
            properties = {}
        if not isinstance(properties, dict):
            raise ValueError(f"feature {i} properties must be an object")
        last_updated = properties.get("lastUpdated")
        if not (isinstance(last_updated, str) and LAST_UPDATED_PATTERN.match(last_updated)):
            raise ValueError(f"feature {i} needs a lastUpdated date (YYYY-MM-DD)")
        normalised.append({
            "type": "Feature",
            "id": region.region_id,
            "properties": {**standard, **{k: v for k, v in properties.items() if k not in standard}},
            "geometry": geometry,
        })
    return {"type": "FeatureCollection", "features": normalised}


class ShapefileService(BaseService):
    """Lists, serves and uploads versioned shapefiles."""

    def __init__(self):
        super().__init__()
        self.filestore_service = FilestoreService()

    def _require_version(self, version: str):
        row = database.get_shapefile_version(version)
        if row is None:
            raise HTTPException(status_code=404, detail=f"Unknown shapefile version '{version}'")
        return row

    def list_versions(self):
        try:
            return database.list_shapefile_versions()
        except Exception as e:
            self.logger.error(f"Failed to list shapefile versions: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to list shapefile versions. Contact support."
            ) from None

    def list_shapefiles(self, version: str | None = None, parent_id: str | None = None):
        try:
            if version is not None:
                self._require_version(version)
            return database.list_shapefile_records(version=version, parent_id=parent_id)
        except HTTPException:
            raise
        except Exception as e:
            self.logger.error(f"Failed to get shapefiles list: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to get shapefiles list. Contact support."
            ) from None

    def get_shapefile(self, region_id: str, user_email: str, version: str | None = None):
        """Returns (gzipped GeoJSON bytes, version served)."""
        try:
            region = database.get_region(region_id)
            if region is None:
                raise HTTPException(status_code=400, detail="Invalid region id")
            if version is not None:
                self._require_version(version)

            record = database.get_shapefile_record(region_id, version)
            if record is not None:
                key, served_version = record.s3_key, record.version
            elif version in (None, LEGACY_VERSION):
                # Not indexed yet (shapefile-backfill not run): try the legacy key.
                key = FilestoreService.shapefile_key(LEGACY_VERSION, region.parent_region_id, region_id)
                served_version = LEGACY_VERSION
            else:
                raise HTTPException(
                    status_code=404,
                    detail=f"No shapefile for region {region_id} in version '{version}'",
                )

            if database.check_rate_limit_exceeded(user_email, "shapefile"):
                raise HTTPException(
                    status_code=429,
                    detail="You have reached the maximum number of requests. Please try again later.",
                )
            database.update_shapefile_rate_limit(user_email)
            return self.filestore_service.get_shapefile(key), served_version
        except HTTPException as e:
            self.logger.error(f"Failed to get shapefile: {e!s}")
            raise
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                self.logger.error(f"Shapefile not present in S3: {region_id}")
                raise HTTPException(
                    status_code=404,
                    detail="Shapefile not present in our data-store. Please check shapefile list for available shapefiles.",
                ) from None
            self.logger.error(f"Failed to get shapefile: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to get shapefile. Contact support."
            ) from None
        except Exception as e:
            self.logger.error(f"Failed to get shapefile: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to get shapefile. Contact support."
            ) from None

    def create_version(self, payload: ShapefileVersionCreate, user: User):
        if not VERSION_PATTERN.match(payload.version):
            raise HTTPException(
                status_code=400,
                detail="Version must be lowercase letters, digits, '.' or '-' (e.g. 'kgis-2026-08')",
            )
        if payload.version in RESERVED_VERSIONS:
            raise HTTPException(
                status_code=400,
                detail=f"'{payload.version}' is a region type and can't be a version name",
            )
        if not payload.source.strip():
            raise HTTPException(status_code=400, detail="Source is required")
        if payload.dataset_id is not None and not DATASET_ID_PATTERN.match(payload.dataset_id):
            raise HTTPException(status_code=400, detail=f"Invalid dataset id '{payload.dataset_id}'")
        try:
            if database.get_shapefile_version(payload.version) is not None:
                raise HTTPException(
                    status_code=409, detail=f"Shapefile version '{payload.version}' already exists"
                )
            row = database.create_shapefile_version(
                version=payload.version,
                source=payload.source.strip(),
                dataset_id=payload.dataset_id,
                description=payload.description,
                released_at=payload.released_at,
                created_by=user.email,
            )
            return {
                "version": row.version,
                "source": row.source,
                "dataset_id": row.dataset_id,
                "description": row.description,
                "released_at": row.released_at.isoformat() if row.released_at else None,
            }
        except HTTPException:
            raise
        except Exception as e:
            self.logger.error(f"Failed to create shapefile version: {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to create shapefile version. Contact support."
            ) from None

    def upload_shapefile(
        self,
        file: UploadFile,
        region_id: str,
        version: str,
        overwrite: bool,
        user: User,
    ):
        """Validates and stores one region's boundary GeoJSON in a version."""
        try:
            if version == LEGACY_VERSION:
                raise HTTPException(
                    status_code=400,
                    detail="The legacy version is read-only. Create a new version and upload to it.",
                )
            self._require_version(version)
            region = database.get_region(region_id)
            if region is None:
                raise HTTPException(
                    status_code=400,
                    detail=f"Unknown region id '{region_id}'. Add it to the regions table first.",
                )
            existing = database.get_shapefile_record(region_id, version)
            if existing is not None and not overwrite:
                raise HTTPException(
                    status_code=409,
                    detail=f"Version '{version}' already has a shapefile for {region_id}. "
                    "Pass overwrite=true to replace it.",
                )
            parent = database.get_region(region.parent_region_id) if region.parent_region_id else None
            try:
                geojson = normalise_geojson(file.file.read(), region, parent)
            except ValueError as e:
                raise HTTPException(status_code=400, detail=f"Invalid GeoJSON: {e}") from None

            body = gzip.compress(
                json.dumps(geojson, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            )
            key = FilestoreService.shapefile_key(version, region.parent_region_id, region_id)
            self.filestore_service.upload_shapefile(body, key)
            database.save_shapefile_record(region_id, version, key, len(body), user.email)
            return {
                "region_id": region_id,
                "version": version,
                "size_bytes": len(body),
                "overwritten": existing is not None,
            }
        except HTTPException:
            raise
        except Exception as e:
            self.logger.error(f"Failed to upload shapefile {region_id} ({version}): {e!s}")
            raise HTTPException(
                status_code=500, detail="Failed to upload shape file."
            ) from None
