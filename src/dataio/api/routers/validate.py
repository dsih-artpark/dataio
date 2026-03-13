from __future__ import annotations

import json
import tempfile

from fastapi import APIRouter, File, Form, UploadFile

from dataio.api.services.platform_manifest_validation_service import (
    apply_platform_manifest_checks,
)
from dataio.validate import DataIOValidationService, DatasetKind, ValidationRequest

validate_router = APIRouter(prefix="/api/v1/validate", tags=["validate"])
validation_service = DataIOValidationService(
    platform_manifest_checker=apply_platform_manifest_checks
)


@validate_router.post("")
async def validate_dataset(
    dataset_kind: DatasetKind = Form(...),  # noqa: B008
    manifest_file: UploadFile = File(...),  # noqa: B008
    data: UploadFile | None = File(default=None),  # noqa: B008
    data_files: str | None = Form(default=None),
    deep_check: bool = Form(False),
):
    request = ValidationRequest(
        dataset_kind=dataset_kind,
        manifest_source=await manifest_file.read(),
        data=await data.read() if data is not None else None,
        data_files=json.loads(data_files) if data_files else {},
        deep_check=deep_check,
    )
    return validation_service.validate(request).model_dump()


@validate_router.post("/tabular")
async def validate_tabular(
    manifest_file: UploadFile = File(...),  # noqa: B008
    table_name: str = Form(...),
    table_file: UploadFile = File(...),  # noqa: B008
    deep_check: bool = Form(False),
):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as handle:
        handle.write(await table_file.read())
        csv_path = handle.name

    request = ValidationRequest(
        dataset_kind=DatasetKind.TABULAR,
        manifest_source=await manifest_file.read(),
        data_files={table_name: csv_path},
        deep_check=deep_check,
    )
    return validation_service.validate(request).model_dump()


@validate_router.post("/geojson")
async def validate_geojson(
    manifest_file: UploadFile = File(...),  # noqa: B008
    geojson: UploadFile = File(...),  # noqa: B008
    deep_check: bool = Form(False),
):
    request = ValidationRequest(
        dataset_kind=DatasetKind.GEOJSON,
        manifest_source=await manifest_file.read(),
        data=await geojson.read(),
        deep_check=deep_check,
    )
    return validation_service.validate(request).model_dump()
