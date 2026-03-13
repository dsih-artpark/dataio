from __future__ import annotations

import json
import os
from pathlib import Path

import dotenv
import requests

from dataio.validate.contracts.models import DatasetKind, ValidationRequest
from dataio.validate.reports.models import ValidationResult
from dataio.validate.service import DataIOValidationService


class DataIOValidator:
    def __init__(
        self,
        *,
        api_base_url: str | None = None,
        api_key: str | None = None,
        timeout: int = 30,
    ):
        dotenv.load_dotenv(override=True)
        self.service = DataIOValidationService()
        self.api_base_url = api_base_url or os.getenv("DATAIO_API_BASE_URL")
        self.timeout = timeout
        self.session = requests.Session()
        if api_key or os.getenv("DATAIO_API_KEY"):
            self.session.headers.update({"X-API-Key": api_key or os.getenv("DATAIO_API_KEY", "")})

    def validate_tabular(
        self,
        *,
        manifest: str | bytes | dict,
        data_files: dict[str, str],
        deep_check: bool = False,
        full_scan: bool = True,
        max_rows: int | None = None,
        extra_column_policy: str = "warn",
    ) -> ValidationResult:
        if deep_check:
            return self._validate_tabular_via_api(
                manifest=manifest,
                data_files=data_files,
                deep_check=deep_check,
            )
        request = ValidationRequest(
            dataset_kind=DatasetKind.TABULAR,
            manifest_source=manifest,
            data_files=data_files,
            deep_check=deep_check,
            full_scan=full_scan,
            max_rows=max_rows,
            extra_column_policy=extra_column_policy,
        )
        return self.service.validate(request)

    def validate_geojson(
        self,
        *,
        manifest: str | bytes | dict,
        data: str | bytes | dict,
        deep_check: bool = False,
    ) -> ValidationResult:
        if deep_check:
            return self._validate_geojson_via_api(
                manifest=manifest,
                data=data,
                deep_check=deep_check,
            )
        request = ValidationRequest(
            dataset_kind=DatasetKind.GEOJSON,
            manifest_source=manifest,
            data=data,
            deep_check=deep_check,
        )
        return self.service.validate(request)

    def _require_api_base_url(self) -> str:
        if not self.api_base_url:
            raise ValueError(
                "deep_check requires API access. Set DATAIO_API_BASE_URL or pass api_base_url="
            )
        return self.api_base_url.rstrip("/")

    def _normalize_manifest_bytes(self, manifest: str | bytes | dict) -> bytes:
        if isinstance(manifest, bytes):
            return manifest
        if isinstance(manifest, str):
            try:
                path = Path(manifest)
                if path.exists():
                    return path.read_bytes()
            except OSError:
                pass
            return manifest.encode("utf-8")
        return json.dumps(manifest).encode("utf-8")

    def _normalize_tabular_payload(self, data_files: dict[str, str]) -> dict[str, str]:
        normalized: dict[str, str] = {}
        for table_name, source in data_files.items():
            try:
                path = Path(source)
                if path.exists():
                    normalized[table_name] = path.read_text(encoding="utf-8")
                    continue
            except OSError:
                pass
            normalized[table_name] = source
        return normalized

    def _normalize_geojson_bytes(self, data: str | bytes | dict) -> bytes:
        if isinstance(data, bytes):
            return data
        if isinstance(data, str):
            try:
                path = Path(data)
                if path.exists():
                    return path.read_bytes()
            except OSError:
                pass
            return data.encode("utf-8")
        return json.dumps(data).encode("utf-8")

    def _validate_tabular_via_api(
        self,
        *,
        manifest: str | bytes | dict,
        data_files: dict[str, str],
        deep_check: bool,
    ) -> ValidationResult:
        base_url = self._require_api_base_url()
        response = self.session.post(
            f"{base_url}/validate",
            files={
                "manifest_file": (
                    "manifest.yaml",
                    self._normalize_manifest_bytes(manifest),
                    "application/x-yaml",
                ),
            },
            data={
                "dataset_kind": DatasetKind.TABULAR.value,
                "data_files": json.dumps(self._normalize_tabular_payload(data_files)),
                "deep_check": json.dumps(deep_check),
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        return ValidationResult.model_validate(response.json())

    def _validate_geojson_via_api(
        self,
        *,
        manifest: str | bytes | dict,
        data: str | bytes | dict,
        deep_check: bool,
    ) -> ValidationResult:
        base_url = self._require_api_base_url()
        response = self.session.post(
            f"{base_url}/validate/geojson",
            files={
                "manifest_file": (
                    "manifest.yaml",
                    self._normalize_manifest_bytes(manifest),
                    "application/x-yaml",
                ),
                "geojson": (
                    "data.geojson",
                    self._normalize_geojson_bytes(data),
                    "application/geo+json",
                ),
            },
            data={"deep_check": json.dumps(deep_check)},
            timeout=self.timeout,
        )
        response.raise_for_status()
        return ValidationResult.model_validate(response.json())
