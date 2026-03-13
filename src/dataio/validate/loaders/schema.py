from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from dataio.validate.contracts.models import DatasetManifest


def load_manifest(source: str | bytes | dict[str, Any] | DatasetManifest) -> DatasetManifest:
    if isinstance(source, DatasetManifest):
        return source
    if isinstance(source, dict):
        return DatasetManifest.model_validate(source)

    text = source.decode("utf-8") if isinstance(source, bytes) else _read_text(source)
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError:
        raw = json.loads(text)
    if not isinstance(raw, dict):
        raise ValueError("manifest must deserialize to an object")
    return DatasetManifest.model_validate(raw)


def _read_text(source: str) -> str:
    if "\n" in source or "\r" in source:
        return source
    path = Path(source)
    if path.exists():
        return path.read_text(encoding="utf-8")
    return source
