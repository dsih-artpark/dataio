from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from typing import Any


def _looks_like_existing_path(source: str) -> bool:
    try:
        return Path(source).exists()
    except OSError:
        # Inline payloads can be much longer than filesystem path limits.
        return False


def load_tabular_rows(source: str | bytes, max_rows: int | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(source, bytes):
        with io.StringIO(source.decode("utf-8")) as handle:
            reader = csv.DictReader(handle)
            for index, row in enumerate(reader):
                rows.append(dict(row))
                if max_rows is not None and index + 1 >= max_rows:
                    break
        return rows

    if _looks_like_existing_path(source):
        with Path(source).open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for index, row in enumerate(reader):
                rows.append(dict(row))
                if max_rows is not None and index + 1 >= max_rows:
                    break
    else:
        with io.StringIO(source) as handle:
            reader = csv.DictReader(handle)
            for index, row in enumerate(reader):
                rows.append(dict(row))
                if max_rows is not None and index + 1 >= max_rows:
                    break
    return rows


def load_geojson_data(source: str | bytes | dict[str, Any]) -> dict[str, Any]:
    if isinstance(source, dict):
        return source
    if isinstance(source, bytes):
        return json.loads(source.decode("utf-8"))
    if _looks_like_existing_path(source):
        return json.loads(Path(source).read_text(encoding="utf-8"))
    return json.loads(source)
