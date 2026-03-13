from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from typing import Any


def load_tabular_rows(source: str | bytes, max_rows: int | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(source, bytes):
        handle = io.StringIO(source.decode("utf-8"))
    else:
        path = Path(source)
        if path.exists():
            handle = path.open("r", encoding="utf-8", newline="")
        else:
            handle = io.StringIO(source)

    with handle:
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
    path = Path(source)
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return json.loads(source)
