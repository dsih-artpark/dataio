from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


def load_tabular_rows(path: str, max_rows: int | None = None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8", newline="") as handle:
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
