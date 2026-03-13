from __future__ import annotations

import re
from collections.abc import Callable
from datetime import datetime
from typing import Any

from dataio.validate.contracts.models import ManifestField


def _is_empty(value: Any) -> bool:
    return value is None or value == ""


def validate_string(value: Any, _: ManifestField) -> bool:
    return isinstance(value, str)


def validate_boolean(value: Any, _: ManifestField) -> bool:
    return isinstance(value, bool) or str(value).lower() in {"true", "false"}


def validate_int(value: Any, field: ManifestField) -> bool:
    if _is_empty(value):
        return False
    try:
        parsed = int(str(value))
    except ValueError:
        return False
    return _validate_numeric_bounds(float(parsed), field)


def validate_float(value: Any, field: ManifestField) -> bool:
    if _is_empty(value):
        return False
    try:
        parsed = float(str(value))
    except ValueError:
        return False
    return _validate_numeric_bounds(parsed, field)


def validate_enum(value: Any, field: ManifestField) -> bool:
    if field.allowedValues is None:
        return True
    return value in field.allowedValues


def validate_region_id(value: Any, _: ManifestField) -> bool:
    return bool(re.match(r"^[a-z]+_[A-Za-z0-9]+$", str(value)))


def validate_region_name(value: Any, _: ManifestField) -> bool:
    return isinstance(value, str) and bool(value.strip())


def validate_date(value: Any, field: ManifestField) -> bool:
    text = str(value)
    patterns = {
        "YYYY": r"^\d{4}$",
        "YYYY-MM": r"^\d{4}-\d{2}$",
        "YYYY-MM-DD": r"^\d{4}-\d{2}-\d{2}$",
    }
    pattern = patterns.get(field.format or "")
    if pattern is None or re.match(pattern, text) is None:
        return False
    try:
        if field.format == "YYYY":
            datetime.strptime(text, "%Y")
        elif field.format == "YYYY-MM":
            datetime.strptime(text, "%Y-%m")
        elif field.format == "YYYY-MM-DD":
            datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        return False
    return True


def validate_datetime(value: Any, _: ManifestField) -> bool:
    try:
        datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return False
    return True


def _validate_numeric_bounds(value: float, field: ManifestField) -> bool:
    if field.range is not None and len(field.range) == 2:
        return field.range[0] <= value <= field.range[1]
    if field.min is not None and value < field.min:
        return False
    return not (field.max is not None and value > field.max)


TYPE_VALIDATORS: dict[str, Callable[[Any, ManifestField], bool]] = {
    "string": validate_string,
    "boolean": validate_boolean,
    "int": validate_int,
    "float": validate_float,
    "enum": validate_enum,
    "regionID": validate_region_id,
    "regionName": validate_region_name,
    "date": validate_date,
    "dateTime": validate_datetime,
}
