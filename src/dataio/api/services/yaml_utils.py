"""Helpers for YAML parsed with yaml.safe_load."""

from __future__ import annotations

import datetime


def stringify_yaml_dates(value):
    """Turns the date/datetime objects yaml.safe_load produces back into ISO strings.

    yaml.safe_load implicitly parses an unquoted ISO-8601-looking scalar (e.g. a
    temporalCoverage value like 2019-06-30) into a datetime.date/datetime.
    Every manifest field is a plain string everywhere else in the app, and
    neither json.dumps (manifest.json in S3) nor the JSONB columns' serializer
    can write one out, so a manifest containing one fails half-way through
    being persisted. Dict keys are converted too.
    """
    if isinstance(value, dict):
        return {stringify_yaml_dates(k): stringify_yaml_dates(v) for k, v in value.items()}
    if isinstance(value, list):
        return [stringify_yaml_dates(v) for v in value]
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    return value
