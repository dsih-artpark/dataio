from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml
from yaml.nodes import MappingNode, Node, ScalarNode, SequenceNode

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


def build_manifest_source_map(source: str | bytes | dict[str, Any] | DatasetManifest) -> dict[str, tuple[int, int]]:
    if isinstance(source, DatasetManifest) or isinstance(source, dict):
        return {}

    text = source.decode("utf-8") if isinstance(source, bytes) else _read_text(source)
    try:
        root = yaml.compose(text)
    except yaml.YAMLError:
        return {}

    if root is None:
        return {}

    source_map: dict[str, tuple[int, int]] = {}
    _collect_yaml_paths(root, "", source_map)
    return source_map


def _read_text(source: str) -> str:
    if "\n" in source or "\r" in source:
        return source
    path = Path(source)
    if path.exists():
        return path.read_text(encoding="utf-8")
    return source


def _collect_yaml_paths(node: Node, prefix: str, source_map: dict[str, tuple[int, int]]) -> None:
    if isinstance(node, MappingNode):
        for key_node, value_node in node.value:
            if not isinstance(key_node, ScalarNode):
                continue
            key = str(key_node.value)
            path = f"{prefix}.{key}" if prefix else key
            source_map[path] = (key_node.start_mark.line + 1, key_node.start_mark.column + 1)
            _collect_yaml_paths(value_node, path, source_map)
        return

    if isinstance(node, SequenceNode):
        for index, item_node in enumerate(node.value):
            path = f"{prefix}.{index}" if prefix else str(index)
            source_map[path] = (item_node.start_mark.line + 1, item_node.start_mark.column + 1)
            _collect_yaml_paths(item_node, path, source_map)
