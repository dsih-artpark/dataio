"""Loads the curated, deterministic reference tables under
`src/dataio/api/reference_data/` - the region-reorganization history
(region_gap_detector.py) and the canonical cross-dataset enum registries
(field_inference.lookup_canonical_enum_definitions). These are the
deterministic replacement for the LLM drafter's "general/historical
knowledge": static, curator-maintained facts looked up by exact match
instead of recalled/generated per request.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml

_REFERENCE_DATA_DIR = Path(__file__).resolve().parent.parent / "reference_data"


@lru_cache(maxsize=1)
def load_region_history() -> list[dict]:
    """Returns the `events` list from region_history.yaml. Cached since
    it's static reference data re-read on every table profiled otherwise.
    """
    with open(_REFERENCE_DATA_DIR / "region_history.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)["events"]


@lru_cache(maxsize=1)
def load_canonical_enum_registry() -> list[dict]:
    """Returns the `registries` list from canonical_enum_registry.yaml."""
    with open(_REFERENCE_DATA_DIR / "canonical_enum_registry.yaml", encoding="utf-8") as f:
        return yaml.safe_load(f)["registries"]
