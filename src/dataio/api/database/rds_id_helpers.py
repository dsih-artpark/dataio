import re

from dataio.api.database.functions import suggest_next_raw_dataset_id_for_category


def resolve_category_id(meta: dict) -> str:
    """Pull the category prefix (e.g. "CS") out of a metadata.yaml dict.

    Prefers the explicit category.ID field. Falls back to stripping the
    trailing collection sequence number off collection.ID (e.g. "CS0007" ->
    "CS"), matching suggest_next_raw_dataset_id's own fallback when a
    Collection row isn't available.
    """
    category = meta.get("category") or {}
    category_id = str(category.get("ID", "")).strip()
    if category_id:
        return category_id

    collection = meta.get("collection") or {}
    collection_id = str(collection.get("ID", "")).strip()
    return re.sub(r"\d+$", "", collection_id)


def resolve_rds_id(meta: dict) -> str:
    """The single source of truth for raw_dataset.rds_id: the DB-driven
    per-category counter (suggest_next_raw_dataset_id_for_category), not a
    sibling-info.yml scan. See metadata-architecture memo, Limitations ->
    "Two competing raw-dataset-ID generators" for why the old scan is retired.
    """
    category_id = resolve_category_id(meta)
    if not category_id:
        raise ValueError(
            "Cannot resolve rds_id: metadata has no category.ID and no "
            "collection.ID to fall back to."
        )
    return suggest_next_raw_dataset_id_for_category(category_id)
