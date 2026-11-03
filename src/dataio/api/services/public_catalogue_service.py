"""
Public catalogue: dataset discovery, dataset pages and live counts for the
public site. No authentication.

Every dataset is listed here whatever its access level (Open download =
DOWNLOAD, View only = VIEW, Restricted = NONE), but only as metadata: no
download links ever, and for Restricted datasets nothing beyond the catalogue
fields and column names (no README, manifest or data dictionary file).
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterable
from datetime import date
from typing import Any

import yaml
from fastapi import HTTPException
from sqlalchemy import Integer, and_, bindparam, case, cast, func, literal, or_, select, text
from sqlalchemy.orm import aliased, contains_eager, selectinload

from dataio.api.database.config import Session as DBSession
from dataio.api.database.enums import AccessLevel, SpatialResolution, TemporalResolution
from dataio.api.database.models import Collection, DataOwner, Dataset, Region, Tag

logger = logging.getLogger(__name__)

INDIA_REGION_ID = "country_IN"
PUBLIC_ACCESS_LEVELS = (AccessLevel.DOWNLOAD, AccessLevel.VIEW, AccessLevel.NONE)
DATASET_KINDS = ("tabular", "geojson")
SORT_OPTIONS = ("relevance", "updated", "newest", "span", "title")
MAX_PAGE_SIZE = 200
# Words that carry no meaning in a catalogue search ("rainfall data for Odisha").
SEARCH_STOP_WORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "by",
        "data",
        "dataset",
        "datasets",
        "for",
        "from",
        "in",
        "of",
        "on",
        "since",
        "the",
        "to",
        "with",
    }
)
_YEAR_TOKEN = re.compile(r"^(19|20)\d{2}$")

# manifest_json->datasetTables->*->dataDictionary keys, i.e. column names.
# jsonb_each / jsonb_object_keys raise on non-objects, so both levels are guarded.
_COLUMN_NAME_MATCH_SQL = """EXISTS (
    SELECT 1
    FROM jsonb_each(
        CASE WHEN jsonb_typeof(datasets.manifest_json -> 'datasetTables') = 'object'
             THEN datasets.manifest_json -> 'datasetTables' ELSE '{{}}'::jsonb END
    ) AS dataset_table(table_name, table_body)
    CROSS JOIN LATERAL jsonb_object_keys(
        CASE WHEN jsonb_typeof(dataset_table.table_body -> 'dataDictionary') = 'object'
             THEN dataset_table.table_body -> 'dataDictionary' ELSE '{{}}'::jsonb END
    ) AS column_name
    WHERE column_name ILIKE :{param} ESCAPE '\\'
)"""


def _bad_request(detail: str) -> HTTPException:
    return HTTPException(status_code=400, detail=detail)


def _parse_enum_list(values: Iterable[str] | None, enum_cls, label: str) -> list:
    parsed = []
    for raw in values or []:
        for value in str(raw).split(","):
            value = value.strip()
            if not value:
                continue
            try:
                parsed.append(enum_cls(value.upper()))
            except ValueError:
                allowed = ", ".join(member.value for member in enum_cls)
                raise _bad_request(f"Unknown {label} '{value}'. Use one of: {allowed}") from None
    return parsed


def _split_csv(values: Iterable[Any] | None) -> list[str]:
    out: list[str] = []
    for raw in values or []:
        out.extend(part.strip() for part in str(raw).split(",") if part.strip())
    return out


def _parse_int_list(values: Iterable[Any] | None, label: str) -> list[int]:
    try:
        return [int(v) for v in _split_csv(values)]
    except ValueError:
        raise _bad_request(f"{label} must be numbers") from None


def _like_pattern(term: str) -> str:
    escaped = term.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


def search_terms(search: str | None) -> list[str]:
    """Split a search box value into the words every result must match."""
    if not search:
        return []
    words = re.findall(r"[\w\-]+", search.lower())
    terms = [w for w in words if w not in SEARCH_STOP_WORDS and len(w) > 1]
    # A search made only of stop words ("data") still searches for them.
    return terms or [w for w in words if w]


def _iso(value: date | None) -> str | None:
    return value.isoformat() if value else None


def _enum_value(value) -> str | None:
    return value.value if value is not None and hasattr(value, "value") else value


def dataset_kind(dataset: Dataset) -> str:
    kind = (
        (dataset.manifest_json or {}).get("datasetKind")
        if isinstance(dataset.manifest_json, dict)
        else None
    )
    return kind if kind in DATASET_KINDS else "tabular"


def public_columns(dataset: Dataset) -> list[dict]:
    """Tables and their columns, safe to show publicly at any access level.

    Read from the manifest's datasetTables, falling back to the synced
    metadata.json. Only names, descriptions, types and units are returned;
    data-dictionary columns marked ``access: false`` are left out.
    """
    tables: list[dict] = []
    manifest_tables = _manifest_tables(dataset)
    if isinstance(manifest_tables, dict) and manifest_tables:
        for table_name, table in manifest_tables.items():
            table = table if isinstance(table, dict) else {}
            dictionary = (
                table.get("dataDictionary") if isinstance(table.get("dataDictionary"), dict) else {}
            )
            columns = []
            for column_name, field in dictionary.items():
                field = field if isinstance(field, dict) else {}
                if field.get("access") is False:
                    continue
                columns.append(
                    {
                        "name": column_name,
                        "description": field.get("description"),
                        "type": field.get("type"),
                        "unit": field.get("unit"),
                    }
                )
            tables.append(
                {"name": table_name, "description": table.get("description"), "columns": columns}
            )
        return tables

    if not dataset.data_dictionary_json:
        return tables
    try:
        parsed = json.loads(dataset.data_dictionary_json)
    except (TypeError, ValueError):
        return tables
    for table_name, table in ((parsed or {}).get("tables") or {}).items():
        table = table if isinstance(table, dict) else {}
        dictionary = (
            table.get("data_dictionary") if isinstance(table.get("data_dictionary"), dict) else {}
        )
        columns = [
            {
                "name": name,
                "description": (field or {}).get("description"),
                "type": None,
                "unit": None,
            }
            for name, field in dictionary.items()
            if not (isinstance(field, dict) and field.get("access") is False)
        ]
        tables.append(
            {"name": table_name, "description": table.get("description"), "columns": columns}
        )
    return tables


def _manifest_tables(dataset: Dataset) -> dict | None:
    """datasetTables in the order the manifest's author wrote them.

    manifest_json is JSONB, which doesn't keep key order, so tables and
    columns are read from manifest_yaml when it parses, else manifest_json.
    """
    if dataset.manifest_yaml:
        try:
            parsed = yaml.safe_load(dataset.manifest_yaml)
        except yaml.YAMLError:
            parsed = None
        if isinstance(parsed, dict) and isinstance(parsed.get("datasetTables"), dict):
            return parsed["datasetTables"]
    manifest = dataset.manifest_json if isinstance(dataset.manifest_json, dict) else None
    tables = (manifest or {}).get("datasetTables")
    return tables if isinstance(tables, dict) else None


def table_count(dataset: Dataset) -> int:
    """Number of tables the catalogue describes, from the same sources as public_columns."""
    manifest_tables = _manifest_tables(dataset)
    if manifest_tables:
        return len(manifest_tables)
    if not dataset.data_dictionary_json:
        return 0
    try:
        parsed = json.loads(dataset.data_dictionary_json)
    except (TypeError, ValueError):
        return 0
    tables = (parsed or {}).get("tables") if isinstance(parsed, dict) else None
    return len(tables) if isinstance(tables, dict) else 0


def _updated_at(dataset: Dataset) -> str | None:
    value = dataset.manifest_updated_at or dataset.documentation_synced_at
    return value.isoformat() if value else None


def _summary(dataset: Dataset) -> dict:
    collection = dataset.collection
    owner = dataset.data_owner
    region = dataset.spatial_coverage_region
    return {
        "ds_id": dataset.ds_id,
        "title": dataset.title,
        "description": dataset.description,
        "collection_id": collection.id if collection else None,
        "collection_name": collection.collection_name if collection else None,
        "category_id": collection.category_id if collection else None,
        "category_name": collection.category_name if collection else None,
        "data_owner_id": owner.id if owner else None,
        "data_owner_name": owner.name if owner else None,
        "spatial_coverage_region_id": dataset.spatial_coverage_region_id,
        "spatial_coverage_region_name": region.region_name if region else None,
        "spatial_resolution": _enum_value(dataset.spatial_resolution),
        "temporal_coverage_start_date": _iso(dataset.temporal_coverage_start_date),
        "temporal_coverage_end_date": _iso(dataset.temporal_coverage_end_date),
        "temporal_resolution": _enum_value(dataset.temporal_resolution),
        "access_level": _enum_value(dataset.access_level),
        "licence": dataset.licence,
        "dataset_kind": dataset_kind(dataset),
        "tags": sorted(tag.tag_name for tag in (dataset.tags or [])),
        "has_data_dictionary": bool(dataset.manifest_json or dataset.data_dictionary_json),
        "table_count": table_count(dataset),
        "updated_at": _updated_at(dataset),
    }


class PublicCatalogueService:
    """Read-only public catalogue queries."""

    def __init__(self):
        self.logger = logger

    # ------------------------------------------------------------------ list

    def list_datasets(
        self,
        search: str | None = None,
        collection_ids: Iterable[Any] | None = None,
        category_ids: Iterable[str] | None = None,
        region_id: str | Iterable[str] | None = None,
        spatial_resolutions: Iterable[str] | None = None,
        temporal_resolutions: Iterable[str] | None = None,
        year_from: int | None = None,
        year_to: int | None = None,
        access_levels: Iterable[str] | None = None,
        data_owner_ids: Iterable[Any] | None = None,
        dataset_kinds: Iterable[str] | None = None,
        licences: Iterable[str] | None = None,
        sort: str = "relevance",
        limit: int = 20,
        offset: int = 0,
    ) -> dict:
        if limit < 1 or limit > MAX_PAGE_SIZE:
            raise _bad_request(f"limit must be between 1 and {MAX_PAGE_SIZE}")
        if offset < 0:
            raise _bad_request("offset must be 0 or more")
        if sort not in SORT_OPTIONS:
            raise _bad_request(f"Unknown sort '{sort}'. Use one of: {', '.join(SORT_OPTIONS)}")
        if year_from is not None and year_to is not None and year_from > year_to:
            raise _bad_request("year_from must not be after year_to")

        levels = _parse_enum_list(access_levels, AccessLevel, "access level")
        spatial = _parse_enum_list(spatial_resolutions, SpatialResolution, "level of detail")
        temporal = _parse_enum_list(temporal_resolutions, TemporalResolution, "time resolution")
        kinds = [k.lower() for k in _split_csv(dataset_kinds)]
        for kind in kinds:
            if kind not in DATASET_KINDS:
                raise _bad_request(
                    f"Unknown data type '{kind}'. Use one of: {', '.join(DATASET_KINDS)}"
                )
        collection_id_list = _parse_int_list(collection_ids, "collection_id")
        owner_id_list = _parse_int_list(data_owner_ids, "data_owner_id")
        category_list = _split_csv(category_ids)
        licence_list = _split_csv(licences)
        # One region or several (a dataset matches if it is in any of them).
        region_list = _split_csv([region_id] if isinstance(region_id, str) else region_id)
        terms = search_terms(search)

        session = DBSession()
        try:
            query = (
                session.query(Dataset)
                .outerjoin(Dataset.collection)
                .outerjoin(Dataset.data_owner)
                .outerjoin(Dataset.spatial_coverage_region)
                .options(
                    contains_eager(Dataset.collection),
                    contains_eager(Dataset.data_owner),
                    contains_eager(Dataset.spatial_coverage_region),
                    selectinload(Dataset.tags),
                )
                .filter(Dataset.access_level.in_(levels or PUBLIC_ACCESS_LEVELS))
            )

            if collection_id_list:
                query = query.filter(Dataset.collection_id.in_(collection_id_list))
            if category_list:
                query = query.filter(Collection.category_id.in_(category_list))
            if owner_id_list:
                query = query.filter(Dataset.data_owner_id.in_(owner_id_list))
            if spatial:
                query = query.filter(Dataset.spatial_resolution.in_(spatial))
            if temporal:
                query = query.filter(Dataset.temporal_resolution.in_(temporal))
            if licence_list:
                query = query.filter(Dataset.licence.in_(licence_list))
            if kinds:
                kind_expr = func.coalesce(Dataset.manifest_json["datasetKind"].astext, "tabular")
                query = query.filter(kind_expr.in_(kinds))
            if region_list:
                # India means the all-India datasets themselves, not every state inside it.
                places = [r for r in region_list if r != INDIA_REGION_ID]
                region_conditions = []
                if INDIA_REGION_ID in region_list:
                    region_conditions.append(Dataset.spatial_coverage_region_id == INDIA_REGION_ID)
                if places:
                    region_conditions.append(
                        Dataset.spatial_coverage_region_id.in_(self._region_and_descendants(places))
                    )
                query = query.filter(or_(*region_conditions))
            if year_from is not None:
                query = query.filter(
                    Dataset.temporal_coverage_start_date.isnot(None),
                    or_(
                        Dataset.temporal_coverage_end_date.is_(None),
                        Dataset.temporal_coverage_end_date >= date(year_from, 1, 1),
                    ),
                )
            if year_to is not None:
                query = query.filter(
                    Dataset.temporal_coverage_start_date.isnot(None),
                    Dataset.temporal_coverage_start_date <= date(year_to, 12, 31),
                )
            for index, term in enumerate(terms):
                query = query.filter(self._term_condition(term, f"search_term_{index}"))

            total = query.order_by(None).count()
            query = query.order_by(*self._ordering(sort, terms))
            datasets = query.offset(offset).limit(limit).all()

            return {
                "datasets": [_summary(d) for d in datasets],
                "total": total,
                "limit": limit,
                "offset": offset,
                "sort": sort,
            }
        except HTTPException:
            raise
        except Exception as exc:
            self.logger.error("Failed to list public datasets: %s", exc)
            raise HTTPException(status_code=500, detail="Failed to get public datasets") from exc
        finally:
            session.close()

    @staticmethod
    def _region_and_descendants(region_ids: list[str]):
        base = (
            select(Region.region_id)
            .where(Region.region_id.in_(region_ids))
            .cte("region_tree", recursive=True)
        )
        child = aliased(Region)
        tree = base.union_all(
            select(child.region_id).where(child.parent_region_id == base.c.region_id)
        )
        return select(tree.c.region_id)

    @staticmethod
    def _term_condition(term: str, param: str):
        pattern = _like_pattern(term)
        conditions = [
            Dataset.title.ilike(pattern, escape="\\"),
            Dataset.description.ilike(pattern, escape="\\"),
            Dataset.ds_id.ilike(pattern, escape="\\"),
            Collection.collection_name.ilike(pattern, escape="\\"),
            Collection.category_name.ilike(pattern, escape="\\"),
            DataOwner.name.ilike(pattern, escape="\\"),
            Region.region_name.ilike(pattern, escape="\\"),
            Dataset.tags.any(Tag.tag_name.ilike(pattern, escape="\\")),
            text(_COLUMN_NAME_MATCH_SQL.format(param=param)).bindparams(**{param: pattern}),
        ]
        if _YEAR_TOKEN.match(term):
            year = int(term)
            conditions.append(
                and_(
                    Dataset.temporal_coverage_start_date <= date(year, 12, 31),
                    or_(
                        Dataset.temporal_coverage_end_date.is_(None),
                        Dataset.temporal_coverage_end_date >= date(year, 1, 1),
                    ),
                )
            )
        return or_(*conditions)

    @staticmethod
    def _ordering(sort: str, terms: list[str]):
        updated = func.coalesce(Dataset.manifest_updated_at, Dataset.documentation_synced_at)
        title = func.lower(Dataset.title)
        if sort == "title":
            return [title.asc(), Dataset.ds_id.asc()]
        if sort == "updated":
            return [updated.desc().nullslast(), title.asc()]
        if sort == "newest":
            return [Dataset.temporal_coverage_end_date.desc().nullslast(), title.asc()]
        if sort == "span":
            span = Dataset.temporal_coverage_end_date - Dataset.temporal_coverage_start_date
            return [span.desc().nullslast(), title.asc()]
        # relevance: titles matching more of the search words first
        if terms:
            title_hits = sum(
                (
                    case((Dataset.title.ilike(_like_pattern(t), escape="\\"), 1), else_=0)
                    for t in terms
                ),
                literal(0),
            )
            return [title_hits.desc(), updated.desc().nullslast(), title.asc()]
        return [updated.desc().nullslast(), title.asc()]

    # ---------------------------------------------------------------- detail

    def get_dataset(self, dataset_id: str) -> dict:
        session = DBSession()
        try:
            dataset = (
                session.query(Dataset)
                .options(
                    selectinload(Dataset.collection),
                    selectinload(Dataset.data_owner),
                    selectinload(Dataset.spatial_coverage_region),
                    selectinload(Dataset.tags),
                )
                .filter(Dataset.ds_id == dataset_id, Dataset.access_level.in_(PUBLIC_ACCESS_LEVELS))
                .first()
            )
            if not dataset:
                raise HTTPException(status_code=404, detail="Dataset not found")

            detail = _summary(dataset)
            restricted = dataset.access_level == AccessLevel.NONE
            # Older clients read collection/data_owner as nested objects.
            detail["collection"] = (
                {
                    "id": dataset.collection.id,
                    "name": dataset.collection.collection_name,
                    "category": dataset.collection.category_name,
                }
                if dataset.collection
                else None
            )
            detail["data_owner"] = (
                {"id": dataset.data_owner.id, "name": dataset.data_owner.name}
                if dataset.data_owner
                else None
            )
            detail["tables"] = public_columns(dataset)
            detail["can_download"] = False  # download links are never part of this response
            detail["raw_datasets"] = []
            detail["readme_md"] = None if restricted else dataset.readme_md
            detail["data_dictionary_json"] = None if restricted else dataset.data_dictionary_json
            detail["manifest_yaml"] = None if restricted else dataset.manifest_yaml
            detail["manifest_json"] = None if restricted else dataset.manifest_json
            detail["has_manifest"] = (not restricted) and bool(
                dataset.manifest_yaml or dataset.manifest_json
            )
            detail["manifest_updated_at"] = (
                dataset.manifest_updated_at.isoformat() if dataset.manifest_updated_at else None
            )
            detail["manifest_updated_by"] = None if restricted else dataset.manifest_updated_by
            return detail
        except HTTPException:
            raise
        except Exception as exc:
            self.logger.error("Failed to get public dataset %s: %s", dataset_id, exc)
            raise HTTPException(status_code=500, detail="Failed to get dataset") from exc
        finally:
            session.close()

    # ----------------------------------------------------------------- stats

    def list_child_regions(self, parent_id: str) -> dict:
        """Regions directly inside ``parent_id`` (e.g. the districts of state_29), A–Z.

        For the state and district pickers on the public site.
        """
        session = DBSession()
        try:
            rows = (
                session.query(Region.region_id, Region.region_name)
                .filter(Region.parent_region_id == parent_id)
                .order_by(Region.region_name)
                .limit(2000)
                .all()
            )
            return {
                "parent_id": parent_id,
                "regions": [{"region_id": rid, "region_name": name} for rid, name in rows],
            }
        finally:
            session.close()

    def get_stats(self) -> dict:
        """Live counts for the landing page: totals, themes, access, coverage."""
        session = DBSession()
        try:
            listed = Dataset.access_level.in_(PUBLIC_ACCESS_LEVELS)

            total, sources, earliest, latest = (
                session.query(
                    func.count(Dataset.id),
                    func.count(func.distinct(Dataset.data_owner_id)),
                    func.min(
                        cast(func.extract("year", Dataset.temporal_coverage_start_date), Integer)
                    ),
                    func.max(
                        cast(func.extract("year", Dataset.temporal_coverage_end_date), Integer)
                    ),
                )
                .filter(listed)
                .one()
            )

            by_access = {level.value: 0 for level in PUBLIC_ACCESS_LEVELS}
            for level, count in (
                session.query(Dataset.access_level, func.count(Dataset.id))
                .filter(listed)
                .group_by(Dataset.access_level)
            ):
                by_access[_enum_value(level)] = count

            themes = [
                {"category_id": category_id, "category_name": category_name, "count": count}
                for category_id, category_name, count in (
                    session.query(
                        Collection.category_id, Collection.category_name, func.count(Dataset.id)
                    )
                    .join(Dataset, Dataset.collection_id == Collection.id)
                    .filter(listed)
                    .group_by(Collection.category_id, Collection.category_name)
                    .order_by(func.count(Dataset.id).desc(), Collection.category_name)
                )
            ]

            kind_expr = func.coalesce(Dataset.manifest_json["datasetKind"].astext, "tabular")
            by_kind = dict.fromkeys(DATASET_KINDS, 0)
            for kind, count in (
                session.query(kind_expr, func.count(Dataset.id)).filter(listed).group_by(kind_expr)
            ):
                # An unrecognised datasetKind is shown as a table, as on the cards.
                key = kind if kind in by_kind else "tabular"
                by_kind[key] += count

            return {
                "total_datasets": total or 0,
                "total_sources": sources or 0,
                "earliest_year": earliest,
                "latest_year": latest,
                "by_access_level": by_access,
                "by_dataset_kind": by_kind,
                "themes": themes,
                "coverage": self._coverage(session),
            }
        except Exception as exc:
            self.logger.error("Failed to compute public catalogue stats: %s", exc)
            raise HTTPException(status_code=500, detail="Failed to get catalogue stats") from exc
        finally:
            session.close()

    @staticmethod
    def _coverage(session) -> dict:
        """Datasets per state/UT, plus all-India and unknown-coverage counts.

        Each dataset counts once, under the state or UT its coverage region
        sits in (a district dataset counts for its state).
        """
        rows = session.execute(
            text(
                """
                WITH RECURSIVE walk(dataset_id, region_id, parent_id, depth) AS (
                    SELECT d.id, r.region_id, r.parent_region_id, 0
                    FROM datasets d
                    JOIN regions r ON r.region_id = d.spatial_coverage_region_id
                    WHERE d.access_level::text IN :levels
                      AND d.spatial_coverage_region_id <> :india
                    UNION ALL
                    SELECT w.dataset_id, p.region_id, p.parent_region_id, w.depth + 1
                    FROM walk w
                    JOIN regions p ON p.region_id = w.parent_id
                    WHERE w.parent_id IS NOT NULL AND w.parent_id <> :india AND w.depth < 12
                )
                SELECT w.region_id, r.region_name, COUNT(DISTINCT w.dataset_id)
                FROM walk w
                JOIN regions r ON r.region_id = w.region_id
                WHERE w.parent_id = :india
                GROUP BY w.region_id, r.region_name
                ORDER BY COUNT(DISTINCT w.dataset_id) DESC, r.region_name
                """
            ).bindparams(bindparam("levels", expanding=True)),
            {"levels": [level.value for level in PUBLIC_ACCESS_LEVELS], "india": INDIA_REGION_ID},
        ).all()
        listed = Dataset.access_level.in_(PUBLIC_ACCESS_LEVELS)
        national = (
            session.query(func.count(Dataset.id))
            .filter(listed, Dataset.spatial_coverage_region_id == INDIA_REGION_ID)
            .scalar()
        )
        unknown = (
            session.query(func.count(Dataset.id))
            .filter(listed, Dataset.spatial_coverage_region_id.is_(None))
            .scalar()
        )
        return {
            "national": national or 0,
            "unknown": unknown or 0,
            "states": [
                {"region_id": rid, "region_name": name, "count": count} for rid, name, count in rows
            ],
        }
