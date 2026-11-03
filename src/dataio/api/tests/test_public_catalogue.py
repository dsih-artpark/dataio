"""Public catalogue (/public/datasets, /public/datasets/{id}, /public/stats)
against a real PostgreSQL database.

The filters, search and coverage counts are SQL (recursive CTEs, JSONB,
ILIKE), so they are tested on Postgres rather than with query stubs; see
postgres_schema.py for the DATAIO_TEST_DATABASE_URL these tests need.
"""

from __future__ import annotations

import importlib
import json
import os
from datetime import date, datetime

import pytest
import yaml
from fastapi.testclient import TestClient

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

from sqlalchemy.orm import sessionmaker

from dataio.api.database.enums import AccessLevel, SpatialResolution, TemporalResolution
from dataio.api.database.models import Collection, DataOwner, Dataset, Region, Tag
from dataio.api.services import public_catalogue_service as catalogue_module
from dataio.api.services.public_catalogue_service import PublicCatalogueService, search_terms
from dataio.api.tests.postgres_schema import fresh_schema_engine, requires_postgres

pytestmark = requires_postgres

app = importlib.import_module("dataio.api").app
client = TestClient(app)


def _manifest(kind="tabular", columns=()):
    return {
        "datasetKind": kind,
        "datasetTables": {
            "main": {
                "description": "Main table",
                "dataDictionary": {
                    name: {"type": col_type, "description": f"{name} description", **extra}
                    for name, col_type, extra in columns
                },
            }
        },
    }


@pytest.fixture(scope="module")
def session_factory():
    engine = fresh_schema_engine()
    factory = sessionmaker(bind=engine)
    _seed(factory)
    yield factory
    engine.dispose()


@pytest.fixture(autouse=True)
def use_test_database(session_factory, monkeypatch):
    monkeypatch.setattr(catalogue_module, "DBSession", session_factory)


def _seed(factory):
    session = factory()
    session.add_all(
        [
            Region(region_id="country_IN", region_name="India", parent_region_id=None),
            Region(region_id="state_29", region_name="Karnataka", parent_region_id="country_IN"),
            Region(
                region_id="district_525", region_name="Bengaluru Urban", parent_region_id="state_29"
            ),
            Region(
                region_id="subdistrict_9001", region_name="Anekal", parent_region_id="district_525"
            ),
            Region(region_id="state_27", region_name="Maharashtra", parent_region_id="country_IN"),
            Region(region_id="state_21", region_name="Odisha", parent_region_id="country_IN"),
        ]
    )
    epi = Collection(
        collection_id="EP0001",
        collection_name="Dengue surveillance",
        category_id="EP",
        category_name="Disease surveillance",
    )
    census = Collection(
        collection_id="CS0001",
        collection_name="Livestock census",
        category_id="CS",
        category_name="Census & surveys",
    )
    survey = Collection(
        collection_id="CS0002",
        collection_name="Household surveys",
        category_id="CS",
        category_name="Census & surveys",
    )
    geo = Collection(
        collection_id="GS0001",
        collection_name="Boundaries",
        category_id="GS",
        category_name="Geospatial",
    )
    weather = Collection(
        collection_id="WE0001",
        collection_name="Rainfall",
        category_id="WE",
        category_name="Weather & climate",
    )
    nvbdcp = DataOwner(name="National Center for Vector Borne Diseases Control")
    dahd = DataOwner(name="Department of Animal Husbandry and Dairying")
    imd = DataOwner(name="India Meteorological Department")
    session.add_all([epi, census, survey, geo, weather, nvbdcp, dahd, imd])
    session.flush()

    def tag(name):
        existing = session.query(Tag).filter(Tag.tag_name == name).first()
        if existing:
            return existing
        new = Tag(tag_name=name)
        session.add(new)
        session.flush()
        return new

    datasets = [
        Dataset(
            ds_id="EP0001DS0001",
            title="Dengue cases by district",
            description="Monthly confirmed dengue cases reported by district health offices.",
            collection_id=epi.id,
            data_owner_id=nvbdcp.id,
            spatial_coverage_region_id="state_29",
            spatial_resolution=SpatialResolution.DISTRICT,
            temporal_coverage_start_date=date(2015, 1, 1),
            temporal_coverage_end_date=date(2023, 12, 31),
            temporal_resolution=TemporalResolution.MONTH,
            access_level=AccessLevel.DOWNLOAD,
            licence="CC BY 4.0",
            manifest_json=_manifest(
                columns=[
                    ("lgd_district_code", "integer", {}),
                    ("dengue_cases", "integer", {"unit": "cases"}),
                ]
            ),
            # same manifest as YAML: the column order to show (JSONB reorders keys)
            manifest_yaml=yaml.safe_dump(
                _manifest(
                    columns=[
                        ("lgd_district_code", "integer", {}),
                        ("dengue_cases", "integer", {"unit": "cases"}),
                    ]
                ),
                sort_keys=False,
            ),
            manifest_updated_at=datetime(2026, 10, 1),
            tags=[tag("dengue"), tag("vector borne")],
        ),
        Dataset(
            ds_id="CS0001DS0002",
            title="Livestock census 2019",
            description="Animal counts by species and breed.",
            collection_id=census.id,
            data_owner_id=dahd.id,
            spatial_coverage_region_id="country_IN",
            spatial_resolution=SpatialResolution.DISTRICT,
            temporal_coverage_start_date=date(2019, 1, 1),
            temporal_coverage_end_date=date(2019, 12, 31),
            temporal_resolution=TemporalResolution.YEAR,
            access_level=AccessLevel.VIEW,
            readme_md="# Livestock census",
            manifest_updated_at=datetime(2026, 9, 1),
            tags=[tag("cattle"), tag("animal husbandry")],
        ),
        Dataset(
            ds_id="CS0002DS0003",
            title="Bengaluru ward health survey",
            description="Household health survey by ward.",
            collection_id=survey.id,
            data_owner_id=nvbdcp.id,
            spatial_coverage_region_id="subdistrict_9001",
            spatial_resolution=SpatialResolution.WARD,
            temporal_coverage_start_date=date(2021, 1, 1),
            temporal_coverage_end_date=date(2022, 12, 31),
            temporal_resolution=TemporalResolution.NONE,
            access_level=AccessLevel.NONE,
            readme_md="internal notes: survey contact sheet in drive",
            manifest_yaml="datasetTitle: secret",
            data_dictionary_json=json.dumps(
                {
                    "tables": {
                        "responses": {
                            "description": "One row per household",
                            "data_dictionary": {
                                "respondent_age": {"description": "Age in years", "access": True},
                                "phone_number": {"description": "Contact number", "access": False},
                            },
                        }
                    }
                }
            ),
            manifest_updated_by="curator@example.org",
        ),
        Dataset(
            ds_id="GS0001DS0004",
            title="District boundaries",
            description="Administrative boundaries.",
            collection_id=geo.id,
            data_owner_id=imd.id,
            spatial_coverage_region_id=None,
            spatial_resolution=SpatialResolution.DISTRICT,
            temporal_resolution=TemporalResolution.NONE,
            access_level=AccessLevel.DOWNLOAD,
            manifest_json={"datasetKind": "geojson"},
        ),
        Dataset(
            ds_id="WE0001DS0005",
            title="Daily rainfall",
            description="Gridded rainfall aggregated to districts.",
            collection_id=weather.id,
            data_owner_id=imd.id,
            spatial_coverage_region_id="state_21",
            spatial_resolution=SpatialResolution.DISTRICT,
            temporal_coverage_start_date=date(1990, 1, 1),
            temporal_coverage_end_date=date(2020, 12, 31),
            temporal_resolution=TemporalResolution.DATE,
            access_level=AccessLevel.DOWNLOAD,
            licence="GODL-India",
            documentation_synced_at=datetime(2026, 10, 20),
            tags=[tag("rainfall")],
        ),
    ]
    # One insert per row: SQLAlchemy's multi-row INSERT casts enum parameters
    # to its own type names (spatialresolution), which the real schema doesn't use.
    for dataset in datasets:
        session.add(dataset)
        session.flush()
    session.commit()
    session.close()


def ids(response: dict) -> list[str]:
    return [d["ds_id"] for d in response["datasets"]]


service = PublicCatalogueService()


# ---------------------------------------------------------------- listing


def test_lists_every_access_level_including_restricted():
    result = service.list_datasets(sort="title", limit=50)
    assert result["total"] == 5
    assert {d["access_level"] for d in result["datasets"]} == {"DOWNLOAD", "VIEW", "NONE"}


def test_list_items_carry_the_card_fields():
    dengue = next(
        d for d in service.list_datasets(limit=50)["datasets"] if d["ds_id"] == "EP0001DS0001"
    )
    assert dengue["spatial_coverage_region_name"] == "Karnataka"
    assert dengue["spatial_resolution"] == "DISTRICT"
    assert dengue["temporal_resolution"] == "MONTH"
    assert dengue["category_name"] == "Disease surveillance"
    assert dengue["data_owner_name"] == "National Center for Vector Borne Diseases Control"
    assert dengue["licence"] == "CC BY 4.0"
    assert dengue["dataset_kind"] == "tabular"
    assert dengue["tags"] == ["dengue", "vector borne"]
    assert dengue["has_data_dictionary"] is True
    assert dengue["table_count"] == 1
    assert dengue["updated_at"].startswith("2026-10-01")


def test_table_count_reads_manifest_then_data_dictionary():
    by_id = {d["ds_id"]: d for d in service.list_datasets(limit=50)["datasets"]}
    assert by_id["EP0001DS0001"]["table_count"] == 1  # manifest datasetTables
    assert by_id["CS0002DS0003"]["table_count"] == 1  # data dictionary only
    assert by_id["GS0001DS0004"]["table_count"] == 0  # manifest without tables


def test_list_never_includes_documents_or_download_links():
    forbidden = {
        "readme_md",
        "manifest_yaml",
        "manifest_json",
        "data_dictionary_json",
        "raw_datasets",
        "download_url",
    }
    for item in service.list_datasets(limit=50)["datasets"]:
        assert forbidden.isdisjoint(item)


def test_region_filter_includes_regions_inside_it():
    # Karnataka: the state-level dengue data and the survey of a Bengaluru subdistrict
    assert set(ids(service.list_datasets(region_id="state_29"))) == {"EP0001DS0001", "CS0002DS0003"}
    assert ids(service.list_datasets(region_id="district_525")) == ["CS0002DS0003"]
    assert ids(service.list_datasets(region_id="state_27")) == []


def test_region_filter_matches_any_of_several_regions():
    assert set(ids(service.list_datasets(region_id=["district_525", "state_21"]))) == {
        "CS0002DS0003",
        "WE0001DS0005",
    }
    response = client.get(
        "/api/v1/web/public/datasets", params=[("region_id", "district_525"), ("region_id", "state_21")]
    )
    assert set(ids(response.json())) == {"CS0002DS0003", "WE0001DS0005"}


def test_india_region_means_all_india_datasets_only():
    assert ids(service.list_datasets(region_id="country_IN")) == ["CS0001DS0002"]
    assert set(ids(service.list_datasets(region_id=["country_IN", "state_21"]))) == {
        "CS0001DS0002",
        "WE0001DS0005",
    }


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        ({"access_levels": ["DOWNLOAD"]}, {"EP0001DS0001", "GS0001DS0004", "WE0001DS0005"}),
        ({"access_levels": ["view,none"]}, {"CS0001DS0002", "CS0002DS0003"}),
        ({"category_ids": ["CS"]}, {"CS0001DS0002", "CS0002DS0003"}),
        ({"spatial_resolutions": ["WARD"]}, {"CS0002DS0003"}),
        ({"temporal_resolutions": ["MONTH", "YEAR"]}, {"EP0001DS0001", "CS0001DS0002"}),
        ({"dataset_kinds": ["geojson"]}, {"GS0001DS0004"}),
        (
            {"dataset_kinds": ["tabular"]},
            {"EP0001DS0001", "CS0001DS0002", "CS0002DS0003", "WE0001DS0005"},
        ),
        ({"licences": ["GODL-India"]}, {"WE0001DS0005"}),
        # time overlap: anything covering part of 2018-2019; undated datasets drop out
        ({"year_from": 2018, "year_to": 2019}, {"EP0001DS0001", "CS0001DS0002", "WE0001DS0005"}),
        ({"year_from": 2021}, {"EP0001DS0001", "CS0002DS0003"}),
        ({"year_to": 1995}, {"WE0001DS0005"}),
    ],
)
def test_filters(kwargs, expected):
    assert set(ids(service.list_datasets(limit=50, **kwargs))) == expected


def test_collection_and_owner_filters_take_ids():
    session = catalogue_module.DBSession()
    imd = session.query(DataOwner).filter(DataOwner.name == "India Meteorological Department").one()
    census = session.query(Collection).filter(Collection.collection_id == "CS0001").one()
    session.close()
    assert set(ids(service.list_datasets(data_owner_ids=[str(imd.id)]))) == {
        "GS0001DS0004",
        "WE0001DS0005",
    }
    assert ids(service.list_datasets(collection_ids=[census.id])) == ["CS0001DS0002"]


@pytest.mark.parametrize(
    "search, expected",
    [
        ("dengue", {"EP0001DS0001"}),
        ("livestock", {"CS0001DS0002"}),  # title
        ("cattle", {"CS0001DS0002"}),  # tag
        ("husbandry", {"CS0001DS0002"}),  # tag and data owner
        ("meteorological", {"GS0001DS0004", "WE0001DS0005"}),  # data owner
        ("odisha", {"WE0001DS0005"}),  # coverage region name
        ("rainfall data for Odisha", {"WE0001DS0005"}),  # stop words ignored, every word must match
        ("lgd_district_code", {"EP0001DS0001"}),  # column name in the manifest
        ("dengue cases since 2015", {"EP0001DS0001"}),  # year matches coverage
        ("dengue 2010", set()),
        ("census & surveys", {"CS0001DS0002", "CS0002DS0003"}),  # theme name
        ("100%", set()),  # LIKE wildcards are literal
        ("EP0001DS0001", {"EP0001DS0001"}),
    ],
)
def test_search(search, expected):
    assert set(ids(service.list_datasets(search=search, limit=50))) == expected


def test_search_terms_drop_stop_words_but_keep_a_lone_one():
    assert search_terms("Rainfall data for Odisha") == ["rainfall", "odisha"]
    assert search_terms("data") == ["data"]
    assert search_terms("   ") == []


def test_relevance_puts_title_matches_first():
    # "district" is in the dengue and boundaries titles, and in the rainfall description
    result = ids(service.list_datasets(search="district", limit=50))
    assert set(result[:2]) == {"EP0001DS0001", "GS0001DS0004"}
    assert result[2:] == ["WE0001DS0005"]


@pytest.mark.parametrize(
    "sort, first",
    [
        ("title", "CS0002DS0003"),  # Bengaluru ward health survey
        ("updated", "WE0001DS0005"),  # synced 20 Oct 2026
        ("newest", "EP0001DS0001"),  # data to 2023
        ("span", "WE0001DS0005"),  # 1990-2020
    ],
)
def test_sorts(sort, first):
    assert ids(service.list_datasets(sort=sort, limit=50))[0] == first


def test_pagination_reports_the_full_total():
    page = service.list_datasets(sort="title", limit=2, offset=2)
    assert page["total"] == 5
    assert len(page["datasets"]) == 2
    assert ids(page) == ids(service.list_datasets(sort="title", limit=50))[2:4]


@pytest.mark.parametrize(
    "params",
    [
        {"limit": 0},
        {"limit": 500},
        {"offset": -1},
        {"sort": "popular"},
        {"access_levels": ["PUBLIC"]},
        {"spatial_resolutions": ["planet"]},
        {"dataset_kinds": ["raster"]},
        {"year_from": 2020, "year_to": 2010},
        {"data_owner_ids": ["imd"]},
    ],
)
def test_bad_parameters_are_400(params):
    with pytest.raises(Exception) as exc_info:
        service.list_datasets(**params)
    assert getattr(exc_info.value, "status_code", None) == 400


# ----------------------------------------------------------------- detail


def test_restricted_detail_shows_columns_but_no_documents():
    detail = service.get_dataset("CS0002DS0003")
    assert detail["access_level"] == "NONE"
    assert detail["readme_md"] is None
    assert detail["manifest_yaml"] is None
    assert detail["manifest_json"] is None
    assert detail["data_dictionary_json"] is None
    assert detail["has_manifest"] is False
    assert detail["manifest_updated_by"] is None
    assert detail["can_download"] is False
    assert detail["raw_datasets"] == []
    assert detail["tables"] == [
        {
            "name": "responses",
            "description": "One row per household",
            "columns": [
                {
                    "name": "respondent_age",
                    "description": "Age in years",
                    "type": None,
                    "unit": None,
                }
            ],
        }
    ]
    assert "phone_number" not in json.dumps(detail)
    assert "contact sheet" not in json.dumps(detail)


def test_view_only_detail_keeps_documents_for_existing_clients():
    detail = service.get_dataset("CS0001DS0002")
    assert detail["readme_md"] == "# Livestock census"
    assert detail["collection"] == {
        "id": detail["collection_id"],
        "name": "Livestock census",
        "category": "Census & surveys",
    }
    assert detail["data_owner"]["name"] == "Department of Animal Husbandry and Dairying"
    assert detail["can_download"] is False


def test_detail_columns_come_from_the_manifest_in_written_order():
    detail = service.get_dataset("EP0001DS0001")
    assert detail["tables"][0]["columns"] == [
        {
            "name": "lgd_district_code",
            "description": "lgd_district_code description",
            "type": "integer",
            "unit": None,
        },
        {
            "name": "dengue_cases",
            "description": "dengue_cases description",
            "type": "integer",
            "unit": "cases",
        },
    ]


def test_detail_columns_fall_back_to_manifest_json():
    session = catalogue_module.DBSession()
    dataset = session.query(Dataset).filter(Dataset.ds_id == "EP0001DS0001").one()
    session.expunge(dataset)
    session.close()
    dataset.manifest_yaml = "not: [valid"
    names = {c["name"] for c in catalogue_module.public_columns(dataset)[0]["columns"]}
    assert names == {"lgd_district_code", "dengue_cases"}


def test_unknown_dataset_is_404():
    with pytest.raises(Exception) as exc_info:
        service.get_dataset("XX0000DS0000")
    assert exc_info.value.status_code == 404


# ------------------------------------------------------------------ stats


def test_stats():
    stats = service.get_stats()
    assert stats["total_datasets"] == 5
    assert stats["total_sources"] == 3
    assert stats["earliest_year"] == 1990
    assert stats["latest_year"] == 2023
    assert stats["by_access_level"] == {"DOWNLOAD": 3, "VIEW": 1, "NONE": 1}
    assert stats["by_dataset_kind"] == {"tabular": 4, "geojson": 1}
    assert stats["themes"][0] == {
        "category_id": "CS",
        "category_name": "Census & surveys",
        "count": 2,
    }
    assert {t["category_id"]: t["count"] for t in stats["themes"]} == {
        "CS": 2,
        "EP": 1,
        "GS": 1,
        "WE": 1,
    }
    assert stats["coverage"] == {
        "national": 1,
        "unknown": 1,
        # the Bengaluru subdistrict survey counts for Karnataka
        "states": [
            {"region_id": "state_29", "region_name": "Karnataka", "count": 2},
            {"region_id": "state_21", "region_name": "Odisha", "count": 1},
        ],
    }


# ------------------------------------------------------------------ routes


def test_route_accepts_repeated_and_comma_separated_filters():
    response = client.get(
        "/api/v1/web/public/datasets",
        params=[
            ("access_level", "DOWNLOAD,VIEW"),
            ("category_id", "CS"),
            ("category_id", "EP"),
            ("sort", "title"),
        ],
    )
    assert response.status_code == 200
    assert ids(response.json()) == ["EP0001DS0001", "CS0001DS0002"]


def test_route_rejects_bad_values_with_400():
    assert client.get("/api/v1/web/public/datasets", params={"sort": "popular"}).status_code == 400
    assert client.get("/api/v1/web/public/datasets", params={"year_from": "abc"}).status_code == 422


def test_stats_and_detail_routes():
    assert client.get("/api/v1/web/public/stats").json()["total_datasets"] == 5
    assert client.get("/api/v1/web/public/datasets/CS0002DS0003").json()["readme_md"] is None
    assert client.get("/api/v1/web/public/datasets/XX0000DS0000").status_code == 404


def test_regions_route_lists_children_a_to_z():
    states = client.get("/api/v1/web/public/regions", params={"parent_id": "country_IN"}).json()
    assert [r["region_name"] for r in states["regions"]] == ["Karnataka", "Maharashtra", "Odisha"]
    districts = client.get("/api/v1/web/public/regions", params={"parent_id": "state_29"}).json()
    assert districts["regions"] == [{"region_id": "district_525", "region_name": "Bengaluru Urban"}]
    assert client.get("/api/v1/web/public/regions").status_code == 422
