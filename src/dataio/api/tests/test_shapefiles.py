from __future__ import annotations

import gzip
import importlib
import io
import json
import os
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from botocore.exceptions import ClientError
from fastapi import HTTPException
from fastapi.testclient import TestClient

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

from dataio.api import shapefile_backfill
from dataio.api.database.models import User as UserModel
from dataio.api.models import ShapefileVersionCreate
from dataio.api.services import shapefile_service as svc
from dataio.api.services.filestore_service import FilestoreService

REGION = SimpleNamespace(region_id="subdistrict_5433", region_name="Chikkodi", parent_region_id="district_527")
PARENT = SimpleNamespace(region_id="district_527", region_name="Belagavi", parent_region_id="state_29")
POLYGON = {"type": "Polygon", "coordinates": [[[74.5, 16.4], [74.6, 16.4], [74.6, 16.5], [74.5, 16.4]]]}
KGIS_FEATURE = {
    "type": "Feature",
    "id": "subdistrict_1",
    "properties": {"regionName": "Chikkodi Taluk", "regionType": "taluk", "lgdCode": 5433,
                   "parentID": "district_1", "lastUpdated": "2026-08-04"},
    "geometry": POLYGON,
}
USER = SimpleNamespace(email="admin@example.com")


def _bytes(obj) -> bytes:
    return json.dumps(obj).encode("utf-8")


# normalise_geojson


def test_feature_becomes_feature_collection_following_the_geojson_spec():
    result = svc.normalise_geojson(_bytes(KGIS_FEATURE), REGION, PARENT)

    assert result["type"] == "FeatureCollection"
    [feature] = result["features"]
    # The region id replaces the source's id; region fields come from the regions table.
    assert feature["id"] == "subdistrict_5433"
    assert list(feature["properties"])[:4] == ["regionName", "regionType", "parentID", "parentName"]
    assert feature["properties"]["regionName"] == "CHIKKODI"
    assert feature["properties"]["regionType"] == "subdistrict"
    assert feature["properties"]["parentID"] == "district_527"
    assert feature["properties"]["parentName"] == "BELAGAVI"
    # The source's other properties are kept unchanged.
    assert feature["properties"]["lgdCode"] == 5433
    assert feature["properties"]["lastUpdated"] == "2026-08-04"
    assert not {"region_id", "name", "parent", "parent_name"} & set(feature["properties"])
    assert feature["geometry"] == POLYGON


def test_feature_collection_keeps_every_feature():
    collection = {"type": "FeatureCollection", "features": [KGIS_FEATURE, KGIS_FEATURE]}

    result = svc.normalise_geojson(_bytes(collection), REGION, None)

    assert len(result["features"]) == 2
    assert [f["id"] for f in result["features"]] == ["subdistrict_5433", "subdistrict_5433"]
    assert result["features"][0]["properties"]["parentName"] is None


def test_last_updated_may_carry_a_time():
    feature = {**KGIS_FEATURE, "properties": {**KGIS_FEATURE["properties"], "lastUpdated": "2026-01-21T11:43:48+05:30"}}

    [result] = svc.normalise_geojson(_bytes(feature), REGION, PARENT)["features"]

    assert result["properties"]["lastUpdated"] == "2026-01-21T11:43:48+05:30"


@pytest.mark.parametrize(
    ("content", "message"),
    [
        (b"not json", "not valid JSON"),
        (_bytes([1, 2]), "expected a GeoJSON object"),
        (_bytes({"type": "Polygon", "coordinates": []}), "Feature or FeatureCollection"),
        (_bytes({"type": "FeatureCollection", "features": []}), "no features"),
        (_bytes({**KGIS_FEATURE, "geometry": {"type": "Point", "coordinates": [74.5, 16.4]}}), "Polygon or MultiPolygon"),
        (_bytes({**KGIS_FEATURE, "geometry": {"type": "Polygon", "coordinates": []}}), "no coordinates"),
        (_bytes({**KGIS_FEATURE, "properties": [1]}), "properties must be an object"),
        (_bytes({**KGIS_FEATURE, "properties": {"lgdCode": 5433}}), "needs a lastUpdated"),
        (_bytes({**KGIS_FEATURE, "properties": {"lastUpdated": "2026"}}), "needs a lastUpdated"),
        # Projected metres (e.g. UTM) instead of longitude/latitude.
        (_bytes({**KGIS_FEATURE, "geometry": {"type": "Polygon", "coordinates": [[[512000.0, 1815000.0]]]}}), "EPSG:4326"),
    ],
)
def test_unusable_geojson_is_rejected(content, message):
    with pytest.raises(ValueError, match=message):
        svc.normalise_geojson(content, REGION, PARENT)


# Keys


def test_legacy_key_keeps_the_pre_versioning_layout():
    assert FilestoreService.shapefile_key("legacy", "district_527", "subdistrict_5433") == (
        "shapefiles/district_527/subdistrict_5433.geojson.gz"
    )


@pytest.mark.parametrize(
    ("parent_id", "region_id", "key"),
    [
        ("district_527", "subdistrict_5433",
         "shapefiles/kgis-2026-08/subdistrict/subdistrict_5433/subdistrict_5433-boundary.geojson.gz"),
        ("ulb_272000", "ward_272000-16",
         "shapefiles/kgis-2026-08/ward/ward_272000-16/ward_272000-16-boundary.geojson.gz"),
        (None, "country_IN", "shapefiles/kgis-2026-08/country/country_IN/country_IN-boundary.geojson.gz"),
    ],
)
def test_versioned_key_groups_by_region_type_not_parent(parent_id, region_id, key):
    assert FilestoreService.shapefile_key("kgis-2026-08", parent_id, region_id) == key
    assert FilestoreService.shapefile_key("kgis-2026-08", "state_29", region_id) == key


def test_list_legacy_shapefiles_leaves_out_versioned_keys():
    keys = [
        "shapefiles/",
        "shapefiles/country_IN/state_29.geojson.gz",
        "shapefiles/country_IN/state_36",  # stray object without the extension
        "shapefiles/kgis-2026-08/state_29/district_527.geojson.gz",
        "shapefiles/kgis-2026-08/district/district_527/district_527-boundary.geojson.gz",
        "shapefiles/state/state_29/state_29-boundary.geojson.gz",
    ]
    filestore = object.__new__(FilestoreService)
    filestore.bucket = SimpleNamespace(objects=SimpleNamespace(filter=lambda Prefix: [
        SimpleNamespace(key=k, size=10, last_modified=None) for k in keys if k.startswith(Prefix)
    ]))

    listed = filestore.list_legacy_shapefiles()

    assert [(f["region_id"], f["parent_id"]) for f in listed] == [("state_29", "country_IN")]


# ShapefileService


class FakeFilestore:
    def __init__(self, objects=None):
        self.objects = dict(objects or {})

    def upload_shapefile(self, body, key):
        self.objects[key] = body

    def get_shapefile(self, key):
        if key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return self.objects[key]


@pytest.fixture
def db(monkeypatch):
    """Stand-in for the database functions the service uses."""
    state = SimpleNamespace(
        versions={"legacy", "kgis-2026-08"},
        regions={r.region_id: r for r in (REGION, PARENT)},
        records={},
        saved=[],
        rate_limited=False,
    )
    fake = {
        "get_shapefile_version": lambda v: SimpleNamespace(version=v) if v in state.versions else None,
        "get_region": lambda rid: state.regions.get(rid),
        "get_shapefile_record": lambda rid, v=None: state.records.get((rid, v)),
        "save_shapefile_record": lambda *args: state.saved.append(args),
        "check_rate_limit_exceeded": lambda email, point: state.rate_limited,
        "update_shapefile_rate_limit": lambda email: None,
    }
    for name, fn in fake.items():
        monkeypatch.setattr(svc.database, name, fn)
    return state


def _service(filestore=None) -> svc.ShapefileService:
    service = object.__new__(svc.ShapefileService)
    service.logger = SimpleNamespace(error=lambda *a, **k: None)
    service.filestore_service = filestore or FakeFilestore()
    return service


def _upload(content=None):
    return SimpleNamespace(file=io.BytesIO(content if content is not None else _bytes(KGIS_FEATURE)))


def test_upload_stores_normalised_gzipped_geojson_under_the_version(db):
    filestore = FakeFilestore()

    result = _service(filestore).upload_shapefile(_upload(), "subdistrict_5433", "kgis-2026-08", False, USER)

    key = "shapefiles/kgis-2026-08/subdistrict/subdistrict_5433/subdistrict_5433-boundary.geojson.gz"
    stored = json.loads(gzip.decompress(filestore.objects[key]))
    assert stored["features"][0]["id"] == "subdistrict_5433"
    assert db.saved == [("subdistrict_5433", "kgis-2026-08", key, len(filestore.objects[key]), USER.email)]
    assert result == {"region_id": "subdistrict_5433", "version": "kgis-2026-08",
                      "size_bytes": len(filestore.objects[key]), "overwritten": False}


@pytest.mark.parametrize(
    ("region_id", "version", "status", "detail"),
    [
        ("subdistrict_5433", "legacy", 400, "read-only"),
        ("subdistrict_5433", "kgis-2099", 404, "Unknown shapefile version"),
        ("subdistrict_9999", "kgis-2026-08", 400, "Unknown region id"),
    ],
)
def test_upload_refuses_legacy_unknown_versions_and_unknown_regions(db, region_id, version, status, detail):
    with pytest.raises(HTTPException) as exc:
        _service().upload_shapefile(_upload(), region_id, version, False, USER)

    assert exc.value.status_code == status
    assert detail in exc.value.detail
    assert db.saved == []


def test_upload_does_not_replace_an_existing_file_unless_told_to(db):
    db.records[("subdistrict_5433", "kgis-2026-08")] = SimpleNamespace(s3_key="k", version="kgis-2026-08")

    with pytest.raises(HTTPException) as exc:
        _service().upload_shapefile(_upload(), "subdistrict_5433", "kgis-2026-08", False, USER)
    assert exc.value.status_code == 409

    result = _service().upload_shapefile(_upload(), "subdistrict_5433", "kgis-2026-08", True, USER)
    assert result["overwritten"] is True


def test_upload_reports_invalid_geojson_as_bad_request(db):
    with pytest.raises(HTTPException) as exc:
        _service().upload_shapefile(_upload(b"{}"), "subdistrict_5433", "kgis-2026-08", False, USER)

    assert exc.value.status_code == 400
    assert exc.value.detail.startswith("Invalid GeoJSON")


def test_download_serves_the_indexed_file_and_its_version(db):
    db.records[("subdistrict_5433", None)] = SimpleNamespace(s3_key="shapefiles/kgis/x.geojson.gz", version="kgis-2026-08")
    service = _service(FakeFilestore({"shapefiles/kgis/x.geojson.gz": b"gz"}))

    assert service.get_shapefile("subdistrict_5433", USER.email) == (b"gz", "kgis-2026-08")


def test_download_falls_back_to_the_legacy_key_before_backfill(db):
    legacy_key = "shapefiles/district_527/subdistrict_5433.geojson.gz"
    service = _service(FakeFilestore({legacy_key: b"old"}))

    assert service.get_shapefile("subdistrict_5433", USER.email) == (b"old", "legacy")


def test_download_of_a_version_without_the_region_is_not_found(db):
    with pytest.raises(HTTPException) as exc:
        _service().get_shapefile("subdistrict_5433", USER.email, "kgis-2026-08")

    assert exc.value.status_code == 404


def test_download_of_a_missing_s3_object_is_not_found(db):
    with pytest.raises(HTTPException) as exc:
        _service().get_shapefile("subdistrict_5433", USER.email)

    assert exc.value.status_code == 404


def test_download_respects_the_rate_limit(db):
    db.rate_limited = True
    db.records[("subdistrict_5433", None)] = SimpleNamespace(s3_key="k", version="v")

    with pytest.raises(HTTPException) as exc:
        _service(FakeFilestore({"k": b""})).get_shapefile("subdistrict_5433", USER.email)

    assert exc.value.status_code == 429


@pytest.mark.parametrize(
    ("version", "dataset_id", "status"),
    [
        ("KGIS 2026", None, 400),
        ("kgis-2026-08", "DS0123", 400),
        ("legacy", None, 409),
        ("state", None, 400),
        ("village", None, 400),
    ],
)
def test_create_version_validates_names_and_rejects_duplicates(db, version, dataset_id, status):
    payload = ShapefileVersionCreate(version=version, source="KGIS", dataset_id=dataset_id)

    with pytest.raises(HTTPException) as exc:
        _service().create_version(payload, USER)

    assert exc.value.status_code == status


# Routes

app = importlib.import_module("dataio.api").app
get_user = importlib.import_module("dataio.api.auth").get_user
client = TestClient(app)


class ShapefileServiceStub:
    def __init__(self):
        self.calls = []

    def list_versions(self):
        self.calls.append(("versions",))
        return [{"version": "kgis-2026-08"}]

    def get_shapefile(self, region_id, email, version=None):
        self.calls.append(("get", region_id, version))
        return b"gz", "kgis-2026-08"

    def upload_shapefile(self, file, region_id, version, overwrite, user):
        self.calls.append(("upload", region_id, version, overwrite))
        return {"region_id": region_id}


@pytest.fixture
def stub():
    stub = ShapefileServiceStub()
    app.dependency_overrides[get_user] = lambda: UserModel(email="admin@example.com", is_group=False, is_admin=True)
    app.dependency_overrides[svc.ShapefileService] = lambda: stub
    yield stub
    app.dependency_overrides.clear()


def test_versions_route_is_not_taken_for_a_region_id(stub):
    response = client.get("/api/v1/shapefiles/versions")

    assert response.status_code == 200
    assert stub.calls == [("versions",)]


def test_download_route_passes_the_version_and_reports_the_one_served(stub):
    response = client.get("/api/v1/shapefiles/district_527", params={"version": "kgis-2026-08"})

    assert response.status_code == 200
    assert response.headers["X-Shapefile-Version"] == "kgis-2026-08"
    assert stub.calls == [("get", "district_527", "kgis-2026-08")]


def test_admin_upload_requires_a_version(stub):
    files = {"shapefile": ("a.geojson", _bytes(KGIS_FEATURE), "application/geo+json")}

    assert client.post("/api/v1/admin/shapefiles/district_527", files=files).status_code == 422

    response = client.post(
        "/api/v1/admin/shapefiles/district_527",
        params={"version": "kgis-2026-08", "overwrite": "true"},
        files=files,
    )
    assert response.status_code == 200
    assert stub.calls == [("upload", "district_527", "kgis-2026-08", True)]


# Backfill


def test_backfill_indexes_legacy_files_and_reports_unknown_regions(capsys):
    stamp = datetime(2025, 9, 16, tzinfo=timezone.utc)
    files = [
        {"key": "shapefiles/state_29/district_525.geojson.gz", "region_id": "district_525",
         "parent_id": "state_29", "size": 100, "last_modified": stamp},
        {"key": "shapefiles/state_29/district_999.geojson.gz", "region_id": "district_999",
         "parent_id": "state_29", "size": 50, "last_modified": stamp},
    ]
    added = []
    db = SimpleNamespace(
        get_shapefile_version=lambda v: object(),
        get_region=lambda rid: SimpleNamespace(parent_region_id="state_29") if rid == "district_525" else None,
        add_shapefile_records=lambda rows: added.extend(rows) or len(rows),
    )

    code = shapefile_backfill.main([], filestore=SimpleNamespace(list_legacy_shapefiles=lambda: files), db=db)

    assert code == 0
    assert added == [{"region_id": "district_525", "version": "legacy",
                      "s3_key": "shapefiles/state_29/district_525.geojson.gz",
                      "size_bytes": 100, "uploaded_at": stamp}]
    out = capsys.readouterr().out
    assert "Indexed 1 new" in out
    assert "shapefiles/state_29/district_999.geojson.gz" in out


def test_backfill_dry_run_writes_nothing():
    db = SimpleNamespace(
        get_shapefile_version=lambda v: object(),
        get_region=lambda rid: SimpleNamespace(parent_region_id="state_29"),
        add_shapefile_records=lambda rows: pytest.fail("dry run wrote rows"),
    )
    files = [{"key": "shapefiles/state_29/district_525.geojson.gz", "region_id": "district_525",
              "parent_id": "state_29", "size": 1, "last_modified": None}]

    assert shapefile_backfill.main(["--dry-run"], filestore=SimpleNamespace(list_legacy_shapefiles=lambda: files), db=db) == 0
