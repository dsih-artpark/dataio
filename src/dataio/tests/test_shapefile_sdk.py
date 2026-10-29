from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
import requests

from dataio.sdk.admin import DataIOAdminAPI, _raise_for_status_with_detail, _region_id_from_geojson_file
from dataio.sdk.user import DataIOAPI

GEOMETRY = {"type": "Polygon", "coordinates": [[[74.5, 16.4], [74.6, 16.4], [74.5, 16.4]]]}


def _write(path, properties, collection=False):
    feature = {"type": "Feature", "properties": properties, "geometry": GEOMETRY}
    data = {"type": "FeatureCollection", "features": [feature]} if collection else feature
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def test_region_id_comes_from_region_type_and_lgd_code_not_the_file_name(tmp_path):
    path = _write(tmp_path / "district_1.geojson", {"regionType": "district", "lgdCode": 527})

    assert _region_id_from_geojson_file(path) == ("district_527", None)


def test_single_feature_collection_is_accepted(tmp_path):
    path = _write(tmp_path / "v.geojson", {"regionType": "village", "lgdCode": "597186"}, collection=True)

    assert _region_id_from_geojson_file(path) == ("village_597186", None)


def test_files_that_cannot_be_mapped_say_why(tmp_path):
    no_lgd = _write(tmp_path / "a.geojson", {"regionType": "village", "regionName": "NON-CADASTRAL PARCEL"})
    no_type = _write(tmp_path / "b.geojson", {"lgdCode": 5})
    bad = tmp_path / "c.geojson"
    bad.write_text("{", encoding="utf-8")

    assert _region_id_from_geojson_file(no_lgd) == (None, "no lgdCode property")
    assert _region_id_from_geojson_file(no_type) == (None, "no regionType property")
    assert _region_id_from_geojson_file(bad)[1].startswith("not readable JSON")


@pytest.mark.parametrize(
    ("lgd_code", "expected"),
    [(524, "district_524"), (524.0, "district_524"), ("524", "district_524"),
     (527.9, None), (True, None), (-5, None), ("52a", None)],
)
def test_only_whole_number_lgd_codes_make_a_region_id(tmp_path, lgd_code, expected):
    path = _write(tmp_path / "d.geojson", {"regionType": "district", "lgdCode": lgd_code})

    region_id, reason = _region_id_from_geojson_file(path)

    assert region_id == expected
    if expected is None:
        assert "isn't a whole number" in reason


@pytest.mark.parametrize(
    "data",
    [
        {"type": "FeatureCollection", "features": [1]},
        {"type": "FeatureCollection", "features": {"a": 1}},
        {"type": "Feature", "properties": "x", "geometry": GEOMETRY},
    ],
)
def test_malformed_features_are_skipped_not_fatal(tmp_path, data):
    path = tmp_path / "bad.geojson"
    path.write_text(json.dumps(data), encoding="utf-8")

    region_id, reason = _region_id_from_geojson_file(path)

    assert region_id is None and reason


def _response(status, body: bytes):
    response = requests.Response()
    response.status_code = status
    response._content = body
    return response


def test_error_bodies_that_are_not_json_objects_still_raise_http_error():
    with pytest.raises(requests.HTTPError, match=r"^500: \[\]"):
        _raise_for_status_with_detail(_response(500, b"[]"))
    with pytest.raises(requests.HTTPError, match="^409: exists"):
        _raise_for_status_with_detail(_response(409, b'{"detail": "exists"}'))


def test_download_not_found_with_a_non_object_body_is_a_value_error(tmp_path):
    client = object.__new__(DataIOAPI)
    client.base_url = "http://test"
    client.data_dir = str(tmp_path)
    client.session = SimpleNamespace(request=lambda *a, **k: _response(404, b"[]"))

    with pytest.raises(ValueError, match="Shapefile for region district_527 not found"):
        client.download_shapefile("district_527")


def test_folder_dry_run_plans_uploads_and_skips_duplicates_without_calling_the_api(tmp_path):
    _write(tmp_path / "district_1.geojson", {"regionType": "district", "lgdCode": 527})
    _write(tmp_path / "district_2.geojson", {"regionType": "district", "lgdCode": 524})
    _write(tmp_path / "dup.geojson", {"regionType": "district", "lgdCode": 524})
    _write(tmp_path / "forest.geojson", {"regionType": "village"})
    client = object.__new__(DataIOAdminAPI)  # no session: any API call would fail

    results = client.upload_shapefile_folder(str(tmp_path), "kgis-2026-08", dry_run=True)

    by_status = {}
    for r in results:
        by_status.setdefault(r["status"], []).append(r["region_id"])
    assert by_status["planned"] == ["district_527"]
    assert by_status["skipped"] == [None, "district_524", "district_524"]


def test_folder_id_map_renames_and_skips_ids_not_in_the_map(tmp_path):
    _write(tmp_path / "ward_1.geojson", {"regionType": "ward", "lgdCode": 1295586})
    _write(tmp_path / "ward_2.geojson", {"regionType": "ward", "lgdCode": 1295587})
    client = object.__new__(DataIOAdminAPI)

    results = client.upload_shapefile_folder(
        str(tmp_path), "kgis-2026-08", dry_run=True, id_map={"ward_1295586": "ward_276618-1"}
    )

    assert [(r["status"], r["region_id"]) for r in results] == [("skipped", None), ("planned", "ward_276618-1")]
    assert results[0]["detail"] == "ward_1295587 not in the id map"
