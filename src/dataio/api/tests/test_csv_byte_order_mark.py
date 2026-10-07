"""Excel's "CSV UTF-8" files start with a UTF-8 byte-order mark. It must not
end up in the first column name, or the column no longer matches the
metadata and validation reports it as missing."""

from __future__ import annotations

import io
import os
from types import SimpleNamespace

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

import pytest

from dataio.api.models import TableMetadata, VersionType
from dataio.api.services.filestore_service import FilestoreService
from dataio.validate.loaders.data import load_tabular_rows

CSV = "district,count\nPune,3\n"
BOM = b"\xef\xbb\xbf"


@pytest.mark.parametrize(
    "make_source",
    [
        lambda tmp_path: BOM + CSV.encode("utf-8"),
        lambda tmp_path: "\ufeff" + CSV,
        lambda tmp_path: str(_write(tmp_path / "t.csv", BOM + CSV.encode("utf-8"))),
    ],
    ids=["bytes", "inline-text", "file-path"],
)
def test_load_tabular_rows_ignores_a_byte_order_mark(tmp_path, make_source):
    rows = load_tabular_rows(make_source(tmp_path))

    assert rows == [{"district": "Pune", "count": "3"}]


def _write(path, data: bytes):
    path.write_bytes(data)
    return path


class _FakeBucket:
    def __init__(self):
        self.uploaded = {}

    def upload_fileobj(self, fileobj, key):
        self.uploaded[key] = fileobj.read()

    def put_object(self, Body, Key, **kwargs):
        pass


@pytest.mark.parametrize("prefix", [BOM, b""], ids=["with-bom", "without-bom"])
def test_upload_file_stores_csvs_without_a_byte_order_mark(monkeypatch, prefix):
    service = FilestoreService()
    service.bucket = _FakeBucket()
    monkeypatch.setattr(service, "_get_metadata_object", lambda dataset_id, version_type: {"tables": {}})
    upload = SimpleNamespace(filename="t.csv", file=io.BytesIO(prefix + CSV.encode("utf-8")))

    service.upload_file("CS0007DS0999", VersionType.STANDARDISED, upload, TableMetadata(table_name="t"))

    assert service.bucket.uploaded == {"filestore/STANDARDISED/CS0007DS0999/t.csv": CSV.encode("utf-8")}
