from __future__ import annotations

import io
from pathlib import Path

from fastapi import UploadFile

from dataio.api.services import draft_upload_storage


def test_save_upload_writes_file_and_returns_path(tmp_path, monkeypatch):
    monkeypatch.setattr(draft_upload_storage, "DRAFT_UPLOAD_DIR", str(tmp_path))

    upload = UploadFile(filename="data.csv", file=io.BytesIO(b"a,b\n1,2\n"))
    saved_path = draft_upload_storage.save_upload(upload)

    assert saved_path.endswith("data.csv")
    with open(saved_path, "rb") as f:
        assert f.read() == b"a,b\n1,2\n"


def test_save_upload_strips_leading_utf8_bom(tmp_path, monkeypatch):
    """A CSV saved by Excel as "CSV UTF-8" carries a 3-byte BOM marker
    before the first column header - stripping it at intake means it
    never reaches the validator (or anything else) mismatched against a
    manifest's clean column names.
    """
    monkeypatch.setattr(draft_upload_storage, "DRAFT_UPLOAD_DIR", str(tmp_path))

    bom_prefixed = b"\xef\xbb\xbfstate.lgd_code,name\n29,Karnataka\n"
    upload = UploadFile(filename="data.csv", file=io.BytesIO(bom_prefixed))
    saved_path = draft_upload_storage.save_upload(upload)

    with open(saved_path, "rb") as f:
        content = f.read()
    assert not content.startswith(draft_upload_storage._UTF8_BOM)
    assert content == b"state.lgd_code,name\n29,Karnataka\n"


def test_save_upload_leaves_non_bom_content_untouched(tmp_path, monkeypatch):
    monkeypatch.setattr(draft_upload_storage, "DRAFT_UPLOAD_DIR", str(tmp_path))

    upload = UploadFile(filename="data.csv", file=io.BytesIO(b"state.lgd_code,name\n29,Karnataka\n"))
    saved_path = draft_upload_storage.save_upload(upload)

    with open(saved_path, "rb") as f:
        assert f.read() == b"state.lgd_code,name\n29,Karnataka\n"


def test_save_upload_is_collision_proof(tmp_path, monkeypatch):
    monkeypatch.setattr(draft_upload_storage, "DRAFT_UPLOAD_DIR", str(tmp_path))

    upload_a = UploadFile(filename="data.csv", file=io.BytesIO(b"first"))
    upload_b = UploadFile(filename="data.csv", file=io.BytesIO(b"second"))

    path_a = draft_upload_storage.save_upload(upload_a)
    path_b = draft_upload_storage.save_upload(upload_b)

    assert path_a != path_b
    with open(path_a, "rb") as f:
        assert f.read() == b"first"
    with open(path_b, "rb") as f:
        assert f.read() == b"second"


def test_save_upload_rejects_path_traversal_in_filename(tmp_path, monkeypatch):
    monkeypatch.setattr(draft_upload_storage, "DRAFT_UPLOAD_DIR", str(tmp_path))

    upload = UploadFile(filename="../../etc/evil.csv", file=io.BytesIO(b"malicious"))
    saved_path = draft_upload_storage.save_upload(upload)

    saved = Path(saved_path).resolve()
    assert saved.is_relative_to(tmp_path.resolve())
    assert saved.name == "evil.csv"


def test_save_upload_rejects_absolute_path_filename(tmp_path, monkeypatch):
    monkeypatch.setattr(draft_upload_storage, "DRAFT_UPLOAD_DIR", str(tmp_path))

    absolute_target = tmp_path.parent / "outside.csv"
    upload = UploadFile(filename=str(absolute_target), file=io.BytesIO(b"malicious"))
    saved_path = draft_upload_storage.save_upload(upload)

    saved = Path(saved_path).resolve()
    assert saved.is_relative_to(tmp_path.resolve())
    assert not absolute_target.exists()
