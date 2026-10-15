from __future__ import annotations

from types import SimpleNamespace

from dataio.api.models import VersionType
from dataio.api.services.filestore_service import FilestoreService


class _FakeBucket:
    """The parts of a boto3 Bucket the archive helpers use, backed by a dict."""

    name = "test-bucket"

    def __init__(self, objects: dict[str, bytes]):
        self.store = dict(objects)
        self.objects = SimpleNamespace(filter=self._filter)

    def _filter(self, Prefix: str):
        return [SimpleNamespace(key=key) for key in sorted(self.store) if key.startswith(Prefix)]

    def copy(self, source: dict, key: str) -> None:
        self.store[key] = self.store[source["Key"]]

    def delete_objects(self, Delete: dict) -> None:
        for obj in Delete["Objects"]:
            self.store.pop(obj["Key"], None)


def _filestore(objects: dict[str, bytes]) -> FilestoreService:
    service = object.__new__(FilestoreService)
    service.bucket = _FakeBucket(objects)
    return service


DATASET = "filestore/STANDARDISED/CS0007DS0999"
OTHER = "filestore/STANDARDISED/CS0007DS0998/table.csv"
STAMP = "20261015T000000Z"
ARCHIVE = f"archive/{DATASET}/{STAMP}"


def test_archive_copies_every_file_outside_the_dataset_folder():
    filestore = _filestore({
        f"{DATASET}/table.csv": b"old",
        f"{DATASET}/metadata.json": b"{}",
        f"filestore/PREPROCESSED/CS0007DS0999/raw.csv": b"pre",
        OTHER: b"other",
    })

    prefix, copied = filestore.archive_dataset_version("CS0007DS0999", VersionType.STANDARDISED, STAMP)

    assert prefix == ARCHIVE
    assert sorted(copied) == ["metadata.json", "table.csv"]
    store = filestore.bucket.store
    assert store[f"{ARCHIVE}/table.csv"] == b"old"
    assert store[f"{ARCHIVE}/metadata.json"] == b"{}"
    assert f"{DATASET}/table.csv" in store  # archiving copies, it doesn't move
    assert not any(key.startswith(f"archive/filestore/PREPROCESSED") for key in store)


def test_delete_dataset_version_only_removes_that_version_of_that_dataset():
    filestore = _filestore({
        f"{DATASET}/table.csv": b"old",
        f"filestore/PREPROCESSED/CS0007DS0999/raw.csv": b"pre",
        f"{ARCHIVE}/table.csv": b"old",
        OTHER: b"other",
    })

    filestore.delete_dataset_version("CS0007DS0999", VersionType.STANDARDISED)

    assert sorted(filestore.bucket.store) == sorted([
        "filestore/PREPROCESSED/CS0007DS0999/raw.csv", f"{ARCHIVE}/table.csv", OTHER,
    ])


def test_restore_puts_the_archived_files_back_and_drops_new_ones():
    filestore = _filestore({
        f"{DATASET}/new_table.csv": b"new",
        f"{DATASET}/manifest.yaml": b"new manifest",
        f"{ARCHIVE}/table.csv": b"old",
        f"{ARCHIVE}/metadata.json": b"{}",
    })

    filestore.restore_dataset_version("CS0007DS0999", VersionType.STANDARDISED, ARCHIVE)

    store = filestore.bucket.store
    assert store[f"{DATASET}/table.csv"] == b"old"
    assert store[f"{DATASET}/metadata.json"] == b"{}"
    assert f"{DATASET}/new_table.csv" not in store
    assert f"{DATASET}/manifest.yaml" not in store
    assert f"{ARCHIVE}/table.csv" in store  # the archive copy is kept


def test_list_dataset_version_files_returns_names_inside_the_folder():
    filestore = _filestore({f"{DATASET}/table.csv": b"", f"{DATASET}/metadata.json": b"", OTHER: b""})

    assert sorted(filestore.list_dataset_version_files("CS0007DS0999", VersionType.STANDARDISED)) == [
        "metadata.json", "table.csv",
    ]
