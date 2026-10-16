from __future__ import annotations

from types import SimpleNamespace

from dataio.api.models import VersionType
from dataio.api.services.filestore_service import FilestoreService


class _FakeBucket:
    """The parts of a boto3 Bucket the archive helpers use, backed by a dict."""

    name = "test-bucket"

    def __init__(self, objects: dict[str, bytes], undeletable: set[str] = frozenset()):
        self.store = dict(objects)
        self.undeletable = set(undeletable)
        self.objects = SimpleNamespace(filter=self._filter)

    def _filter(self, Prefix: str):
        return [SimpleNamespace(key=key) for key in sorted(self.store) if key.startswith(Prefix)]

    def copy(self, source: dict, key: str) -> None:
        self.store[key] = self.store[source["Key"]]

    def delete_objects(self, Delete: dict) -> dict:
        # Like S3: a 200 response that lists the keys it could not delete.
        errors = []
        for obj in Delete["Objects"]:
            if obj["Key"] in self.undeletable:
                errors.append({"Key": obj["Key"], "Code": "AccessDenied"})
            else:
                self.store.pop(obj["Key"], None)
        return {"Errors": errors} if errors else {}


def _filestore(objects: dict[str, bytes], undeletable: set[str] = frozenset()) -> FilestoreService:
    service = object.__new__(FilestoreService)
    service.bucket = _FakeBucket(objects, undeletable)
    return service


DATASET = "filestore/STANDARDISED/CS0007DS0999"
OTHER = "filestore/STANDARDISED/CS0007DS0998/table.csv"
STAMP = "20261015T000000Z"
ARCHIVE = f"archive/{DATASET}/{STAMP}"


def test_archive_copies_every_file_outside_the_dataset_folder():
    filestore = _filestore({
        f"{DATASET}/table.csv": b"old",
        f"{DATASET}/metadata.json": b"{}",
        "filestore/PREPROCESSED/CS0007DS0999/raw.csv": b"pre",
        OTHER: b"other",
    })

    prefix, copied = filestore.archive_dataset_version("CS0007DS0999", VersionType.STANDARDISED, STAMP)

    assert prefix == ARCHIVE
    assert sorted(copied) == ["metadata.json", "table.csv"]
    store = filestore.bucket.store
    assert store[f"{ARCHIVE}/table.csv"] == b"old"
    assert store[f"{ARCHIVE}/metadata.json"] == b"{}"
    assert f"{DATASET}/table.csv" in store  # archiving copies, it doesn't move
    assert not any(key.startswith("archive/filestore/PREPROCESSED") for key in store)


def test_delete_dataset_version_only_removes_that_version_of_that_dataset():
    filestore = _filestore({
        f"{DATASET}/table.csv": b"old",
        "filestore/PREPROCESSED/CS0007DS0999/raw.csv": b"pre",
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


def test_delete_dataset_version_raises_when_s3_reports_undeleted_keys():
    filestore = _filestore(
        {f"{DATASET}/table.csv": b"old", f"{DATASET}/metadata.json": b"{}"},
        undeletable={f"{DATASET}/table.csv"},
    )

    try:
        filestore.delete_dataset_version("CS0007DS0999", VersionType.STANDARDISED)
    except RuntimeError as exc:
        assert "Could not delete 1 file(s) of CS0007DS0999" in str(exc)
    else:
        raise AssertionError("a partial delete must not look like success")


def test_a_shorter_dataset_id_never_touches_a_longer_sibling():
    # CS0007DS099 is a prefix of CS0007DS0999: without the trailing slash the
    # listing would pick up the sibling's files, then archive and delete them.
    short = "filestore/STANDARDISED/CS0007DS099"
    filestore = _filestore({f"{short}/table.csv": b"short", f"{DATASET}/table.csv": b"sibling"})

    assert filestore.list_dataset_version_files("CS0007DS099", VersionType.STANDARDISED) == ["table.csv"]
    _, copied = filestore.archive_dataset_version("CS0007DS099", VersionType.STANDARDISED, STAMP)
    filestore.delete_dataset_version("CS0007DS099", VersionType.STANDARDISED)
    filestore.delete_dataset("CS0007DS099")

    assert copied == ["table.csv"]
    assert filestore.bucket.store[f"{DATASET}/table.csv"] == b"sibling"
    assert not any(key.startswith(f"archive/{DATASET}") for key in filestore.bucket.store)


def test_restore_keeps_current_files_when_the_expected_archive_is_missing():
    filestore = _filestore({f"{DATASET}/table.csv": b"new"})

    try:
        filestore.restore_dataset_version(
            "CS0007DS0999", VersionType.STANDARDISED, ARCHIVE, ["table.csv"]
        )
    except RuntimeError as exc:
        assert "should hold 1 file(s)" in str(exc)
    else:
        raise AssertionError("restoring from a missing archive must not look like success")
    assert filestore.bucket.store == {f"{DATASET}/table.csv": b"new"}


def test_restore_of_a_dataset_that_had_no_files_empties_it():
    # Nothing was archived because nothing was there: restoring means empty again.
    filestore = _filestore({f"{DATASET}/table.csv": b"new"})

    filestore.restore_dataset_version("CS0007DS0999", VersionType.STANDARDISED, ARCHIVE, [])

    assert filestore.bucket.store == {}
