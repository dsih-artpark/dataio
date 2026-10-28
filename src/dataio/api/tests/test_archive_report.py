from __future__ import annotations

from datetime import datetime, timezone

from dataio.api.archive_report import main


class _FakeFilestore:
    def __init__(self, archives, unrecognised=()):
        self.archives = archives
        self.unrecognised = list(unrecognised)

    def list_archives(self):
        return self.archives, self.unrecognised


def _archive(dataset_id: str, stamp: str, files: int = 1, size: int = 2048) -> dict:
    prefix = f"archive/filestore/STANDARDISED/{dataset_id}/{stamp}"
    return {"prefix": prefix, "version_type": "STANDARDISED", "dataset_id": dataset_id,
            "stamp": stamp, "files": files, "size": size}


NOW = datetime(2026, 11, 20, tzinfo=timezone.utc)
OLD = _archive("CS0007DS0999", "20261016T064317Z", files=4, size=3 * 1024 * 1024)
RECENT = _archive("CS0007DS0998", "20261110T000000Z")
BAD_STAMP = _archive("CS0007DS0997", "not-a-stamp")


def test_lists_only_archives_past_the_cutoff(capsys):
    assert main([], filestore=_FakeFilestore([OLD, RECENT]), now=NOW) == 0

    out = capsys.readouterr().out
    assert "(1 of 2)" in out
    assert "CS0007DS0999" in out and "2026-10-16" in out and "34 days" in out and "3.0 MB" in out
    assert "CS0007DS0998" not in out
    assert "Read-only: nothing was deleted." in out


def test_all_lists_every_archive(capsys):
    main(["--all"], filestore=_FakeFilestore([OLD, RECENT]), now=NOW)

    out = capsys.readouterr().out
    assert "(2 of 2)" in out and "CS0007DS0998" in out


def test_older_than_days_moves_the_cutoff(capsys):
    main(["--older-than-days", "5"], filestore=_FakeFilestore([OLD, RECENT]), now=NOW)

    assert "(2 of 2)" in capsys.readouterr().out


def test_an_unreadable_stamp_is_always_listed(capsys):
    main([], filestore=_FakeFilestore([BAD_STAMP]), now=NOW)

    out = capsys.readouterr().out
    assert "CS0007DS0997" in out and "unknown" in out


def test_keys_outside_the_layout_are_reported(capsys):
    main([], filestore=_FakeFilestore([], ["archive/filestore/STANDARDISED/stray.csv"]), now=NOW)

    out = capsys.readouterr().out
    assert "(0 of 0)" in out
    assert "archive/filestore/STANDARDISED/stray.csv" in out
