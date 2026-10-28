"""
Lists dataset archive copies older than a cut-off, so they can be reviewed for removal.

A dataset replace copies the old files to archive/filestore/<version>/<dataset_id>/<stamp>/
and keeps them for a month. This report is read-only: it never deletes anything.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from dataio.api.services.filestore_service import FilestoreService

STAMP_FORMAT = "%Y%m%dT%H%M%SZ"


def parse_stamp(stamp: str) -> datetime | None:
    try:
        return datetime.strptime(stamp, STAMP_FORMAT).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def format_size(size: int) -> str:
    for unit in ("B", "KB", "MB"):
        if size < 1024:
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="archive-report",
        description=(
            "List dataset archive copies older than a cut-off. Read-only: nothing is deleted."
        ),
    )
    parser.add_argument(
        "--older-than-days",
        type=int,
        default=30,
        help="Only list archives at least this many days old (default: 30).",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="List every archive, whatever its age.",
    )
    return parser


def main(argv: list[str] | None = None, filestore: FilestoreService | None = None,
         now: datetime | None = None) -> int:
    args = build_parser().parse_args(argv)
    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=args.older_than_days)
    archives, unrecognised = (filestore or FilestoreService()).list_archives()

    rows = []
    for archive in archives:
        archived_on = parse_stamp(archive["stamp"])
        # An unreadable stamp is always listed: its age can't be known.
        if args.all or archived_on is None or archived_on <= cutoff:
            rows.append((archive, archived_on))

    scope = "all archives" if args.all else f"archives at least {args.older_than_days} days old"
    print(f"Dataset archive copies: {scope} ({len(rows)} of {len(archives)})")
    if rows:
        header = f"{'Dataset':<16}{'Version':<14}{'Archived on':<13}{'Age':>9}{'Files':>7}{'Size':>11}  Folder"
        print(header)
        for archive, archived_on in rows:
            date = archived_on.strftime("%Y-%m-%d") if archived_on else "unknown"
            age = f"{(now - archived_on).days} days" if archived_on else "?"
            print(
                f"{archive['dataset_id']:<16}{archive['version_type']:<14}{date:<13}{age:>9}"
                f"{archive['files']:>7}{format_size(archive['size']):>11}  {archive['prefix']}/"
            )
        print(f"Total: {sum(a['files'] for a, _ in rows)} files, "
              f"{format_size(sum(a['size'] for a, _ in rows))}")
    if unrecognised:
        print(f"\n{len(unrecognised)} file(s) under archive/filestore/ don't match "
              "archive/filestore/<version>/<dataset_id>/<stamp>/<file>:")
        for key in unrecognised:
            print(f"  {key}")
    print("\nRead-only: nothing was deleted.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
