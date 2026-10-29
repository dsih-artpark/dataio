"""
Indexes the shapefiles uploaded before versioning as version 'legacy'.

Migration 026 adds the shapefiles index; the files already in S3 at
shapefiles/<parent_id>/<region_id>.geojson.gz aren't in it until this runs.
Run it once after the migration. Re-running is harmless: indexed files are
skipped. Nothing in S3 is changed.
"""

from __future__ import annotations

import argparse

from dataio.api.database import functions as database
from dataio.api.services.filestore_service import FilestoreService
from dataio.api.services.shapefile_service import LEGACY_VERSION


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="shapefile-backfill",
        description="Index the pre-versioning shapefiles in S3 as version 'legacy'.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only report what would be indexed; write nothing.",
    )
    return parser


def main(argv: list[str] | None = None, filestore: FilestoreService | None = None, db=database) -> int:
    args = build_parser().parse_args(argv)
    if db.get_shapefile_version(LEGACY_VERSION) is None:
        print("Version 'legacy' doesn't exist: run migration 026 first.")
        return 1

    files = (filestore or FilestoreService()).list_legacy_shapefiles()
    rows, unknown, moved = [], [], []
    for f in files:
        region = db.get_region(f["region_id"])
        if region is None:
            unknown.append(f["key"])
            continue
        if (region.parent_region_id or "") != f["parent_id"]:
            # Served from its key regardless; noted because the folder no
            # longer matches the region's parent.
            moved.append(f"{f['key']} (parent is now {region.parent_region_id})")
        rows.append({
            "region_id": f["region_id"],
            "version": LEGACY_VERSION,
            "s3_key": f["key"],
            "size_bytes": f["size"],
            "uploaded_at": f["last_modified"],
        })

    print(f"Legacy shapefiles in S3: {len(files)}; indexable: {len(rows)}")
    if args.dry_run:
        print("Dry run: nothing was written.")
    else:
        added = db.add_shapefile_records(rows)
        print(f"Indexed {added} new; {len(rows) - added} were already indexed.")
    if moved:
        print(f"\n{len(moved)} file(s) sit under a different parent than the regions table says:")
        for line in moved:
            print(f"  {line}")
    if unknown:
        print(f"\n{len(unknown)} file(s) skipped: region id not in the regions table:")
        for key in unknown:
            print(f"  {key}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
