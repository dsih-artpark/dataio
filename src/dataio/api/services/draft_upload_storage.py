"""Persists CSV/digitization-log uploads from the "Generate draft" web form
to local disk. Deliberately not S3: a draft is by definition pre-upload -
the whole point of the review gate is that nothing reaches the real
filestore until a curator approves it. The path returned here is what gets
stored as source_csv_path/digitization_log_path on the draft row, and is
read again on re-validate/regenerate, so it must be stable, not a
transient tempfile.
"""

from __future__ import annotations

import os
import shutil
import uuid
from pathlib import Path

from fastapi import UploadFile

DRAFT_UPLOAD_DIR = os.getenv("DRAFT_UPLOAD_DIR", "data/manifest_draft_uploads")

# A CSV saved by Excel as "CSV UTF-8" carries this 3-byte marker at the
# very start of the file. It's invisible to a human and to pandas (which
# strips it automatically), but Python's stdlib csv module does not - it
# silently glues ﻿ onto the first column's name, so e.g. "state.
# lgd_code" as declared in a manifest no longer matches the actual first
# column header the file contains, byte for byte. Stripping it here, once,
# at intake means no CSV saved to disk (or later validated, uploaded to
# S3, etc.) ever carries one - simpler than making every downstream reader
# BOM-aware individually.
_UTF8_BOM = b"\xef\xbb\xbf"


def save_upload(upload_file: UploadFile) -> str:
    """Writes an uploaded file to DRAFT_UPLOAD_DIR under a UUID-named
    subdirectory (collision-proof) while preserving the original filename
    exactly - the filename's stem is used as the table name in the drafted
    manifest and as the dataset's default title, so it must survive
    untouched rather than being prefixed with a random ID.
    """
    upload_dir = Path(DRAFT_UPLOAD_DIR) / str(uuid.uuid4())
    upload_dir.mkdir(parents=True, exist_ok=True)

    # .name strips any directory components (and yields '' for '.'/'..'),
    # so a crafted filename like '../../etc/passwd' or an absolute path
    # can't escape upload_dir - the client-supplied filename is otherwise
    # untrusted input written straight into a server-side file path.
    original_name = Path(upload_file.filename or "upload").name or "upload"
    dest_path = upload_dir / original_name

    # Streamed in chunks (not upload_file.file.read() then write()) - a
    # large CSV would otherwise be buffered whole in memory before any of
    # it reaches disk. Peek at the first 3 bytes to strip a UTF-8 BOM if
    # present (see _UTF8_BOM above); leave the stream positioned right
    # after it so copyfileobj picks up from there, or rewind to the very
    # start if there was no BOM to strip.
    upload_file.file.seek(0)
    if upload_file.file.read(len(_UTF8_BOM)) != _UTF8_BOM:
        upload_file.file.seek(0)
    with open(dest_path, "wb") as f:
        shutil.copyfileobj(upload_file.file, f)

    return str(dest_path.resolve())
