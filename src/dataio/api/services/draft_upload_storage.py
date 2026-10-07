"""Persists CSV/digitization-log uploads from the "Generate draft" web form
to local disk. Deliberately not S3: a draft is by definition pre-upload -
the whole point of the review gate is that nothing reaches the real
filestore until a curator approves it. The path returned here is what gets
stored as source_csv_path/digitization_log_path on the draft row, and is
read again on re-validate/regenerate, so it must be stable, not a
transient tempfile.
"""

from __future__ import annotations

import contextlib
import logging
import os
import shutil
import uuid
from pathlib import Path

from fastapi import UploadFile

def resolve_upload_dir(configured: str) -> str:
    """A relative DRAFT_UPLOAD_DIR is resolved against the home directory,
    not the working directory: the deploy script deletes and re-clones the
    checkout the app runs from, so files kept inside it would be lost on
    every deploy."""
    path = Path(configured).expanduser()
    return str(path if path.is_absolute() else Path.home() / path)


DRAFT_UPLOAD_DIR = resolve_upload_dir(os.getenv("DRAFT_UPLOAD_DIR", "data/manifest_draft_uploads"))
logging.getLogger(__name__).info("Draft uploads are stored under %s", DRAFT_UPLOAD_DIR)

# A CSV saved by Excel as "CSV UTF-8" carries this 3-byte marker at the
# very start of the file. It's invisible to a human and to pandas (which
# strips it automatically), but Python's stdlib csv module does not - it
# silently glues U+FEFF onto the first column's name, so e.g. "state.
# lgd_code" as declared in a manifest no longer matches the actual first
# column header the file contains, byte for byte. Stripping it here, once,
# at intake means no CSV saved to disk (or later validated, uploaded to
# S3, etc.) ever carries one - simpler than making every downstream reader
# BOM-aware individually.
_UTF8_BOM = b"\xef\xbb\xbf"


def skip_utf8_bom(fileobj) -> None:
    """Rewinds a seekable binary stream to its start, then past a UTF-8 BOM
    if it begins with one, so whatever reads on gets the content without it."""
    fileobj.seek(0)
    if fileobj.read(len(_UTF8_BOM)) != _UTF8_BOM:
        fileobj.seek(0)


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
    # it reaches disk. copyfileobj picks up after any UTF-8 BOM (see
    # _UTF8_BOM above).
    skip_utf8_bom(upload_file.file)
    with open(dest_path, "wb") as f:
        shutil.copyfileobj(upload_file.file, f)

    return str(dest_path.resolve())


def copy_into_storage(source_path: str) -> str:
    """Copies an already-stored draft file into its own UUID directory, so a
    regenerated draft owns its inputs instead of sharing the original's -
    deleting either draft then can't remove the other's CSVs."""
    upload_dir = Path(DRAFT_UPLOAD_DIR) / str(uuid.uuid4())
    upload_dir.mkdir(parents=True, exist_ok=True)
    dest_path = upload_dir / Path(source_path).name
    shutil.copyfile(source_path, dest_path)
    return str(dest_path.resolve())


def is_managed_path(path: str) -> bool:
    """True only for files this module wrote under DRAFT_UPLOAD_DIR. Drafts
    created by the CLI store the operator's own source paths, which must
    never be deleted on the server's behalf."""
    try:
        return Path(path).resolve().is_relative_to(Path(DRAFT_UPLOAD_DIR).resolve())
    except (OSError, ValueError):
        return False


def delete_managed_file(path: str) -> None:
    """Best-effort removal of a stored draft file and its (then empty) UUID
    directory. Paths outside DRAFT_UPLOAD_DIR are left alone."""
    if not path or not is_managed_path(path):
        return
    resolved = Path(path).resolve()
    try:
        resolved.unlink(missing_ok=True)
    except OSError:
        # Callers run this mid-delete or inside error handling; a file that
        # can't be removed must not abort them.
        logging.getLogger(__name__).warning(
            "Could not delete draft file %s", resolved, exc_info=True
        )
        return
    with contextlib.suppress(OSError):
        resolved.parent.rmdir()
