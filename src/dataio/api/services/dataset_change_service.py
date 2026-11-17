"""Dataset changelog entries (DIO-53): admins record them as drafts and
publish them; the public site shows published entries.

Entries are checked by the pydantic models in dataset_changes.py before they
are saved. Public output is built from those models, so internal columns
(archive_prefix, status, who did what) can't reach the public site.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import HTTPException

from dataio.api.database.config import Session as DBSession
from dataio.api.database.enums import AccessLevel
from dataio.api.database.models import Dataset, DatasetChangeRecord, User
from dataio.api.services.base_service import BaseService
from dataio.api.services.dataset_changes import Change, DatasetChangeInput

CHANGE_STATUSES = ("DRAFT", "PUBLISHED")


def _iso(value) -> Optional[str]:
    return value.isoformat() if value else None


def _require_admin(user: User) -> None:
    if not user.is_admin:
        raise HTTPException(status_code=403, detail="Admin access required")


def _change_uuid(change_id: str) -> uuid.UUID:
    try:
        return uuid.UUID(str(change_id))
    except ValueError:
        raise HTTPException(status_code=404, detail="Change entry not found")


def _check_paging(offset: int) -> None:
    if offset < 0:
        # PostgreSQL rejects a negative OFFSET, which would surface as a 500.
        raise HTTPException(status_code=400, detail="offset must be 0 or more")


class DatasetChangeService(BaseService):
    # ------------------------------------------------------------ admin

    @staticmethod
    def _admin_dict(row: DatasetChangeRecord, title: Optional[str]) -> dict:
        return {
            "id": str(row.id),
            "dataset_id": row.dataset_id,
            "dataset_title": title,
            "date": _iso(row.change_date),
            "type": row.change_type,
            "breaking": row.breaking,
            "summary": row.summary,
            "reason": row.reason,
            "changes": row.changes,
            "status": row.status,
            "archive_prefix": row.archive_prefix,
            "created_by": row.created_by,
            "created_at": _iso(row.created_at),
            "updated_by": row.updated_by,
            "updated_at": _iso(row.updated_at),
            "published_by": row.published_by,
            "published_at": _iso(row.published_at),
        }

    @staticmethod
    def _apply(row: DatasetChangeRecord, body: DatasetChangeInput) -> None:
        row.change_date = body.date or datetime.now(timezone.utc).date()
        row.change_type = body.type
        row.breaking = body.breaking
        row.summary = body.summary
        row.reason = body.reason
        row.changes = [change.model_dump() for change in body.changes]

    def _admin_result(self, session, row: DatasetChangeRecord) -> dict:
        title = session.query(Dataset.title).filter(Dataset.ds_id == row.dataset_id).scalar()
        return self._admin_dict(row, title)

    def create_draft(
        self,
        admin_user: User,
        dataset_id: str,
        body: DatasetChangeInput,
        archive_prefix: Optional[str] = None,
    ) -> dict:
        _require_admin(admin_user)
        session = DBSession()
        try:
            if not session.query(Dataset.ds_id).filter(Dataset.ds_id == dataset_id).first():
                raise HTTPException(status_code=404, detail="Dataset not found")
            row = DatasetChangeRecord(
                dataset_id=dataset_id,
                status="DRAFT",
                archive_prefix=archive_prefix,
                created_by=admin_user.email,
            )
            self._apply(row, body)
            session.add(row)
            session.commit()
            session.refresh(row)
            return self._admin_result(session, row)
        finally:
            session.close()

    def _locked_row(self, session, change_id: str) -> DatasetChangeRecord:
        row = (
            session.query(DatasetChangeRecord)
            .filter(DatasetChangeRecord.id == _change_uuid(change_id))
            .with_for_update()
            .first()
        )
        if not row:
            raise HTTPException(status_code=404, detail="Change entry not found")
        return row

    def update(self, admin_user: User, change_id: str, body: DatasetChangeInput) -> dict:
        """Edit a draft or a published entry; corrections record who made them."""
        _require_admin(admin_user)
        session = DBSession()
        try:
            row = self._locked_row(session, change_id)
            self._apply(row, body)
            row.updated_by = admin_user.email
            row.updated_at = datetime.now(timezone.utc)
            session.commit()
            session.refresh(row)
            return self._admin_result(session, row)
        finally:
            session.close()

    def publish(self, admin_user: User, change_id: str) -> dict:
        """Make a draft public. The row lock makes two admins publishing at
        once publish it once; the second gets 409."""
        _require_admin(admin_user)
        session = DBSession()
        try:
            row = self._locked_row(session, change_id)
            if row.status == "PUBLISHED":
                raise HTTPException(status_code=409, detail="Already published")
            row.status = "PUBLISHED"
            row.published_by = admin_user.email
            row.published_at = datetime.now(timezone.utc)
            session.commit()
            session.refresh(row)
            return self._admin_result(session, row)
        finally:
            session.close()

    def delete_draft(self, admin_user: User, change_id: str) -> None:
        """Delete a draft. Published entries can only be corrected."""
        _require_admin(admin_user)
        session = DBSession()
        try:
            row = self._locked_row(session, change_id)
            if row.status == "PUBLISHED":
                raise HTTPException(status_code=409, detail="Published entries can't be deleted, only edited")
            session.delete(row)
            session.commit()
        finally:
            session.close()

    def list_admin(
        self,
        admin_user: User,
        status: Optional[str] = None,
        dataset_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict:
        """All entries for the admin panel, newest first, with the dataset title."""
        _require_admin(admin_user)
        if status and status not in CHANGE_STATUSES:
            raise HTTPException(status_code=422, detail="Unknown status")
        _check_paging(offset)
        limit = max(1, min(limit, 500))
        session = DBSession()
        try:
            query = session.query(DatasetChangeRecord, Dataset.title).outerjoin(
                Dataset, Dataset.ds_id == DatasetChangeRecord.dataset_id
            )
            if status:
                query = query.filter(DatasetChangeRecord.status == status)
            if dataset_id:
                query = query.filter(DatasetChangeRecord.dataset_id == dataset_id)
            total = query.count()
            rows = (
                query.order_by(DatasetChangeRecord.change_date.desc(), DatasetChangeRecord.created_at.desc())
                .offset(offset)
                .limit(limit)
                .all()
            )
            drafts = session.query(DatasetChangeRecord).filter(DatasetChangeRecord.status == "DRAFT").count()
            return {
                "entries": [self._admin_dict(row, title) for row, title in rows],
                "total": total,
                "drafts": drafts,
            }
        finally:
            session.close()

    # ----------------------------------------------------------- public

    @staticmethod
    def _public_dict(row: DatasetChangeRecord, dataset: Dataset) -> dict:
        """Only the agreed public fields. For Restricted (NONE) datasets the
        old/new values and notes are hidden, decided on every read so a later
        access-level change applies to older entries too."""
        restricted = dataset.access_level == AccessLevel.NONE
        changes = []
        for change in row.changes or []:
            shown = Change.model_validate(change).model_dump()
            if restricted:
                shown.update(old=None, new=None, note=None)
            changes.append(shown)
        return {
            "id": str(row.id),
            "dataset_id": row.dataset_id,
            "dataset_title": dataset.title,
            "restricted": restricted,
            "date": _iso(row.change_date),
            "type": row.change_type,
            "breaking": row.breaking,
            "summary": row.summary,
            "reason": row.reason,
            "changes": changes,
        }

    def _public_query(self, session):
        # Inner join: entries whose dataset was deleted are not shown.
        return (
            session.query(DatasetChangeRecord, Dataset)
            .join(Dataset, Dataset.ds_id == DatasetChangeRecord.dataset_id)
            .filter(DatasetChangeRecord.status == "PUBLISHED")
            .order_by(DatasetChangeRecord.change_date.desc(), DatasetChangeRecord.published_at.desc())
        )

    def list_public(self, dataset_id: Optional[str] = None, limit: int = 20, offset: int = 0) -> dict:
        """Published entries for the "Dataset changes" page, newest first."""
        _check_paging(offset)
        limit = max(1, min(limit, 100))
        session = DBSession()
        try:
            query = self._public_query(session)
            if dataset_id:
                query = query.filter(DatasetChangeRecord.dataset_id == dataset_id)
            total = query.count()
            rows = query.offset(offset).limit(limit).all()
            return {"entries": [self._public_dict(row, ds) for row, ds in rows], "total": total}
        finally:
            session.close()

    def list_public_for_dataset(self, dataset_id: str) -> dict:
        """A dataset page's "Version history": all its published entries."""
        session = DBSession()
        try:
            if not session.query(Dataset.ds_id).filter(Dataset.ds_id == dataset_id).first():
                raise HTTPException(status_code=404, detail="Dataset not found")
            rows = self._public_query(session).filter(DatasetChangeRecord.dataset_id == dataset_id).all()
            return {"entries": [self._public_dict(row, ds) for row, ds in rows], "total": len(rows)}
        finally:
            session.close()
