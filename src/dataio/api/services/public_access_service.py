"""
Public-site actions that go beyond browsing the catalogue: signed-out
downloads of Open datasets, the dataset page preview, and "Request access"
submissions with their admin review.

Access levels: Open download = DOWNLOAD, View only = VIEW, Restricted = NONE.
- Download links are only ever handed out here for DOWNLOAD datasets.
- The preview shows real rows for DOWNLOAD, synthetic rows for VIEW (place
  names, codes and dates kept, numbers randomised, other text hidden), and
  nothing for NONE.
"""

from __future__ import annotations

import hashlib
import html
import os
import random
import re
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import HTTPException

from dataio.api.database.config import Session as DBSession
from dataio.api.database.enums import AccessLevel, ResourceType
from dataio.api.database.models import AccessRequest, Dataset, DatasetDownload, User, UserPermission
from dataio.api.models import VersionType
from dataio.api.services.base_service import BaseService

ACCESS_REQUEST_NOTIFY_EMAIL = os.getenv("ACCESS_REQUEST_NOTIFY_EMAIL", "onehealth@artpark.in")
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")

ORGANISATION_TYPES = (
    "NGO / non-profit",
    "University or research institute",
    "Government",
    "Philanthropy / foundation",
    "Private company",
    "Other",
)
ACCESS_REQUEST_STATUSES = ("PENDING", "APPROVED", "REJECTED")
PURPOSE_MAX_WORDS = 250
PREVIEW_ROWS = 5
# 428 detail when a signed-out download needs the email + terms pop-up first.
CONSENT_REQUIRED = "Enter your email and accept the terms to download."

# Column names whose values are kept as-is in a synthetic preview: places,
# codes and time. Everything else numeric is randomised, other text hidden.
_KEEP_COLUMN = re.compile(
    r"(^|_)(state|district|sub_?district|taluk|tehsil|block|mandal|village|ward|city|town|"
    r"region|zone|division|circle|lgd|code|id|year|month|week|day|date|quarter|season|"
    r"period|time|gender|sex|age_group|category|type|unit|level)(_|$)",
    re.IGNORECASE,
)
_INTEGER = re.compile(r"^-?\d+$")
_DECIMAL = re.compile(r"^-?\d*\.(\d+)$")
HIDDEN_TEXT = "…"


def _access(dataset: Dataset) -> str:
    level = dataset.access_level
    return level.value if hasattr(level, "value") else str(level or "NONE")


def synthetic_rows(dataset_id: str, columns: list[str], rows: list[list[str]]) -> list[list[str]]:
    """Real place/code/date values, random numbers, other text hidden.

    Seeded by the dataset ID so the preview doesn't change on every visit.
    """
    rng = random.Random(hashlib.sha256(dataset_id.encode()).hexdigest())
    keep = [bool(_KEEP_COLUMN.search(name.strip())) for name in columns]
    result = []
    for row in rows:
        out = []
        for index, value in enumerate(row):
            value = value.strip()
            if not value or keep[index]:
                out.append(value)
            elif _INTEGER.match(value):
                out.append(str(rng.randint(0, 120)))
            elif match := _DECIMAL.match(value):
                places = min(len(match.group(1)), 3)
                out.append(f"{rng.uniform(0, 100):.{places}f}")
            else:
                out.append(HIDDEN_TEXT)
        result.append(out)
    return result


def _parse_uuid(value: Optional[str]) -> Optional[uuid.UUID]:
    try:
        return uuid.UUID(str(value)) if value else None
    except ValueError:
        return None


def count_words(text: str) -> int:
    return len(re.findall(r"\S+", text or ""))


class PublicAccessService(BaseService):
    """Open downloads, previews and access requests for the public site."""

    # ------------------------------------------------------------ downloads

    def _get_dataset(self, session, dataset_id: str) -> Dataset:
        dataset = session.query(Dataset).filter(Dataset.ds_id == dataset_id).first()
        if not dataset:
            raise HTTPException(status_code=404, detail="Dataset not found")
        return dataset

    def _resolve_visitor(
        self,
        session,
        email: Optional[str],
        visitor_id: Optional[str],
        accept_terms: bool,
    ) -> tuple[str, uuid.UUID, datetime]:
        """Email, visitor ID and terms-accepted time for a signed-out download.

        A first-time visitor gives an email and ticks the terms box; a returning
        one sends only the visitor ID from their cookie, which is matched to
        their earlier signed-out download. Otherwise 428, and the site shows
        the pop-up.
        """
        parsed_id = _parse_uuid(visitor_id)
        if email and email.strip():
            if not accept_terms:
                raise HTTPException(
                    status_code=400,
                    detail="Please accept the Terms of Use and the dataset's licence to download.",
                )
            return email.strip().lower(), parsed_id or uuid.uuid4(), datetime.now(timezone.utc)
        if parsed_id:
            previous = (
                session.query(DatasetDownload.visitor_email, DatasetDownload.terms_accepted_at)
                .filter(
                    DatasetDownload.visitor_id == parsed_id,
                    DatasetDownload.visitor_email.isnot(None),
                )
                .order_by(DatasetDownload.downloaded_at.desc())
                .first()
            )
            if previous:
                return previous.visitor_email, parsed_id, previous.terms_accepted_at
        raise HTTPException(status_code=428, detail=CONSENT_REQUIRED)

    def get_open_download_urls(
        self,
        dataset_id: str,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        email: Optional[str] = None,
        visitor_id: Optional[str] = None,
        accept_terms: bool = False,
    ) -> dict:
        """Short-lived presigned links for an Open download dataset, no sign-in.

        View only and Restricted datasets answer 403. The visitor must have
        given an email and accepted the terms, now or on an earlier download
        from the same browser (visitor ID); otherwise 428. The download is
        logged with no user (access_channel PUBLIC) but with the visitor's
        email, ID and IP, once there is something to download.
        """
        from dataio.api.services.filestore_service import FilestoreService

        session = DBSession()
        try:
            dataset = self._get_dataset(session, dataset_id)
            if _access(dataset) != AccessLevel.DOWNLOAD.value:
                raise HTTPException(
                    status_code=403,
                    detail="This dataset isn't open for download. Request access to it instead.",
                )
            visitor_email, visitor_uuid, terms_accepted_at = self._resolve_visitor(
                session, email, visitor_id, accept_terms
            )

            filestore = FilestoreService()
            tables = []
            for version_type in (VersionType.STANDARDISED, VersionType.PREPROCESSED):
                try:
                    files = filestore.list_files_in_s3(dataset_id, version_type)
                except Exception:
                    self.logger.warning(
                        "No %s files for %s", version_type.value, dataset_id, exc_info=True
                    )
                    continue
                if files:
                    tables = [
                        {
                            "table_name": f["table_name"],
                            "download_url": f["download_link"],
                            "metadata": f.get("metadata", {}),
                        }
                        for f in files
                    ]
                    break

            if tables:
                try:
                    session.add(
                        DatasetDownload(
                            user_email=None,
                            dataset_id=dataset_id,
                            access_channel="PUBLIC",
                            ip_address=ip_address,
                            user_agent=(user_agent or "")[:500] or None,
                            visitor_email=visitor_email,
                            visitor_id=visitor_uuid,
                            terms_accepted_at=terms_accepted_at,
                        )
                    )
                    session.commit()
                except Exception:
                    session.rollback()
                    self.logger.error("Failed to log public download of %s", dataset_id, exc_info=True)

            return {
                "dataset_id": dataset_id,
                "title": dataset.title,
                "tables": tables,
                "visitor_id": str(visitor_uuid),
            }
        finally:
            session.close()

    # -------------------------------------------------------------- preview

    def get_preview(self, dataset_id: str) -> dict:
        """First rows of the first table: real (Open), synthetic (View only) or none (Restricted)."""
        from dataio.api.services.filestore_service import FilestoreService

        session = DBSession()
        try:
            dataset = self._get_dataset(session, dataset_id)
            access = _access(dataset)
        finally:
            session.close()

        if access == AccessLevel.NONE.value:
            return {"mode": "none", "table": None, "columns": [], "rows": []}

        head = None
        filestore = FilestoreService()
        for version_type in (VersionType.STANDARDISED, VersionType.PREPROCESSED):
            try:
                head = filestore.read_table_head(dataset_id, version_type, max_rows=PREVIEW_ROWS)
            except Exception:
                self.logger.warning("Preview read failed for %s/%s", dataset_id, version_type.value, exc_info=True)
                head = None
            if head:
                break

        if not head:
            return {"mode": "unavailable", "table": None, "columns": [], "rows": []}

        if access == AccessLevel.DOWNLOAD.value:
            return {"mode": "real", **head}
        return {
            "mode": "synthetic",
            "table": head["table"],
            "columns": head["columns"],
            "rows": synthetic_rows(dataset_id, head["columns"], head["rows"]),
        }

    # ------------------------------------------------------ access requests

    def submit_access_request(
        self,
        *,
        dataset_id: str,
        full_name: str,
        email: str,
        organisation: str,
        organisation_type: str,
        organisation_type_other: Optional[str],
        purpose: str,
        consent: bool,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> dict:
        """Store a request, email the team and send the requester a receipt."""
        from dataio.api.services.email_service import EmailService

        full_name = full_name.strip()
        organisation = organisation.strip()
        purpose = purpose.strip()
        other = (organisation_type_other or "").strip() or None
        email = email.strip().lower()

        if organisation_type not in ORGANISATION_TYPES:
            raise HTTPException(status_code=422, detail="Choose an organisation type from the list")
        if organisation_type == "Other" and not other:
            raise HTTPException(status_code=422, detail="Please specify your organisation type")
        if organisation_type != "Other":
            other = None
        if not consent:
            raise HTTPException(status_code=422, detail="Please confirm how you will use the data")
        if count_words(purpose) > PURPOSE_MAX_WORDS:
            raise HTTPException(
                status_code=422, detail=f"Please keep the description to {PURPOSE_MAX_WORDS} words or fewer"
            )

        session = DBSession()
        try:
            dataset = self._get_dataset(session, dataset_id)
            if _access(dataset) == AccessLevel.DOWNLOAD.value:
                raise HTTPException(
                    status_code=400, detail="This dataset is open: you can download it without asking."
                )
            request_row = AccessRequest(
                id=uuid.uuid4(),
                dataset_id=dataset_id,
                full_name=full_name,
                email=email,
                organisation=organisation,
                organisation_type=organisation_type,
                organisation_type_other=other,
                purpose=purpose,
                consent=True,
                status="PENDING",
                ip_address=ip_address,
                user_agent=(user_agent or "")[:500] or None,
            )
            session.add(request_row)
            session.commit()
            request_id = str(request_row.id)
            title = dataset.title
        except HTTPException:
            session.rollback()
            raise
        except Exception:
            session.rollback()
            self.logger.error("Failed to store access request for %s", dataset_id, exc_info=True)
            raise HTTPException(status_code=500, detail="Couldn't save your request. Please try again.")
        finally:
            session.close()

        # Emails are best effort: the request is saved and listed in the admin panel either way.
        email_service = EmailService()
        org_line = organisation_type if not other else f"{organisation_type}: {other}"
        fields = [
            ("Dataset", f"{title} ({dataset_id})"),
            ("Name", full_name),
            ("Email", email),
            ("Organisation", f"{organisation} ({org_line})"),
            ("How they will use it", purpose),
        ]
        admin_link = f"{FRONTEND_URL}/admin/access-requests"
        team_text = "\n".join(f"{label}: {value}" for label, value in fields) + f"\n\nReview: {admin_link}\n"
        team_html = (
            "<p>New access request on the One Health data commons.</p><table>"
            + "".join(
                f"<tr><th align='left' valign='top'>{html.escape(label)}</th>"
                f"<td>{html.escape(value).replace(chr(10), '<br>')}</td></tr>"
                for label, value in fields
            )
            + f"</table><p><a href='{html.escape(admin_link)}'>Review it in the admin panel</a></p>"
        )
        try:
            email_service.send_email(
                to_email=ACCESS_REQUEST_NOTIFY_EMAIL,
                subject=f"Access request: {title} ({dataset_id})",
                html_body=team_html,
                text_body=team_text,
            )
            receipt = (
                f"Hi {full_name},\n\nWe've received your request for access to “{title}” ({dataset_id}). "
                "We usually reply within 3 working days.\n\n"
                f"If you have questions, write to {ACCESS_REQUEST_NOTIFY_EMAIL}.\n"
            )
            email_service.send_email(
                to_email=email,
                subject=f"We received your request: {title}",
                html_body="<p>" + html.escape(receipt).replace("\n\n", "</p><p>").replace("\n", "<br>") + "</p>",
                text_body=receipt,
            )
        except Exception:
            self.logger.error("Failed to send access request emails for %s", request_id, exc_info=True)

        return {"submitted": True, "id": request_id}

    @staticmethod
    def _request_dict(row: AccessRequest, title: Optional[str]) -> dict:
        return {
            "id": str(row.id),
            "dataset_id": row.dataset_id,
            "dataset_title": title,
            "full_name": row.full_name,
            "email": row.email,
            "organisation": row.organisation,
            "organisation_type": row.organisation_type,
            "organisation_type_other": row.organisation_type_other,
            "purpose": row.purpose,
            "status": row.status,
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "reviewed_at": row.reviewed_at.isoformat() if row.reviewed_at else None,
            "reviewed_by": row.reviewed_by,
            "review_note": row.review_note,
        }

    def list_access_requests(self, status: Optional[str] = None, limit: int = 100, offset: int = 0) -> dict:
        """Requests for the admin panel, newest first, with the dataset title."""
        if status and status not in ACCESS_REQUEST_STATUSES:
            raise HTTPException(status_code=422, detail="Unknown status")
        limit = max(1, min(limit, 500))
        session = DBSession()
        try:
            query = session.query(AccessRequest, Dataset.title).outerjoin(
                Dataset, Dataset.ds_id == AccessRequest.dataset_id
            )
            if status:
                query = query.filter(AccessRequest.status == status)
            total = query.count()
            rows = query.order_by(AccessRequest.created_at.desc()).offset(offset).limit(limit).all()
            pending = session.query(AccessRequest).filter(AccessRequest.status == "PENDING").count()
            return {
                "requests": [self._request_dict(row, title) for row, title in rows],
                "total": total,
                "pending": pending,
            }
        finally:
            session.close()

    def review_access_request(
        self,
        admin_user: User,
        request_id: str,
        approve: bool,
        note: Optional[str] = None,
    ) -> dict:
        """Approve (add the requester to the dataset's access list) or reject.

        Approving gives DOWNLOAD permission on the dataset. A requester with no
        account is invited first (magic link email), then granted access.
        """
        from dataio.api.services.email_service import EmailService
        from dataio.api.services.web_admin_service import WebAdminService

        if not admin_user.is_admin:
            raise HTTPException(status_code=403, detail="Admin access required")
        try:
            request_uuid = uuid.UUID(request_id)
        except ValueError:
            raise HTTPException(status_code=404, detail="Request not found")

        session = DBSession()
        try:
            row = session.query(AccessRequest).filter(AccessRequest.id == request_uuid).first()
            if not row:
                raise HTTPException(status_code=404, detail="Request not found")
            if row.status != "PENDING":
                raise HTTPException(status_code=409, detail=f"Request already {row.status.lower()}")
            dataset = session.query(Dataset).filter(Dataset.ds_id == row.dataset_id).first()
            if approve and not dataset:
                raise HTTPException(status_code=404, detail="The dataset no longer exists")
            email, full_name, dataset_id = row.email, row.full_name, row.dataset_id
            title = dataset.title if dataset else dataset_id
            has_account = session.query(User).filter(User.email == email).first() is not None
        finally:
            session.close()

        invited = False
        if approve:
            admin_service = WebAdminService()
            if not has_account:
                admin_service.invite_user(admin_user, email, display_name=full_name)
                invited = True
            session = DBSession()
            try:
                existing = (
                    session.query(UserPermission)
                    .filter(
                        UserPermission.user_email == email,
                        UserPermission.resource_type == ResourceType.DATASET,
                        UserPermission.resource_id == dataset_id,
                    )
                    .first()
                )
                if existing:
                    existing.permission = AccessLevel.DOWNLOAD
                else:
                    session.add(
                        UserPermission(
                            user_email=email,
                            resource_type=ResourceType.DATASET,
                            resource_id=dataset_id,
                            permission=AccessLevel.DOWNLOAD,
                        )
                    )
                session.commit()
            except Exception:
                session.rollback()
                self.logger.error("Failed to grant %s on %s", email, dataset_id, exc_info=True)
                raise HTTPException(status_code=500, detail="Couldn't grant access")
            finally:
                session.close()

        session = DBSession()
        try:
            row = session.query(AccessRequest).filter(AccessRequest.id == request_uuid).first()
            row.status = "APPROVED" if approve else "REJECTED"
            row.reviewed_at = datetime.now(timezone.utc)
            row.reviewed_by = admin_user.email
            row.review_note = (note or "").strip() or None
            session.commit()
            result = self._request_dict(row, title)
        finally:
            session.close()

        # Tell the requester. A new account already got the invitation email.
        try:
            if approve:
                body = (
                    f"Hi {full_name},\n\nYour request for “{title}” ({dataset_id}) has been approved. "
                    + (
                        "We've sent you a separate email to set up your account; once signed in you can download it."
                        if invited
                        else f"Sign in to download it: {FRONTEND_URL}/login"
                    )
                    + "\n"
                )
                subject = f"Access approved: {title}"
            else:
                body = (
                    f"Hi {full_name},\n\nThank you for your interest in “{title}” ({dataset_id}). "
                    "We're not able to share this dataset with you at the moment."
                    + (f"\n\n{note.strip()}" if note and note.strip() else "")
                    + f"\n\nIf you have questions, write to {ACCESS_REQUEST_NOTIFY_EMAIL}.\n"
                )
                subject = f"About your request: {title}"
            EmailService().send_email(
                to_email=email,
                subject=subject,
                html_body="<p>" + html.escape(body).replace("\n\n", "</p><p>").replace("\n", "<br>") + "</p>",
                text_body=body,
            )
        except Exception:
            self.logger.error("Failed to email requester for %s", request_id, exc_info=True)

        return {**result, "invited": invited}
