"""
Public-site actions that go beyond browsing the catalogue: signed-out
downloads of Open datasets, the dataset page preview, and "Request access"
submissions with their admin review.

Access levels: Open download = DOWNLOAD, View only = VIEW, Restricted = NONE.
- Download links are only ever handed out here for DOWNLOAD datasets.
- The preview shows real rows for DOWNLOAD, synthetic rows for VIEW and
  nothing for NONE. Synthetic rows are made up from the v2 data dictionary
  alone (types, allowed values, ranges, date formats); the data files are
  never read for them, so no real value can reach a View only preview.
"""

from __future__ import annotations

import hashlib
import html
import math
import os
import random
import re
import uuid
from datetime import date, datetime, timezone
from typing import Optional

from fastapi import HTTPException
from sqlalchemy import func

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

# Shown for free-text and untyped columns in a synthetic preview.
SAMPLE_TEXT = "Sample text"
# Region level guessed from a column name such as "district.ID" or "lgd_village_code".
_REGION_LEVEL = re.compile(
    r"(village|sub_?district|district|state|ward|block|taluk|tehsil|city|ulb|zone|country)",
    re.IGNORECASE,
)
_DEFAULT_START = date(2020, 1, 1)
_DEFAULT_END = date(2024, 12, 31)


def _access(dataset: Dataset) -> str:
    level = dataset.access_level
    return level.value if hasattr(level, "value") else str(level or "NONE")


def _number(value, default: float) -> float:
    """``value`` as a finite number, or ``default`` when it isn't one ("n/a", "inf")."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _bounds(field: dict) -> tuple[float, float]:
    """Lowest and highest value for a made-up number; odd dictionary values fall back to 0-100."""
    span = field.get("range")
    if isinstance(span, list) and len(span) == 2:
        low = _number(span[0], 0.0)
        return low, max(low, _number(span[1], low + 100))
    low = _number(field.get("min"), 0.0)
    return low, max(low, _number(field.get("max"), low + 100))


def _fake_value(rng: random.Random, name: str, field: dict, enums: dict, start: date, end: date) -> str:
    kind = field.get("type")
    if kind == "enum":
        enum = enums.get(field.get("enumRef")) if field.get("enumRef") else None
        values = field.get("allowedValues") or sorted(((enum or {}).get("values") or {}).keys())
        return str(rng.choice(values)) if values else SAMPLE_TEXT
    if kind == "int":
        low, high = _bounds(field)
        return str(rng.randint(math.ceil(low), max(math.ceil(low), math.floor(high))))
    if kind == "float":
        low, high = _bounds(field)
        return f"{rng.uniform(low, high):.2f}"
    if kind == "boolean":
        return rng.choice(["true", "false"])
    if kind in ("date", "dateTime"):
        day = date.fromordinal(rng.randint(start.toordinal(), end.toordinal()))
        moment = datetime(day.year, day.month, day.day, rng.randint(0, 23), rng.randint(0, 59), tzinfo=timezone.utc)
        return moment.strftime(field.get("format") or "%Y-%m-%d")
    if kind in ("regionID", "regionName"):
        match = _REGION_LEVEL.search(name)
        level = match.group(1).lower().replace("sub_", "sub") if match else "region"
        return f"{level}_{rng.randint(100, 999)}" if kind == "regionID" else f"Sample {level}"
    return SAMPLE_TEXT


def synthetic_rows(
    dataset_id: str,
    fields: dict[str, dict],
    enums: dict | None = None,
    start: Optional[date] = None,
    end: Optional[date] = None,
    count: int = PREVIEW_ROWS,
) -> list[list[str]]:
    """Made-up rows that follow a data dictionary; no real value is ever used.

    Numbers fall inside the column's range, enums use the allowed values,
    dates fall inside the dataset's time coverage, places get sample names
    and IDs, and free text is SAMPLE_TEXT. Seeded by the dataset ID so the
    preview doesn't change on every visit.
    """
    rng = random.Random(hashlib.sha256(dataset_id.encode()).hexdigest())
    start = start or _DEFAULT_START
    end = end if end and end >= start else max(start, _DEFAULT_END)
    enums = enums if isinstance(enums, dict) else {}
    return [
        [_fake_value(rng, name, field if isinstance(field, dict) else {}, enums, start, end) for name, field in fields.items()]
        for _ in range(count)
    ]


def _synthetic_preview(dataset: Dataset) -> Optional[dict]:
    """Synthetic preview of the first table, or None without a typed v2 data dictionary."""
    from dataio.api.services.public_catalogue_service import _manifest_tables

    tables = _manifest_tables(dataset) or {}
    for table_name, table in tables.items():
        dictionary = table.get("dataDictionary") if isinstance(table, dict) else None
        if not isinstance(dictionary, dict) or not dictionary:
            continue
        if not any(isinstance(field, dict) and field.get("type") for field in dictionary.values()):
            return None
        manifest = dataset.manifest_json if isinstance(dataset.manifest_json, dict) else {}
        return {
            "mode": "synthetic",
            "table": table_name,
            "columns": list(dictionary.keys()),
            "rows": synthetic_rows(
                dataset.ds_id,
                dictionary,
                manifest.get("enumDefinitions"),
                dataset.temporal_coverage_start_date,
                dataset.temporal_coverage_end_date,
            ),
        }
    return None


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
            # Files first: with nothing to download, say so before asking for an
            # email, so the pop-up only appears when consent leads to a download
            # (and is recorded with it).
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
            if not tables:
                raise HTTPException(
                    status_code=404,
                    detail="No data files are available for this dataset yet. Please try again later.",
                )

            visitor_email, visitor_uuid, terms_accepted_at = self._resolve_visitor(
                session, email, visitor_id, accept_terms
            )
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
        """First rows of the first table: real (Open), synthetic (View only) or none (Restricted).

        Only Open datasets read the data file; View only previews are made up
        from the data dictionary.
        """
        from dataio.api.services.filestore_service import FilestoreService

        unavailable = {"mode": "unavailable", "table": None, "columns": [], "rows": []}
        session = DBSession()
        try:
            dataset = self._get_dataset(session, dataset_id)
            access = _access(dataset)
            if access == AccessLevel.NONE.value:
                return {"mode": "none", "table": None, "columns": [], "rows": []}
            if access != AccessLevel.DOWNLOAD.value:
                return _synthetic_preview(dataset) or unavailable
        finally:
            session.close()

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
            return unavailable
        return {"mode": "real", **head}

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
        honeypot: Optional[str] = None,
    ) -> dict:
        """Store a request, email the team and send the requester a receipt.

        Anyone can type any email address here, so the form must not become a
        way to send our email to strangers: the receipt carries no text the
        sender chose (not even their name), a filled-in honeypot field is
        dropped, and a repeat of a request still pending for the same email
        and dataset sends nothing. Both of those still answer "submitted", so
        neither a bot nor someone probing an address can tell.
        """
        from dataio.api.services.email_service import EmailService

        if honeypot and honeypot.strip():
            self.logger.info("Access request for %s dropped: honeypot filled", dataset_id)
            return {"submitted": True, "id": str(uuid.uuid4())}

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
            pending = (
                session.query(AccessRequest.id)
                .filter(
                    AccessRequest.dataset_id == dataset_id,
                    AccessRequest.email == email,
                    AccessRequest.status == "PENDING",
                )
                .first()
            )
            if pending:
                self.logger.info("Repeat access request for %s ignored: one is pending", dataset_id)
                return {"submitted": True, "id": str(uuid.uuid4())}
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
            # No name or other text from the form: see the docstring.
            receipt = (
                f"Hi,\n\nWe've received your request for access to “{title}” ({dataset_id}). "
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
        if offset < 0:
            # PostgreSQL rejects a negative OFFSET, which surfaced as a 500.
            raise HTTPException(status_code=400, detail="offset must be 0 or more")
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

        # One transaction holding a lock on the request: if two admins review it
        # at the same time, the second waits, then finds it no longer PENDING
        # (409) instead of granting, inviting and emailing a second time.
        # Anything that fails before the commit leaves the request PENDING.
        invited = False
        session = DBSession()
        try:
            row = (
                session.query(AccessRequest)
                .filter(AccessRequest.id == request_uuid)
                .with_for_update()
                .first()
            )
            if not row:
                raise HTTPException(status_code=404, detail="Request not found")
            if row.status != "PENDING":
                raise HTTPException(status_code=409, detail=f"Request already {row.status.lower()}")
            dataset = session.query(Dataset).filter(Dataset.ds_id == row.dataset_id).first()
            if approve and not dataset:
                raise HTTPException(status_code=404, detail="The dataset no longer exists")
            email, full_name, dataset_id = row.email, row.full_name, row.dataset_id
            title = dataset.title if dataset else dataset_id

            if approve:
                # Request emails are stored lower-cased; an account may have capitals.
                account = session.query(User).filter(func.lower(User.email) == email.lower()).first()
                if account:
                    # Grant to the account's own spelling, which its permission checks use.
                    email = account.email
                else:
                    # Only built when needed: it needs the S3 settings.
                    WebAdminService().invite_user(admin_user, email, display_name=full_name)
                    invited = True
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

            row.status = "APPROVED" if approve else "REJECTED"
            row.reviewed_at = datetime.now(timezone.utc)
            row.reviewed_by = admin_user.email
            row.review_note = (note or "").strip() or None
            session.commit()
            result = self._request_dict(row, title)
        except HTTPException:
            session.rollback()
            raise
        except Exception:
            session.rollback()
            # The request ID, not the email: logs aren't covered by the privacy policy.
            self.logger.error("Failed to review access request %s", request_id, exc_info=True)
            raise HTTPException(status_code=500, detail="Couldn't save the review. Please try again.")
        finally:
            session.close()

        # Tell the requester. A new account already got the invitation email.
        try:
            # No name from the form here either: rejecting a request made with
            # someone else's address must not pass the sender's text on to them.
            if approve:
                body = (
                    f"Hi,\n\nYour request for “{title}” ({dataset_id}) has been approved. "
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
                    f"Hi,\n\nThank you for your interest in “{title}” ({dataset_id}). "
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
