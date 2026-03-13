"""
Shared security helpers for web authentication.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import HTTPException

from dataio.api.database.config import Session as DBSession
from dataio.api.database.models import AuthAuditLog, AuthRateLimit, User

logger = logging.getLogger(__name__)

AUTH_RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("AUTH_RATE_LIMIT_WINDOW_SECONDS", "60"))
AUTH_RATE_LIMIT_BLOCK_SECONDS = int(os.getenv("AUTH_RATE_LIMIT_BLOCK_SECONDS", "300"))
GENERIC_AUTH_INITIATE_MESSAGE = "If the account is eligible, a sign-in option will be sent"
GENERIC_PASSKEY_UNAVAILABLE_MESSAGE = "Passkey sign-in is not available for this account"


def normalize_email(email: str) -> str:
    """Normalize an email address for auth flows."""
    return email.lower().strip()


def get_auth_block_reason(user: User) -> Optional[str]:
    """Return a user-facing reason if the account must not authenticate."""
    if user.is_group:
        return "Groups cannot sign in to the web application"
    if user.suspended_at:
        return "This account has been suspended. Please contact an administrator."
    if user.verification_status == "pending":
        return "Your account is pending administrator approval."
    if user.verification_status == "rejected":
        return "Your account approval request was rejected. Please contact support."
    return None


def ensure_user_can_authenticate(user: User, reveal_reason: bool = True) -> None:
    """Raise if the account should not be allowed to authenticate."""
    reason = get_auth_block_reason(user)
    if reason:
        detail = reason if reveal_reason else GENERIC_PASSKEY_UNAVAILABLE_MESSAGE
        raise HTTPException(status_code=403, detail=detail)


def record_auth_event(
    event_type: str,
    outcome: str,
    actor_email: Optional[str] = None,
    target_email: Optional[str] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None,
    details: Optional[dict] = None,
) -> None:
    """Best-effort audit logging for auth/security events."""
    session = DBSession()
    try:
        log_entry = AuthAuditLog(
            event_type=event_type,
            outcome=outcome,
            actor_email=actor_email,
            target_email=target_email,
            ip_address=ip_address,
            user_agent=user_agent,
            details=details,
        )
        session.add(log_entry)
        session.commit()
    except Exception as exc:
        session.rollback()
        logger.warning("Failed to record auth audit event %s: %s", event_type, exc)
    finally:
        session.close()


def enforce_rate_limit(
    action: str,
    subject: str,
    limit: int,
    window_seconds: int = AUTH_RATE_LIMIT_WINDOW_SECONDS,
    block_seconds: int = AUTH_RATE_LIMIT_BLOCK_SECONDS,
) -> None:
    """Increment a DB-backed rate limit bucket and raise if blocked."""
    now = datetime.now(timezone.utc)
    session = DBSession()
    try:
        bucket = (
            session.query(AuthRateLimit)
            .filter(
                AuthRateLimit.action == action,
                AuthRateLimit.subject == subject,
            )
            .first()
        )

        if not bucket:
            bucket = AuthRateLimit(
                action=action,
                subject=subject,
                attempt_count=1,
                window_started_at=now,
                created_at=now,
                updated_at=now,
            )
            session.add(bucket)
            session.commit()
            return

        blocked_until = bucket.blocked_until
        if blocked_until and blocked_until.tzinfo is None:
            blocked_until = blocked_until.replace(tzinfo=timezone.utc)
        if blocked_until and blocked_until > now:
            retry_after = max(1, int((blocked_until - now).total_seconds()))
            raise HTTPException(
                status_code=429,
                detail=f"Too many attempts. Try again in {retry_after} seconds.",
            )

        window_started_at = bucket.window_started_at
        if window_started_at.tzinfo is None:
            window_started_at = window_started_at.replace(tzinfo=timezone.utc)

        if window_started_at + timedelta(seconds=window_seconds) <= now:
            bucket.attempt_count = 0
            bucket.window_started_at = now
            bucket.blocked_until = None

        bucket.attempt_count += 1
        bucket.updated_at = now

        if bucket.attempt_count > limit:
            bucket.blocked_until = now + timedelta(seconds=block_seconds)
            session.commit()
            raise HTTPException(
                status_code=429,
                detail=f"Too many attempts. Try again in {block_seconds} seconds.",
            )

        session.commit()
    except HTTPException:
        session.rollback()
        raise
    except Exception as exc:
        session.rollback()
        logger.warning("Failed to enforce rate limit %s/%s: %s", action, subject, exc)
    finally:
        session.close()
