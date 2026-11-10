"""
Shared security helpers for web authentication.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import HTTPException, Request
from sqlalchemy import text

from dataio.api.database.config import Session as DBSession
from dataio.api.database.models import AuthAuditLog, User

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


# One statement, so two requests at the same moment can't both read the old
# count (or both try to create the bucket) and slip under the limit. While a
# bucket is blocked it is left as it is; once its window has passed it starts
# again at 1; otherwise the count goes up by one and, over the limit, the
# bucket is blocked.
_COUNT_ATTEMPT_SQL = text(
    """
    INSERT INTO auth_rate_limits (action, subject, attempt_count, window_started_at, created_at, updated_at)
    VALUES (:action, :subject, 1, :now, :now, :now)
    ON CONFLICT (action, subject) DO UPDATE SET
        attempt_count = CASE
            WHEN auth_rate_limits.blocked_until > :now THEN auth_rate_limits.attempt_count
            WHEN auth_rate_limits.window_started_at <= :window_cutoff THEN 1
            ELSE auth_rate_limits.attempt_count + 1
        END,
        window_started_at = CASE
            WHEN auth_rate_limits.blocked_until > :now THEN auth_rate_limits.window_started_at
            WHEN auth_rate_limits.window_started_at <= :window_cutoff THEN :now
            ELSE auth_rate_limits.window_started_at
        END,
        blocked_until = CASE
            WHEN auth_rate_limits.blocked_until > :now THEN auth_rate_limits.blocked_until
            WHEN auth_rate_limits.window_started_at <= :window_cutoff THEN NULL
            WHEN auth_rate_limits.attempt_count + 1 > :limit THEN :block_until
            ELSE NULL
        END,
        updated_at = :now
    RETURNING blocked_until
    """
)
RATE_LIMIT_UNAVAILABLE_MESSAGE = "This is temporarily unavailable. Please try again in a few minutes."


def enforce_rate_limit(
    action: str,
    subject: str,
    limit: int,
    window_seconds: int = AUTH_RATE_LIMIT_WINDOW_SECONDS,
    block_seconds: int = AUTH_RATE_LIMIT_BLOCK_SECONDS,
    fail_closed: bool = False,
) -> None:
    """Count one attempt in a DB-backed rate limit bucket and raise 429 if blocked.

    If the counter can't be read or written, the call is let through, or
    with ``fail_closed`` refused with 503 (for public actions that cost
    money or send email, where an unlimited burst is worse than a pause).
    """
    # The columns are timestamp without time zone, holding UTC.
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    session = DBSession()
    try:
        blocked_until = session.execute(
            _COUNT_ATTEMPT_SQL,
            {
                "action": action,
                "subject": subject,
                "now": now,
                "window_cutoff": now - timedelta(seconds=window_seconds),
                "block_until": now + timedelta(seconds=block_seconds),
                "limit": limit,
            },
        ).scalar_one()
        session.commit()
    except Exception as exc:
        session.rollback()
        logger.warning("Failed to enforce rate limit %s/%s: %s", action, subject, exc)
        if fail_closed:
            raise HTTPException(status_code=503, detail=RATE_LIMIT_UNAVAILABLE_MESSAGE) from exc
        return
    finally:
        session.close()

    if blocked_until is not None and blocked_until > now:
        retry_after = max(1, int((blocked_until - now).total_seconds()))
        raise HTTPException(
            status_code=429,
            detail=f"Too many attempts. Try again in {retry_after} seconds.",
        )


def get_client_ip(request: Request) -> Optional[str]:
    """The caller's IP address.

    Proxy headers are only trusted when TRUST_PROXY_HEADERS=true, i.e. when
    the API is reachable only through our nginx proxy; otherwise anyone could
    pick their own address. Only headers nginx writes itself are used:
    X-Real-IP (nginx sets it to the connecting address, replacing anything
    the visitor sent), else the LAST X-Forwarded-For entry (the one nginx
    appended; earlier entries come from the visitor). CF-Connecting-IP is
    deliberately ignored: there is no Cloudflare in front, and nginx passes
    that header through unchanged, so a visitor could set it to any IP.
    """
    ip_address = request.client.host if request.client else None
    if os.getenv("TRUST_PROXY_HEADERS", "false").lower() == "true":
        real_ip = (request.headers.get("x-real-ip") or "").strip()
        forwarded_for = [part.strip() for part in request.headers.get("x-forwarded-for", "").split(",")]
        forwarded_for = [part for part in forwarded_for if part]
        if real_ip:
            ip_address = real_ip
        elif forwarded_for:
            ip_address = forwarded_for[-1]
    return ip_address


def enforce_ip_rate_limit(
    request: Request,
    action: str,
    limit: int,
    window_seconds: int,
    block_seconds: Optional[int] = None,
) -> None:
    """Per-IP limit for public (signed-out) actions such as open downloads,
    the assistant and request forms.

    Allows ``limit`` calls per ``window_seconds`` from one IP address, then
    answers 429 for ``block_seconds`` (default: one window). Counters share
    the auth_rate_limits table under the action ``public:<action>``. Unlike
    the sign-in limits, it refuses the call (503) if the counter can't be
    read or written.
    """
    ip_address = get_client_ip(request) or "unknown"
    enforce_rate_limit(
        action=f"public:{action}",
        subject=f"ip:{ip_address}",
        limit=limit,
        window_seconds=window_seconds,
        block_seconds=block_seconds if block_seconds is not None else window_seconds,
        fail_closed=True,
    )
