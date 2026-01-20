"""
OTP (One-Time Password) utilities for email-based authentication.

Provides functions for generating, storing, and verifying OTP codes
for passwordless authentication via email.
"""

import os
import secrets
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from dataio.api.database.config import Session as DBSession
from dataio.api.database.models import OTPToken

logger = logging.getLogger(__name__)

# Configuration
OTP_LENGTH = int(os.getenv("OTP_LENGTH", "6"))
OTP_EXPIRY_MINUTES = int(os.getenv("OTP_EXPIRY_MINUTES", "10"))
OTP_MAX_ATTEMPTS = int(os.getenv("OTP_MAX_ATTEMPTS", "5"))
OTP_RATE_LIMIT_MINUTES = int(os.getenv("OTP_RATE_LIMIT_MINUTES", "1"))


def generate_otp_code(length: int = OTP_LENGTH) -> str:
    """
    Generate a cryptographically secure numeric OTP code.

    Args:
        length: Number of digits in the OTP

    Returns:
        str: The generated OTP code
    """
    # Generate random digits
    return "".join(str(secrets.randbelow(10)) for _ in range(length))


def create_otp(
    email: str,
    purpose: str = "login",
    expires_minutes: int = OTP_EXPIRY_MINUTES,
) -> tuple[str, OTPToken]:
    """
    Create and store a new OTP for an email address.

    Args:
        email: The email address to associate with the OTP
        purpose: The purpose of the OTP ('login', 'verify_email', 'invite')
        expires_minutes: How long the OTP is valid

    Returns:
        tuple: (otp_code, otp_token_record)

    Raises:
        ValueError: If rate limit is exceeded
    """
    session = DBSession()
    try:
        # Check for recent OTP requests (rate limiting)
        recent_cutoff = datetime.now(timezone.utc) - timedelta(minutes=OTP_RATE_LIMIT_MINUTES)
        recent_otp = (
            session.query(OTPToken)
            .filter(
                OTPToken.email == email,
                OTPToken.purpose == purpose,
                OTPToken.created_at > recent_cutoff,
                OTPToken.used_at.is_(None),
            )
            .first()
        )

        if recent_otp:
            time_remaining = (
                recent_otp.created_at + timedelta(minutes=OTP_RATE_LIMIT_MINUTES)
            ) - datetime.now(timezone.utc)
            seconds = max(0, int(time_remaining.total_seconds()))
            raise ValueError(
                f"Please wait {seconds} seconds before requesting another code"
            )

        # Invalidate any existing unused OTPs for this email/purpose
        session.query(OTPToken).filter(
            OTPToken.email == email,
            OTPToken.purpose == purpose,
            OTPToken.used_at.is_(None),
        ).update({"used_at": datetime.now(timezone.utc)})

        # Generate new OTP
        code = generate_otp_code()
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=expires_minutes)

        otp_token = OTPToken(
            email=email,
            code=code,
            purpose=purpose,
            expires_at=expires_at,
        )

        session.add(otp_token)
        session.commit()
        session.refresh(otp_token)

        logger.info(f"Created OTP for email: {email}, purpose: {purpose}")
        return code, otp_token

    except ValueError:
        raise
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to create OTP: {str(e)}")
        raise
    finally:
        session.close()


def verify_otp(
    email: str,
    code: str,
    purpose: str = "login",
) -> bool:
    """
    Verify an OTP code for an email address.

    Args:
        email: The email address
        code: The OTP code to verify
        purpose: The purpose of the OTP

    Returns:
        bool: True if OTP is valid, False otherwise
    """
    session = DBSession()
    try:
        # Find the most recent unused OTP for this email/purpose
        otp_token = (
            session.query(OTPToken)
            .filter(
                OTPToken.email == email,
                OTPToken.purpose == purpose,
                OTPToken.used_at.is_(None),
                OTPToken.expires_at > datetime.now(timezone.utc),
            )
            .order_by(OTPToken.created_at.desc())
            .first()
        )

        if not otp_token:
            logger.warning(f"No valid OTP found for email: {email}")
            return False

        # Increment attempt counter
        otp_token.attempts += 1

        # Check if max attempts exceeded
        if otp_token.attempts > OTP_MAX_ATTEMPTS:
            otp_token.used_at = datetime.now(timezone.utc)
            session.commit()
            logger.warning(f"OTP max attempts exceeded for email: {email}")
            return False

        # Verify the code (constant-time comparison)
        if not secrets.compare_digest(otp_token.code, code):
            session.commit()
            logger.warning(
                f"Invalid OTP attempt for email: {email}, "
                f"attempt {otp_token.attempts}/{OTP_MAX_ATTEMPTS}"
            )
            return False

        # Mark as used
        otp_token.used_at = datetime.now(timezone.utc)
        session.commit()

        logger.info(f"OTP verified successfully for email: {email}")
        return True

    except Exception as e:
        session.rollback()
        logger.error(f"Failed to verify OTP: {str(e)}")
        return False
    finally:
        session.close()


def get_remaining_attempts(email: str, purpose: str = "login") -> Optional[int]:
    """
    Get the remaining OTP attempts for an email.

    Args:
        email: The email address
        purpose: The purpose of the OTP

    Returns:
        Optional[int]: Remaining attempts, or None if no active OTP
    """
    session = DBSession()
    try:
        otp_token = (
            session.query(OTPToken)
            .filter(
                OTPToken.email == email,
                OTPToken.purpose == purpose,
                OTPToken.used_at.is_(None),
                OTPToken.expires_at > datetime.now(timezone.utc),
            )
            .order_by(OTPToken.created_at.desc())
            .first()
        )

        if not otp_token:
            return None

        return max(0, OTP_MAX_ATTEMPTS - otp_token.attempts)
    finally:
        session.close()


def cleanup_expired_otps() -> int:
    """
    Remove expired OTP tokens from the database.

    Returns:
        int: Number of tokens deleted
    """
    session = DBSession()
    try:
        result = session.query(OTPToken).filter(
            OTPToken.expires_at < datetime.now(timezone.utc)
        ).delete()
        session.commit()
        logger.info(f"Cleaned up {result} expired OTP tokens")
        return result
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to cleanup OTPs: {str(e)}")
        raise
    finally:
        session.close()


def invalidate_all_otps(email: str, purpose: Optional[str] = None) -> int:
    """
    Invalidate all unused OTPs for an email.

    Args:
        email: The email address
        purpose: Optional purpose to filter by

    Returns:
        int: Number of OTPs invalidated
    """
    session = DBSession()
    try:
        query = session.query(OTPToken).filter(
            OTPToken.email == email,
            OTPToken.used_at.is_(None),
        )

        if purpose:
            query = query.filter(OTPToken.purpose == purpose)

        result = query.update({"used_at": datetime.now(timezone.utc)})
        session.commit()
        logger.info(f"Invalidated {result} OTPs for email: {email}")
        return result
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to invalidate OTPs: {str(e)}")
        raise
    finally:
        session.close()
