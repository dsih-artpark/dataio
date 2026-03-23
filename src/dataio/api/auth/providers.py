import os
from datetime import datetime, timezone

from fastapi import Request, Security
from fastapi.security import APIKeyHeader
from dataio.api.models import User
from dataio.api.database.models import User as DBUser, UserAPIKey
from dataio.api.database.config import Session
from dataio.api.auth.exceptions import AuthenticationError
import bcrypt
import logging

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

logger = logging.getLogger(__name__)

# API Key configuration (must match web_user_service.py)
API_KEY_PREFIX = os.getenv("API_KEY_PREFIX", "dio_")


def check_api_key(api_key: str) -> User:
    """
    Validate API key against database.

    Checks both:
    1. Legacy keys in users.key column
    2. Web-generated keys in user_api_keys table (prefixed with dio_)

    Args:
        api_key: API key to validate

    Returns:
        User: Authenticated user object if valid, None otherwise
    """
    session = Session()
    try:
        # Check if this is a web-generated API key (has prefix)
        if api_key.startswith(API_KEY_PREFIX):
            # Extract the raw key (without prefix)
            raw_key = api_key[len(API_KEY_PREFIX):]

            # Get non-revoked, non-expired keys from user_api_keys table
            api_keys = (
                session.query(UserAPIKey)
                .filter(UserAPIKey.revoked_at.is_(None))
                .all()
            )

            for key_record in api_keys:
                # Check expiration
                if key_record.expires_at and key_record.expires_at < datetime.now(timezone.utc):
                    continue

                # Verify the key hash
                try:
                    key_hash = key_record.key_hash
                    if isinstance(key_hash, str):
                        key_hash = key_hash.encode("utf-8")

                    if bcrypt.checkpw(raw_key.encode("utf-8"), key_hash):
                        # Update last_used_at
                        key_record.last_used_at = datetime.now(timezone.utc)
                        session.commit()

                        # Get the associated user
                        user = session.query(DBUser).filter(DBUser.email == key_record.user_email).first()
                        if user:
                            logger.info(f"Web API key verified for user: {user.email}")
                            # Expunge to detach from session before returning
                            session.expunge(user)
                            return user
                except Exception as e:
                    logger.debug(f"Key check failed for {key_record.key_prefix}: {str(e)}")
                    continue

            logger.warning("Web API key validation failed - no matching key found")
            return None

        # Legacy key check (no prefix) - check users.key column
        # DEPRECATED: Legacy keys will be removed in a future version
        users = session.query(DBUser).all()
        for user in users:
            if user.key:
                try:
                    if bcrypt.checkpw(api_key.encode("utf-8"), user.key):
                        logger.warning(
                            f"DEPRECATION: Legacy API key used for user: {user.email}. "
                            "Legacy API keys are deprecated and will be removed in a future version. "
                            "Please generate a new API key at https://data.artpark.ai/account"
                        )
                        session.expunge(user)
                        # Mark this as a legacy key authentication for response header
                        user._legacy_key_used = True
                        return user
                except Exception:
                    continue

        return None
    except Exception as e:
        logger.error(f"Error checking API key: {str(e)}")
        return None
    finally:
        session.close()


def get_user(request: Request, api_key_header: str = Security(api_key_header)) -> User:
    """
    Validate API key and return authenticated user.

    Args:
        api_key_header: API key from request header

    Returns:
        User: Authenticated user object

    Raises:
        AuthenticationError: If API key is invalid or missing
    """
    if not api_key_header:
        raise AuthenticationError("Missing API key")
    user = check_api_key(api_key_header)
    if user:
        if getattr(user, "_legacy_key_used", False):
            request.state.legacy_api_key_authenticated = True
        return user
    raise AuthenticationError("Invalid API key")

