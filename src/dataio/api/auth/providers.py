from fastapi import Security
from fastapi.security import APIKeyHeader
from dataio.api.models import User
from dataio.api.database.models import User as DBUser
from dataio.api.database.config import Session
from dataio.api.auth.exceptions import AuthenticationError
import bcrypt
import logging

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

logger = logging.getLogger(__name__)


def check_api_key(api_key: str) -> User:
    """
    Validate API key against database.

    Args:
        api_key: API key to validate

    Returns:
        User: Authenticated user object if valid, None otherwise
    """
    try:
        users = Session().query(DBUser).all()
        for user in users:
            if user.key:
                if bcrypt.checkpw(api_key.encode("utf-8"), user.key):
                    logger.info("User found - key verified")
                    return user
        return None
    except Exception as e:
        logger.error(f"Error checking API key: {str(e)}")
        return None


def get_user(api_key_header: str = Security(api_key_header)) -> User:
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
        return user
    raise AuthenticationError("Invalid API key")


