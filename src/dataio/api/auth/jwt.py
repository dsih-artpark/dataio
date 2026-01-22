"""
JWT (JSON Web Token) utilities for web authentication.

Provides functions for creating and verifying access and refresh tokens
for session-based authentication in the web interface.
"""

import os
import uuid
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from dataio.api.database.config import Session as DBSession
from dataio.api.database.models import User, Session
from dataio.api.auth.exceptions import AuthenticationError

logger = logging.getLogger(__name__)

# Security scheme for Bearer token authentication
bearer_scheme = HTTPBearer(auto_error=False)

# Configuration from environment variables
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")
if not JWT_SECRET_KEY:
    raise ValueError("JWT_SECRET_KEY environment variable must be set")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
JWT_ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("JWT_ACCESS_TOKEN_EXPIRE_MINUTES", "15"))
JWT_REFRESH_TOKEN_EXPIRE_DAYS = int(os.getenv("JWT_REFRESH_TOKEN_EXPIRE_DAYS", "7"))


class TokenPayload:
    """Decoded JWT token payload."""

    def __init__(self, sub: str, exp: datetime, type: str, jti: Optional[str] = None):
        self.sub = sub  # Subject (user email)
        self.exp = exp  # Expiration time
        self.type = type  # Token type ('access' or 'refresh')
        self.jti = jti  # JWT ID (for refresh tokens)


def create_access_token(
    user_email: str,
    expires_delta: Optional[timedelta] = None,
) -> str:
    """
    Create a new access token for a user.

    Args:
        user_email: The email of the user
        expires_delta: Optional custom expiration time

    Returns:
        str: Encoded JWT access token
    """
    if expires_delta is None:
        expires_delta = timedelta(minutes=JWT_ACCESS_TOKEN_EXPIRE_MINUTES)

    expire = datetime.now(timezone.utc) + expires_delta

    payload = {
        "sub": user_email,
        "exp": expire,
        "iat": datetime.now(timezone.utc),
        "type": "access",
    }

    token = jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    logger.info(f"Created access token for user: {user_email}")
    return token


def create_refresh_token(
    user_email: str,
    expires_delta: Optional[timedelta] = None,
) -> tuple[str, str, datetime]:
    """
    Create a new refresh token for a user.

    Args:
        user_email: The email of the user
        expires_delta: Optional custom expiration time

    Returns:
        tuple: (encoded_token, jti, expires_at)
    """
    if expires_delta is None:
        expires_delta = timedelta(days=JWT_REFRESH_TOKEN_EXPIRE_DAYS)

    expire = datetime.now(timezone.utc) + expires_delta
    jti = str(uuid.uuid4())

    payload = {
        "sub": user_email,
        "exp": expire,
        "iat": datetime.now(timezone.utc),
        "type": "refresh",
        "jti": jti,
    }

    token = jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)
    logger.info(f"Created refresh token for user: {user_email}")
    return token, jti, expire


def verify_token(token: str) -> TokenPayload:
    """
    Verify and decode a JWT token.

    Args:
        token: The JWT token to verify

    Returns:
        TokenPayload: Decoded token payload

    Raises:
        AuthenticationError: If token is invalid or expired
    """
    try:
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        return TokenPayload(
            sub=payload["sub"],
            exp=datetime.fromtimestamp(payload["exp"], tz=timezone.utc),
            type=payload.get("type", "access"),
            jti=payload.get("jti"),
        )
    except jwt.ExpiredSignatureError:
        logger.warning("Token has expired")
        raise AuthenticationError("Token has expired")
    except jwt.InvalidTokenError as e:
        logger.warning(f"Invalid token: {str(e)}")
        raise AuthenticationError("Invalid token")


def get_current_web_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> User:
    """
    FastAPI dependency to get the current authenticated user from JWT token.

    Args:
        credentials: Bearer token credentials from request

    Returns:
        User: The authenticated user object

    Raises:
        HTTPException: If authentication fails
    """
    if not credentials:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        payload = verify_token(credentials.credentials)

        if payload.type != "access":
            raise AuthenticationError("Invalid token type")

        # Get user from database
        session = DBSession()
        try:
            user = session.query(User).filter(User.email == payload.sub).first()
            if not user:
                raise AuthenticationError("User not found")
            if user.is_group:
                raise AuthenticationError("Groups cannot authenticate")

            # Force load all attributes before expunging from session
            # This ensures is_admin and other fields are accessible after session closes
            is_admin_val = user.is_admin
            is_group_val = user.is_group
            email_verified_val = user.email_verified
            display_name_val = user.display_name

            logger.info(f"User {user.email} loaded: is_admin={is_admin_val}, is_group={is_group_val}")

            # Expunge user from session so it can be used after session closes
            session.expunge(user)
            return user
        finally:
            session.close()

    except AuthenticationError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(e),
            headers={"WWW-Authenticate": "Bearer"},
        )


def get_optional_web_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
) -> Optional[User]:
    """
    FastAPI dependency to optionally get the current authenticated user.
    Returns None if no valid token is provided instead of raising an error.

    Args:
        credentials: Bearer token credentials from request

    Returns:
        Optional[User]: The authenticated user object or None
    """
    if not credentials:
        return None

    try:
        return get_current_web_user(credentials)
    except HTTPException:
        return None


def create_session(
    user_email: str,
    refresh_token: str,
    expires_at: datetime,
    user_agent: Optional[str] = None,
    ip_address: Optional[str] = None,
) -> Session:
    """
    Create a new session record in the database.

    Args:
        user_email: The email of the user
        refresh_token: The refresh token (stored as-is since JWT is already cryptographically signed)
        expires_at: When the session expires
        user_agent: Optional browser user agent
        ip_address: Optional client IP address

    Returns:
        Session: The created session object

    Note:
        Refresh tokens are stored as-is (not hashed) because they are already
        cryptographically signed JWTs. The token's signature provides integrity
        protection, and we verify the signature before accepting the token.
    """
    session = DBSession()
    try:
        db_session = Session(
            user_email=user_email,
            refresh_token=refresh_token,
            expires_at=expires_at,
            user_agent=user_agent,
            ip_address=ip_address,
        )
        session.add(db_session)
        session.commit()
        session.refresh(db_session)
        logger.info(f"Created session for user: {user_email}")
        return db_session
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to create session: {str(e)}")
        raise
    finally:
        session.close()


def revoke_session(refresh_token: str) -> bool:
    """
    Revoke a session by its refresh token.

    Args:
        refresh_token: The refresh token to revoke

    Returns:
        bool: True if session was found and revoked
    """
    session = DBSession()
    try:
        db_session = (
            session.query(Session)
            .filter(Session.refresh_token == refresh_token)
            .first()
        )
        if db_session:
            db_session.revoked_at = datetime.now(timezone.utc)
            session.commit()
            logger.info(f"Revoked session for user: {db_session.user_email}")
            return True
        return False
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to revoke session: {str(e)}")
        raise
    finally:
        session.close()


def revoke_all_user_sessions(user_email: str) -> int:
    """
    Revoke all sessions for a user.

    Args:
        user_email: The email of the user

    Returns:
        int: Number of sessions revoked
    """
    session = DBSession()
    try:
        result = (
            session.query(Session)
            .filter(
                Session.user_email == user_email,
                Session.revoked_at.is_(None),
            )
            .update({"revoked_at": datetime.now(timezone.utc)})
        )
        session.commit()
        logger.info(f"Revoked {result} sessions for user: {user_email}")
        return result
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to revoke user sessions: {str(e)}")
        raise
    finally:
        session.close()


def validate_refresh_token(refresh_token: str) -> Optional[Session]:
    """
    Validate a refresh token and return the associated session.

    Args:
        refresh_token: The refresh token to validate

    Returns:
        Optional[Session]: The session if valid, None otherwise
    """
    try:
        # First verify the token cryptographically
        payload = verify_token(refresh_token)
        if payload.type != "refresh":
            logger.warning("Token is not a refresh token")
            return None

        # Check if session exists and is not revoked
        session = DBSession()
        try:
            db_session = (
                session.query(Session)
                .filter(
                    Session.refresh_token == refresh_token,
                    Session.revoked_at.is_(None),
                    Session.expires_at > datetime.now(timezone.utc),
                )
                .first()
            )
            return db_session
        finally:
            session.close()

    except AuthenticationError:
        return None
