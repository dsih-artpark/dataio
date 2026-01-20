"""
Web authentication service for handling login, logout, and session management.

Coordinates between OTP, JWT, and passkey authentication mechanisms.
"""

import logging
from datetime import datetime, timezone
from typing import Optional
from fastapi import HTTPException

from dataio.api.database.config import Session as DBSession
from dataio.api.database.models import User
from dataio.api.services.base_service import BaseService
from dataio.api.services.email_service import EmailService
from dataio.api.auth.otp import create_otp, verify_otp
from dataio.api.auth.jwt import (
    create_access_token,
    create_refresh_token,
    create_session,
    revoke_session,
    revoke_all_user_sessions,
    validate_refresh_token,
)
from dataio.api.auth.passkey import (
    get_registration_options,
    verify_registration,
    get_authentication_options,
    verify_authentication,
    has_passkey,
    get_user_passkeys,
    delete_passkey,
)

logger = logging.getLogger(__name__)


class WebAuthService(BaseService):
    """Service for web-based authentication operations."""

    def __init__(self):
        super().__init__()
        self.email_service = EmailService()

    def initiate_login(self, email: str) -> dict:
        """
        Initiate login by sending OTP to email.

        Args:
            email: The user's email address

        Returns:
            dict: Response with status

        Raises:
            HTTPException: If rate limit exceeded or email sending fails
        """
        session = DBSession()
        try:
            # Check if user exists
            user = session.query(User).filter(User.email == email).first()
            if not user:
                self.logger.info(f"Login attempt for non-existent user: {email}")
                raise HTTPException(
                    status_code=404,
                    detail="No account found with this email. Please contact an administrator."
                )

            if user.is_group:
                raise HTTPException(status_code=400, detail="Cannot login as a group")

            # Generate OTP
            try:
                otp_code, _ = create_otp(email, purpose="login")
            except ValueError as e:
                raise HTTPException(status_code=429, detail=str(e))

            # Send OTP email
            if not self.email_service.send_otp_email(email, otp_code):
                self.logger.error(f"Failed to send OTP email to: {email}")
                raise HTTPException(
                    status_code=500,
                    detail="Failed to send login code. Please try again."
                )

            self.logger.info(f"OTP sent for login: {email}")
            return {"sent": True, "message": "Login code sent to your email"}

        finally:
            session.close()

    def verify_login(
        self,
        email: str,
        code: str,
        user_agent: Optional[str] = None,
        ip_address: Optional[str] = None,
    ) -> dict:
        """
        Verify OTP and create session.

        Args:
            email: The user's email address
            code: The OTP code
            user_agent: Optional browser user agent
            ip_address: Optional client IP address

        Returns:
            dict: Response with tokens and user info

        Raises:
            HTTPException: If verification fails
        """
        session = DBSession()
        try:
            # Verify OTP
            if not verify_otp(email, code, purpose="login"):
                raise HTTPException(
                    status_code=401,
                    detail="Invalid or expired code"
                )

            # Get user
            user = session.query(User).filter(User.email == email).first()
            if not user:
                raise HTTPException(status_code=401, detail="User not found")

            # Update last login and email verification
            user.last_login = datetime.now(timezone.utc)
            user.email_verified = True
            session.commit()

            # Create tokens
            access_token = create_access_token(email)
            refresh_token, _, expires_at = create_refresh_token(email)

            # Store session
            create_session(
                user_email=email,
                refresh_token=refresh_token,
                expires_at=expires_at,
                user_agent=user_agent,
                ip_address=ip_address,
            )

            # Check if user needs passkey setup
            needs_passkey = not has_passkey(email)

            self.logger.info(f"User logged in: {email}")
            return {
                "access_token": access_token,
                "refresh_token": refresh_token,
                "token_type": "bearer",
                "user": {
                    "email": user.email,
                    "display_name": user.display_name,
                    "is_admin": user.is_admin,
                    "email_verified": user.email_verified,
                },
                "needs_passkey": needs_passkey,
            }

        except HTTPException:
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"Login verification failed: {str(e)}")
            raise HTTPException(status_code=500, detail="Login failed")
        finally:
            session.close()

    def refresh_tokens(
        self,
        refresh_token: str,
        user_agent: Optional[str] = None,
        ip_address: Optional[str] = None,
    ) -> dict:
        """
        Refresh access token using refresh token.

        Args:
            refresh_token: The refresh token
            user_agent: Optional browser user agent
            ip_address: Optional client IP address

        Returns:
            dict: Response with new tokens

        Raises:
            HTTPException: If refresh fails
        """
        # Validate refresh token
        db_session = validate_refresh_token(refresh_token)
        if not db_session:
            raise HTTPException(
                status_code=401,
                detail="Invalid or expired refresh token"
            )

        user_email = db_session.user_email

        # Revoke old session
        revoke_session(refresh_token)

        # Create new tokens
        new_access_token = create_access_token(user_email)
        new_refresh_token, _, expires_at = create_refresh_token(user_email)

        # Store new session
        create_session(
            user_email=user_email,
            refresh_token=new_refresh_token,
            expires_at=expires_at,
            user_agent=user_agent,
            ip_address=ip_address,
        )

        self.logger.info(f"Tokens refreshed for user: {user_email}")
        return {
            "access_token": new_access_token,
            "refresh_token": new_refresh_token,
            "token_type": "bearer",
        }

    def logout(self, refresh_token: str) -> dict:
        """
        Logout by revoking the session.

        Args:
            refresh_token: The refresh token to revoke

        Returns:
            dict: Response with status
        """
        revoked = revoke_session(refresh_token)
        if revoked:
            self.logger.info("Session revoked successfully")
        return {"logged_out": True}

    def logout_all_sessions(self, user_email: str) -> dict:
        """
        Logout from all sessions.

        Args:
            user_email: The user's email address

        Returns:
            dict: Response with count of revoked sessions
        """
        count = revoke_all_user_sessions(user_email)
        self.logger.info(f"Revoked {count} sessions for user: {user_email}")
        return {"revoked_sessions": count}

    # Passkey methods

    def get_passkey_registration_options(self, user_email: str) -> dict:
        """
        Get WebAuthn registration options for passkey setup.

        Args:
            user_email: The user's email address

        Returns:
            dict: WebAuthn registration options
        """
        try:
            options = get_registration_options(user_email)
            return {"options": options}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    def verify_passkey_registration(
        self,
        user_email: str,
        credential: dict,
        device_name: Optional[str] = None,
    ) -> dict:
        """
        Verify and store a new passkey.

        Args:
            user_email: The user's email address
            credential: The WebAuthn credential response
            device_name: Optional name for the passkey

        Returns:
            dict: Response with passkey info
        """
        try:
            db_credential = verify_registration(user_email, credential, device_name)

            # Send notification email
            self.email_service.send_passkey_added_email(
                user_email, db_credential.device_name
            )

            self.logger.info(f"Passkey registered for user: {user_email}")
            return {
                "success": True,
                "passkey": {
                    "id": str(db_credential.id),
                    "device_name": db_credential.device_name,
                    "created_at": db_credential.created_at.isoformat(),
                },
            }
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    def get_passkey_authentication_options(self, email: str) -> dict:
        """
        Get WebAuthn authentication options for passkey login.

        Args:
            email: The user's email address

        Returns:
            dict: WebAuthn authentication options
        """
        session = DBSession()
        try:
            # Check if user exists
            user = session.query(User).filter(User.email == email).first()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            options = get_authentication_options(email)
            return {"options": options}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        finally:
            session.close()

    def verify_passkey_authentication(
        self,
        email: str,
        credential: dict,
        user_agent: Optional[str] = None,
        ip_address: Optional[str] = None,
    ) -> dict:
        """
        Verify passkey authentication and create session.

        Args:
            email: The user's email address
            credential: The WebAuthn credential response
            user_agent: Optional browser user agent
            ip_address: Optional client IP address

        Returns:
            dict: Response with tokens and user info
        """
        session = DBSession()
        try:
            # Verify passkey
            try:
                verify_authentication(email, credential)
            except ValueError as e:
                raise HTTPException(status_code=401, detail=str(e))

            # Get user
            user = session.query(User).filter(User.email == email).first()
            if not user:
                raise HTTPException(status_code=401, detail="User not found")

            # Update last login
            user.last_login = datetime.now(timezone.utc)
            session.commit()

            # Create tokens
            access_token = create_access_token(email)
            refresh_token, _, expires_at = create_refresh_token(email)

            # Store session
            create_session(
                user_email=email,
                refresh_token=refresh_token,
                expires_at=expires_at,
                user_agent=user_agent,
                ip_address=ip_address,
            )

            self.logger.info(f"User logged in via passkey: {email}")
            return {
                "access_token": access_token,
                "refresh_token": refresh_token,
                "token_type": "bearer",
                "user": {
                    "email": user.email,
                    "display_name": user.display_name,
                    "is_admin": user.is_admin,
                    "email_verified": user.email_verified,
                },
            }

        except HTTPException:
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"Passkey authentication failed: {str(e)}")
            raise HTTPException(status_code=500, detail="Authentication failed")
        finally:
            session.close()

    def list_user_passkeys(self, user_email: str) -> dict:
        """
        List all passkeys for a user.

        Args:
            user_email: The user's email address

        Returns:
            dict: Response with list of passkeys
        """
        passkeys = get_user_passkeys(user_email)
        return {"passkeys": passkeys}

    def remove_passkey(self, user_email: str, passkey_id: str) -> dict:
        """
        Remove a passkey.

        Args:
            user_email: The user's email address
            passkey_id: The ID of the passkey to remove

        Returns:
            dict: Response with status
        """
        deleted = delete_passkey(user_email, passkey_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Passkey not found")
        return {"deleted": True}
