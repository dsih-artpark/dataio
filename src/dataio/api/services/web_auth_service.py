"""
Web authentication service for handling login, logout, and session management.

Coordinates between OTP, JWT, and passkey authentication mechanisms.
"""

import os
import secrets
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional
from fastapi import HTTPException

from dataio.api.database.config import Session as DBSession
from dataio.api.database.models import User, MagicLinkToken, UserAPIKey, WebAuthnCredential
from dataio.api.services.base_service import BaseService
from dataio.api.services.email_service import EmailService
from dataio.api.auth.otp import create_otp, verify_otp
from dataio.api.auth.email_validator import validate_registration_email
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

# Magic link configuration
MAGIC_LINK_EXPIRY_MINUTES = int(os.getenv("MAGIC_LINK_EXPIRY_MINUTES", "30"))
INVITATION_LINK_EXPIRY_HOURS = int(os.getenv("INVITATION_LINK_EXPIRY_HOURS", "48"))
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")

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
                # For security, don't reveal if user exists
                # Still "send" OTP to prevent enumeration attacks
                self.logger.info(f"Login attempt for non-existent user: {email}")
                return {"sent": True, "message": "If the email exists, a code has been sent"}

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

    # =============================================================================
    # Registration Methods
    # =============================================================================

    def _create_magic_link_token(self, email: str, purpose: str) -> str:
        """Create a magic link token and store it in the database."""
        token = secrets.token_urlsafe(32)
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=MAGIC_LINK_EXPIRY_MINUTES)

        session = DBSession()
        try:
            magic_link = MagicLinkToken(
                email=email,
                token=token,
                purpose=purpose,
                expires_at=expires_at,
            )
            session.add(magic_link)
            session.commit()
            return token
        finally:
            session.close()

    def _verify_magic_link_token(self, token: str, purpose: str) -> Optional[str]:
        """Verify a magic link token and return the email if valid."""
        session = DBSession()
        try:
            magic_link = session.query(MagicLinkToken).filter(
                MagicLinkToken.token == token,
                MagicLinkToken.purpose == purpose,
                MagicLinkToken.used_at.is_(None),
                MagicLinkToken.expires_at > datetime.now(timezone.utc),
            ).first()

            if not magic_link:
                return None

            # Mark as used
            magic_link.used_at = datetime.now(timezone.utc)
            session.commit()

            return magic_link.email
        finally:
            session.close()

    def initiate_registration(self, email: str) -> dict:
        """
        Initiate registration by validating email and sending verification.

        Args:
            email: The user's email address

        Returns:
            dict: Response with status and verification info
        """
        email = email.lower().strip()
        session = DBSession()
        try:
            # Check if user already exists
            existing_user = session.query(User).filter(User.email == email).first()
            if existing_user:
                raise HTTPException(
                    status_code=400,
                    detail="An account with this email already exists. Please sign in instead."
                )

            # Validate email domain
            is_valid, verification_status, message = validate_registration_email(email)
            if not is_valid:
                raise HTTPException(status_code=400, detail=message)

            # Generate OTP
            try:
                otp_code, _ = create_otp(email, purpose="registration")
            except ValueError as e:
                raise HTTPException(status_code=429, detail=str(e))

            # Generate magic link token
            magic_token = self._create_magic_link_token(email, purpose="registration")
            magic_link = f"{FRONTEND_URL}/verify-email?token={magic_token}&email={email}"

            # Send registration email with OTP and magic link
            if not self.email_service.send_registration_email(email, otp_code, magic_link):
                self.logger.error(f"Failed to send registration email to: {email}")
                raise HTTPException(
                    status_code=500,
                    detail="Failed to send verification email. Please try again."
                )

            self.logger.info(f"Registration initiated for: {email} (status: {verification_status})")
            return {
                "sent": True,
                "message": "Verification email sent. Check your inbox.",
                "verification_status": verification_status,
            }

        except HTTPException:
            raise
        except Exception as e:
            self.logger.error(f"Registration initiation failed: {str(e)}")
            raise HTTPException(status_code=500, detail="Registration failed. Please try again.")
        finally:
            session.close()

    def verify_registration(
        self,
        email: str,
        code: Optional[str] = None,
        magic_token: Optional[str] = None,
        user_agent: Optional[str] = None,
        ip_address: Optional[str] = None,
    ) -> dict:
        """
        Complete registration by verifying OTP or magic link.

        Args:
            email: The user's email address
            code: The OTP code (if using OTP verification)
            magic_token: The magic link token (if using magic link)
            user_agent: Optional browser user agent
            ip_address: Optional client IP address

        Returns:
            dict: Response with tokens and user info
        """
        email = email.lower().strip()

        # Verify either OTP or magic link
        verified = False
        if code:
            verified = verify_otp(email, code, purpose="registration")
        elif magic_token:
            verified_email = self._verify_magic_link_token(magic_token, purpose="registration")
            verified = verified_email == email

        if not verified:
            raise HTTPException(
                status_code=401,
                detail="Invalid or expired verification code"
            )

        session = DBSession()
        try:
            # Check if user already exists (race condition protection)
            existing_user = session.query(User).filter(User.email == email).first()
            if existing_user:
                raise HTTPException(
                    status_code=400,
                    detail="Account already exists. Please sign in."
                )

            # Determine verification status based on email domain
            _, verification_status, status_message = validate_registration_email(email)

            # Create new user
            now = datetime.now(timezone.utc)
            user = User(
                email=email,
                is_group=False,
                is_admin=False,
                email_verified=True,
                last_login=now,
                created_at=now,
                verification_status=verification_status,
                registered_at=now,
                verified_at=now if verification_status == "verified" else None,
            )
            session.add(user)
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

            self.logger.info(f"User registered: {email} (status: {verification_status})")
            return {
                "access_token": access_token,
                "refresh_token": refresh_token,
                "token_type": "bearer",
                "user": {
                    "email": user.email,
                    "display_name": user.display_name,
                    "is_admin": user.is_admin,
                    "email_verified": user.email_verified,
                    "verification_status": user.verification_status,
                },
                "verification_status": verification_status,
                "verification_message": status_message if verification_status == "pending" else None,
            }

        except HTTPException:
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"Registration failed: {str(e)}")
            raise HTTPException(status_code=500, detail="Registration failed")
        finally:
            session.close()

    # =============================================================================
    # Account Deletion Methods
    # =============================================================================

    def initiate_account_deletion(self, user_email: str) -> dict:
        """
        Initiate account deletion by sending verification code.

        Args:
            user_email: The user's email address

        Returns:
            dict: Response with status
        """
        session = DBSession()
        try:
            user = session.query(User).filter(User.email == user_email).first()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            # Generate OTP
            try:
                otp_code, _ = create_otp(user_email, purpose="account_deletion")
            except ValueError as e:
                raise HTTPException(status_code=429, detail=str(e))

            # Send deletion confirmation email
            if not self.email_service.send_account_deletion_email(user_email, otp_code):
                self.logger.error(f"Failed to send deletion email to: {user_email}")
                raise HTTPException(
                    status_code=500,
                    detail="Failed to send verification email. Please try again."
                )

            self.logger.info(f"Account deletion initiated for: {user_email}")
            return {
                "sent": True,
                "message": "Verification code sent. Check your email to confirm deletion.",
            }

        finally:
            session.close()

    def verify_account_deletion(self, user_email: str, code: str) -> dict:
        """
        Complete account deletion after OTP verification.

        Args:
            user_email: The user's email address
            code: The OTP code

        Returns:
            dict: Response with status
        """
        # Verify OTP
        if not verify_otp(user_email, code, purpose="account_deletion"):
            raise HTTPException(
                status_code=401,
                detail="Invalid or expired verification code"
            )

        session = DBSession()
        try:
            user = session.query(User).filter(User.email == user_email).first()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            # Delete all sessions (must delete, not just revoke, to avoid FK issues)
            from dataio.api.database.models import UserGroup, UserPermission, Session as SessionModel
            session.query(SessionModel).filter(SessionModel.user_email == user_email).delete()

            # Delete group memberships (no CASCADE on FK)
            session.query(UserGroup).filter(UserGroup.user_email == user_email).delete()

            # Delete permissions (no CASCADE on FK)
            session.query(UserPermission).filter(UserPermission.user_email == user_email).delete()

            # Delete API keys (CASCADE should handle this, but being explicit)
            session.query(UserAPIKey).filter(UserAPIKey.user_email == user_email).delete()

            # Delete passkeys (CASCADE should handle this, but being explicit)
            session.query(WebAuthnCredential).filter(WebAuthnCredential.user_email == user_email).delete()

            # Delete magic link tokens
            session.query(MagicLinkToken).filter(MagicLinkToken.email == user_email).delete()

            # Delete OTP tokens
            from dataio.api.database.models import OTPToken
            session.query(OTPToken).filter(OTPToken.email == user_email).delete()

            # Delete the user
            session.delete(user)
            session.commit()

            self.logger.info(f"Account deleted: {user_email}")
            return {
                "deleted": True,
                "message": "Your account has been permanently deleted.",
            }

        except HTTPException:
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"Account deletion failed: {str(e)}")
            raise HTTPException(status_code=500, detail="Account deletion failed")
        finally:
            session.close()

    # =============================================================================
    # Admin User Verification Methods
    # =============================================================================

    def get_pending_users(self, limit: int = 50, offset: int = 0) -> dict:
        """
        Get list of users pending verification.

        Args:
            limit: Maximum number of users to return
            offset: Number of users to skip

        Returns:
            dict: Response with list of pending users
        """
        session = DBSession()
        try:
            query = session.query(User).filter(
                User.verification_status == "pending",
                User.is_group == False,
            )

            total = query.count()
            users = query.order_by(User.registered_at.desc()).offset(offset).limit(limit).all()

            return {
                "users": [
                    {
                        "email": u.email,
                        "display_name": u.display_name,
                        "registered_at": u.registered_at.isoformat() if u.registered_at else None,
                        "verification_status": u.verification_status,
                    }
                    for u in users
                ],
                "total": total,
                "limit": limit,
                "offset": offset,
            }
        finally:
            session.close()

    def verify_user(self, user_email: str, admin_email: str) -> dict:
        """
        Verify (approve) a pending user.

        Args:
            user_email: The user to verify
            admin_email: The admin performing the verification

        Returns:
            dict: Response with status
        """
        session = DBSession()
        try:
            user = session.query(User).filter(User.email == user_email).first()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            if user.verification_status == "verified":
                raise HTTPException(status_code=400, detail="User is already verified")

            user.verification_status = "verified"
            user.verified_at = datetime.now(timezone.utc)
            user.verified_by = admin_email
            session.commit()

            # Send notification email
            self.email_service.send_verification_approved_email(user_email)

            self.logger.info(f"User verified: {user_email} by {admin_email}")
            return {"verified": True, "email": user_email}

        except HTTPException:
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"User verification failed: {str(e)}")
            raise HTTPException(status_code=500, detail="Verification failed")
        finally:
            session.close()

    def reject_user(self, user_email: str, admin_email: str) -> dict:
        """
        Reject a pending user.

        Args:
            user_email: The user to reject
            admin_email: The admin performing the rejection

        Returns:
            dict: Response with status
        """
        session = DBSession()
        try:
            user = session.query(User).filter(User.email == user_email).first()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            if user.verification_status == "verified":
                raise HTTPException(status_code=400, detail="Cannot reject a verified user")

            user.verification_status = "rejected"
            user.verified_by = admin_email
            session.commit()

            # Send notification email
            self.email_service.send_verification_rejected_email(user_email)

            self.logger.info(f"User rejected: {user_email} by {admin_email}")
            return {"rejected": True, "email": user_email}

        except HTTPException:
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"User rejection failed: {str(e)}")
            raise HTTPException(status_code=500, detail="Rejection failed")
        finally:
            session.close()

    # =============================================================================
    # Invitation Methods
    # =============================================================================

    def create_invitation_token(self, email: str, invited_by: str) -> str:
        """
        Create an invitation magic link token with 48-hour expiry.

        Args:
            email: The invited user's email
            invited_by: Email of admin who sent the invitation

        Returns:
            str: The invitation token
        """
        token = secrets.token_urlsafe(32)
        expires_at = datetime.now(timezone.utc) + timedelta(hours=INVITATION_LINK_EXPIRY_HOURS)

        session = DBSession()
        try:
            # Invalidate any existing unused invitation tokens for this email
            session.query(MagicLinkToken).filter(
                MagicLinkToken.email == email,
                MagicLinkToken.purpose == "invitation",
                MagicLinkToken.used_at.is_(None),
            ).update({"used_at": datetime.now(timezone.utc)})

            magic_link = MagicLinkToken(
                email=email,
                token=token,
                purpose="invitation",
                expires_at=expires_at,
                invited_by=invited_by,
            )
            session.add(magic_link)
            session.commit()
            return token
        finally:
            session.close()

    def get_invitation_link(self, email: str, invited_by: str) -> str:
        """
        Create invitation token and return the full magic link URL.

        Args:
            email: The invited user's email
            invited_by: Email of admin who sent the invitation

        Returns:
            str: The full invitation URL
        """
        token = self.create_invitation_token(email, invited_by)
        return f"{FRONTEND_URL}/accept-invite?token={token}"

    def accept_invitation(
        self,
        token: str,
        user_agent: Optional[str] = None,
        ip_address: Optional[str] = None,
    ) -> dict:
        """
        Accept an invitation by verifying the magic link token.

        Args:
            token: The invitation token
            user_agent: Optional browser user agent
            ip_address: Optional client IP address

        Returns:
            dict: Response with tokens and user info

        Raises:
            HTTPException: If token is invalid or expired
        """
        session = DBSession()
        try:
            # Find and validate the invitation token
            magic_link = session.query(MagicLinkToken).filter(
                MagicLinkToken.token == token,
                MagicLinkToken.purpose == "invitation",
                MagicLinkToken.used_at.is_(None),
                MagicLinkToken.expires_at > datetime.now(timezone.utc),
            ).first()

            if not magic_link:
                raise HTTPException(
                    status_code=401,
                    detail="Invalid or expired invitation link"
                )

            email = magic_link.email

            # Get the user (should exist, created during invite)
            user = session.query(User).filter(User.email == email).first()
            if not user:
                raise HTTPException(
                    status_code=404,
                    detail="User not found. The invitation may have been revoked."
                )

            # Check if user is already verified
            if user.email_verified:
                raise HTTPException(
                    status_code=400,
                    detail="This invitation has already been used. Please sign in."
                )

            # Mark token as used
            magic_link.used_at = datetime.now(timezone.utc)

            # Update user as verified
            now = datetime.now(timezone.utc)
            user.email_verified = True
            user.last_login = now
            user.verification_status = "verified"
            user.verified_at = now

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

            self.logger.info(f"Invitation accepted: {email}")
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
            self.logger.error(f"Invitation acceptance failed: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to accept invitation")
        finally:
            session.close()

    def revoke_invitation(self, email: str, admin_email: str) -> dict:
        """
        Revoke a pending invitation.

        Args:
            email: The invited user's email
            admin_email: The admin performing the revocation

        Returns:
            dict: Response with status
        """
        session = DBSession()
        try:
            # Find the user
            user = session.query(User).filter(User.email == email).first()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            # Check if user has already accepted the invitation
            if user.email_verified:
                raise HTTPException(
                    status_code=400,
                    detail="Cannot revoke invitation for a verified user"
                )

            # Invalidate any unused invitation tokens
            invalidated = session.query(MagicLinkToken).filter(
                MagicLinkToken.email == email,
                MagicLinkToken.purpose == "invitation",
                MagicLinkToken.used_at.is_(None),
            ).update({"used_at": datetime.now(timezone.utc)})

            # Delete the unverified user
            # First, delete group memberships
            from dataio.api.database.models import UserGroup
            session.query(UserGroup).filter(UserGroup.user_email == email).delete()

            # Delete the user
            session.delete(user)
            session.commit()

            self.logger.info(f"Invitation revoked: {email} by {admin_email}")
            return {
                "revoked": True,
                "email": email,
                "tokens_invalidated": invalidated,
            }

        except HTTPException:
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"Invitation revocation failed: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to revoke invitation")
        finally:
            session.close()

    def get_invitation_status(self, email: str) -> dict:
        """
        Get the status of an invitation for a user.

        Args:
            email: The user's email

        Returns:
            dict: Invitation status info
        """
        session = DBSession()
        try:
            # Find the user
            user = session.query(User).filter(User.email == email).first()
            if not user:
                return {"exists": False}

            # Check for pending invitation token
            pending_token = session.query(MagicLinkToken).filter(
                MagicLinkToken.email == email,
                MagicLinkToken.purpose == "invitation",
                MagicLinkToken.used_at.is_(None),
                MagicLinkToken.expires_at > datetime.now(timezone.utc),
            ).first()

            return {
                "exists": True,
                "email_verified": user.email_verified,
                "has_pending_invitation": pending_token is not None,
                "invitation_expires_at": pending_token.expires_at.isoformat() if pending_token else None,
                "invited_by": pending_token.invited_by if pending_token else None,
            }
        finally:
            session.close()
