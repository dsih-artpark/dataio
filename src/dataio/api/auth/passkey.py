"""
WebAuthn/Passkey utilities for passwordless authentication.

Provides functions for passkey registration and authentication
using the WebAuthn standard (FIDO2).
"""

import os
import base64
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List

from webauthn import (
    generate_registration_options,
    verify_registration_response,
    generate_authentication_options,
    verify_authentication_response,
    options_to_json,
)
from webauthn.helpers import bytes_to_base64url, base64url_to_bytes
from webauthn.helpers.structs import (
    AuthenticatorSelectionCriteria,
    UserVerificationRequirement,
    ResidentKeyRequirement,
    AuthenticatorAttachment,
    PublicKeyCredentialDescriptor,
    AuthenticatorTransport,
)

from dataio.api.database.config import Session as DBSession
from dataio.api.database.models import User, WebAuthnCredential, WebAuthnChallenge

logger = logging.getLogger(__name__)

# Configuration from environment variables
WEBAUTHN_RP_ID = os.getenv("WEBAUTHN_RP_ID", "localhost")
WEBAUTHN_RP_NAME = os.getenv("WEBAUTHN_RP_NAME", "DataIO")
WEBAUTHN_ORIGIN = os.getenv("WEBAUTHN_ORIGIN", "http://localhost:3000")
WEBAUTHN_CHALLENGE_EXPIRY_MINUTES = int(
    os.getenv("WEBAUTHN_CHALLENGE_EXPIRY_MINUTES", "5")
)


def _store_challenge(user_email: str, challenge: bytes, purpose: str) -> None:
    """Store a WebAuthn challenge in the database."""
    session = DBSession()
    try:
        # Remove any existing challenges for this user/purpose
        session.query(WebAuthnChallenge).filter(
            WebAuthnChallenge.user_email == user_email,
            WebAuthnChallenge.purpose == purpose,
        ).delete()

        # Store new challenge
        db_challenge = WebAuthnChallenge(
            user_email=user_email,
            challenge=bytes_to_base64url(challenge),
            purpose=purpose,
            expires_at=datetime.now(timezone.utc)
            + timedelta(minutes=WEBAUTHN_CHALLENGE_EXPIRY_MINUTES),
        )
        session.add(db_challenge)
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to store challenge: {str(e)}")
        raise
    finally:
        session.close()


def _get_challenge(user_email: str, purpose: str) -> Optional[bytes]:
    """Retrieve and consume a WebAuthn challenge from the database."""
    session = DBSession()
    try:
        db_challenge = (
            session.query(WebAuthnChallenge)
            .filter(
                WebAuthnChallenge.user_email == user_email,
                WebAuthnChallenge.purpose == purpose,
                WebAuthnChallenge.expires_at > datetime.now(timezone.utc),
            )
            .first()
        )

        if not db_challenge:
            return None

        challenge = base64url_to_bytes(db_challenge.challenge)

        # Delete the challenge (one-time use)
        session.delete(db_challenge)
        session.commit()

        return challenge
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to get challenge: {str(e)}")
        raise
    finally:
        session.close()


def _get_user_credentials(user_email: str) -> List[WebAuthnCredential]:
    """Get all WebAuthn credentials for a user."""
    session = DBSession()
    try:
        return (
            session.query(WebAuthnCredential)
            .filter(WebAuthnCredential.user_email == user_email)
            .all()
        )
    finally:
        session.close()


def get_registration_options(user_email: str) -> dict:
    """
    Generate WebAuthn registration options for a user.

    Args:
        user_email: The email of the user registering a passkey

    Returns:
        dict: Registration options to send to the client
    """
    session = DBSession()
    try:
        # Get user
        user = session.query(User).filter(User.email == user_email).first()
        if not user:
            raise ValueError("User not found")

        # Get existing credentials to exclude
        existing_credentials = _get_user_credentials(user_email)
        exclude_credentials = [
            PublicKeyCredentialDescriptor(
                id=base64url_to_bytes(cred.credential_id),
                transports=[AuthenticatorTransport(t) for t in (cred.transports or [])],
            )
            for cred in existing_credentials
        ]

        # Generate options
        options = generate_registration_options(
            rp_id=WEBAUTHN_RP_ID,
            rp_name=WEBAUTHN_RP_NAME,
            user_id=user_email.encode("utf-8"),
            user_name=user_email,
            user_display_name=user.display_name or user_email,
            exclude_credentials=exclude_credentials,
            authenticator_selection=AuthenticatorSelectionCriteria(
                authenticator_attachment=AuthenticatorAttachment.PLATFORM,
                resident_key=ResidentKeyRequirement.PREFERRED,
                user_verification=UserVerificationRequirement.PREFERRED,
            ),
        )

        # Store challenge for verification
        _store_challenge(user_email, options.challenge, "registration")

        logger.info(f"Generated registration options for user: {user_email}")
        return options_to_json(options)

    except ValueError:
        raise
    except Exception as e:
        logger.error(f"Failed to generate registration options: {str(e)}")
        raise
    finally:
        session.close()


def verify_registration(
    user_email: str,
    credential: dict,
    device_name: Optional[str] = None,
) -> WebAuthnCredential:
    """
    Verify a WebAuthn registration response and store the credential.

    Args:
        user_email: The email of the user
        credential: The credential response from the client
        device_name: Optional name for the device/passkey

    Returns:
        WebAuthnCredential: The stored credential

    Raises:
        ValueError: If verification fails
    """
    session = DBSession()
    try:
        # Get the stored challenge
        expected_challenge = _get_challenge(user_email, "registration")
        if not expected_challenge:
            raise ValueError("No valid challenge found. Please restart registration.")

        # Verify the registration response
        verification = verify_registration_response(
            credential=credential,
            expected_challenge=expected_challenge,
            expected_rp_id=WEBAUTHN_RP_ID,
            expected_origin=WEBAUTHN_ORIGIN,
        )

        # Store the credential
        transports = []
        if hasattr(credential, "response") and hasattr(credential["response"], "transports"):
            transports = credential["response"].get("transports", [])
        elif isinstance(credential, dict) and "response" in credential:
            transports = credential.get("response", {}).get("transports", [])

        db_credential = WebAuthnCredential(
            user_email=user_email,
            credential_id=bytes_to_base64url(verification.credential_id),
            public_key=verification.credential_public_key,
            sign_count=verification.sign_count,
            device_name=device_name or "Passkey",
            transports=transports,
        )

        session.add(db_credential)
        session.commit()
        session.refresh(db_credential)

        logger.info(f"Registered new passkey for user: {user_email}")
        return db_credential

    except ValueError:
        raise
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to verify registration: {str(e)}")
        raise ValueError(f"Registration verification failed: {str(e)}")
    finally:
        session.close()


def get_authentication_options(user_email: str) -> dict:
    """
    Generate WebAuthn authentication options for a user.

    Args:
        user_email: The email of the user authenticating

    Returns:
        dict: Authentication options to send to the client

    Raises:
        ValueError: If user has no registered passkeys
    """
    # Get user's credentials
    credentials = _get_user_credentials(user_email)
    if not credentials:
        raise ValueError("No passkeys registered for this account")

    allow_credentials = [
        PublicKeyCredentialDescriptor(
            id=base64url_to_bytes(cred.credential_id),
            transports=[AuthenticatorTransport(t) for t in (cred.transports or [])],
        )
        for cred in credentials
    ]

    # Generate options
    options = generate_authentication_options(
        rp_id=WEBAUTHN_RP_ID,
        allow_credentials=allow_credentials,
        user_verification=UserVerificationRequirement.PREFERRED,
    )

    # Store challenge for verification
    _store_challenge(user_email, options.challenge, "authentication")

    logger.info(f"Generated authentication options for user: {user_email}")
    return options_to_json(options)


def verify_authentication(user_email: str, credential: dict) -> WebAuthnCredential:
    """
    Verify a WebAuthn authentication response.

    Args:
        user_email: The email of the user
        credential: The credential response from the client

    Returns:
        WebAuthnCredential: The authenticated credential

    Raises:
        ValueError: If verification fails
    """
    session = DBSession()
    try:
        # Get the stored challenge
        expected_challenge = _get_challenge(user_email, "authentication")
        if not expected_challenge:
            raise ValueError("No valid challenge found. Please restart authentication.")

        # Get the credential ID from the response
        credential_id = credential.get("id") or credential.get("rawId")
        if not credential_id:
            raise ValueError("Missing credential ID")

        # Find the stored credential
        db_credential = (
            session.query(WebAuthnCredential)
            .filter(
                WebAuthnCredential.user_email == user_email,
                WebAuthnCredential.credential_id == credential_id,
            )
            .first()
        )

        if not db_credential:
            raise ValueError("Credential not found")

        # Verify the authentication response
        verification = verify_authentication_response(
            credential=credential,
            expected_challenge=expected_challenge,
            expected_rp_id=WEBAUTHN_RP_ID,
            expected_origin=WEBAUTHN_ORIGIN,
            credential_public_key=db_credential.public_key,
            credential_current_sign_count=db_credential.sign_count,
        )

        # Update sign count and last used timestamp
        db_credential.sign_count = verification.new_sign_count
        db_credential.last_used_at = datetime.now(timezone.utc)
        session.commit()

        logger.info(f"Passkey authentication successful for user: {user_email}")
        return db_credential

    except ValueError:
        raise
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to verify authentication: {str(e)}")
        raise ValueError(f"Authentication verification failed: {str(e)}")
    finally:
        session.close()


def get_user_passkeys(user_email: str) -> List[dict]:
    """
    Get a list of all passkeys for a user.

    Args:
        user_email: The email of the user

    Returns:
        List[dict]: List of passkey info (without sensitive data)
    """
    credentials = _get_user_credentials(user_email)
    return [
        {
            "id": str(cred.id),
            "device_name": cred.device_name,
            "created_at": cred.created_at.isoformat() if cred.created_at else None,
            "last_used_at": cred.last_used_at.isoformat() if cred.last_used_at else None,
        }
        for cred in credentials
    ]


def delete_passkey(user_email: str, passkey_id: str) -> bool:
    """
    Delete a passkey for a user.

    Args:
        user_email: The email of the user
        passkey_id: The ID of the passkey to delete

    Returns:
        bool: True if deleted, False if not found
    """
    session = DBSession()
    try:
        result = (
            session.query(WebAuthnCredential)
            .filter(
                WebAuthnCredential.user_email == user_email,
                WebAuthnCredential.id == passkey_id,
            )
            .delete()
        )
        session.commit()
        if result:
            logger.info(f"Deleted passkey {passkey_id} for user: {user_email}")
        return result > 0
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to delete passkey: {str(e)}")
        raise
    finally:
        session.close()


def has_passkey(user_email: str) -> bool:
    """
    Check if a user has any registered passkeys.

    Args:
        user_email: The email of the user

    Returns:
        bool: True if user has at least one passkey
    """
    session = DBSession()
    try:
        count = (
            session.query(WebAuthnCredential)
            .filter(WebAuthnCredential.user_email == user_email)
            .count()
        )
        return count > 0
    finally:
        session.close()
