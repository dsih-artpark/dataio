"""
Authentication and authorization module for dataio API.

This module provides:
- API key authentication
- JWT-based web authentication
- OTP (One-Time Password) generation and verification
- Passkey/WebAuthn support
- Permission checking and validation
- Decorators for route protection
- Custom exceptions for auth errors
"""

from .providers import get_user
from .permissions import (
    is_admin,
    determine_highest_permission,
    determine_user_permissions,
    require_admin,
    user_has_preprocessed_access,
    user_has_dataset_download_access,
    user_has_weather_data_view_access,
    user_has_weather_data_download_access,
)
from .decorators import (
    admin_required,
)
from .exceptions import (
    AuthenticationError,
    AuthorizationError,
)
from .jwt import (
    create_access_token,
    create_refresh_token,
    verify_token,
    get_current_web_user,
    get_optional_web_user,
)
from .otp import (
    create_otp,
    verify_otp,
    cleanup_expired_otps,
)
from .passkey import (
    get_registration_options,
    verify_registration,
    get_authentication_options,
    verify_authentication,
    has_passkey,
)

__all__ = [
    # Providers (API key auth)
    "get_user",
    # JWT (Web auth)
    "create_access_token",
    "create_refresh_token",
    "verify_token",
    "get_current_web_user",
    "get_optional_web_user",
    # OTP
    "create_otp",
    "verify_otp",
    "cleanup_expired_otps",
    # Passkey/WebAuthn
    "get_registration_options",
    "verify_registration",
    "get_authentication_options",
    "verify_authentication",
    "has_passkey",
    # Permissions
    "is_admin",
    "determine_highest_permission",
    "determine_user_permissions",
    "require_admin",
    "user_has_preprocessed_access",
    "user_has_dataset_download_access",
    "user_has_weather_data_view_access",
    "user_has_weather_data_download_access",
    # Decorators
    "admin_required",
    # Exceptions
    "AuthenticationError",
    "AuthorizationError",
]
