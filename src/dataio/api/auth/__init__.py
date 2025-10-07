"""
Authentication and authorization module for dataio API.

This module provides:
- API key authentication
- Permission checking and validation
- Decorators for route protection
- Custom exceptions for auth errors
"""

from .decorators import (
    admin_required,
)
from .exceptions import (
    AuthenticationError,
    AuthorizationError,
)
from .permissions import (
    determine_highest_permission,
    determine_user_permissions,
    is_admin,
    require_admin,
    user_has_dataset_download_access,
    user_has_preprocessed_access,
)
from .providers import get_user, get_user_with_request_state

__all__ = [
    # Providers
    "get_user",
    "get_user_with_request_state",
    # Permissions
    "is_admin",
    "determine_highest_permission",
    "determine_user_permissions",
    "require_admin",
    "user_has_preprocessed_access",
    "user_has_dataset_download_access",
    # Decorators
    "admin_required",
    # Exceptions
    "AuthenticationError",
    "AuthorizationError",
]
