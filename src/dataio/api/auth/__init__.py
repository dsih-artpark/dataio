"""
Authentication and authorization module for dataio API.

This module provides:
- API key authentication
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
)
from .decorators import (
    admin_required,
)
from .exceptions import (
    AuthenticationError,
    AuthorizationError,
)

__all__ = [
    # Providers
    "get_user",
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
