from typing import List
from dataio.api.api.models import User
from dataio.api.database.models import UserPermission, AccessLevel, UserGroup
from dataio.api.database.config import Session
from .exceptions import AuthorizationError


def is_admin(user: User) -> bool:
    """
    Check if user has admin privileges.

    Args:
        user: User object to check

    Returns:
        bool: True if user is admin, False otherwise

    Raises:
        AuthorizationError: If user is a group (groups cannot be admin)
    """
    if user.is_group:
        raise AuthorizationError("Groups cannot have admin privileges")
    return user.email == "admin@artpark.in"


def determine_highest_permission(permissions: List[AccessLevel]) -> AccessLevel:
    """
    Determine the highest permission level from a list of permissions.

    Args:
        permissions: List of AccessLevel permissions

    Returns:
        AccessLevel: Highest permission level (DOWNLOAD > VIEW > NONE)
    """
    if AccessLevel.DOWNLOAD in permissions:
        return AccessLevel.DOWNLOAD
    elif AccessLevel.VIEW in permissions:
        return AccessLevel.VIEW
    else:
        return AccessLevel.NONE


def determine_user_permissions(user: User) -> List[UserPermission]:
    """
    Determine all permissions for a user including group permissions.

    Args:
        user: User object to get permissions for

    Returns:
        List[UserPermission]: List of all user permissions

    Raises:
        AuthorizationError: If user is a group
    """
    if user.is_group:
        raise AuthorizationError("Groups cannot have permissions determined")

    session = Session()
    try:
        user_permissions = []

        # Admin users get all permissions
        if user.is_admin is True:
            user_permissions.append(
                UserPermission(
                    user_email=user.email,
                    resource_type="*",
                    resource_id="*",
                    permission="DOWNLOAD",
                )
            )

        # Get user groups
        user_groups = (
            session.query(UserGroup).filter(UserGroup.user_email == user.email).all()
        )

        # Get direct user permissions
        direct_permissions = (
            session.query(UserPermission)
            .filter(UserPermission.user_email == user.email)
            .all()
        )
        user_permissions.extend(direct_permissions)

        # Get group permissions
        for user_group in user_groups:
            group_permissions = (
                session.query(UserPermission)
                .filter(UserPermission.user_email == user_group.group_email)
                .all()
            )
            user_permissions.extend(group_permissions)

        return user_permissions
    finally:
        session.close()


def has_permission(
    user: User, resource_type: str, resource_id: str, required_permission: AccessLevel
) -> bool:
    """
    Check if user has required permission for a specific resource.

    Args:
        user: User to check permissions for
        resource_type: Type of resource (DATASET, GROUP, BUCKET)
        resource_id: ID of the resource
        required_permission: Required permission level

    Returns:
        bool: True if user has required permission, False otherwise
    """
    user_permissions = determine_user_permissions(user)

    # Check for wildcard permissions (admin)
    for permission in user_permissions:
        if (
            permission.resource_type == "*"
            and permission.resource_id == "*"
            and permission.permission == AccessLevel.DOWNLOAD
        ):
            return True

    # Check for specific resource permissions
    relevant_permissions = [
        perm.permission
        for perm in user_permissions
        if (perm.resource_type == resource_type and perm.resource_id == resource_id)
    ]

    if not relevant_permissions:
        return False

    highest_permission = determine_highest_permission(relevant_permissions)

    # Check if highest permission meets requirement
    if required_permission == AccessLevel.DOWNLOAD:
        return highest_permission == AccessLevel.DOWNLOAD
    elif required_permission == AccessLevel.VIEW:
        return highest_permission in [AccessLevel.VIEW, AccessLevel.DOWNLOAD]
    else:
        return True


def require_admin(user: User) -> None:
    """
    Ensure user has admin privileges.

    Args:
        user: User to check

    Raises:
        AuthorizationError: If user is not admin
    """
    if not is_admin(user):
        raise AuthorizationError("Admin privileges required")


def require_permission(
    user: User, resource_type: str, resource_id: str, required_permission: AccessLevel
) -> None:
    """
    Ensure user has required permission for resource.

    Args:
        user: User to check
        resource_type: Type of resource
        resource_id: ID of resource
        required_permission: Required permission level

    Raises:
        AuthorizationError: If user lacks required permission
    """
    if not has_permission(user, resource_type, resource_id, required_permission):
        raise AuthorizationError(
            f"Insufficient permissions for {resource_type}:{resource_id}"
        )
