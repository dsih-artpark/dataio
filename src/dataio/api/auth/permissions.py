from typing import List
from dataio.api.models import User
from dataio.api.database.models import (
    UserPermission,
    AccessLevel,
    UserGroup,
    Dataset,
    ResourceGroupMember,
)
from dataio.api.database.config import Session
from dataio.api.auth.exceptions import AuthorizationError


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

        # expand resource group permissions
        for user_permission in user_permissions:
            if user_permission.resource_type == "GROUP":
                group_members = (
                    session.query(ResourceGroupMember)
                    .filter(
                        ResourceGroupMember.resource_group_id
                        == user_permission.resource_id
                    )
                    .all()
                )
                for group_member in group_members:
                    user_permissions.append(
                        UserPermission(
                            user_email=user_permission.user_email,
                            resource_type=group_member.resource_type,
                            resource_id=group_member.resource_id,
                            permission=user_permission.permission,
                        )
                    )

        return user_permissions
    finally:
        session.close()




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




def check_for_global_permission(user_permission: UserPermission) -> bool:
    """
    Check if user permission is a global admin permission.

    EXACT BUSINESS LOGIC from utils.py:32-39
    """
    if (
        user_permission.resource_type == "*"
        and user_permission.resource_id == "*"
        and user_permission.permission == "DOWNLOAD"
    ):
        return True
    return False


def user_has_preprocessed_access(user_permissions: List[UserPermission]) -> bool:
    """
    Check if user has access to preprocessed bucket.

    EXACT BUSINESS LOGIC from utils.py:5-12
    """
    for user_permission in user_permissions:
        if (
            user_permission.resource_type == "BUCKET"
            and user_permission.resource_id == "PREPROCESSED"
        ) or check_for_global_permission(user_permission):
            return True
    return False


def user_has_dataset_download_access(
    user_permissions: List[UserPermission], dataset: Dataset
) -> bool:
    """
    Check if user has download access to specific dataset.

    EXACT BUSINESS LOGIC from utils.py:15-29
    """
    if dataset.access_level == AccessLevel.DOWNLOAD:
        return True

    for user_permission in user_permissions:
        if (
            user_permission.resource_type == "DATASET"
            and user_permission.resource_id == dataset.ds_id
            and user_permission.permission == "DOWNLOAD"
        ) or check_for_global_permission(user_permission):
            return True
    return False
