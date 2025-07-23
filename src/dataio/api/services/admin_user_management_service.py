from fastapi import HTTPException
import sqlalchemy.exc
from dataio.api.models import (
    UserCreate,
    UserGroupCreate,
    ResourceGroupCreate,
    ResourceGroupMemberCreate,
    UserPermissionCreate,
)
from dataio.api.database import functions as database
from dataio.api.services.base_service import BaseService


class AdminUserManagementService(BaseService):
    """Service for admin user management operations."""

    def __init__(self):
        super().__init__()

    def create_user(self, user_to_be_created: UserCreate):
        """
        Create a new user.
        """
        try:
            created_user = database.create_user(user_to_be_created)
            return created_user
        except sqlalchemy.exc.IntegrityError:
            raise HTTPException(
                status_code=400, detail="Error creating user. User already exists"
            )
        except Exception as e:
            self.logger.error(f"Failed to create user: {str(e)}")
            raise HTTPException(
                status_code=500, detail="Failed to create user. Contact support."
            )

    def get_users(self):
        """
        Get all users.
        """
        try:
            return database.get_users()
        except Exception as e:
            self.logger.error(f"Failed to get users: {str(e)}")
            raise HTTPException(
                status_code=500, detail="Failed to get users. Contact support."
            )

    def create_user_group(self, user_group: UserGroupCreate):
        """
        Create a new user group.
        """
        try:
            created_user_group = database.create_user_group(user_group)
            return created_user_group
        except Exception as e:
            self.logger.error(f"Failed to create user group: {str(e)}")
            raise HTTPException(
                status_code=500, detail="Failed to create user group. Contact support."
            )

    def create_resource_group(self, resource_group: ResourceGroupCreate):
        """
        Create a new resource group.
        """
        try:
            created_resource_group = database.create_resource_group(resource_group)
            return created_resource_group
        except Exception as e:
            self.logger.error(f"Failed to create resource group: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Failed to create resource group. Contact support.",
            )

    def create_resource_group_member(
        self, resource_group_member: ResourceGroupMemberCreate
    ):
        """
        Create a new resource group member.
        """
        try:
            created_resource_group_member = database.create_resource_group_member(
                resource_group_member
            )
            return created_resource_group_member
        except Exception as e:
            self.logger.error(f"Failed to create resource group member: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Failed to create resource group member. Contact support.",
            )

    def create_user_permission(self, user_permission: UserPermissionCreate):
        """
        Create a new user permission.
        """
        try:
            created_user_permission = database.create_user_permission(user_permission)
            return created_user_permission
        except Exception as e:
            self.logger.error(f"Failed to create user permission: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Failed to create user permission. Contact support.",
            )
