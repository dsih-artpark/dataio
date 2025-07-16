from fastapi import HTTPException
import sqlalchemy.exc
from dataio.api.models import (
    UserCreate, 
    UserGroupCreate, 
    ResourceGroupCreate, 
    ResourceGroupMemberCreate, 
    UserPermissionCreate
)
from dataio.api.database import functions as database
from .base_service import BaseService


class UserService(BaseService):
    """Service for user-related operations."""
    
    def create_user(self, user_to_be_created: UserCreate):
        """
        Create a new user.
        
        EXACT BUSINESS LOGIC from admin.py:42-53
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
                status_code=500, detail=f"Failed to create user. Contact support."
            )
    
    def get_users(self):
        """
        Get all users.
        
        EXACT BUSINESS LOGIC from admin.py:59-65
        """
        try:
            return database.get_users()
        except Exception as e:
            self.logger.error(f"Failed to get users: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Failed to get users. Contact support."
            )
    
    def create_user_group(self, user_group: UserGroupCreate):
        """
        Create a new user group.
        
        EXACT BUSINESS LOGIC from admin.py:73-80
        """
        try:
            created_user_group = database.create_user_group(user_group)
            return created_user_group
        except Exception as e:
            self.logger.error(f"Failed to create user group: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Failed to create user group. Contact support."
            )
    
    def create_resource_group(self, resource_group: ResourceGroupCreate):
        """
        Create a new resource group.
        
        EXACT BUSINESS LOGIC from admin.py:88-95
        """
        try:
            created_resource_group = database.create_resource_group(resource_group)
            return created_resource_group
        except Exception as e:
            self.logger.error(f"Failed to create resource group: {str(e)}")
            raise HTTPException(
                status_code=500, detail=f"Failed to create resource group. Contact support."
            )
    
    def create_resource_group_member(self, resource_group_member: ResourceGroupMemberCreate):
        """
        Create a new resource group member.
        
        EXACT BUSINESS LOGIC from admin.py:102-109
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
                detail=f"Failed to create resource group member. Contact support.",
            )
    
    def create_user_permission(self, user_permission: UserPermissionCreate):
        """
        Create a new user permission.
        
        EXACT BUSINESS LOGIC from admin.py:121-129
        """
        try:
            created_user_permission = database.create_user_permission(user_permission)
            return created_user_permission
        except Exception as e:
            self.logger.error(f"Failed to create user permission: {str(e)}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create user permission. Contact support.",
            )