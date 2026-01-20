"""
Web admin service for user and group management.

Provides functionality for admin users to manage users, groups,
and permissions through the web interface.
"""

import logging
from typing import Optional, List

from fastapi import HTTPException

from dataio.api.database.config import Session as DBSession
from dataio.api.database.models import User, UserGroup, UserPermission
from dataio.api.services.base_service import BaseService
from dataio.api.services.email_service import EmailService
from dataio.api.auth.otp import create_otp
from dataio.api.auth.permissions import is_admin

logger = logging.getLogger(__name__)


class WebAdminService(BaseService):
    """Service for web admin operations."""

    def __init__(self):
        super().__init__()
        self.email_service = EmailService()

    def _require_admin(self, user: User) -> None:
        """Verify user has admin privileges."""
        if not is_admin(user):
            raise HTTPException(status_code=403, detail="Admin privileges required")

    # User Management

    def list_users(
        self,
        admin_user: User,
        search: Optional[str] = None,
        include_groups: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> dict:
        """
        List all users with optional filtering.

        Args:
            admin_user: The authenticated admin user
            search: Optional search term for email/display name
            include_groups: Whether to include group users
            limit: Maximum number of results
            offset: Pagination offset

        Returns:
            dict: List of users and pagination info
        """
        self._require_admin(admin_user)

        session = DBSession()
        try:
            query = session.query(User)

            if not include_groups:
                query = query.filter(User.is_group == False)

            if search:
                search_pattern = f"%{search}%"
                query = query.filter(
                    (User.email.ilike(search_pattern)) |
                    (User.display_name.ilike(search_pattern))
                )

            total = query.count()
            users = query.order_by(User.email).offset(offset).limit(limit).all()

            return {
                "users": [
                    {
                        "email": u.email,
                        "display_name": u.display_name,
                        "is_admin": u.is_admin,
                        "is_group": u.is_group,
                        "email_verified": u.email_verified,
                        "last_login": u.last_login.isoformat() if u.last_login else None,
                        "created_at": u.created_at.isoformat() if u.created_at else None,
                    }
                    for u in users
                ],
                "total": total,
                "limit": limit,
                "offset": offset,
            }
        finally:
            session.close()

    def get_user(self, admin_user: User, email: str) -> dict:
        """
        Get detailed information about a user.

        Args:
            admin_user: The authenticated admin user
            email: The user's email address

        Returns:
            dict: User details including groups and permissions
        """
        self._require_admin(admin_user)

        session = DBSession()
        try:
            user = session.query(User).filter(User.email == email).first()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            # Get user's group memberships
            groups = (
                session.query(UserGroup)
                .filter(UserGroup.user_email == email)
                .all()
            )

            # Get direct permissions
            permissions = (
                session.query(UserPermission)
                .filter(UserPermission.user_email == email)
                .all()
            )

            return {
                "email": user.email,
                "display_name": user.display_name,
                "is_admin": user.is_admin,
                "is_group": user.is_group,
                "email_verified": user.email_verified,
                "last_login": user.last_login.isoformat() if user.last_login else None,
                "created_at": user.created_at.isoformat() if user.created_at else None,
                "groups": [g.group_email for g in groups],
                "permissions": [
                    {
                        "resource_type": p.resource_type.value,
                        "resource_id": p.resource_id,
                        "permission": p.permission.value,
                    }
                    for p in permissions
                ],
            }
        finally:
            session.close()

    def invite_user(
        self,
        admin_user: User,
        email: str,
        display_name: Optional[str] = None,
        is_admin: bool = False,
        groups: Optional[List[str]] = None,
    ) -> dict:
        """
        Invite a new user by sending them an OTP email.

        Args:
            admin_user: The authenticated admin user
            email: The new user's email address
            display_name: Optional display name
            is_admin: Whether to grant admin privileges
            groups: Optional list of group emails to add user to

        Returns:
            dict: Response with status
        """
        self._require_admin(admin_user)

        session = DBSession()
        try:
            # Check if user already exists
            existing = session.query(User).filter(User.email == email).first()
            if existing:
                raise HTTPException(status_code=400, detail="User already exists")

            # Create user
            new_user = User(
                email=email,
                display_name=display_name,
                is_admin=is_admin,
                is_group=False,
                email_verified=False,
            )
            session.add(new_user)

            # Add to groups if specified
            if groups:
                for group_email in groups:
                    # Verify group exists
                    group = (
                        session.query(User)
                        .filter(User.email == group_email, User.is_group == True)
                        .first()
                    )
                    if not group:
                        raise HTTPException(
                            status_code=400, detail=f"Group not found: {group_email}"
                        )
                    user_group = UserGroup(group_email=group_email, user_email=email)
                    session.add(user_group)

            # Generate OTP for invitation before final commit
            try:
                otp_code, _ = create_otp(email, purpose="invite")
            except ValueError as e:
                session.rollback()
                raise HTTPException(status_code=429, detail=str(e))

            session.commit()

            # Send invitation email
            if not self.email_service.send_invite_email(
                email, otp_code, admin_user.display_name or admin_user.email
            ):
                self.logger.error(f"Failed to send invitation email to: {email}")
                raise HTTPException(
                    status_code=500,
                    detail="Failed to send invitation email"
                )

            self.logger.info(f"User invited: {email} by {admin_user.email}")
            return {"invited": True, "email": email}

        except HTTPException:
            session.rollback()
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"Failed to invite user: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to invite user")
        finally:
            session.close()

    def update_user(
        self,
        admin_user: User,
        email: str,
        display_name: Optional[str] = None,
        is_admin: Optional[bool] = None,
    ) -> dict:
        """
        Update a user's profile.

        Args:
            admin_user: The authenticated admin user
            email: The user's email address
            display_name: Optional new display name
            is_admin: Optional admin status

        Returns:
            dict: Updated user info
        """
        self._require_admin(admin_user)

        session = DBSession()
        try:
            user = session.query(User).filter(User.email == email).first()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            if user.is_group:
                raise HTTPException(status_code=400, detail="Cannot update group users")

            if display_name is not None:
                user.display_name = display_name

            if is_admin is not None:
                user.is_admin = is_admin

            session.commit()
            self.logger.info(f"User updated: {email} by {admin_user.email}")

            return self.get_user(admin_user, email)
        except HTTPException:
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"Failed to update user: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to update user")
        finally:
            session.close()

    # Group Management

    def list_groups(
        self,
        admin_user: User,
        search: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict:
        """
        List all groups.

        Args:
            admin_user: The authenticated admin user
            search: Optional search term
            limit: Maximum number of results
            offset: Pagination offset

        Returns:
            dict: List of groups
        """
        self._require_admin(admin_user)

        session = DBSession()
        try:
            query = session.query(User).filter(User.is_group == True)

            if search:
                query = query.filter(User.email.ilike(f"%{search}%"))

            total = query.count()
            groups = query.order_by(User.email).offset(offset).limit(limit).all()

            # Get member counts for each group
            result = []
            for group in groups:
                member_count = (
                    session.query(UserGroup)
                    .filter(UserGroup.group_email == group.email)
                    .count()
                )
                result.append({
                    "email": group.email,
                    "display_name": group.display_name,
                    "member_count": member_count,
                    "created_at": group.created_at.isoformat() if group.created_at else None,
                })

            return {
                "groups": result,
                "total": total,
                "limit": limit,
                "offset": offset,
            }
        finally:
            session.close()

    def get_group(self, admin_user: User, group_email: str) -> dict:
        """
        Get detailed information about a group.

        Args:
            admin_user: The authenticated admin user
            group_email: The group's email address

        Returns:
            dict: Group details including members
        """
        self._require_admin(admin_user)

        session = DBSession()
        try:
            group = (
                session.query(User)
                .filter(User.email == group_email, User.is_group == True)
                .first()
            )
            if not group:
                raise HTTPException(status_code=404, detail="Group not found")

            # Get members
            memberships = (
                session.query(UserGroup)
                .filter(UserGroup.group_email == group_email)
                .all()
            )
            member_emails = [m.user_email for m in memberships]

            # Get member details
            members = (
                session.query(User)
                .filter(User.email.in_(member_emails))
                .all()
            ) if member_emails else []

            # Get group permissions
            permissions = (
                session.query(UserPermission)
                .filter(UserPermission.user_email == group_email)
                .all()
            )

            return {
                "email": group.email,
                "display_name": group.display_name,
                "created_at": group.created_at.isoformat() if group.created_at else None,
                "members": [
                    {
                        "email": m.email,
                        "display_name": m.display_name,
                    }
                    for m in members
                ],
                "permissions": [
                    {
                        "resource_type": p.resource_type.value,
                        "resource_id": p.resource_id,
                        "permission": p.permission.value,
                    }
                    for p in permissions
                ],
            }
        finally:
            session.close()

    def create_group(
        self,
        admin_user: User,
        email: str,
        display_name: Optional[str] = None,
    ) -> dict:
        """
        Create a new group.

        Args:
            admin_user: The authenticated admin user
            email: The group's email address
            display_name: Optional display name

        Returns:
            dict: Created group info
        """
        self._require_admin(admin_user)

        session = DBSession()
        try:
            # Check if group already exists
            existing = session.query(User).filter(User.email == email).first()
            if existing:
                raise HTTPException(status_code=400, detail="Email already in use")

            # Create group
            group = User(
                email=email,
                display_name=display_name or email,
                is_group=True,
                is_admin=False,
            )
            session.add(group)
            session.commit()

            self.logger.info(f"Group created: {email} by {admin_user.email}")
            return {
                "email": group.email,
                "display_name": group.display_name,
                "created": True,
            }
        except HTTPException:
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"Failed to create group: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to create group")
        finally:
            session.close()

    def add_user_to_group(
        self,
        admin_user: User,
        group_email: str,
        user_email: str,
    ) -> dict:
        """
        Add a user to a group.

        Args:
            admin_user: The authenticated admin user
            group_email: The group's email address
            user_email: The user's email address

        Returns:
            dict: Response with status
        """
        self._require_admin(admin_user)

        session = DBSession()
        try:
            # Verify group exists
            group = (
                session.query(User)
                .filter(User.email == group_email, User.is_group == True)
                .first()
            )
            if not group:
                raise HTTPException(status_code=404, detail="Group not found")

            # Verify user exists
            user = (
                session.query(User)
                .filter(User.email == user_email, User.is_group == False)
                .first()
            )
            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            # Check if already a member
            existing = (
                session.query(UserGroup)
                .filter(
                    UserGroup.group_email == group_email,
                    UserGroup.user_email == user_email,
                )
                .first()
            )
            if existing:
                raise HTTPException(status_code=400, detail="User already in group")

            # Add to group
            membership = UserGroup(group_email=group_email, user_email=user_email)
            session.add(membership)
            session.commit()

            self.logger.info(
                f"User {user_email} added to group {group_email} by {admin_user.email}"
            )
            return {"added": True}
        except HTTPException:
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"Failed to add user to group: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to add user to group")
        finally:
            session.close()

    def remove_user_from_group(
        self,
        admin_user: User,
        group_email: str,
        user_email: str,
    ) -> dict:
        """
        Remove a user from a group.

        Args:
            admin_user: The authenticated admin user
            group_email: The group's email address
            user_email: The user's email address

        Returns:
            dict: Response with status
        """
        self._require_admin(admin_user)

        session = DBSession()
        try:
            result = (
                session.query(UserGroup)
                .filter(
                    UserGroup.group_email == group_email,
                    UserGroup.user_email == user_email,
                )
                .delete()
            )

            if result == 0:
                raise HTTPException(status_code=404, detail="Membership not found")

            session.commit()
            self.logger.info(
                f"User {user_email} removed from group {group_email} by {admin_user.email}"
            )
            return {"removed": True}
        except HTTPException:
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"Failed to remove user from group: {str(e)}")
            raise HTTPException(
                status_code=500, detail="Failed to remove user from group"
            )
        finally:
            session.close()
