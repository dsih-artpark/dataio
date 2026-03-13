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
from dataio.api.auth.permissions import is_admin
from dataio.api.auth.security import record_auth_event

logger = logging.getLogger(__name__)


class WebAdminService(BaseService):
    """Service for web admin operations."""

    def __init__(self):
        super().__init__()
        self.email_service = EmailService()

    def _require_admin(self, user: User) -> None:
        """Verify user has admin privileges."""
        logger.info(f"_require_admin called for user: {getattr(user, 'email', 'N/A')}")
        is_admin_result = is_admin(user)
        logger.info(f"_require_admin - is_admin returned: {is_admin_result}")
        if not is_admin_result:
            logger.warning(f"_require_admin - denying access for user: {getattr(user, 'email', 'N/A')}")
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
                        "suspended_at": u.suspended_at.isoformat() if u.suspended_at else None,
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
                "suspended_at": user.suspended_at.isoformat() if user.suspended_at else None,
                "suspended_by": user.suspended_by,
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
        Invite a new user by sending them a magic link email (expires in 48 hours).

        Args:
            admin_user: The authenticated admin user
            email: The new user's email address
            display_name: Optional display name
            is_admin: Whether to grant admin privileges
            groups: Optional list of group emails to add user to

        Returns:
            dict: Response with status
        """
        from dataio.api.services.web_auth_service import WebAuthService

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

            session.commit()

            # Generate invitation magic link (48-hour expiry)
            auth_service = WebAuthService()
            invitation_link = auth_service.get_invitation_link(
                email=email,
                invited_by=admin_user.email,
            )

            # Send invitation email with magic link
            if not self.email_service.send_invite_email(
                to_email=email,
                invitation_link=invitation_link,
                inviter_name=admin_user.display_name or admin_user.email,
            ):
                self.logger.error(f"Failed to send invitation email to: {email}")
                raise HTTPException(
                    status_code=500,
                    detail="Failed to send invitation email"
                )

            self.logger.info(f"User invited: {email} by {admin_user.email}")
            record_auth_event(
                event_type="invitation.send",
                outcome="success",
                actor_email=admin_user.email,
                target_email=email,
            )
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

    def resend_invitation(
        self,
        admin_user: User,
        email: str,
    ) -> dict:
        """
        Resend an invitation email to a pending user.

        Args:
            admin_user: The authenticated admin user
            email: The user's email address

        Returns:
            dict: Response with status
        """
        from dataio.api.services.web_auth_service import WebAuthService

        self._require_admin(admin_user)

        session = DBSession()
        try:
            # Check if user exists
            user = session.query(User).filter(User.email == email).first()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            # Check if user has already accepted the invitation
            if user.email_verified:
                raise HTTPException(
                    status_code=400,
                    detail="User has already accepted their invitation"
                )

            # Generate new invitation magic link (48-hour expiry)
            auth_service = WebAuthService()
            invitation_link = auth_service.get_invitation_link(
                email=email,
                invited_by=admin_user.email,
            )

            # Send invitation email with magic link
            if not self.email_service.send_invite_email(
                to_email=email,
                invitation_link=invitation_link,
                inviter_name=admin_user.display_name or admin_user.email,
            ):
                self.logger.error(f"Failed to resend invitation email to: {email}")
                raise HTTPException(
                    status_code=500,
                    detail="Failed to send invitation email"
                )

            self.logger.info(f"Invitation resent: {email} by {admin_user.email}")
            record_auth_event(
                event_type="invitation.resend",
                outcome="success",
                actor_email=admin_user.email,
                target_email=email,
            )
            return {"resent": True, "email": email}

        except HTTPException:
            raise
        except Exception as e:
            self.logger.error(f"Failed to resend invitation: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to resend invitation")
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

    def suspend_user(
        self,
        admin_user: User,
        email: str,
    ) -> dict:
        """
        Suspend a user, preventing them from accessing the platform.

        Args:
            admin_user: The authenticated admin user
            email: The user's email address

        Returns:
            dict: Updated user info
        """
        from datetime import datetime, timezone

        self._require_admin(admin_user)

        if admin_user.email == email:
            raise HTTPException(status_code=400, detail="Cannot suspend yourself")

        session = DBSession()
        try:
            user = session.query(User).filter(User.email == email).first()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            if user.is_group:
                raise HTTPException(status_code=400, detail="Cannot suspend groups")

            if user.suspended_at:
                raise HTTPException(status_code=400, detail="User already suspended")

            user.suspended_at = datetime.now(timezone.utc)
            user.suspended_by = admin_user.email
            session.commit()

            self.logger.info(f"User suspended: {email} by {admin_user.email}")
            record_auth_event(
                event_type="admin.user_suspend",
                outcome="success",
                actor_email=admin_user.email,
                target_email=email,
            )
            return {"suspended": True, "email": email}
        except HTTPException:
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"Failed to suspend user: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to suspend user")
        finally:
            session.close()

    def unsuspend_user(
        self,
        admin_user: User,
        email: str,
    ) -> dict:
        """
        Unsuspend a user, restoring their access to the platform.

        Args:
            admin_user: The authenticated admin user
            email: The user's email address

        Returns:
            dict: Updated user info
        """
        self._require_admin(admin_user)

        session = DBSession()
        try:
            user = session.query(User).filter(User.email == email).first()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            if not user.suspended_at:
                raise HTTPException(status_code=400, detail="User is not suspended")

            user.suspended_at = None
            user.suspended_by = None
            session.commit()

            self.logger.info(f"User unsuspended: {email} by {admin_user.email}")
            record_auth_event(
                event_type="admin.user_unsuspend",
                outcome="success",
                actor_email=admin_user.email,
                target_email=email,
            )
            return {"unsuspended": True, "email": email}
        except HTTPException:
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"Failed to unsuspend user: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to unsuspend user")
        finally:
            session.close()

    def delete_user(
        self,
        admin_user: User,
        email: str,
    ) -> dict:
        """
        Delete a user from the platform.

        Args:
            admin_user: The authenticated admin user
            email: The user's email address

        Returns:
            dict: Deletion confirmation
        """
        self._require_admin(admin_user)

        if admin_user.email == email:
            raise HTTPException(status_code=400, detail="Cannot delete yourself")

        session = DBSession()
        try:
            user = session.query(User).filter(User.email == email).first()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            if user.is_group:
                raise HTTPException(status_code=400, detail="Use delete_group for groups")

            # Delete all related data to avoid FK constraint issues
            from dataio.api.database.models import (
                Session as SessionModel,
                UserAPIKey,
                WebAuthnCredential,
                MagicLinkToken,
                OTPToken,
                WebAuthnChallenge,
            )

            # Delete sessions
            session.query(SessionModel).filter(SessionModel.user_email == email).delete()

            # Delete group memberships
            session.query(UserGroup).filter(UserGroup.user_email == email).delete()

            # Delete permissions
            session.query(UserPermission).filter(UserPermission.user_email == email).delete()

            # Delete API keys
            session.query(UserAPIKey).filter(UserAPIKey.user_email == email).delete()

            # Delete passkeys
            session.query(WebAuthnCredential).filter(WebAuthnCredential.user_email == email).delete()

            # Delete WebAuthn challenges
            session.query(WebAuthnChallenge).filter(WebAuthnChallenge.user_email == email).delete()

            # Delete magic link tokens
            session.query(MagicLinkToken).filter(MagicLinkToken.email == email).delete()

            # Delete OTP tokens
            session.query(OTPToken).filter(OTPToken.email == email).delete()

            # Delete user
            session.delete(user)
            session.commit()

            self.logger.info(f"User deleted: {email} by {admin_user.email}")
            return {"deleted": True, "email": email}
        except HTTPException:
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"Failed to delete user: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to delete user")
        finally:
            session.close()

    def bulk_invite_users(
        self,
        admin_user: User,
        users_data: List[dict],
    ) -> dict:
        """
        Bulk invite users from a list (e.g., CSV upload).

        Args:
            admin_user: The authenticated admin user
            users_data: List of user data dicts with 'email', optional 'display_name', 'is_admin', 'groups'

        Returns:
            dict: Results summary
        """
        self._require_admin(admin_user)

        results = {
            "success": [],
            "failed": [],
            "total": len(users_data),
        }

        for user_data in users_data:
            email = user_data.get("email", "").strip().lower()
            if not email:
                results["failed"].append({"email": "", "error": "Missing email"})
                continue

            try:
                self.invite_user(
                    admin_user=admin_user,
                    email=email,
                    display_name=user_data.get("display_name"),
                    is_admin=user_data.get("is_admin", False),
                    groups=user_data.get("groups"),
                )
                results["success"].append(email)
            except HTTPException as e:
                results["failed"].append({"email": email, "error": e.detail})
            except Exception as e:
                results["failed"].append({"email": email, "error": str(e)})

        return results

    def set_user_dataset_permission(
        self,
        admin_user: User,
        email: str,
        dataset_id: str,
        permission: str,
    ) -> dict:
        """
        Set a user's permission for a specific dataset.

        Args:
            admin_user: The authenticated admin user
            email: The user's email address
            dataset_id: The dataset ID
            permission: Permission level ('VIEW', 'DOWNLOAD', or 'NONE' to remove)

        Returns:
            dict: Updated permission info
        """
        from dataio.api.database.enums import AccessLevel, ResourceType

        self._require_admin(admin_user)

        session = DBSession()
        try:
            # Verify user exists
            user = session.query(User).filter(User.email == email).first()
            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            # Verify dataset exists
            from dataio.api.database.models import Dataset
            dataset = session.query(Dataset).filter(Dataset.ds_id == dataset_id).first()
            if not dataset:
                raise HTTPException(status_code=404, detail="Dataset not found")

            # Handle removal
            if permission == "NONE":
                session.query(UserPermission).filter(
                    UserPermission.user_email == email,
                    UserPermission.resource_type == ResourceType.DATASET,
                    UserPermission.resource_id == dataset_id,
                ).delete()
                session.commit()
                return {"set": True, "email": email, "dataset_id": dataset_id, "permission": None}

            # Validate permission level
            try:
                perm_level = AccessLevel[permission]
            except KeyError:
                raise HTTPException(status_code=400, detail=f"Invalid permission: {permission}")

            # Check if permission exists
            existing = session.query(UserPermission).filter(
                UserPermission.user_email == email,
                UserPermission.resource_type == ResourceType.DATASET,
                UserPermission.resource_id == dataset_id,
            ).first()

            if existing:
                existing.permission = perm_level
            else:
                new_perm = UserPermission(
                    user_email=email,
                    resource_type=ResourceType.DATASET,
                    resource_id=dataset_id,
                    permission=perm_level,
                )
                session.add(new_perm)

            session.commit()
            self.logger.info(f"Permission set: {email} -> {dataset_id} = {permission} by {admin_user.email}")
            return {"set": True, "email": email, "dataset_id": dataset_id, "permission": permission}
        except HTTPException:
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"Failed to set permission: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to set permission")
        finally:
            session.close()

    def delete_group(
        self,
        admin_user: User,
        group_email: str,
    ) -> dict:
        """
        Delete a group from the platform.

        Args:
            admin_user: The authenticated admin user
            group_email: The group's email address

        Returns:
            dict: Deletion confirmation
        """
        self._require_admin(admin_user)

        session = DBSession()
        try:
            group = session.query(User).filter(
                User.email == group_email,
                User.is_group == True
            ).first()
            if not group:
                raise HTTPException(status_code=404, detail="Group not found")

            # Delete memberships
            session.query(UserGroup).filter(UserGroup.group_email == group_email).delete()
            # Delete permissions
            session.query(UserPermission).filter(UserPermission.user_email == group_email).delete()
            # Delete group
            session.delete(group)
            session.commit()

            self.logger.info(f"Group deleted: {group_email} by {admin_user.email}")
            return {"deleted": True, "group_email": group_email}
        except HTTPException:
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"Failed to delete group: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to delete group")
        finally:
            session.close()

    def set_group_dataset_permission(
        self,
        admin_user: User,
        group_email: str,
        dataset_id: str,
        permission: str,
    ) -> dict:
        """
        Set a group's permission for a specific dataset.

        Args:
            admin_user: The authenticated admin user
            group_email: The group's email address
            dataset_id: The dataset ID
            permission: Permission level ('VIEW', 'DOWNLOAD', or 'NONE' to remove)

        Returns:
            dict: Updated permission info
        """
        from dataio.api.database.enums import AccessLevel, ResourceType

        self._require_admin(admin_user)

        session = DBSession()
        try:
            # Verify group exists
            group = session.query(User).filter(
                User.email == group_email,
                User.is_group == True
            ).first()
            if not group:
                raise HTTPException(status_code=404, detail="Group not found")

            # Verify dataset exists
            from dataio.api.database.models import Dataset
            dataset = session.query(Dataset).filter(Dataset.ds_id == dataset_id).first()
            if not dataset:
                raise HTTPException(status_code=404, detail="Dataset not found")

            # Handle removal
            if permission == "NONE":
                session.query(UserPermission).filter(
                    UserPermission.user_email == group_email,
                    UserPermission.resource_type == ResourceType.DATASET,
                    UserPermission.resource_id == dataset_id,
                ).delete()
                session.commit()
                return {"set": True, "group_email": group_email, "dataset_id": dataset_id, "permission": None}

            # Validate permission level
            try:
                perm_level = AccessLevel[permission]
            except KeyError:
                raise HTTPException(status_code=400, detail=f"Invalid permission: {permission}")

            # Check if permission exists
            existing = session.query(UserPermission).filter(
                UserPermission.user_email == group_email,
                UserPermission.resource_type == ResourceType.DATASET,
                UserPermission.resource_id == dataset_id,
            ).first()

            if existing:
                existing.permission = perm_level
            else:
                new_perm = UserPermission(
                    user_email=group_email,
                    resource_type=ResourceType.DATASET,
                    resource_id=dataset_id,
                    permission=perm_level,
                )
                session.add(new_perm)

            session.commit()
            self.logger.info(f"Group permission set: {group_email} -> {dataset_id} = {permission} by {admin_user.email}")
            return {"set": True, "group_email": group_email, "dataset_id": dataset_id, "permission": permission}
        except HTTPException:
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"Failed to set group permission: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to set group permission")
        finally:
            session.close()

    def list_datasets_for_permissions(
        self,
        admin_user: User,
        search: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict:
        """
        List datasets available for permission assignment.

        Args:
            admin_user: The authenticated admin user
            search: Optional search term
            limit: Maximum number of results
            offset: Pagination offset

        Returns:
            dict: List of datasets
        """
        from dataio.api.database.models import Dataset

        self._require_admin(admin_user)

        session = DBSession()
        try:
            query = session.query(Dataset)

            if search:
                query = query.filter(
                    (Dataset.ds_id.ilike(f"%{search}%")) |
                    (Dataset.title.ilike(f"%{search}%"))
                )

            total = query.count()
            datasets = query.order_by(Dataset.title).offset(offset).limit(limit).all()

            return {
                "datasets": [
                    {
                        "ds_id": d.ds_id,
                        "title": d.title,
                        "access_level": d.access_level.value if d.access_level else None,
                    }
                    for d in datasets
                ],
                "total": total,
                "limit": limit,
                "offset": offset,
            }
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
