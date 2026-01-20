"""
Web user service for user profile and API key management.

Provides functionality for the web dashboard including profile management,
API key generation, and dataset access.
"""

import os
import secrets
import logging
from datetime import datetime, timezone
from typing import Optional, List

import bcrypt
from fastapi import HTTPException

from dataio.api.database.config import Session as DBSession
from dataio.api.database.models import User, UserAPIKey, Dataset, Collection, DataOwner
from dataio.api.services.base_service import BaseService
from dataio.api.services.email_service import EmailService
from dataio.api.auth.permissions import determine_user_permissions
from dataio.api.database import functions as database

logger = logging.getLogger(__name__)

# API Key configuration
API_KEY_LENGTH = int(os.getenv("API_KEY_LENGTH", "32"))
API_KEY_PREFIX = os.getenv("API_KEY_PREFIX", "dio_")


class WebUserService(BaseService):
    """Service for web user operations."""

    def __init__(self):
        super().__init__()
        self.email_service = EmailService()

    def get_current_user_profile(self, user: User) -> dict:
        """
        Get the current user's profile.

        Args:
            user: The authenticated user

        Returns:
            dict: User profile information
        """
        session = DBSession()
        try:
            # Refresh user from database
            db_user = session.query(User).filter(User.email == user.email).first()
            if not db_user:
                raise HTTPException(status_code=404, detail="User not found")

            return {
                "email": db_user.email,
                "display_name": db_user.display_name,
                "is_admin": db_user.is_admin,
                "email_verified": db_user.email_verified,
                "last_login": db_user.last_login.isoformat() if db_user.last_login else None,
                "created_at": db_user.created_at.isoformat() if db_user.created_at else None,
            }
        finally:
            session.close()

    def update_user_profile(
        self,
        user: User,
        display_name: Optional[str] = None,
    ) -> dict:
        """
        Update the current user's profile.

        Args:
            user: The authenticated user
            display_name: Optional new display name

        Returns:
            dict: Updated user profile
        """
        session = DBSession()
        try:
            db_user = session.query(User).filter(User.email == user.email).first()
            if not db_user:
                raise HTTPException(status_code=404, detail="User not found")

            if display_name is not None:
                db_user.display_name = display_name

            session.commit()
            self.logger.info(f"Updated profile for user: {user.email}")

            return self.get_current_user_profile(db_user)
        except HTTPException:
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"Failed to update profile: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to update profile")
        finally:
            session.close()

    # API Key Management

    def list_api_keys(self, user: User) -> dict:
        """
        List all API keys for a user.

        Args:
            user: The authenticated user

        Returns:
            dict: List of API keys (without actual key values)
        """
        session = DBSession()
        try:
            api_keys = (
                session.query(UserAPIKey)
                .filter(
                    UserAPIKey.user_email == user.email,
                    UserAPIKey.revoked_at.is_(None),
                )
                .order_by(UserAPIKey.created_at.desc())
                .all()
            )

            return {
                "api_keys": [
                    {
                        "id": str(key.id),
                        "name": key.name,
                        "key_prefix": f"{API_KEY_PREFIX}{key.key_prefix}...",
                        "created_at": key.created_at.isoformat() if key.created_at else None,
                        "last_used_at": key.last_used_at.isoformat() if key.last_used_at else None,
                        "expires_at": key.expires_at.isoformat() if key.expires_at else None,
                    }
                    for key in api_keys
                ]
            }
        finally:
            session.close()

    def create_api_key(
        self,
        user: User,
        name: str,
        expires_at: Optional[datetime] = None,
    ) -> dict:
        """
        Create a new API key for a user.

        Args:
            user: The authenticated user
            name: Name/description for the API key
            expires_at: Optional expiration datetime

        Returns:
            dict: The created API key (including the actual key, shown only once)
        """
        session = DBSession()
        try:
            # Generate a secure random key
            raw_key = secrets.token_urlsafe(API_KEY_LENGTH)
            full_key = f"{API_KEY_PREFIX}{raw_key}"

            # Hash the key for storage
            key_hash = bcrypt.hashpw(raw_key.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

            # Create API key record
            api_key = UserAPIKey(
                user_email=user.email,
                key_hash=key_hash,
                key_prefix=raw_key[:8],
                name=name,
                expires_at=expires_at,
            )

            session.add(api_key)
            session.commit()
            session.refresh(api_key)

            # Send notification email
            self.email_service.send_api_key_created_email(user.email, name)

            self.logger.info(f"Created API key '{name}' for user: {user.email}")

            return {
                "id": str(api_key.id),
                "name": api_key.name,
                "key": full_key,  # Only returned once!
                "key_prefix": f"{API_KEY_PREFIX}{api_key.key_prefix}...",
                "created_at": api_key.created_at.isoformat(),
                "expires_at": api_key.expires_at.isoformat() if api_key.expires_at else None,
                "warning": "This is the only time the full key will be shown. Please save it securely.",
            }
        except Exception as e:
            session.rollback()
            self.logger.error(f"Failed to create API key: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to create API key")
        finally:
            session.close()

    def revoke_api_key(self, user: User, key_id: str) -> dict:
        """
        Revoke an API key.

        Args:
            user: The authenticated user
            key_id: The ID of the API key to revoke

        Returns:
            dict: Response with status
        """
        session = DBSession()
        try:
            api_key = (
                session.query(UserAPIKey)
                .filter(
                    UserAPIKey.id == key_id,
                    UserAPIKey.user_email == user.email,
                    UserAPIKey.revoked_at.is_(None),
                )
                .first()
            )

            if not api_key:
                raise HTTPException(status_code=404, detail="API key not found")

            api_key.revoked_at = datetime.now(timezone.utc)
            session.commit()

            self.logger.info(f"Revoked API key '{api_key.name}' for user: {user.email}")
            return {"revoked": True}
        except HTTPException:
            raise
        except Exception as e:
            session.rollback()
            self.logger.error(f"Failed to revoke API key: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to revoke API key")
        finally:
            session.close()

    # Dataset Access

    def get_datasets(
        self,
        user: User,
        search: Optional[str] = None,
        collection_id: Optional[int] = None,
        data_owner_id: Optional[int] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict:
        """
        Get datasets accessible to the user with optional filters.

        Args:
            user: The authenticated user
            search: Optional search term for title/description
            collection_id: Optional filter by collection
            data_owner_id: Optional filter by data owner
            limit: Maximum number of results
            offset: Pagination offset

        Returns:
            dict: List of datasets and pagination info
        """
        session = DBSession()
        try:
            # Get user permissions
            user_permissions = determine_user_permissions(user)

            # Get datasets with permission filtering
            datasets = database.get_datasets(limit=limit, user_permissions=user_permissions)

            # Apply additional filters
            if search:
                search_lower = search.lower()
                datasets = [
                    d for d in datasets
                    if search_lower in d.get("title", "").lower()
                    or search_lower in d.get("description", "").lower()
                ]

            if collection_id:
                datasets = [
                    d for d in datasets
                    if d.get("collection_id") == collection_id
                ]

            if data_owner_id:
                datasets = [
                    d for d in datasets
                    if d.get("data_owner_id") == data_owner_id
                ]

            # Apply pagination
            total = len(datasets)
            datasets = datasets[offset:offset + limit]

            return {
                "datasets": datasets,
                "total": total,
                "limit": limit,
                "offset": offset,
            }
        except Exception as e:
            self.logger.error(f"Failed to get datasets: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to get datasets")
        finally:
            session.close()

    def get_dataset(self, user: User, dataset_id: str) -> dict:
        """
        Get a single dataset by ID.

        Args:
            user: The authenticated user
            dataset_id: The dataset ID

        Returns:
            dict: Dataset details
        """
        session = DBSession()
        try:
            dataset = database.get_dataset(dataset_id)
            if not dataset:
                raise HTTPException(status_code=404, detail="Dataset not found")

            # Check permissions - verify user can access this dataset
            user_permissions = determine_user_permissions(user)
            if not user.is_admin:
                # Get accessible datasets for this user and check if requested dataset is included
                accessible = database.get_datasets(limit=10000, user_permissions=user_permissions)
                accessible_ids = {d.get("ds_id") for d in accessible}
                if dataset_id not in accessible_ids:
                    raise HTTPException(status_code=403, detail="Access denied to this dataset")

            # Get additional details
            return {
                "ds_id": dataset.ds_id,
                "title": dataset.title,
                "description": dataset.description,
                "collection": {
                    "id": dataset.collection.id,
                    "name": dataset.collection.collection_name,
                    "category": dataset.collection.category_name,
                } if dataset.collection else None,
                "data_owner": {
                    "id": dataset.data_owner.id,
                    "name": dataset.data_owner.name,
                } if dataset.data_owner else None,
                "spatial_coverage_region_id": dataset.spatial_coverage_region_id,
                "spatial_resolution": dataset.spatial_resolution.value if dataset.spatial_resolution else None,
                "temporal_coverage_start_date": dataset.temporal_coverage_start_date.isoformat() if dataset.temporal_coverage_start_date else None,
                "temporal_coverage_end_date": dataset.temporal_coverage_end_date.isoformat() if dataset.temporal_coverage_end_date else None,
                "temporal_resolution": dataset.temporal_resolution.value if dataset.temporal_resolution else None,
                "access_level": dataset.access_level.value if dataset.access_level else None,
                "additional_metadata": dataset.additional_metadata,
            }
        except HTTPException:
            raise
        except Exception as e:
            self.logger.error(f"Failed to get dataset: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to get dataset")
        finally:
            session.close()

    def get_collections(self) -> dict:
        """
        Get all collections for filtering.

        Returns:
            dict: List of collections
        """
        session = DBSession()
        try:
            collections = session.query(Collection).all()
            return {
                "collections": [
                    {
                        "id": c.id,
                        "collection_id": c.collection_id,
                        "collection_name": c.collection_name,
                        "category_id": c.category_id,
                        "category_name": c.category_name,
                    }
                    for c in collections
                ]
            }
        finally:
            session.close()

    def get_data_owners(self) -> dict:
        """
        Get all data owners for filtering.

        Returns:
            dict: List of data owners
        """
        session = DBSession()
        try:
            owners = session.query(DataOwner).all()
            return {
                "data_owners": [
                    {
                        "id": o.id,
                        "name": o.name,
                    }
                    for o in owners
                ]
            }
        finally:
            session.close()
