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
from dataio.api.auth.security import record_auth_event
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
            # bcrypt 4.0+ returns str, older versions return bytes
            hash_result = bcrypt.hashpw(raw_key.encode("utf-8"), bcrypt.gensalt())
            key_hash = hash_result if isinstance(hash_result, str) else hash_result.decode("utf-8")

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

            self.logger.info(f"Created API key '{name}' for user: {user.email}")

            # Send notification email (non-blocking - don't fail if email fails)
            try:
                self.email_service.send_api_key_created_email(user.email, name)
            except Exception as email_error:
                self.logger.warning(f"Failed to send API key notification email: {str(email_error)}")

            record_auth_event(
                event_type="api_key.create",
                outcome="success",
                actor_email=user.email,
                target_email=user.email,
                details={"name": name},
            )
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
            record_auth_event(
                event_type="api_key.revoke",
                outcome="success",
                actor_email=user.email,
                target_email=user.email,
                details={"key_id": key_id, "name": api_key.name},
            )
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

            # Get datasets with permission filtering (returns Dataset ORM objects)
            dataset_objects = database.get_datasets(limit=10000, user_permissions=user_permissions)

            # Apply additional filters on ORM objects
            if search:
                search_lower = search.lower()
                dataset_objects = [
                    d for d in dataset_objects
                    if search_lower in (d.title or "").lower()
                    or search_lower in (d.description or "").lower()
                ]

            if collection_id:
                dataset_objects = [
                    d for d in dataset_objects
                    if d.collection and d.collection.id == collection_id
                ]

            if data_owner_id:
                dataset_objects = [
                    d for d in dataset_objects
                    if d.data_owner and d.data_owner.id == data_owner_id
                ]

            # Get total before pagination
            total = len(dataset_objects)

            # Apply pagination
            dataset_objects = dataset_objects[offset:offset + limit]

            # Convert to dicts for response
            datasets = [
                {
                    "ds_id": d.ds_id,
                    "title": d.title,
                    "description": d.description,
                    "collection_id": d.collection.id if d.collection else None,
                    "collection_name": d.collection.collection_name if d.collection else None,
                    "data_owner_id": d.data_owner.id if d.data_owner else None,
                    "data_owner_name": d.data_owner.name if d.data_owner else None,
                    "temporal_coverage_start_date": d.temporal_coverage_start_date.isoformat() if d.temporal_coverage_start_date else None,
                    "temporal_coverage_end_date": d.temporal_coverage_end_date.isoformat() if d.temporal_coverage_end_date else None,
                    "access_level": d.access_level.value if d.access_level else None,
                }
                for d in dataset_objects
            ]

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
                accessible_ids = {d.ds_id for d in accessible}
                if dataset_id not in accessible_ids:
                    raise HTTPException(status_code=403, detail="Access denied to this dataset")

            # Determine user's access level for this dataset
            can_download = dataset.access_level and dataset.access_level.value == "DOWNLOAD"
            if not can_download and not user.is_admin:
                # Check if user has explicit download permission
                for perm in user_permissions:
                    if perm.resource_type in ("DATASET", "*") and perm.resource_id in (dataset_id, "*"):
                        if perm.permission and perm.permission.value == "DOWNLOAD":
                            can_download = True
                            break

            # Get additional details including raw datasets for download
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
                "can_download": can_download,
                "raw_datasets": [
                    {
                        "id": rd.id,
                        "rds_id": rd.rds_id,
                        "title": rd.title,
                        "source": rd.source if can_download else None,
                    }
                    for rd in (dataset.raw_datasets or [])
                ] if can_download else [],
                "tags": [tag.tag_name for tag in (dataset.tags or [])],
                # Documentation fields (cached from file server)
                "readme_md": dataset.readme_md if hasattr(dataset, 'readme_md') else None,
                "data_dictionary_json": dataset.data_dictionary_json if hasattr(dataset, 'data_dictionary_json') else None,
                "documentation_synced_at": dataset.documentation_synced_at.isoformat() if hasattr(dataset, 'documentation_synced_at') and dataset.documentation_synced_at else None,
            }
        except HTTPException:
            raise
        except Exception as e:
            self.logger.error(f"Failed to get dataset: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to get dataset")
        finally:
            session.close()

    def get_dataset_download_urls(self, user: User, dataset_id: str) -> dict:
        """
        Get presigned download URLs for all tables in a dataset.

        Args:
            user: The authenticated user
            dataset_id: The dataset ID

        Returns:
            dict: Download URLs for tables and metadata
        """
        from dataio.api.services.filestore_service import FilestoreService
        from dataio.api.models import VersionType

        session = DBSession()
        try:
            dataset = database.get_dataset(dataset_id)
            if not dataset:
                raise HTTPException(status_code=404, detail="Dataset not found")

            # Check permissions - verify user can download this dataset
            user_permissions = determine_user_permissions(user)
            can_download = dataset.access_level and dataset.access_level.value == "DOWNLOAD"

            if not can_download and not user.is_admin:
                # Check if user has explicit download permission
                for perm in user_permissions:
                    if perm.resource_type in ("DATASET", "*") and perm.resource_id in (dataset_id, "*"):
                        if perm.permission and perm.permission.value == "DOWNLOAD":
                            can_download = True
                            break

            if not can_download:
                raise HTTPException(status_code=403, detail="Download permission required")

            # Get presigned URLs for all tables
            filestore = FilestoreService()
            tables = []

            # Try STANDARDISED first, fall back to PREPROCESSED
            last_error = None
            for version_type in [VersionType.STANDARDISED, VersionType.PREPROCESSED]:
                try:
                    self.logger.info(f"Trying to list files for {dataset_id} with version {version_type.value}")
                    files = filestore.list_files_in_s3(dataset_id, version_type)
                    self.logger.info(f"Found {len(files) if files else 0} files for {dataset_id} with {version_type.value}")
                    if files:
                        tables = [
                            {
                                "table_name": f["table_name"],
                                "download_url": f["download_link"],
                                "metadata": f.get("metadata", {}),
                            }
                            for f in files
                        ]
                        self.logger.info(f"Successfully retrieved {len(tables)} tables for download")
                        break
                except Exception as e:
                    last_error = e
                    self.logger.warning(f"Failed to list files for {dataset_id} with {version_type.value}: {str(e)}", exc_info=True)
                    continue

            if not tables and last_error:
                self.logger.error(f"No tables found for {dataset_id}. Last error: {str(last_error)}")

            return {
                "ds_id": dataset.ds_id,
                "title": dataset.title,
                "tables": tables,
                "readme_md": dataset.readme_md if hasattr(dataset, 'readme_md') else None,
                "data_dictionary_json": dataset.data_dictionary_json if hasattr(dataset, 'data_dictionary_json') else None,
            }
        except HTTPException:
            raise
        except Exception as e:
            self.logger.error(f"Failed to get download URLs: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to get download URLs")
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

    # ==========================================================================
    # Public Dataset Access (No Authentication Required)
    # ==========================================================================

    def get_public_datasets(
        self,
        search: Optional[str] = None,
        collection_id: Optional[int] = None,
        data_owner_id: Optional[int] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict:
        """
        Get publicly accessible datasets (VIEW or DOWNLOAD access level).

        No authentication required. Returns only datasets that are publicly
        visible based on their access_level field.

        Args:
            search: Optional search term for title/description
            collection_id: Optional filter by collection
            data_owner_id: Optional filter by data owner
            limit: Maximum number of results
            offset: Pagination offset

        Returns:
            dict: List of public datasets and pagination info
        """
        session = DBSession()
        try:
            from dataio.api.database.models import AccessLevel
            from sqlalchemy.orm import joinedload

            # Query datasets with public access (VIEW or DOWNLOAD)
            query = (
                session.query(Dataset)
                .options(
                    joinedload(Dataset.collection),
                    joinedload(Dataset.data_owner),
                )
                .filter(Dataset.access_level.in_([AccessLevel.VIEW, AccessLevel.DOWNLOAD]))
            )

            # Get all matching datasets first
            dataset_objects = query.all()

            # Apply additional filters in memory
            if search:
                search_lower = search.lower()
                dataset_objects = [
                    d for d in dataset_objects
                    if search_lower in (d.title or "").lower()
                    or search_lower in (d.description or "").lower()
                ]

            if collection_id:
                dataset_objects = [
                    d for d in dataset_objects
                    if d.collection and d.collection.id == collection_id
                ]

            if data_owner_id:
                dataset_objects = [
                    d for d in dataset_objects
                    if d.data_owner and d.data_owner.id == data_owner_id
                ]

            # Get total before pagination
            total = len(dataset_objects)

            # Apply pagination
            dataset_objects = dataset_objects[offset:offset + limit]

            # Convert to dicts for response (metadata only, no download info)
            datasets = [
                {
                    "ds_id": d.ds_id,
                    "title": d.title,
                    "description": d.description,
                    "collection_id": d.collection.id if d.collection else None,
                    "collection_name": d.collection.collection_name if d.collection else None,
                    "data_owner_id": d.data_owner.id if d.data_owner else None,
                    "data_owner_name": d.data_owner.name if d.data_owner else None,
                    "temporal_coverage_start_date": d.temporal_coverage_start_date.isoformat() if d.temporal_coverage_start_date else None,
                    "temporal_coverage_end_date": d.temporal_coverage_end_date.isoformat() if d.temporal_coverage_end_date else None,
                    "access_level": d.access_level.value if d.access_level else None,
                }
                for d in dataset_objects
            ]

            return {
                "datasets": datasets,
                "total": total,
                "limit": limit,
                "offset": offset,
            }
        except Exception as e:
            self.logger.error(f"Failed to get public datasets: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to get public datasets")
        finally:
            session.close()

    def get_public_dataset(self, dataset_id: str) -> dict:
        """
        Get a single public dataset by ID.

        No authentication required. Only returns datasets with VIEW or DOWNLOAD
        access level. Never includes download URLs.

        Args:
            dataset_id: The dataset ID

        Returns:
            dict: Dataset details (metadata only, no download links)
        """
        session = DBSession()
        try:
            from dataio.api.database.models import AccessLevel

            dataset = database.get_dataset(dataset_id)
            if not dataset:
                raise HTTPException(status_code=404, detail="Dataset not found")

            # Check if dataset is publicly accessible
            if dataset.access_level not in [AccessLevel.VIEW, AccessLevel.DOWNLOAD]:
                raise HTTPException(status_code=404, detail="Dataset not found")

            # Return metadata only - never include download URLs for public access
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
                "can_download": False,  # Always false for public access - must login to download
                "raw_datasets": [],  # Never expose download links for public access
                "tags": [tag.tag_name for tag in (dataset.tags or [])],
                # Documentation fields (cached from file server)
                "readme_md": dataset.readme_md if hasattr(dataset, 'readme_md') else None,
                "data_dictionary_json": dataset.data_dictionary_json if hasattr(dataset, 'data_dictionary_json') else None,
                "documentation_synced_at": dataset.documentation_synced_at.isoformat() if hasattr(dataset, 'documentation_synced_at') and dataset.documentation_synced_at else None,
            }
        except HTTPException:
            raise
        except Exception as e:
            self.logger.error(f"Failed to get public dataset: {str(e)}")
            raise HTTPException(status_code=500, detail="Failed to get public dataset")
        finally:
            session.close()
