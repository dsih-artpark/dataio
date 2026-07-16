"""
Web admin service for user, group, and dataset administration.

Provides functionality for admin users to manage users, groups,
permissions, manifests, and validation through the web interface.
"""

import logging
import json
import re
from io import BytesIO
from pathlib import Path
from typing import List, Optional

from fastapi import HTTPException, UploadFile
import yaml

from dataio.api.database import functions as database
from dataio.api.database.config import Session as DBSession
from dataio.api.database.enums import VersionType
from dataio.api.models import (
    DatasetCreate,
    DatasetDocumentationUpdate,
    DatasetUpdate,
    RawDatasetCreate,
    RawDatasetUpdate,
    TableMetadata,
)
from dataio.api.database.models import Collection, DataOwner, Dataset, User, UserGroup, UserPermission
from dataio.api.auth.otp import create_otp, verify_otp
from dataio.api.auth.security import enforce_rate_limit
from dataio.api.services.base_service import BaseService
from dataio.api.services.admin_dataset_service import AdminDatasetService
from dataio.api.services.email_service import EmailService
from dataio.api.auth.permissions import is_admin
from dataio.api.auth.security import record_auth_event
from dataio.api.services.platform_manifest_validation_service import (
    apply_platform_manifest_checks,
)
from dataio.validate import DataIOValidationService, DatasetKind, ValidationRequest

logger = logging.getLogger(__name__)


class WebAdminService(BaseService):
    """Service for web admin operations."""

    def __init__(self):
        super().__init__()
        self.email_service = EmailService()
        self.admin_dataset_service = AdminDatasetService()
        self.validation_service = DataIOValidationService(
            platform_manifest_checker=apply_platform_manifest_checks
        )

    def _require_admin(self, user: User) -> None:
        """Verify user has admin privileges."""
        logger.info(f"_require_admin called for user: {getattr(user, 'email', 'N/A')}")
        is_admin_result = is_admin(user)
        logger.info(f"_require_admin - is_admin returned: {is_admin_result}")
        if not is_admin_result:
            logger.warning(f"_require_admin - denying access for user: {getattr(user, 'email', 'N/A')}")
            raise HTTPException(status_code=403, detail="Admin privileges required")

    def _slugify(self, value: str) -> str:
        slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
        return slug or "dataset"

    def _build_manifest_field(self, field_name: str, field_spec: dict, enum_scope: Optional[dict] = None) -> dict:
        field_type = field_spec.get("type")
        manifest_field = {
            "description": field_spec.get("description"),
            "comments": field_spec.get("comments"),
            "nullable": field_spec.get("nullable", True),
        }
        if field_type == "year":
            manifest_field["type"] = "date"
            manifest_field["format"] = "%Y"
        elif field_type == "enum":
            manifest_field["type"] = "enum"
            allowed_values = field_spec.get("enum") or field_spec.get("allowedValues")
            if not allowed_values and field_spec.get("enumRef") and enum_scope:
                enum_ref = field_spec["enumRef"]
                # Enum blocks may be authored as flat top-level keys, or nested
                # under a top-level "enumDefinitions" container. Support both.
                nested_definitions = enum_scope.get("enumDefinitions")
                enum_def = enum_scope.get(enum_ref)
                if not isinstance(enum_def, dict) and isinstance(nested_definitions, dict):
                    enum_def = nested_definitions.get(enum_ref)
                if isinstance(enum_def, dict):
                    allowed_values = list((enum_def.get("values") or {}).keys())
            manifest_field["allowedValues"] = allowed_values or []
        elif field_type in {"string", "boolean", "int", "float", "regionID", "regionName", "date", "dateTime"}:
            manifest_field["type"] = field_type
            if field_spec.get("format"):
                manifest_field["format"] = field_spec["format"]
        else:
            if field_name == "year":
                manifest_field["type"] = "date"
                manifest_field["format"] = "%Y"
            elif field_name.endswith(".ID"):
                manifest_field["type"] = "regionID"
            elif field_name.endswith(".name"):
                manifest_field["type"] = "regionName"
            else:
                manifest_field["type"] = "string"
        if field_spec.get("range") is not None:
            manifest_field["range"] = field_spec["range"]
        if field_spec.get("min") is not None:
            manifest_field["min"] = field_spec["min"]
        if field_spec.get("max") is not None:
            manifest_field["max"] = field_spec["max"]
        # Carry through any remaining authored keys verbatim (e.g. isJoinKey,
        # joinKeyType, unit) so documentation-only annotations survive into
        # the downloadable manifest instead of being silently dropped.
        # ManifestField allows extra fields, so this is safe. Excludes keys
        # already deliberately resolved above (e.g. "type" here is the raw
        # authored value like "year", which must not clobber the resolved
        # "date" set on manifest_field).
        handled_keys = {
            "type", "description", "comments", "nullable", "format",
            "enum", "allowedValues", "enumRef", "range", "min", "max",
        }
        for key, value in field_spec.items():
            if key not in handled_keys and key not in manifest_field:
                manifest_field[key] = value
        return manifest_field

    def _parse_dataset_package(
        self,
        info_text: str,
        metadata_text: str,
        *,
        csv_files: Optional[List] = None,
        dataset_override: Optional[dict] = None,
        raw_dataset_override: Optional[dict] = None,
    ) -> dict:
        try:
            info = yaml.safe_load(info_text) or {}
        except yaml.YAMLError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid info.yml: {exc}") from exc
        try:
            metadata = yaml.safe_load(metadata_text) or {}
        except yaml.YAMLError as exc:
            raise HTTPException(status_code=400, detail=f"Invalid metadata.yml: {exc}") from exc

        dataset_override = dataset_override or {}
        raw_dataset_override = raw_dataset_override or {}
        csv_files = csv_files or []

        session = DBSession()
        try:
            collection_id = dataset_override.get("collection_id") or info.get("collection_id") or ""
            collection = None
            if collection_id:
                collection = (
                    session.query(Collection)
                    .filter(Collection.collection_id == collection_id)
                    .first()
                )
            suggested_dataset_id = (
                database.suggest_next_dataset_id(collection_id) if collection_id and collection else ""
            )
            dataset_payload = {
                "ds_id": dataset_override.get("ds_id") or info.get("ds_id") or suggested_dataset_id,
                "title": dataset_override.get("title") or info.get("title") or "",
                "collection_id": collection_id,
                "data_owner_name": dataset_override.get("data_owner_name") or info.get("data_owner_name") or "",
                "description": dataset_override.get("description") if "description" in dataset_override else info.get("description"),
                "spatial_coverage_region_id": dataset_override.get("spatial_coverage_region_id") if "spatial_coverage_region_id" in dataset_override else info.get("spatial_coverage_region_id"),
                "spatial_resolution": dataset_override.get("spatial_resolution") or info.get("spatial_resolution"),
                "temporal_coverage_start_date": dataset_override.get("temporal_coverage_start_date") if "temporal_coverage_start_date" in dataset_override else info.get("temporal_coverage_start_date"),
                "temporal_coverage_end_date": dataset_override.get("temporal_coverage_end_date") if "temporal_coverage_end_date" in dataset_override else info.get("temporal_coverage_end_date"),
                "temporal_resolution": dataset_override.get("temporal_resolution") or info.get("temporal_resolution"),
                "access_level": dataset_override.get("access_level") or info.get("access_level") or "NONE",
                "additional_metadata": dataset_override.get("additional_metadata") if "additional_metadata" in dataset_override else info.get("additional_metadata"),
                "tags": dataset_override.get("tags") if "tags" in dataset_override else info.get("tags", []),
            }

            raw_info = info.get("raw_dataset", {}) or {}
            raw_payload = {
                "rds_id": raw_dataset_override.get("rds_id") or raw_info.get("rds_id") or (
                    f"{dataset_payload['ds_id']}-raw-001" if dataset_payload["ds_id"] else ""
                ),
                "title": raw_dataset_override.get("title") or raw_info.get("title") or (
                    f"Raw data for {dataset_payload['title']}" if dataset_payload["title"] else "Raw dataset"
                ),
                "source": raw_dataset_override.get("source") or raw_info.get("source") or "Manual upload",
            }

            tables = metadata.get("tables", {}) or {}
            table_uploads = []
            manifest_tables = {}
            custom_findings = []
            csv_by_stem = {Path(file.filename or "").stem: file for file in csv_files if file.filename}
            matched_csv_stems = set()
            inline_data_files = {}

            for table_key, table_definition in tables.items():
                info_block = table_definition.get("info", {}) or {}
                table_name = info_block.get("table_name") or table_key
                data_dictionary = table_definition.get("data_dictionary", {}) or {}
                table_metadata = {
                    "table_name": table_name,
                    "description": info_block.get("about") or table_definition.get("description"),
                    "source": info_block.get("source"),
                    "data_dictionary": {
                        field_name: {
                            "description": field_spec.get("description"),
                            "comments": field_spec.get("comments"),
                            "access": field_spec.get("access", True),
                        }
                        for field_name, field_spec in data_dictionary.items()
                        if isinstance(field_spec, dict)
                    },
                }
                table_uploads.append(
                    {
                        "table_name": table_name,
                        "description": table_metadata["description"],
                        "source": table_metadata["source"],
                        "table_metadata": table_metadata,
                    }
                )
                manifest_tables[table_name] = {
                    "description": table_metadata["description"],
                    "path": f"{table_name}.csv",
                    "dataDictionary": {
                        field_name: self._build_manifest_field(field_name, field_spec, metadata)
                        for field_name, field_spec in data_dictionary.items()
                        if isinstance(field_spec, dict)
                    },
                }
                # Carry through any remaining authored table-level keys
                # verbatim (e.g. source, joinKeys, comments) so they survive
                # into the downloadable manifest instead of being silently
                # dropped. ManifestTable allows extra fields, so this is safe.
                for key, value in table_definition.items():
                    if key not in {"info", "data_dictionary"} and key not in manifest_tables[table_name]:
                        manifest_tables[table_name][key] = value
                matched_file = csv_by_stem.get(table_name)
                if matched_file is None:
                    custom_findings.append(
                        {
                            "severity": "warning",
                            "code": "missing_csv_upload",
                            "message": f"No CSV uploaded yet for table '{table_name}'.",
                            "table": table_name,
                            "path": f"tables.{table_name}",
                        }
                    )
                    continue
                matched_csv_stems.add(table_name)
                matched_file.file.seek(0)
                inline_data_files[table_name] = matched_file.file.read().decode("utf-8")
                matched_file.file.seek(0)

            for stem in sorted(set(csv_by_stem.keys()) - matched_csv_stems):
                custom_findings.append(
                    {
                        "severity": "warning",
                        "code": "unmatched_csv_upload",
                        "message": f"Uploaded CSV '{stem}' is not declared in metadata.yml.",
                        "table": stem,
                        "path": f"tables.{stem}",
                    }
                )

            if not dataset_payload["title"]:
                custom_findings.append({"severity": "error", "code": "missing_title", "message": "Dataset title is required.", "path": "info.title"})
            if not dataset_payload["data_owner_name"]:
                custom_findings.append({"severity": "error", "code": "missing_data_owner", "message": "Data owner name is required.", "path": "info.data_owner_name"})
            elif session.query(DataOwner).filter(DataOwner.name == dataset_payload["data_owner_name"]).first() is None:
                custom_findings.append({"severity": "error", "code": "unknown_data_owner", "message": f"Data owner '{dataset_payload['data_owner_name']}' does not exist.", "path": "info.data_owner_name"})
            if not dataset_payload["collection_id"]:
                custom_findings.append({"severity": "error", "code": "missing_collection", "message": "Collection ID is required.", "path": "info.collection_id"})
            elif collection is None:
                custom_findings.append({"severity": "error", "code": "unknown_collection", "message": f"Collection '{dataset_payload['collection_id']}' does not exist.", "path": "info.collection_id"})
            if not dataset_payload["ds_id"]:
                custom_findings.append({"severity": "error", "code": "missing_dataset_id", "message": "Dataset ID is required.", "path": "info.ds_id"})
            elif (
                session.query(Dataset).filter(Dataset.ds_id == dataset_payload["ds_id"]).first() is not None
                and dataset_override.get("existing_dataset_id") != dataset_payload["ds_id"]
            ):
                custom_findings.append({"severity": "error", "code": "duplicate_dataset_id", "message": f"Dataset ID '{dataset_payload['ds_id']}' already exists.", "path": "info.ds_id"})
            if not raw_payload["rds_id"]:
                custom_findings.append({"severity": "error", "code": "missing_raw_dataset_id", "message": "Raw dataset ID is required.", "path": "info.raw_dataset.rds_id"})

            # "enumDefinitions" needs special handling: some authors set it to
            # null and define enum vocab as flat top-level blocks instead (a
            # bare null would collide with the manifest's formal dict-typed
            # field), while others nest real enum vocab under this key
            # directly. Drop it when it's not a populated dict, but pass it
            # through verbatim when it is - otherwise the enum value
            # descriptions authors wrote are silently dropped from every
            # downloaded package for datasets using the nested convention.
            raw_enum_definitions = metadata.get("enumDefinitions")
            enum_definitions_passthrough = (
                {"enumDefinitions": raw_enum_definitions}
                if isinstance(raw_enum_definitions, dict) and raw_enum_definitions
                else {}
            )

            manifest_payload = {
                # Carry through every top-level key authored in metadata.yaml
                # (tags, spatial/temporal coverage, comments, references, custom
                # enum-definition blocks, etc.) verbatim. "tables" is excluded
                # since the processed/enumRef-resolved version is rebuilt below
                # as "datasetTables". "enumDefinitions" is excluded here and
                # conditionally re-added above (see comment).
                **{
                    key: value
                    for key, value in metadata.items()
                    if key not in {"tables", "enumDefinitions"}
                },
                **enum_definitions_passthrough,
                "metadataSpecVersion": "v2",
                "datasetTitle": dataset_payload["title"] or "Untitled dataset",
                "datasetSlug": self._slugify(f"{dataset_payload['ds_id']} {dataset_payload['title']}"),
                "datasetDescription": dataset_payload["description"] or dataset_payload["title"] or "Dataset import",
                "source": raw_payload["source"],
                "category": {
                    "ID": collection.category_id if collection else "UNKNOWN",
                    "name": collection.category_name if collection else "Unknown category",
                },
                "collection": {
                    "ID": dataset_payload["collection_id"] or "UNKNOWN",
                    "name": collection.collection_name if collection else (dataset_payload["collection_id"] or "Unknown collection"),
                },
                "datasetID": dataset_payload["ds_id"] or None,
                "datasetKind": "tabular",
                "datasetTables": manifest_tables,
            }
            manifest_text = yaml.safe_dump(manifest_payload, sort_keys=False)

            validation_request = ValidationRequest(
                dataset_kind=DatasetKind.TABULAR,
                manifest_source=manifest_text,
                data_files=inline_data_files,
                validate_data=bool(inline_data_files),
            )
            validation_result = self.validation_service.validate(validation_request).model_dump()
            findings = custom_findings + validation_result["findings"]
            can_import = bool(inline_data_files) and not any(
                finding.get("severity") == "error" for finding in findings
            )

            return {
                "dataset": {**dataset_payload, "raw_dataset_ids": [raw_payload["rds_id"]] if raw_payload["rds_id"] else []},
                "raw_dataset": raw_payload,
                "tables": table_uploads,
                "manifest_yaml": manifest_text,
                "findings": findings,
                "suggested_dataset_id": suggested_dataset_id or None,
                "can_import": can_import,
            }
        finally:
            session.close()

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

    def get_dataset_detail(self, admin_user: User, dataset_id: str) -> dict:
        self._require_admin(admin_user)
        return self.admin_dataset_service.get_dataset_admin_detail(dataset_id)

    def create_raw_dataset(self, admin_user: User, raw_dataset: RawDatasetCreate):
        self._require_admin(admin_user)
        return self.admin_dataset_service.create_raw_dataset(raw_dataset)

    def update_raw_dataset(
        self,
        admin_user: User,
        raw_dataset_id: str,
        raw_dataset: RawDatasetUpdate,
    ):
        self._require_admin(admin_user)
        return self.admin_dataset_service.update_raw_dataset(raw_dataset_id, raw_dataset)

    def list_raw_datasets(
        self,
        admin_user: User,
        search: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict:
        self._require_admin(admin_user)
        return self.admin_dataset_service.list_raw_datasets(search=search, limit=limit, offset=offset)

    def create_dataset(self, admin_user: User, dataset: DatasetCreate):
        self._require_admin(admin_user)
        return self.admin_dataset_service.create_dataset(dataset)

    def update_dataset(self, admin_user: User, dataset_id: str, dataset: DatasetUpdate):
        self._require_admin(admin_user)
        return self.admin_dataset_service.update_dataset(dataset_id, dataset)

    def update_dataset_documentation(
        self,
        admin_user: User,
        dataset_id: str,
        documentation: DatasetDocumentationUpdate,
    ):
        self._require_admin(admin_user)

        try:
            session = DBSession()
            dataset = session.query(Dataset).filter(Dataset.ds_id == dataset_id).first()
            if not dataset:
                raise HTTPException(status_code=404, detail="Dataset not found")
            session.close()

            if "readme_md" in documentation.model_fields_set:
                self.admin_dataset_service.filestore_service.upsert_dataset_readme(
                    dataset_id, documentation.readme_md
                )

            if "data_dictionary_json" in documentation.model_fields_set:
                self.admin_dataset_service.filestore_service.upsert_dataset_metadata_json(
                    dataset_id, documentation.data_dictionary_json
                )

            self.admin_dataset_service.refresh_dataset_documentation_cache(dataset_id)
            return self.admin_dataset_service.get_dataset_admin_detail(dataset_id)
        except HTTPException:
            raise
        except Exception as e:
            self.logger.error(f"Failed to update dataset documentation: {e!s}")
            raise HTTPException(
                status_code=500,
                detail="Failed to update dataset documentation. Contact support.",
            ) from e

    def suggest_next_dataset_id(self, admin_user: User, collection_id: str) -> dict:
        self._require_admin(admin_user)
        return self.admin_dataset_service.suggest_next_dataset_id(collection_id)

    def suggest_next_raw_dataset_id(self, admin_user: User, collection_id: str) -> dict:
        self._require_admin(admin_user)
        return self.admin_dataset_service.suggest_next_raw_dataset_id(collection_id)

    def list_reserved_dataset_ids(
        self,
        admin_user: User,
        search: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict:
        self._require_admin(admin_user)
        rows, total = database.list_reserved_dataset_ids(search=search, limit=limit, offset=offset)
        return {
            "reservations": [
                {
                    "ds_id": row.ds_id,
                    "collection_id": row.collection_id,
                    "note": row.note,
                    "reserved_by": row.reserved_by,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                }
                for row in rows
            ],
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    def reserve_dataset_id(
        self,
        admin_user: User,
        ds_id: str,
        collection_id: Optional[str] = None,
        note: Optional[str] = None,
    ) -> dict:
        self._require_admin(admin_user)
        reservation = database.create_reserved_dataset_id(ds_id, collection_id, note, admin_user.email)
        return {
            "ds_id": reservation.ds_id,
            "collection_id": reservation.collection_id,
            "note": reservation.note,
            "reserved_by": reservation.reserved_by,
            "created_at": reservation.created_at.isoformat() if reservation.created_at else None,
        }

    def delete_reserved_dataset_id(self, admin_user: User, ds_id: str) -> dict:
        self._require_admin(admin_user)
        database.delete_reserved_dataset_id(ds_id)
        return {"deleted": True, "ds_id": ds_id}

    def preview_dataset_package_import(
        self,
        admin_user: User,
        info_file,
        metadata_file,
        csv_files: Optional[List] = None,
        dataset_override: Optional[dict] = None,
        raw_dataset_override: Optional[dict] = None,
    ) -> dict:
        self._require_admin(admin_user)
        info_text = info_file.file.read().decode("utf-8")
        metadata_text = metadata_file.file.read().decode("utf-8")
        info_file.file.seek(0)
        metadata_file.file.seek(0)
        return self._parse_dataset_package(
            info_text,
            metadata_text,
            csv_files=csv_files,
            dataset_override=dataset_override,
            raw_dataset_override=raw_dataset_override,
        )

    def import_dataset_package(
        self,
        admin_user: User,
        info_file,
        metadata_file,
        csv_files: List,
        dataset_override: Optional[dict] = None,
        raw_dataset_override: Optional[dict] = None,
        bucket_type: VersionType = VersionType.STANDARDISED,
    ) -> dict:
        self._require_admin(admin_user)
        preview = self.preview_dataset_package_import(
            admin_user,
            info_file,
            metadata_file,
            csv_files=csv_files,
            dataset_override=dataset_override,
            raw_dataset_override=raw_dataset_override,
        )
        if not preview["can_import"]:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "Package validation failed",
                    "findings": preview["findings"],
                },
            )

        raw_dataset_payload = preview["raw_dataset"]
        existing_raw = database.get_raw_dataset_by_identifier(raw_dataset_payload["rds_id"])
        if existing_raw:
            self.admin_dataset_service.update_raw_dataset(
                raw_dataset_payload["rds_id"],
                RawDatasetUpdate(
                    title=raw_dataset_payload["title"],
                    source=raw_dataset_payload["source"],
                ),
            )
        else:
            self.admin_dataset_service.create_raw_dataset(
                RawDatasetCreate(**raw_dataset_payload)
            )

        dataset_payload = dict(preview["dataset"])
        dataset_payload["raw_dataset_ids"] = [raw_dataset_payload["rds_id"]]
        self.admin_dataset_service.create_dataset(DatasetCreate(**dataset_payload))

        table_files = {Path(file.filename or "").stem: file for file in csv_files if file.filename}
        uploaded_tables = []
        for table in preview["tables"]:
            table_file = table_files.get(table["table_name"])
            if table_file is None:
                continue
            table_file.file.seek(0)
            metadata_upload = UploadFile(
                filename="table-metadata.json",
                file=BytesIO(json.dumps(table["table_metadata"]).encode("utf-8")),
            )
            self.admin_dataset_service.create_dataset_table(
                dataset_payload["ds_id"],
                bucket_type,
                table_file,
                metadata_upload,
            )
            uploaded_tables.append(table["table_name"])

        manifest_upload = UploadFile(
            filename="manifest.yaml",
            file=BytesIO(preview["manifest_yaml"].encode("utf-8")),
        )
        self.admin_dataset_service.upsert_dataset_manifest(
            dataset_payload["ds_id"],
            bucket_type,
            manifest_upload,
            admin_user.email,
        )

        return {
            "dataset_id": dataset_payload["ds_id"],
            "bucket_type": bucket_type.value,
            "uploaded_tables": uploaded_tables,
            "manifest_uploaded": True,
        }

    def initiate_dataset_deletion(self, admin_user: User, dataset_id: str) -> dict:
        self._require_admin(admin_user)
        if not database.check_if_dataset_exists(dataset_id):
            raise HTTPException(status_code=404, detail="Dataset not found")
        enforce_rate_limit("dataset_delete_initiate", f"{admin_user.email}:{dataset_id}", limit=3)
        try:
            otp_code, _ = create_otp(admin_user.email, purpose=f"dataset_deletion:{dataset_id}")
        except ValueError as exc:
            raise HTTPException(status_code=429, detail=str(exc)) from exc

        subject = f"Confirm deletion of dataset {dataset_id}"
        text_body = (
            f"You requested deletion of dataset {dataset_id}.\n\n"
            f"Enter this verification code to confirm deletion:\n\n{otp_code}\n\n"
            "If you did not request this action, ignore this email."
        )
        html_body = (
            f"<p>You requested deletion of dataset <strong>{dataset_id}</strong>.</p>"
            f"<p>Enter this verification code to confirm deletion:</p>"
            f"<p style='font-size: 32px; font-weight: bold; letter-spacing: 8px;'>{otp_code}</p>"
            "<p>If you did not request this action, ignore this email.</p>"
        )
        if not self.email_service.send_email(admin_user.email, subject, html_body, text_body):
            raise HTTPException(status_code=500, detail="Failed to send verification email. Please try again.")
        record_auth_event(
            event_type="dataset.delete_initiate",
            outcome="success",
            actor_email=admin_user.email,
            target_email=admin_user.email,
            details={"dataset_id": dataset_id},
        )
        return {"sent": True, "message": "Verification code sent. Check your email to confirm dataset deletion."}

    def verify_dataset_deletion(
        self,
        admin_user: User,
        dataset_id: str,
        code: str,
        confirmation_dataset_id: str,
    ) -> dict:
        self._require_admin(admin_user)
        if confirmation_dataset_id != dataset_id:
            raise HTTPException(status_code=400, detail="Confirmation dataset ID does not match the selected dataset")
        enforce_rate_limit("dataset_delete_verify", f"{admin_user.email}:{dataset_id}", limit=5)
        if not verify_otp(admin_user.email, code, purpose=f"dataset_deletion:{dataset_id}"):
            record_auth_event(
                event_type="dataset.delete_verify",
                outcome="failed",
                actor_email=admin_user.email,
                target_email=admin_user.email,
                details={"dataset_id": dataset_id, "reason": "invalid_otp"},
            )
            raise HTTPException(status_code=401, detail="Invalid or expired verification code")
        result = self.admin_dataset_service.delete_dataset(dataset_id)
        record_auth_event(
            event_type="dataset.delete_verify",
            outcome="success",
            actor_email=admin_user.email,
            target_email=admin_user.email,
            details={"dataset_id": dataset_id},
        )
        return result

    def list_dataset_tables(
        self,
        admin_user: User,
        dataset_id: str,
        bucket_type: VersionType,
    ) -> dict:
        self._require_admin(admin_user)
        return self.admin_dataset_service.list_dataset_tables(dataset_id, bucket_type)

    def create_dataset_table(
        self,
        admin_user: User,
        dataset_id: str,
        bucket_type: VersionType,
        file,
        table_metadata_file,
    ) -> dict:
        self._require_admin(admin_user)
        return self.admin_dataset_service.create_dataset_table(
            dataset_id,
            bucket_type,
            file,
            table_metadata_file,
        )

    def check_dataset_documentation_sync(
        self,
        admin_user: User,
        dataset_id: str | None = None,
    ) -> dict:
        self._require_admin(admin_user)
        return self.admin_dataset_service.check_dataset_documentation_sync(dataset_id)

    def sync_dataset_documentation(
        self,
        admin_user: User,
        dataset_id: str | None = None,
        *,
        only_outdated: bool = True,
        force: bool = False,
    ) -> dict:
        self._require_admin(admin_user)
        return self.admin_dataset_service.sync_dataset_documentation(
            dataset_id,
            only_outdated=only_outdated,
            force=force,
        )

    def get_dataset_manifest(
        self,
        admin_user: User,
        dataset_id: str,
        bucket_type: VersionType,
    ) -> dict:
        """Get the canonical manifest for a dataset/version."""
        self._require_admin(admin_user)

        manifest = self.admin_dataset_service.get_dataset_manifest(dataset_id, bucket_type)

        session = DBSession()
        try:
            dataset = session.query(Dataset).filter(Dataset.ds_id == dataset_id).first()
            if dataset is None:
                raise HTTPException(status_code=404, detail="Dataset not found")

            return {
                **manifest,
                "dataset_id": dataset_id,
                "bucket_type": bucket_type.value,
                "manifest_updated_at": (
                    dataset.manifest_updated_at.isoformat()
                    if dataset.manifest_updated_at
                    else None
                ),
                "manifest_updated_by": dataset.manifest_updated_by,
            }
        finally:
            session.close()

    def upsert_dataset_manifest(
        self,
        admin_user: User,
        dataset_id: str,
        bucket_type: VersionType,
        manifest_file,
    ) -> dict:
        """Validate and persist the canonical manifest for a dataset/version."""
        self._require_admin(admin_user)
        return self.admin_dataset_service.upsert_dataset_manifest(
            dataset_id,
            bucket_type,
            manifest_file,
            admin_user.email,
        )

    def validate_dataset(
        self,
        admin_user: User,
        dataset_kind: DatasetKind,
        manifest_file,
        data_file=None,
        table_name: str | None = None,
        deep_check: bool = False,
        extra_column_policy: str = "warn",
    ) -> dict:
        """Run admin validation for a candidate manifest and optional data file."""
        self._require_admin(admin_user)

        manifest_text = manifest_file.file.read().decode("utf-8")
        if dataset_kind == DatasetKind.TABULAR:
            try:
                parsed_manifest = yaml.safe_load(manifest_text) or {}
            except yaml.YAMLError:
                parsed_manifest = {}

            if data_file is not None and table_name is None and isinstance(parsed_manifest, dict):
                dataset_tables = parsed_manifest.get("datasetTables", {})
                if len(dataset_tables) == 1:
                    table_name = next(iter(dataset_tables))

        request = ValidationRequest(
            dataset_kind=dataset_kind,
            manifest_source=manifest_text,
            data=None,
            deep_check=deep_check,
            validate_data=data_file is not None,
            extra_column_policy=extra_column_policy,
        )

        if data_file is not None:
            data_text = data_file.file.read().decode("utf-8")
            if dataset_kind == DatasetKind.TABULAR:
                resolved_table_name = table_name or Path(data_file.filename or "table.csv").stem
                request.data_files = {resolved_table_name: data_text}
            else:
                request.data = data_text

        return self.validation_service.validate(request).model_dump()

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
