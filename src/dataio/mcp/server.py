"""
DataIO MCP Server - Core server implementation.

This server exposes DataIO platform capabilities as MCP tools that can be
used by AI assistants like Claude via AWS Bedrock.
"""

import logging
from contextlib import contextmanager
from typing import Any

from sqlalchemy import and_, or_

from dataio.api.database.config import Session as DBSession
from dataio.api.database.models import Dataset, Collection, DataOwner, Tag, User, UserPermission, UserGroup
from dataio.api.database.enums import AccessLevel
from dataio.mcp.types import (
    ToolResult,
    ToolError,
    UserContext,
    BedrockToolSpec,
    ToolName,
)

logger = logging.getLogger(__name__)


@contextmanager
def get_db_session():
    """Context manager for database sessions."""
    session = DBSession()
    try:
        yield session
    finally:
        session.close()


def check_dataset_access(session, user_email: str, dataset_id: str) -> bool:
    """
    Check if a user has access to a dataset.

    Returns True if:
    - Dataset is public (access_level = DOWNLOAD)
    - User is admin
    - User has direct permission
    - User's group has permission
    """
    # Get the dataset
    dataset = session.query(Dataset).filter(Dataset.ds_id == dataset_id).first()
    if not dataset:
        return False

    # Public datasets are accessible to all
    if dataset.access_level == AccessLevel.DOWNLOAD:
        return True

    # Get user
    user = session.query(User).filter(User.email == user_email).first()
    if not user:
        return False

    # Admins have access to everything
    if user.is_admin:
        return True

    # Check direct permissions
    direct_perm = session.query(UserPermission).filter(
        UserPermission.user_email == user_email,
        UserPermission.resource_type == "DATASET",
        UserPermission.resource_id == dataset_id
    ).first()
    if direct_perm:
        return True

    # Check group permissions
    user_groups = session.query(UserGroup).filter(UserGroup.user_email == user_email).all()
    for ug in user_groups:
        group_perm = session.query(UserPermission).filter(
            UserPermission.user_email == ug.group_email,
            UserPermission.resource_type == "DATASET",
            UserPermission.resource_id == dataset_id
        ).first()
        if group_perm:
            return True

    return False


def can_download(session, user_context: UserContext | None, dataset: Dataset) -> bool:
    """Open download datasets for everyone; others only with a grant (or for admins)."""
    if user_context is None:
        return dataset.access_level == AccessLevel.DOWNLOAD
    return check_dataset_access(session, user_context.email, dataset.ds_id)


def shows_details(session, user_context: UserContext | None, dataset: Dataset) -> bool:
    """Same as the public website: Open and View only details are visible to anyone;
    a Restricted dataset shows only its title unless the user may download it."""
    if dataset.access_level in (AccessLevel.DOWNLOAD, AccessLevel.VIEW):
        return True
    return can_download(session, user_context, dataset)


def _is_admin(user_context: UserContext | None) -> bool:
    return bool(user_context and user_context.is_admin)


RESTRICTED_NOTE = (
    "Restricted dataset: only its title is shared. Details, columns and files are "
    "available after access is granted; use 'Request access' on the dataset's page."
)


class DataIOMCPServer:
    """
    MCP Server for the DataIO platform.

    Provides tools for:
    - Searching datasets by query, category, tags
    - Getting detailed dataset information
    - Listing categories and data owners
    - Getting download information (for authorized users)
    - Getting dataset schemas/data dictionaries
    """

    def __init__(self):
        self._tools = self._build_tool_definitions()

    def _build_tool_definitions(self) -> dict[str, BedrockToolSpec]:
        """Build the tool definitions for Bedrock."""
        return {
            ToolName.SEARCH_DATASETS: BedrockToolSpec(
                name=ToolName.SEARCH_DATASETS,
                description=(
                    "Search for datasets in the DataIO platform. "
                    "Can filter by text query, category, data owner, or tags. "
                    "Returns a list of matching datasets with basic metadata."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "Text search query to match against dataset titles and descriptions"
                        },
                        "category": {
                            "type": "string",
                            "description": "Filter by category name (e.g., 'Weather', 'Satellite', 'Demographics')"
                        },
                        "data_owner": {
                            "type": "string",
                            "description": "Filter by data owner/provider name"
                        },
                        "tags": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Filter by tags (datasets must have ALL specified tags)"
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of results to return (default: 10, max: 50)",
                            "default": 10
                        }
                    }
                }
            ),

            ToolName.GET_DATASET_DETAILS: BedrockToolSpec(
                name=ToolName.GET_DATASET_DETAILS,
                description=(
                    "Get detailed information about a specific dataset including "
                    "description, temporal/spatial coverage, data owner, tags, and README content."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "dataset_id": {
                            "type": "string",
                            "description": "The unique dataset ID (ds_id)"
                        }
                    },
                    "required": ["dataset_id"]
                }
            ),

            ToolName.LIST_CATEGORIES: BedrockToolSpec(
                name=ToolName.LIST_CATEGORIES,
                description=(
                    "List all available dataset categories in the platform. "
                    "Use this to understand what types of data are available."
                ),
                input_schema={
                    "type": "object",
                    "properties": {}
                }
            ),

            ToolName.LIST_DATA_OWNERS: BedrockToolSpec(
                name=ToolName.LIST_DATA_OWNERS,
                description=(
                    "List all data owners/providers in the platform. "
                    "Use this to see which organizations contribute data."
                ),
                input_schema={
                    "type": "object",
                    "properties": {}
                }
            ),

            ToolName.GET_DOWNLOAD_INFO: BedrockToolSpec(
                name=ToolName.GET_DOWNLOAD_INFO,
                description=(
                    "Get download information for a dataset. "
                    "Returns file list and sizes. Actual download URLs require user authorization."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "dataset_id": {
                            "type": "string",
                            "description": "The unique dataset ID (ds_id)"
                        }
                    },
                    "required": ["dataset_id"]
                }
            ),

            ToolName.GET_DATASET_SCHEMA: BedrockToolSpec(
                name=ToolName.GET_DATASET_SCHEMA,
                description=(
                    "Get the data dictionary/schema for a dataset. "
                    "Returns column names, types, and descriptions if available."
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "dataset_id": {
                            "type": "string",
                            "description": "The unique dataset ID (ds_id)"
                        }
                    },
                    "required": ["dataset_id"]
                }
            ),
        }

    def get_tool_definitions(self) -> list[dict]:
        """Get all tool definitions in Bedrock format."""
        return [tool.to_bedrock_format() for tool in self._tools.values()]

    async def execute_tool(
        self,
        tool_name: str,
        tool_input: dict,
        user_context: UserContext | None = None
    ) -> ToolResult:
        """
        Execute a tool with the given input.

        Args:
            tool_name: Name of the tool to execute
            tool_input: Input parameters for the tool
            user_context: Optional user context for permission checks

        Returns:
            ToolResult with success status and data or error
        """
        try:
            if tool_name == ToolName.SEARCH_DATASETS:
                return await self._search_datasets(tool_input, user_context)
            elif tool_name == ToolName.GET_DATASET_DETAILS:
                return await self._get_dataset_details(tool_input, user_context)
            elif tool_name == ToolName.LIST_CATEGORIES:
                return await self._list_categories()
            elif tool_name == ToolName.LIST_DATA_OWNERS:
                return await self._list_data_owners(user_context)
            elif tool_name == ToolName.GET_DOWNLOAD_INFO:
                return await self._get_download_info(tool_input, user_context)
            elif tool_name == ToolName.GET_DATASET_SCHEMA:
                return await self._get_dataset_schema(tool_input, user_context)
            else:
                return ToolResult(
                    success=False,
                    error=f"Unknown tool: {tool_name}"
                )
        except Exception as e:
            logger.exception(f"Error executing tool {tool_name}")
            return ToolResult(success=False, error=str(e))

    async def _search_datasets(
        self,
        params: dict,
        user_context: UserContext | None
    ) -> ToolResult:
        """Search for datasets."""
        query = params.get("query", "")
        category = params.get("category")
        data_owner = params.get("data_owner")
        tags = params.get("tags", [])
        limit = min(params.get("limit", 10), 50)

        with get_db_session() as session:
            q = session.query(Dataset)
            # A Restricted dataset must not match on details it hides (description,
            # category, owner, tags); admins see everything.
            hide_restricted = not _is_admin(user_context)

            # Text search
            if query:
                search_pattern = f"%{query}%"
                description_match = Dataset.description.ilike(search_pattern)
                if hide_restricted:
                    description_match = and_(
                        Dataset.access_level != AccessLevel.NONE, description_match
                    )
                q = q.filter(
                    or_(
                        Dataset.title.ilike(search_pattern),
                        Dataset.ds_id.ilike(search_pattern),
                        description_match,
                    )
                )

            if hide_restricted and (category or data_owner or tags):
                q = q.filter(Dataset.access_level != AccessLevel.NONE)

            # Category filter
            if category:
                q = q.join(Collection).filter(
                    Collection.category_name.ilike(f"%{category}%")
                )

            # Data owner filter
            if data_owner:
                q = q.join(DataOwner).filter(
                    DataOwner.name.ilike(f"%{data_owner}%")
                )

            # Tag filter
            if tags:
                for tag in tags:
                    q = q.filter(Dataset.tags.any(Tag.tag_name.ilike(f"%{tag}%")))

            # Every dataset is listed, as on the website; Restricted ones by title only.
            datasets = q.limit(limit).all()

            results = []
            for ds in datasets:
                downloadable = can_download(session, user_context, ds)
                if not shows_details(session, user_context, ds):
                    results.append({
                        "dataset_id": ds.ds_id,
                        "title": ds.title,
                        "access_level": ds.access_level.value,
                        "can_download": False,
                        "note": RESTRICTED_NOTE,
                    })
                    continue
                results.append({
                    "dataset_id": ds.ds_id,
                    "title": ds.title,
                    "description": ds.description[:200] + "..." if ds.description and len(ds.description) > 200 else ds.description,
                    "category": ds.collection.category_name if ds.collection else None,
                    "data_owner": ds.data_owner.name if ds.data_owner else None,
                    "access_level": ds.access_level.value,
                    "can_download": downloadable,
                    "tags": [t.tag_name for t in ds.tags]
                })

            return ToolResult(
                success=True,
                data={
                    "datasets": results,
                    "count": len(results),
                    "query_params": {
                        "query": query,
                        "category": category,
                        "data_owner": data_owner,
                        "tags": tags
                    }
                }
            )

    async def _get_dataset_details(
        self,
        params: dict,
        user_context: UserContext | None
    ) -> ToolResult:
        """Get detailed dataset information."""
        dataset_id = params.get("dataset_id")
        if not dataset_id:
            return ToolResult(success=False, error="dataset_id is required")

        with get_db_session() as session:
            dataset = session.query(Dataset).filter(
                Dataset.ds_id == dataset_id
            ).first()

            if not dataset:
                return ToolResult(success=False, error=f"Dataset not found: {dataset_id}")

            if not shows_details(session, user_context, dataset):
                return ToolResult(
                    success=True,
                    data={
                        "dataset_id": dataset.ds_id,
                        "title": dataset.title,
                        "access_level": dataset.access_level.value,
                        "can_download": False,
                        "note": RESTRICTED_NOTE,
                    },
                )

            owner = dataset.data_owner
            return ToolResult(
                success=True,
                data={
                    "dataset_id": dataset.ds_id,
                    "title": dataset.title,
                    "description": dataset.description,
                    "category": dataset.collection.category_name if dataset.collection else None,
                    "collection": dataset.collection.collection_name if dataset.collection else None,
                    "can_download": can_download(session, user_context, dataset),
                    "data_owner": {
                        "name": owner.name if owner else None,
                        # A named person: shown to admins only, as on the website.
                        **({"contact": owner.contact_person if owner else None} if _is_admin(user_context) else {}),
                    },
                    "spatial_coverage": {
                        "region": dataset.spatial_coverage_region.region_name if dataset.spatial_coverage_region else None,
                        "resolution": dataset.spatial_resolution.value if dataset.spatial_resolution else None,
                    },
                    "temporal_coverage": {
                        "start_date": str(dataset.temporal_coverage_start_date) if dataset.temporal_coverage_start_date else None,
                        "end_date": str(dataset.temporal_coverage_end_date) if dataset.temporal_coverage_end_date else None,
                        "resolution": dataset.temporal_resolution.value if dataset.temporal_resolution else None,
                    },
                    "access_level": dataset.access_level.value,
                    "tags": [t.tag_name for t in dataset.tags],
                    "readme": dataset.readme_md,
                    "additional_metadata": dataset.additional_metadata,
                }
            )

    async def _list_categories(self) -> ToolResult:
        """List all categories."""
        with get_db_session() as session:
            categories = session.query(
                Collection.category_id,
                Collection.category_name
            ).distinct().all()

            return ToolResult(
                success=True,
                data={
                    "categories": [
                        {"id": c.category_id, "name": c.category_name}
                        for c in categories
                    ]
                }
            )

    async def _list_data_owners(self, user_context: UserContext | None = None) -> ToolResult:
        """List all data owners (contact people for admins only)."""
        with get_db_session() as session:
            owners = session.query(DataOwner).all()
            admin = _is_admin(user_context)

            return ToolResult(
                success=True,
                data={
                    "data_owners": [
                        {"name": o.name, **({"contact_person": o.contact_person} if admin else {})}
                        for o in owners
                    ]
                }
            )

    async def _get_download_info(
        self,
        params: dict,
        user_context: UserContext | None
    ) -> ToolResult:
        """Get download information for a dataset."""
        dataset_id = params.get("dataset_id")
        if not dataset_id:
            return ToolResult(success=False, error="dataset_id is required")

        with get_db_session() as session:
            dataset = session.query(Dataset).filter(
                Dataset.ds_id == dataset_id
            ).first()

            if not dataset:
                return ToolResult(success=False, error=f"Dataset not found: {dataset_id}")

            if not can_download(session, user_context, dataset):
                return ToolResult(
                    success=False,
                    error="You don't have access to download this dataset. Please request access through the DataIO platform."
                )

            # For now, return a message about how to download
            # In a full implementation, this would list files from S3
            return ToolResult(
                success=True,
                data={
                    "dataset_id": dataset_id,
                    "title": dataset.title,
                    "access_level": dataset.access_level.value,
                    "download_instructions": (
                        "To download this dataset, please visit the DataIO web interface "
                        "and use your API key or web session to access the download links."
                    ),
                    "api_endpoint": f"/api/v1/user/datasets/{dataset_id}/download-urls"
                }
            )

    async def _get_dataset_schema(
        self,
        params: dict,
        user_context: UserContext | None
    ) -> ToolResult:
        """Get dataset schema/data dictionary."""
        dataset_id = params.get("dataset_id")
        if not dataset_id:
            return ToolResult(success=False, error="dataset_id is required")

        with get_db_session() as session:
            dataset = session.query(Dataset).filter(
                Dataset.ds_id == dataset_id
            ).first()

            if not dataset:
                return ToolResult(success=False, error=f"Dataset not found: {dataset_id}")

            # Columns are public for Open and View only datasets, as on the website.
            if not shows_details(session, user_context, dataset):
                return ToolResult(success=False, error=RESTRICTED_NOTE)

            # Prefer normalized manifest JSON, then fall back to legacy metadata.json.
            schema_data = None
            if getattr(dataset, "manifest_json", None):
                schema_data = dataset.manifest_json
            elif dataset.data_dictionary_json:
                import json
                try:
                    schema_data = json.loads(dataset.data_dictionary_json)
                except json.JSONDecodeError:
                    schema_data = None

            return ToolResult(
                success=True,
                data={
                    "dataset_id": dataset_id,
                    "title": dataset.title,
                    "schema": schema_data,
                    "manifest": schema_data,
                    "has_schema": schema_data is not None,
                    "has_manifest": schema_data is not None,
                }
            )
