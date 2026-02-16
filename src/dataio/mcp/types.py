"""
Type definitions for the DataIO MCP server.
"""

from dataclasses import dataclass, field
from typing import Any, Literal
from enum import Enum


class ToolName(str, Enum):
    """Available MCP tools."""
    SEARCH_DATASETS = "search_datasets"
    GET_DATASET_DETAILS = "get_dataset_details"
    LIST_CATEGORIES = "list_categories"
    LIST_DATA_OWNERS = "list_data_owners"
    GET_DOWNLOAD_INFO = "get_download_info"
    GET_DATASET_SCHEMA = "get_dataset_schema"


@dataclass
class ToolResult:
    """Result from a tool execution."""
    success: bool
    data: Any = None
    error: str | None = None


@dataclass
class ToolError:
    """Error from a tool execution."""
    code: str
    message: str
    details: dict | None = None


@dataclass
class UserContext:
    """Context about the user making the request."""
    email: str
    is_admin: bool = False
    groups: list[str] = field(default_factory=list)


@dataclass
class ToolDefinition:
    """Definition of an MCP tool."""
    name: str
    description: str
    input_schema: dict


# Bedrock-compatible tool format
@dataclass
class BedrockToolSpec:
    """Tool specification in AWS Bedrock format."""
    name: str
    description: str
    input_schema: dict

    def to_bedrock_format(self) -> dict:
        """Convert to Bedrock API format."""
        return {
            "toolSpec": {
                "name": self.name,
                "description": self.description,
                "inputSchema": {
                    "json": self.input_schema
                }
            }
        }
