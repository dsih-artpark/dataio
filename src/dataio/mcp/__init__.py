"""
DataIO MCP (Model Context Protocol) Server.

This module provides an MCP server that allows AI assistants to interact
with the DataIO platform - searching datasets, viewing metadata, and
requesting downloads.
"""

from dataio.mcp.server import DataIOMCPServer
from dataio.mcp.types import ToolResult, ToolError

__all__ = ["DataIOMCPServer", "ToolResult", "ToolError"]
