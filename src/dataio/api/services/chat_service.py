"""
Chat Service - Orchestrates conversations between users, AWS Bedrock, and MCP tools.

This service handles the agentic loop:
1. Receive user message
2. Send to Bedrock with MCP tool definitions
3. Execute any tool calls
4. Return tool results to Bedrock
5. Stream final response to user
"""

import json
import logging
import os
import uuid
from datetime import datetime
from typing import AsyncGenerator, Any

import boto3
from sqlalchemy.orm import Session

from dataio.api.database.config import Session as DBSession
from dataio.api.database.models import User
from dataio.api.services.base_service import BaseService
from dataio.mcp.server import DataIOMCPServer
from dataio.mcp.types import UserContext

logger = logging.getLogger(__name__)

# Configuration
BEDROCK_REGION = os.getenv("AWS_BEDROCK_REGION", "us-east-1")
BEDROCK_MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "anthropic.claude-3-5-sonnet-20241022-v2:0")
MAX_TOOL_ITERATIONS = int(os.getenv("CHAT_MAX_TOOL_ITERATIONS", "10"))

# System prompt for the chat assistant
SYSTEM_PROMPT = """You are a helpful data assistant for the DataIO platform. Your role is to help users discover, understand, and work with datasets.

You have access to tools that let you:
- Search for datasets by keywords, categories, tags, or data owners
- Get detailed information about specific datasets
- View dataset schemas and data dictionaries
- List available categories and data providers
- Get download information for datasets the user has access to

When helping users:
1. Start by understanding what kind of data they're looking for
2. Use the search tool to find relevant datasets
3. Provide helpful summaries of what you find
4. Offer to get more details on specific datasets if they're interested
5. Be transparent about access restrictions - some datasets require permissions

Always be concise and helpful. If the user doesn't have access to a dataset, explain how they might request access through the platform."""


class ChatMessage:
    """Represents a chat message."""

    def __init__(
        self,
        role: str,
        content: str | list,
        tool_use_id: str | None = None,
        tool_name: str | None = None,
    ):
        self.role = role
        self.content = content
        self.tool_use_id = tool_use_id
        self.tool_name = tool_name

    def to_bedrock_format(self) -> dict:
        """Convert to Bedrock message format."""
        if isinstance(self.content, str):
            return {"role": self.role, "content": [{"text": self.content}]}
        return {"role": self.role, "content": self.content}


class ChatService(BaseService):
    """
    Service for handling chat conversations with Bedrock + MCP.
    """

    def __init__(self):
        super().__init__()
        self.mcp_server = DataIOMCPServer()
        self._bedrock_client = None

    @property
    def bedrock(self):
        """Lazy-load Bedrock client."""
        if self._bedrock_client is None:
            self._bedrock_client = boto3.client(
                "bedrock-runtime",
                region_name=BEDROCK_REGION
            )
        return self._bedrock_client

    def get_tools_config(self) -> dict:
        """Get tool configuration for Bedrock."""
        return {"tools": self.mcp_server.get_tool_definitions()}

    async def chat_stream(
        self,
        user_message: str,
        conversation_history: list[dict],
        user_email: str,
        session_id: str | None = None,
    ) -> AsyncGenerator[dict, None]:
        """
        Process a chat message and stream the response.

        Yields dicts with different event types:
        - {"type": "text", "content": "..."} - Text content
        - {"type": "tool_use", "tool": "...", "input": {...}} - Tool being called
        - {"type": "tool_result", "tool": "...", "result": {...}} - Tool result
        - {"type": "done"} - Stream complete
        - {"type": "error", "message": "..."} - Error occurred

        Args:
            user_message: The user's message
            conversation_history: Previous messages in Bedrock format
            user_email: Email of the authenticated user
            session_id: Optional session ID for tracking

        Yields:
            Stream events as dicts
        """
        # Build user context for MCP
        user_context = await self._get_user_context(user_email)

        # Build messages list
        messages = conversation_history.copy()
        messages.append({"role": "user", "content": [{"text": user_message}]})

        # Agentic loop
        iteration = 0
        while iteration < MAX_TOOL_ITERATIONS:
            iteration += 1

            try:
                # Call Bedrock
                response = self.bedrock.converse_stream(
                    modelId=BEDROCK_MODEL_ID,
                    messages=messages,
                    system=[{"text": SYSTEM_PROMPT}],
                    toolConfig=self.get_tools_config(),
                )

                # Process streaming response
                assistant_content = []
                current_tool_use = None

                for event in response.get("stream", []):
                    # Content block start
                    if "contentBlockStart" in event:
                        start = event["contentBlockStart"]
                        if "toolUse" in start.get("start", {}):
                            current_tool_use = {
                                "toolUseId": start["start"]["toolUse"]["toolUseId"],
                                "name": start["start"]["toolUse"]["name"],
                                "input": ""
                            }

                    # Content block delta
                    if "contentBlockDelta" in event:
                        delta = event["contentBlockDelta"]["delta"]

                        if "text" in delta:
                            yield {"type": "text", "content": delta["text"]}

                        if "toolUse" in delta and current_tool_use:
                            current_tool_use["input"] += delta["toolUse"].get("input", "")

                    # Content block stop
                    if "contentBlockStop" in event:
                        if current_tool_use:
                            # Parse the accumulated input JSON
                            try:
                                tool_input = json.loads(current_tool_use["input"]) if current_tool_use["input"] else {}
                            except json.JSONDecodeError:
                                tool_input = {}

                            assistant_content.append({
                                "toolUse": {
                                    "toolUseId": current_tool_use["toolUseId"],
                                    "name": current_tool_use["name"],
                                    "input": tool_input
                                }
                            })
                            current_tool_use = None

                    # Message stop
                    if "messageStop" in event:
                        stop_reason = event["messageStop"].get("stopReason")

                        if stop_reason == "tool_use":
                            # Execute tools and continue loop
                            yield {"type": "tool_use_start"}

                            tool_results = []
                            for block in assistant_content:
                                if "toolUse" in block:
                                    tool_use = block["toolUse"]
                                    yield {
                                        "type": "tool_use",
                                        "tool": tool_use["name"],
                                        "input": tool_use["input"]
                                    }

                                    # Execute the tool
                                    result = await self.mcp_server.execute_tool(
                                        tool_use["name"],
                                        tool_use["input"],
                                        user_context
                                    )

                                    yield {
                                        "type": "tool_result",
                                        "tool": tool_use["name"],
                                        "success": result.success,
                                        "preview": self._get_result_preview(result.data)
                                    }

                                    tool_results.append({
                                        "toolResult": {
                                            "toolUseId": tool_use["toolUseId"],
                                            "content": [{"json": result.data if result.success else {"error": result.error}}]
                                        }
                                    })

                            # Add assistant message and tool results to history
                            messages.append({"role": "assistant", "content": assistant_content})
                            messages.append({"role": "user", "content": tool_results})

                            # Continue the loop
                            continue
                        else:
                            # End of response
                            yield {"type": "done"}
                            return

            except Exception as e:
                logger.exception("Error in chat stream")
                yield {"type": "error", "message": str(e)}
                return

        # Hit max iterations
        yield {"type": "error", "message": "Maximum tool iterations reached"}

    async def chat(
        self,
        user_message: str,
        conversation_history: list[dict],
        user_email: str,
    ) -> dict:
        """
        Non-streaming chat endpoint. Returns the complete response.

        Returns:
            Dict with 'response' (text) and 'tool_calls' (list of tools used)
        """
        full_response = ""
        tool_calls = []

        async for event in self.chat_stream(
            user_message, conversation_history, user_email
        ):
            if event["type"] == "text":
                full_response += event["content"]
            elif event["type"] == "tool_use":
                tool_calls.append({
                    "tool": event["tool"],
                    "input": event["input"]
                })
            elif event["type"] == "error":
                raise Exception(event["message"])

        return {
            "response": full_response,
            "tool_calls": tool_calls
        }

    async def _get_user_context(self, user_email: str) -> UserContext:
        """Get user context for MCP permission checks."""
        session = DBSession()
        try:
            user = session.query(User).filter(User.email == user_email).first()
            if not user:
                return UserContext(email=user_email, is_admin=False)

            return UserContext(
                email=user_email,
                is_admin=user.is_admin,
                groups=[]  # TODO: Load user groups
            )
        finally:
            session.close()

    def _get_result_preview(self, data: Any, max_length: int = 100) -> str:
        """Get a short preview of tool result data."""
        if data is None:
            return "No data"

        if isinstance(data, dict):
            if "datasets" in data:
                count = len(data["datasets"])
                return f"Found {count} dataset(s)"
            if "categories" in data:
                count = len(data["categories"])
                return f"Found {count} categories"
            if "data_owners" in data:
                count = len(data["data_owners"])
                return f"Found {count} data owners"
            if "title" in data:
                return f"Dataset: {data['title'][:50]}"
            if "error" in data:
                return f"Error: {data['error'][:50]}"

        preview = str(data)
        if len(preview) > max_length:
            return preview[:max_length] + "..."
        return preview


class ChatHistoryService(BaseService):
    """
    Service for managing chat history persistence.
    """

    def __init__(self):
        super().__init__()

    async def create_session(self, user_email: str, title: str | None = None) -> str:
        """Create a new chat session."""
        # This will use the ChatSession model once the migration is run
        session_id = str(uuid.uuid4())
        # TODO: Persist to database
        return session_id

    async def get_session_history(
        self,
        session_id: str,
        user_email: str
    ) -> list[dict]:
        """Get conversation history for a session."""
        # TODO: Load from database
        return []

    async def save_message(
        self,
        session_id: str,
        role: str,
        content: str,
        tool_calls: list[dict] | None = None
    ) -> None:
        """Save a message to the session history."""
        # TODO: Persist to database
        pass

    async def list_sessions(
        self,
        user_email: str,
        limit: int = 20,
        offset: int = 0
    ) -> list[dict]:
        """List chat sessions for a user."""
        # TODO: Load from database
        return []

    async def delete_session(self, session_id: str, user_email: str) -> bool:
        """Delete a chat session."""
        # TODO: Delete from database
        return True
