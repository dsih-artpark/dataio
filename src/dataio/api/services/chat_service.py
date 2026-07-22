"""
Chat Service - Orchestrates conversations between users, AI providers, and MCP tools.

This service handles the agentic loop:
1. Receive user message
2. Send to AI provider (Bedrock or OpenRouter) with MCP tool definitions
3. Execute any tool calls
4. Return tool results to AI provider
5. Stream final response to user

Supports multiple AI providers:
- AWS Bedrock (default)
- OpenRouter (OpenAI-compatible API)
"""

import json
import logging
import os
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import AsyncGenerator, Any, Literal

import boto3
import httpx
from sqlalchemy.orm import Session

from dataio.api.database.config import Session as DBSession
from dataio.api.database.models import User
from dataio.api.services.base_service import BaseService
from dataio.mcp.server import DataIOMCPServer
from dataio.mcp.types import UserContext

logger = logging.getLogger(__name__)

# Provider type
AIProvider = Literal["bedrock", "openrouter"]

# Configuration
CHAT_PROVIDER = os.getenv("CHAT_PROVIDER", "bedrock").lower()
MAX_TOOL_ITERATIONS = int(os.getenv("CHAT_MAX_TOOL_ITERATIONS", "10"))

# Bedrock configuration
BEDROCK_REGION = os.getenv("AWS_BEDROCK_REGION", "us-east-1")
BEDROCK_MODEL_ID = os.getenv("BEDROCK_MODEL_ID", "anthropic.claude-3-5-sonnet-20241022-v2:0")

# OpenRouter configuration
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
OPENROUTER_MODEL_ID = os.getenv("OPENROUTER_MODEL_ID", "anthropic/claude-sonnet-5")

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


# =============================================================================
# AI Provider Abstraction
# =============================================================================


class AIProviderBase(ABC):
    """Abstract base class for AI providers."""

    @abstractmethod
    async def stream_response(
        self,
        messages: list[dict],
        system_prompt: str,
        tools: list[dict],
    ) -> AsyncGenerator[dict, None]:
        """
        Stream a response from the AI provider.

        Yields standardized events:
        - {"type": "text", "content": "..."}
        - {"type": "tool_call", "id": "...", "name": "...", "input": {...}}
        - {"type": "tool_call_complete"}
        - {"type": "message_stop", "stop_reason": "end_turn"|"tool_use"}
        """
        pass

    @abstractmethod
    def format_tool_result(self, tool_use_id: str, result: dict) -> dict:
        """Format a tool result for the provider's expected format."""
        pass


class BedrockProvider(AIProviderBase):
    """AWS Bedrock provider implementation."""

    def __init__(self):
        self._client = None

    @property
    def client(self):
        """Lazy-load Bedrock client."""
        if self._client is None:
            self._client = boto3.client(
                "bedrock-runtime",
                region_name=BEDROCK_REGION
            )
        return self._client

    def _convert_tools_to_bedrock_format(self, tools: list[dict]) -> dict:
        """Convert MCP tool definitions to Bedrock format."""
        return {"tools": tools}

    async def stream_response(
        self,
        messages: list[dict],
        system_prompt: str,
        tools: list[dict],
    ) -> AsyncGenerator[dict, None]:
        """Stream response from Bedrock."""
        response = self.client.converse_stream(
            modelId=BEDROCK_MODEL_ID,
            messages=messages,
            system=[{"text": system_prompt}],
            toolConfig=self._convert_tools_to_bedrock_format(tools),
        )

        current_tool_use = None

        for event in response.get("stream", []):
            # Content block start
            if "contentBlockStart" in event:
                start = event["contentBlockStart"]
                if "toolUse" in start.get("start", {}):
                    current_tool_use = {
                        "id": start["start"]["toolUse"]["toolUseId"],
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

                    yield {
                        "type": "tool_call",
                        "id": current_tool_use["id"],
                        "name": current_tool_use["name"],
                        "input": tool_input
                    }
                    current_tool_use = None

            # Message stop
            if "messageStop" in event:
                stop_reason = event["messageStop"].get("stopReason")
                yield {
                    "type": "message_stop",
                    "stop_reason": "tool_use" if stop_reason == "tool_use" else "end_turn"
                }

    def format_tool_result(self, tool_use_id: str, result: dict) -> dict:
        """Format tool result for Bedrock."""
        return {
            "toolResult": {
                "toolUseId": tool_use_id,
                "content": [{"json": result}]
            }
        }

    def format_assistant_content(self, tool_calls: list[dict]) -> list[dict]:
        """Format assistant content with tool calls for Bedrock."""
        return [
            {
                "toolUse": {
                    "toolUseId": tc["id"],
                    "name": tc["name"],
                    "input": tc["input"]
                }
            }
            for tc in tool_calls
        ]


class OpenRouterProvider(AIProviderBase):
    """OpenRouter provider implementation (OpenAI-compatible API)."""

    def __init__(self):
        pass

    def _create_client(self) -> httpx.AsyncClient:
        """Create a new async HTTP client for each request."""
        return httpx.AsyncClient(
            base_url=OPENROUTER_BASE_URL,
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "HTTP-Referer": os.getenv("FRONTEND_URL", "http://localhost:3000"),
                "X-Title": "DataIO Chat",
                "Content-Type": "application/json",
            },
            timeout=120.0
        )

    def _convert_messages_to_openai_format(self, messages: list[dict], system_prompt: str) -> list[dict]:
        """Convert Bedrock-style messages to OpenAI format."""
        openai_messages = [{"role": "system", "content": system_prompt}]

        for msg in messages:
            role = msg["role"]
            content = msg.get("content", [])

            if isinstance(content, str):
                openai_messages.append({"role": role, "content": content})
            elif isinstance(content, list):
                # Handle Bedrock-style content blocks
                text_parts = []
                tool_calls = []
                tool_results = []

                for block in content:
                    if "text" in block:
                        text_parts.append(block["text"])
                    elif "toolUse" in block:
                        tool_use = block["toolUse"]
                        tool_calls.append({
                            "id": tool_use["toolUseId"],
                            "type": "function",
                            "function": {
                                "name": tool_use["name"],
                                "arguments": json.dumps(tool_use["input"])
                            }
                        })
                    elif "toolResult" in block:
                        tool_result = block["toolResult"]
                        tool_results.append({
                            "tool_call_id": tool_result["toolUseId"],
                            "content": json.dumps(tool_result["content"][0].get("json", {}))
                        })

                if role == "assistant":
                    msg_data = {"role": "assistant"}
                    if text_parts:
                        msg_data["content"] = " ".join(text_parts)
                    if tool_calls:
                        msg_data["tool_calls"] = tool_calls
                        if "content" not in msg_data:
                            msg_data["content"] = None
                    openai_messages.append(msg_data)
                elif role == "user":
                    if tool_results:
                        # Tool results are sent as separate tool role messages
                        for tr in tool_results:
                            openai_messages.append({
                                "role": "tool",
                                "tool_call_id": tr["tool_call_id"],
                                "content": tr["content"]
                            })
                    elif text_parts:
                        openai_messages.append({"role": "user", "content": " ".join(text_parts)})

        return openai_messages

    def _convert_tools_to_openai_format(self, tools: list[dict]) -> list[dict]:
        """Convert Bedrock-style tool definitions to OpenAI format."""
        openai_tools = []
        for tool in tools:
            openai_tool = {
                "type": "function",
                "function": {
                    "name": tool["toolSpec"]["name"],
                    "description": tool["toolSpec"]["description"],
                    "parameters": tool["toolSpec"]["inputSchema"]["json"]
                }
            }
            openai_tools.append(openai_tool)
        return openai_tools

    async def stream_response(
        self,
        messages: list[dict],
        system_prompt: str,
        tools: list[dict],
    ) -> AsyncGenerator[dict, None]:
        """Stream response from OpenRouter."""
        openai_messages = self._convert_messages_to_openai_format(messages, system_prompt)
        openai_tools = self._convert_tools_to_openai_format(tools)

        logger.info(f"OpenRouter request - model: {OPENROUTER_MODEL_ID}, messages: {len(openai_messages)}, tools: {len(openai_tools)}")

        request_body = {
            "model": OPENROUTER_MODEL_ID,
            "messages": openai_messages,
            "tools": openai_tools if openai_tools else None,
            "stream": True,
        }

        # Remove None values
        request_body = {k: v for k, v in request_body.items() if v is not None}

        # Create a new client for this request
        async with self._create_client() as client:
            async with client.stream("POST", "/chat/completions", json=request_body) as response:
                if response.status_code != 200:
                    error_text = await response.aread()
                    logger.error(f"OpenRouter error: {response.status_code} - {error_text}")
                    raise Exception(f"OpenRouter API error: {response.status_code}")

                current_tool_calls: dict[int, dict] = {}
                finish_reason = None
                buffer = ""
                stream_done = False

                # Read the stream as text and parse SSE manually
                async for chunk in response.aiter_text():
                    if stream_done:
                        break

                    buffer += chunk
                    logger.debug(f"OpenRouter received chunk: {len(chunk)} bytes")

                    # Process complete lines from buffer
                    while "\n" in buffer:
                        line, buffer = buffer.split("\n", 1)
                        line = line.strip()

                        if not line:
                            continue

                        if not line.startswith("data: "):
                            logger.debug(f"Skipping non-data line: {line[:50]}")
                            continue

                        data_str = line[6:].strip()
                        if data_str == "[DONE]":
                            logger.info("OpenRouter stream done signal received")
                            stream_done = True
                            break

                        try:
                            data = json.loads(data_str)
                        except json.JSONDecodeError as e:
                            logger.debug(f"JSON decode error: {e}, data: {data_str[:100]}")
                            continue

                        choices = data.get("choices", [])
                        if not choices:
                            continue

                        choice = choices[0]
                        delta = choice.get("delta", {})
                        finish_reason = choice.get("finish_reason") or finish_reason

                        # Handle text content
                        if "content" in delta and delta["content"]:
                            logger.debug(f"OpenRouter text chunk: {delta['content'][:50]}")
                            yield {"type": "text", "content": delta["content"]}

                        # Handle tool calls
                        if "tool_calls" in delta:
                            logger.debug(f"OpenRouter tool_calls delta: {delta['tool_calls']}")
                            for tc in delta["tool_calls"]:
                                idx = tc.get("index", 0)
                                if idx not in current_tool_calls:
                                    current_tool_calls[idx] = {
                                        "id": tc.get("id", ""),
                                        "name": "",
                                        "arguments": ""
                                    }

                                if tc.get("id"):
                                    current_tool_calls[idx]["id"] = tc["id"]

                                func = tc.get("function", {})
                                if func.get("name"):
                                    current_tool_calls[idx]["name"] = func["name"]
                                if func.get("arguments"):
                                    current_tool_calls[idx]["arguments"] += func["arguments"]

                # Emit completed tool calls
                logger.info(f"OpenRouter completed - tool_calls: {len(current_tool_calls)}, finish_reason: {finish_reason}")
                for idx in sorted(current_tool_calls.keys()):
                    tc = current_tool_calls[idx]
                    logger.debug(f"Emitting tool_call: {tc['name']}")
                    try:
                        tool_input = json.loads(tc["arguments"]) if tc["arguments"] else {}
                    except json.JSONDecodeError:
                        tool_input = {}

                    yield {
                        "type": "tool_call",
                        "id": tc["id"],
                        "name": tc["name"],
                        "input": tool_input
                    }

                # Emit stop reason
                stop_reason = "tool_use" if finish_reason == "tool_calls" else "end_turn"
                logger.info(f"OpenRouter emitting message_stop with reason: {stop_reason}")
                yield {"type": "message_stop", "stop_reason": stop_reason}

    def format_tool_result(self, tool_use_id: str, result: dict) -> dict:
        """Format tool result for OpenRouter (stored in Bedrock format for consistency)."""
        return {
            "toolResult": {
                "toolUseId": tool_use_id,
                "content": [{"json": result}]
            }
        }

    def format_assistant_content(self, tool_calls: list[dict]) -> list[dict]:
        """Format assistant content with tool calls (Bedrock format for storage)."""
        return [
            {
                "toolUse": {
                    "toolUseId": tc["id"],
                    "name": tc["name"],
                    "input": tc["input"]
                }
            }
            for tc in tool_calls
        ]


def get_ai_provider(provider: AIProvider | None = None) -> AIProviderBase:
    """Factory function to get the appropriate AI provider."""
    provider_name = provider or CHAT_PROVIDER

    if provider_name == "openrouter":
        if not OPENROUTER_API_KEY:
            raise ValueError("OPENROUTER_API_KEY environment variable is required for OpenRouter provider")
        return OpenRouterProvider()
    elif provider_name == "bedrock":
        return BedrockProvider()
    else:
        raise ValueError(f"Unknown AI provider: {provider_name}")


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
    Service for handling chat conversations with AI providers + MCP.

    Supports multiple AI providers:
    - bedrock: AWS Bedrock (default)
    - openrouter: OpenRouter (OpenAI-compatible API)
    """

    def __init__(self, provider: AIProvider | None = None):
        super().__init__()
        self.mcp_server = DataIOMCPServer()
        self._provider_name = provider or CHAT_PROVIDER
        self._ai_provider: AIProviderBase | None = None

    @property
    def ai_provider(self) -> AIProviderBase:
        """Lazy-load AI provider."""
        if self._ai_provider is None:
            self._ai_provider = get_ai_provider(self._provider_name)
        return self._ai_provider

    def get_tools(self) -> list[dict]:
        """Get tool definitions for the AI provider."""
        return self.mcp_server.get_tool_definitions()

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

        # Get tools
        tools = self.get_tools()

        # Agentic loop
        iteration = 0
        while iteration < MAX_TOOL_ITERATIONS:
            iteration += 1

            try:
                # Collect tool calls from this iteration
                tool_calls: list[dict] = []

                logger.info(f"Chat stream iteration {iteration} with provider {self._provider_name}")

                # Stream response from provider
                async for event in self.ai_provider.stream_response(
                    messages=messages,
                    system_prompt=SYSTEM_PROMPT,
                    tools=tools,
                ):
                    event_type = event.get("type")
                    logger.debug(f"Received event: {event_type}")

                    if event_type == "text":
                        logger.debug(f"Yielding text: {event['content'][:50] if event['content'] else 'empty'}")
                        yield {"type": "text", "content": event["content"]}

                    elif event_type == "tool_call":
                        tool_calls.append({
                            "id": event["id"],
                            "name": event["name"],
                            "input": event["input"]
                        })

                    elif event_type == "message_stop":
                        stop_reason = event.get("stop_reason")

                        if stop_reason == "tool_use" and tool_calls:
                            # Execute tools and continue loop
                            yield {"type": "tool_use_start"}

                            tool_results = []
                            for tc in tool_calls:
                                yield {
                                    "type": "tool_use",
                                    "tool": tc["name"],
                                    "input": tc["input"]
                                }

                                # Execute the tool
                                result = await self.mcp_server.execute_tool(
                                    tc["name"],
                                    tc["input"],
                                    user_context
                                )

                                yield {
                                    "type": "tool_result",
                                    "tool": tc["name"],
                                    "success": result.success,
                                    "preview": self._get_result_preview(result.data)
                                }

                                tool_results.append(
                                    self.ai_provider.format_tool_result(
                                        tc["id"],
                                        result.data if result.success else {"error": result.error}
                                    )
                                )

                            # Add assistant message and tool results to history
                            # Use provider-specific formatting for assistant content
                            if hasattr(self.ai_provider, 'format_assistant_content'):
                                assistant_content = self.ai_provider.format_assistant_content(tool_calls)
                            else:
                                assistant_content = [
                                    {
                                        "toolUse": {
                                            "toolUseId": tc["id"],
                                            "name": tc["name"],
                                            "input": tc["input"]
                                        }
                                    }
                                    for tc in tool_calls
                                ]

                            messages.append({"role": "assistant", "content": assistant_content})
                            messages.append({"role": "user", "content": tool_results})

                            # Reset for next iteration
                            tool_calls = []
                            # Continue the loop
                            break
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
