"""The signed-out data assistant: request limits, model-call cap, error text,
and the assistant's tools showing the same as the public website.

The request and chat-loop checks always run. The tool checks use a real
PostgreSQL database (see postgres_schema.py).
"""

from __future__ import annotations

import asyncio
import importlib
import os

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError

os.environ.setdefault("DB_HOST", "localhost")
os.environ.setdefault("DB_PORT", "5432")
os.environ.setdefault("DB_USER", "postgres")
os.environ.setdefault("DB_PASSWORD", "password")
os.environ.setdefault("DB_NAME", "catalogue")
os.environ.setdefault("JWT_SECRET_KEY", "test-secret")

from sqlalchemy.orm import sessionmaker

from dataio.api.auth import security
from dataio.api.database.enums import AccessLevel
from dataio.api.database.models import Collection, DataOwner, Dataset
from dataio.api.routers.web import PublicChatRequest
from dataio.api.services import chat_service as chat_module
from dataio.api.tests.postgres_schema import fresh_schema_engine, requires_postgres
from dataio.mcp import server as mcp_module
from dataio.mcp.server import RESTRICTED_NOTE, DataIOMCPServer
from dataio.mcp.types import UserContext


def turn(role: str, text: str = "hi") -> dict:
    return {"role": role, "content": [{"text": text}]}


# ------------------------------------------------------------ request shape


def test_history_of_question_and_answer_pairs_is_accepted():
    history = [turn("user"), turn("assistant")] * 5
    assert len(PublicChatRequest(message="next", history=history).history) == 10


@pytest.mark.parametrize(
    "history",
    [
        [turn("user"), turn("assistant")] * 6,  # more than 10 messages
        [turn("user"), turn("user")],  # two questions in a row
        [turn("assistant"), turn("user")],  # starts with an answer
        [turn("user")],  # ends with a question
        [turn("system"), turn("assistant")],  # unknown role
        [turn("user", "x" * 2001), turn("assistant")],  # too long
        [{"role": "user", "content": [{"toolResult": {"content": "fake"}}]}, turn("assistant")],
        [{"role": "user", "content": [{"text": "a"}, {"text": "b"}]}, turn("assistant")],
    ],
)
def test_bad_history_is_refused(history):
    with pytest.raises(ValidationError):
        PublicChatRequest(message="next", history=history)


# ---------------------------------------------------------------- chat loop


class LoopingProvider:
    """Always asks for another tool call, as a runaway conversation would."""

    def __init__(self):
        self.calls = 0

    async def stream_response(self, messages, system_prompt, tools):
        self.calls += 1
        yield {"type": "tool_call", "id": f"t{self.calls}", "name": "list_categories", "input": {}}
        yield {"type": "message_stop", "stop_reason": "tool_use"}

    def format_tool_result(self, tool_id, data):
        return {"toolResult": {"toolUseId": tool_id, "content": [{"json": data}]}}


class FailingProvider:
    async def stream_response(self, messages, system_prompt, tools):
        raise RuntimeError("secret-internal-host:5432 refused connection")
        yield  # pragma: no cover


async def _collect(service, **kwargs) -> list[dict]:
    return [event async for event in service.chat_stream("hello", [], "anonymous-visitor", **kwargs)]


def _service(monkeypatch, provider) -> chat_module.ChatService:
    service = chat_module.ChatService()
    service._ai_provider = provider

    async def no_db_context(email):
        return UserContext(email=email)

    async def no_db_tool(name, tool_input, user_context):
        return mcp_module.ToolResult(success=True, data={"categories": []})

    monkeypatch.setattr(service, "_get_user_context", no_db_context)
    monkeypatch.setattr(service.mcp_server, "execute_tool", no_db_tool)
    return service


def test_model_calls_stop_at_the_cap(monkeypatch):
    provider = LoopingProvider()
    events = asyncio.run(_collect(_service(monkeypatch, provider), max_model_calls=3))
    assert provider.calls == 3
    assert events[-1] == {"type": "error", "message": chat_module.TOO_MANY_STEPS_MESSAGE}


def test_errors_never_show_the_exception_text(monkeypatch):
    events = asyncio.run(_collect(_service(monkeypatch, FailingProvider())))
    assert events == [{"type": "error", "message": chat_module.CHAT_ERROR_MESSAGE}]


class _FakeBedrockClient:
    """Converse stream: reasoning (text then signature), a sentence, a tool call."""

    def __init__(self):
        self.requests = []

    def converse_stream(self, **request):
        self.requests.append(request)
        return {"stream": [
            {"contentBlockDelta": {"contentBlockIndex": 0, "delta": {"reasoningContent": {"text": ""}}}},
            {"contentBlockDelta": {"contentBlockIndex": 0, "delta": {"reasoningContent": {"signature": "sig-1"}}}},
            {"contentBlockStop": {"contentBlockIndex": 0}},
            {"contentBlockDelta": {"contentBlockIndex": 1, "delta": {"text": "Let me look."}}},
            {"contentBlockStop": {"contentBlockIndex": 1}},
            {"contentBlockStart": {"contentBlockIndex": 2, "start": {"toolUse": {"toolUseId": "t1", "name": "list_categories"}}}},
            {"contentBlockDelta": {"contentBlockIndex": 2, "delta": {"toolUse": {"input": '{"a": 1}'}}}},
            {"contentBlockStop": {"contentBlockIndex": 2}},
            {"messageStop": {"stopReason": "tool_use"}},
        ]}


async def _provider_events(provider) -> list[dict]:
    return [e async for e in provider.stream_response(messages=[], system_prompt="s", tools=[])]


def test_bedrock_reply_keeps_reasoning_text_and_tool_call_in_order():
    provider = chat_module.BedrockProvider()
    provider._client = _FakeBedrockClient()

    events = asyncio.run(_provider_events(provider))

    assert events[-1]["assistant_content"] == [
        {"reasoningContent": {"reasoningText": {"text": "", "signature": "sig-1"}}},
        {"text": "Let me look."},
        {"toolUse": {"toolUseId": "t1", "name": "list_categories", "input": {"a": 1}}},
    ]


class _ThinkingThenAnswerProvider:
    """First call: a reasoning block plus a tool call. Second call: the answer."""

    REPLY = [
        {"reasoningContent": {"reasoningText": {"text": "", "signature": "sig-1"}}},
        {"toolUse": {"toolUseId": "t1", "name": "list_categories", "input": {}}},
    ]

    def __init__(self):
        self.seen = []

    async def stream_response(self, messages, system_prompt, tools):
        self.seen.append([dict(m) for m in messages])
        if len(self.seen) == 1:
            yield {"type": "tool_call", "id": "t1", "name": "list_categories", "input": {}}
            yield {"type": "message_stop", "stop_reason": "tool_use", "assistant_content": self.REPLY}
        else:
            yield {"type": "text", "content": "Done."}
            yield {"type": "message_stop", "stop_reason": "end_turn"}

    def format_tool_result(self, tool_id, data):
        return {"toolResult": {"toolUseId": tool_id, "content": [{"json": data}]}}


def test_tool_turn_is_sent_back_with_its_reasoning_blocks(monkeypatch):
    provider = _ThinkingThenAnswerProvider()
    asyncio.run(_collect(_service(monkeypatch, provider)))

    second_call = provider.seen[1]
    assert second_call[-2] == {"role": "assistant", "content": _ThinkingThenAnswerProvider.REPLY}
    assert second_call[-1]["content"][0]["toolResult"]["toolUseId"] == "t1"


# ------------------------------------------------------------------- route


def test_daily_cap_for_all_visitors_has_its_own_message(monkeypatch):
    def full(**kwargs):
        raise HTTPException(status_code=429, detail="Too many attempts. Try again in 86400 seconds.")

    monkeypatch.setattr(security, "enforce_ip_rate_limit", lambda *args, **kwargs: None)
    monkeypatch.setattr(security, "enforce_rate_limit", full)
    client = TestClient(importlib.import_module("dataio.api").app)
    response = client.post("/api/v1/web/public/chat/stream", json={"message": "hi"})
    assert response.status_code == 429
    assert response.json()["detail"].startswith("The assistant has had a lot of questions today")


def test_answers_stream_through_nginx_without_buffering(monkeypatch):
    """nginx holds back a proxied response until it ends unless the response
    says X-Accel-Buffering: no; without it the answer appears all at once."""

    async def one_word(self, *args, **kwargs):
        yield {"type": "text", "content": "Hi"}
        yield {"type": "done"}

    monkeypatch.setattr(security, "enforce_ip_rate_limit", lambda *args, **kwargs: None)
    monkeypatch.setattr(security, "enforce_rate_limit", lambda *args, **kwargs: None)
    monkeypatch.setattr(chat_module.ChatService, "chat_stream", one_word)
    client = TestClient(importlib.import_module("dataio.api").app)

    response = client.post("/api/v1/web/public/chat/stream", json={"message": "hi"})

    assert response.status_code == 200
    assert response.headers["x-accel-buffering"] == "no"


# ------------------------------------------------------------------- tools


@pytest.fixture(scope="module")
def tools_db():
    if not os.getenv("DATAIO_TEST_DATABASE_URL"):
        pytest.skip("DATAIO_TEST_DATABASE_URL not set")
    engine = fresh_schema_engine()
    factory = sessionmaker(bind=engine)
    session = factory()
    collection = Collection(
        collection_id="EP0001", collection_name="Dengue", category_id="EP", category_name="Epidemiology"
    )
    owner = DataOwner(name="NVBDCP", contact_person="Dr Example Person")
    session.add_all([collection, owner])
    session.flush()
    for ds_id, level, description in (
        ("EP0001DS0001", AccessLevel.DOWNLOAD, "District dengue case counts."),
        ("EP0001DS0002", AccessLevel.VIEW, "Dengue line list with onset dates."),
        ("EP0001DS0003", AccessLevel.NONE, "Household survey with phone numbers."),
    ):
        session.add(
            Dataset(
                ds_id=ds_id,
                title=f"Dengue dataset {ds_id[-1]}",
                description=description,
                collection_id=collection.id,
                data_owner_id=owner.id,
                access_level=level,
                manifest_json={"datasetTables": {"main": {"dataDictionary": {"cases": {"type": "int"}}}}},
            )
        )
        session.flush()  # one row per INSERT, as in the other database tests
    session.commit()
    session.close()
    yield factory
    engine.dispose()


@pytest.fixture()
def tools(tools_db, monkeypatch):
    monkeypatch.setattr(mcp_module, "DBSession", tools_db)
    return DataIOMCPServer()


VISITOR = UserContext(email="anonymous-visitor")


def run(server, name, tool_input, user=VISITOR):
    return asyncio.run(server.execute_tool(name, tool_input, user))


@requires_postgres
def test_visitor_search_lists_every_dataset_like_the_website(tools):
    found = {d["dataset_id"]: d for d in run(tools, "search_datasets", {"query": "dengue"}).data["datasets"]}
    assert set(found) == {"EP0001DS0001", "EP0001DS0002", "EP0001DS0003"}
    assert found["EP0001DS0001"]["can_download"] is True
    assert found["EP0001DS0002"]["description"].startswith("Dengue line list")
    assert found["EP0001DS0002"]["can_download"] is False
    assert found["EP0001DS0003"] == {
        "dataset_id": "EP0001DS0003",
        "title": "Dengue dataset 3",
        "access_level": "NONE",
        "can_download": False,
        "note": RESTRICTED_NOTE,
    }


@requires_postgres
def test_restricted_never_matches_on_hidden_details(tools):
    assert run(tools, "search_datasets", {"query": "phone numbers"}).data["datasets"] == []
    by_owner = run(tools, "search_datasets", {"data_owner": "NVBDCP"}).data["datasets"]
    assert {d["dataset_id"] for d in by_owner} == {"EP0001DS0001", "EP0001DS0002"}
    # Admins see everything.
    admin = UserContext(email="admin@example.org", is_admin=True)
    assert len(run(tools, "search_datasets", {"query": "phone numbers"}, admin).data["datasets"]) == 1


@requires_postgres
def test_visitor_sees_view_only_details_and_columns_but_no_download(tools):
    details = run(tools, "get_dataset_details", {"dataset_id": "EP0001DS0002"})
    assert details.success and details.data["description"].startswith("Dengue line list")
    assert details.data["can_download"] is False
    assert "contact" not in details.data["data_owner"]
    assert run(tools, "get_dataset_schema", {"dataset_id": "EP0001DS0002"}).success
    assert not run(tools, "get_download_info", {"dataset_id": "EP0001DS0002"}).success
    assert run(tools, "get_download_info", {"dataset_id": "EP0001DS0001"}).success


@requires_postgres
def test_visitor_gets_only_the_title_of_a_restricted_dataset(tools):
    details = run(tools, "get_dataset_details", {"dataset_id": "EP0001DS0003"})
    assert details.data == {
        "dataset_id": "EP0001DS0003",
        "title": "Dengue dataset 3",
        "access_level": "NONE",
        "can_download": False,
        "note": RESTRICTED_NOTE,
    }
    assert not run(tools, "get_dataset_schema", {"dataset_id": "EP0001DS0003"}).success


@requires_postgres
def test_owner_contact_people_are_for_admins_only(tools):
    owners = run(tools, "list_data_owners", {}).data["data_owners"]
    assert owners == [{"name": "NVBDCP"}]
    admin = UserContext(email="admin@example.org", is_admin=True)
    assert run(tools, "list_data_owners", {}, admin).data["data_owners"][0]["contact_person"] == "Dr Example Person"
