from __future__ import annotations

import httpx
import pytest

from dataio.api.services.openrouter_draft_client import OpenRouterDraftClient


def _canned_response(request: httpx.Request) -> httpx.Response:
    assert request.url.path.endswith("/chat/completions")
    body = request.read()
    import json

    payload = json.loads(body)
    assert payload["stream"] is False
    assert payload["messages"][0]["role"] == "system"
    assert payload["messages"][1]["role"] == "user"
    return httpx.Response(
        200,
        json={
            "model": "anthropic/claude-3.5-sonnet",
            "choices": [{"message": {"role": "assistant", "content": "datasetTitle: Foo"}}],
            "usage": {"prompt_tokens": 123, "completion_tokens": 45},
        },
    )


def test_complete_parses_non_streaming_response():
    client = OpenRouterDraftClient()
    client._client = httpx.Client(
        base_url=client._client.base_url,
        transport=httpx.MockTransport(_canned_response),
    )

    completion = client.complete(system_prompt="be terse", user_prompt="draft it")

    assert completion.text == "datasetTitle: Foo"
    assert completion.model == "anthropic/claude-3.5-sonnet"
    assert completion.prompt_tokens == 123
    assert completion.completion_tokens == 45


def test_complete_raises_on_non_200():
    def error_response(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "boom"})

    client = OpenRouterDraftClient()
    client._client = httpx.Client(
        base_url=client._client.base_url,
        transport=httpx.MockTransport(error_response),
    )

    with pytest.raises(httpx.HTTPStatusError):
        client.complete(system_prompt="be terse", user_prompt="draft it")
