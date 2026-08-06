"""One-shot (non-streaming) OpenRouter completion client for the metadata
drafter. chat_service.py's OpenRouterProvider is async/streaming-only and
built for the interactive chat agentic loop - not reusable as-is for a
single-turn drafting call, so this is a small, separate client using the
same auth/header pattern.
"""

from __future__ import annotations

import os

import httpx
from pydantic import BaseModel

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")

# Drafting is a one-shot, long-context, structured-extraction task with a
# different latency/quality profile than interactive chat, so it gets its
# own model env var - falls back to the chat service's if unset.
DRAFTER_MODEL_ID = os.getenv("DRAFTER_MODEL_ID", os.getenv("OPENROUTER_MODEL_ID", "anthropic/claude-sonnet-5"))

# Optional extended-thinking/reasoning controls, passed through to
# OpenRouter's unified `reasoning` request field (see
# https://openrouter.ai/docs/use-cases/reasoning-tokens). Unset by default -
# drafting already tends to be slow (large CSV profiles, multi-table
# prompts), so reasoning tokens are opt-in, not a silent default cost/latency
# increase. Set at most one of these:
#   DRAFTER_REASONING_EFFORT=high|medium|low   - qualitative effort level
#   DRAFTER_REASONING_MAX_TOKENS=<int>          - explicit thinking-token budget
DRAFTER_REASONING_EFFORT = os.getenv("DRAFTER_REASONING_EFFORT")
DRAFTER_REASONING_MAX_TOKENS = os.getenv("DRAFTER_REASONING_MAX_TOKENS")


class DraftCompletion(BaseModel):
    text: str
    model: str
    prompt_tokens: int | None = None
    completion_tokens: int | None = None


class OpenRouterDraftClient:
    def __init__(self, *, model_id: str | None = None, timeout: float = 300.0):
        self.model_id = model_id or DRAFTER_MODEL_ID
        self._client = httpx.Client(
            base_url=OPENROUTER_BASE_URL,
            headers={
                "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                "HTTP-Referer": os.getenv("FRONTEND_URL", "http://localhost:3000"),
                "X-Title": "DataIO Metadata Drafter",
                "Content-Type": "application/json",
            },
            timeout=timeout,
        )

    def complete(self, *, system_prompt: str, user_prompt: str) -> DraftCompletion:
        payload = {
            "model": self.model_id,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "stream": False,
        }
        reasoning: dict = {}
        if DRAFTER_REASONING_MAX_TOKENS:
            reasoning["max_tokens"] = int(DRAFTER_REASONING_MAX_TOKENS)
        elif DRAFTER_REASONING_EFFORT:
            reasoning["effort"] = DRAFTER_REASONING_EFFORT
        if reasoning:
            payload["reasoning"] = reasoning

        response = self._client.post("/chat/completions", json=payload)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            # OpenRouter's actual rejection reason (e.g. "insufficient
            # credits", a moderation block, a spend-limit message) lives in
            # the response body - httpx's default exception message is just
            # the status code, which isn't enough to tell those cases apart
            # from the caller's error log alone.
            raise httpx.HTTPStatusError(
                f"{exc}. Response body: {response.text[:2000]}",
                request=exc.request,
                response=exc.response,
            ) from exc
        body = response.json()
        if "choices" not in body:
            # OpenRouter can return HTTP 200 with no "choices" key when the
            # upstream provider fails after accepting the request (e.g. a
            # provider-side error or moderation block) - the failure reason
            # lives in body["error"], not in the HTTP status.
            raise RuntimeError(f"OpenRouter returned no choices: {body.get('error', body)}")
        choice = body["choices"][0]["message"]
        usage = body.get("usage", {})
        return DraftCompletion(
            # .get(...) or "" (not choice["content"]) - some providers omit
            # "content" entirely or return it as null in edge-case
            # responses; an empty string here fails parse_llm_output's
            # delimiter check the same way any other malformed response
            # does, routing into _complete_with_retry's existing retry
            # path instead of an unhandled KeyError.
            text=choice.get("content") or "",
            model=body.get("model", self.model_id),
            prompt_tokens=usage.get("prompt_tokens"),
            completion_tokens=usage.get("completion_tokens"),
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "OpenRouterDraftClient":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()
