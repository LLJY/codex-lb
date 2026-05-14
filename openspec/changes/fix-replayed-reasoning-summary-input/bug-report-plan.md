# Bug Report and Plan: Replayed Reasoning/Summary Input Parts

## Bug report

### Summary

Clients can replay prior Responses output containing reasoning or summary-only parts back into `/v1/responses` input. The proxy currently strips some interleaved reasoning fields, but it still forwards unsupported input shapes such as top-level `type: "reasoning"` items and `type: "summary_text"` content parts. Upstream can reject these with reasoning/summary-part validation errors.

### Observed behavior

After redeploying `work/upstream-stream-incomplete`, local traffic began surfacing upstream errors for reasoning parts / summary parts again.

Container logs confirm the redeployed branch is running and handling `/v1/responses` traffic, so this is not an old-container issue.

### Current sanitizer behavior

`ResponsesRequest.to_payload()` still forwards examples like:

```json
[
  {
    "type": "reasoning",
    "summary": [{"type": "summary_text", "text": "hidden"}]
  },
  {
    "role": "assistant",
    "content": [
      {"type": "summary_text", "text": "summary"},
      {"type": "output_text", "text": "visible"}
    ]
  }
]
```

The existing sanitizer removes:

- top-level keys like `reasoning_content`, `reasoning_details`, `tool_calls`, and `function_call`
- content parts with `type` in `{"reasoning", "reasoning_content", "reasoning_details"}`

It does **not** remove top-level `type: "reasoning"` input items or `type: "summary_text"` content parts.

### Why the previous fix did not cover this

The older reasoning fix (`03272796 fix(proxy): preserve v1 response reasoning output`) addressed the response side: it preserved reasoning output by collecting `response.output_item.done` and merging it into final `response.completed.output` for non-streaming Responses.

This bug is on the request replay side: preserved reasoning output can later be sent back as input, and the request sanitizer does not fully remove upstream-incompatible reasoning/summary input shapes.

### Impact

- User-visible failures on continuation/replay flows that include prior reasoning summaries.
- Likely affects `/v1/responses` and any compatibility client that resubmits full transcript history.
- Risk is higher with clients that keep complete assistant output items instead of reducing assistant turns to visible text.

### Non-goals

- Do not strip valid top-level request `reasoning: {"effort": ..., "summary": ...}` controls.
- Do not strip compact/Responses encrypted reasoning state carried as an input item with `type: "reasoning"` and non-empty `encrypted_content`.
- Do not remove reasoning output preservation for downstream response bodies.
- Do not rewrite unrelated compatibility behavior.

## Plan

1. Add focused tests for request sanitization.
   - Parametrize unsupported top-level replay input item types, including at least `reasoning`, `summary_text`, `reasoning_content`, and `reasoning_details`, and assert they are dropped only when they appear under `input`.
   - Preserve valid top-level input item types such as `message`, `function_call`, and `function_call_output`.
   - Preserve `type: "reasoning"` input items that carry non-empty `encrypted_content`, while removing unsupported summary-only fields from those items.
   - Assistant/user direct content part `{"type":"summary_text", ...}` is dropped for both list content and single-mapping content.
   - Items whose content becomes empty after dropping unsupported reasoning/summary-only parts are dropped, while items whose content was already empty are preserved unless existing upstream contract tests require rejection.
   - Top-level request `reasoning.summary` remains preserved.
   - Visible text parts remain preserved.

2. Patch `app/core/openai/requests.py` sanitizer.
   - Use narrow, separate sets for unsupported top-level input item types and unsupported direct content part types.
   - Apply these sets only inside `payload["input"]`; never apply them to request-level fields such as `reasoning`.
   - Include summary-only reasoning shapes, at minimum `summary_text`.
   - Drop a whole input item when its top-level `type` is a summary-only type, when a `type: "reasoning"` item lacks encrypted state, or when removing unsupported content parts leaves no valid content.
   - Preserve encrypted reasoning input items and strip only unsupported summary fields from them.
   - Continue stripping interleaved reasoning metadata keys from otherwise valid items.

3. Add an integration regression for `/v1/responses`.
   - Send replay-style input containing top-level reasoning and summary text parts.
   - Stub upstream and assert the actual serialized upstream JSON, or `payload.to_payload()["input"]` at the fake transport boundary, excludes unsupported reasoning/summary shapes while preserving visible input/output text.

4. Verify locally.
   - `uv run pytest tests/unit/test_openai_requests.py tests/integration/test_proxy_responses.py -q`
   - Run any narrower failing client repro if available.
   - `uv run ruff format app/core/openai/requests.py tests/unit/test_openai_requests.py tests/integration/test_proxy_responses.py`
   - `uv run ruff check app/core/openai/requests.py tests/unit/test_openai_requests.py tests/integration/test_proxy_responses.py`
   - `bunx @fission-ai/openspec validate --specs`
   - `git diff --check`

5. Deploy.
   - Commit the scoped fix.
   - Push branch.
   - Rebuild/recreate local `codex-lb` container with `docker-compose.runtime.yml`.
   - Check `/health/ready` and repeat the user flow.
