# Streaming Transient 5xx Plan

## Goal

Change HTTP Responses bridge behavior so early terminal transient upstream failures are surfaced as HTTP 5xx JSON errors instead of `200` SSE `response.failed` events when no downstream SSE event has been emitted yet. This should make clients such as opencode treat upstream transient failures like retryable HTTP failures while preserving in-band SSE failures once streaming has already started.

## Current context

- The bridge retirement change is already implemented for terminal transient errors.
- Transient terminal codes are:
  - `server_error`
  - `server_is_overloaded`
  - `upstream_error`
  - `stream_incomplete`
  - `upstream_request_timeout`
- Local rewrites of upstream `previous_response_not_found` to `stream_incomplete` must remain excluded from terminal transient retirement and must not be converted into generic retry semantics.
- Current behavior for upstream terminal failures before this change:
  - Streaming `/responses`: usually HTTP `200` with SSE `response.failed`.
  - Non-stream collect: HTTP `502` JSON error via collect/status mapping.
  - Local bridge/container failures: HTTP `502`/`503` JSON errors, which opencode appears to retry.

## Desired behavior

When a bridged HTTP `/responses` stream receives a terminal transient failure before any SSE payload has been yielded downstream:

- Return a JSON OpenAI error envelope with a 5xx HTTP status.
- Preserve upstream error `code`, `type`, `message`, and `param` where available.
- Retire and release the failing bridge session using the existing terminal retirement flow.

Suggested HTTP status mapping:

| Error code | HTTP status |
| --- | ---: |
| `server_error` | `500` |
| `server_is_overloaded` | `503` |
| `upstream_error` | `502` |
| `stream_incomplete` | `502` |
| `upstream_request_timeout` | `504` |

When any SSE event has already been yielded downstream, keep current in-band SSE `response.failed` behavior because the HTTP status is already committed.

## Files in scope

- `app/modules/proxy/service.py`
- `app/modules/proxy/helpers.py` if classifier constants need alignment
- `tests/integration/test_http_responses_bridge.py`
- `openspec/changes/evict-http-bridge-transient-terminal/specs/responses-api-compat/spec.md`
- `openspec/changes/evict-http-bridge-transient-terminal/tasks.md`

## Implementation notes

- `_stream_http_bridge_session_events()` already raises `ProxyResponseError` before yielding when `propagate_http_errors=True`, `block_event_type == "response.failed"`, and `request_state.error_http_status_override >= 400`.
- `_process_http_bridge_upstream_text()` is the right place to set `request_state.error_http_status_override` for terminal transient events before the event block reaches the request queue.
- Transport-generated failures flowing through `_fail_pending_websocket_requests()` should set the same HTTP override for HTTP bridge request states before queueing synthetic `response.failed` events.
- The working tree currently has a partial implementation start:
  - `_HTTP_BRIDGE_TRANSIENT_TERMINAL_HTTP_STATUS_BY_CODE` was introduced.
  - `_process_http_bridge_upstream_text()` sets an override for terminal transient events.
  - `_fail_pending_websocket_requests()` sets an override for HTTP transport transient failures.
  - A helper such as `_http_bridge_terminal_transient_http_status()` still needs to be added/verified.

## Acceptance checks

- Early upstream `server_error` streaming request returns HTTP `500` JSON error, not HTTP `200` SSE.
- Early upstream `server_is_overloaded` streaming request returns HTTP `503` JSON error.
- Early transport `stream_incomplete` before any SSE returns HTTP `502` JSON error.
- Created-then-failed or created-then-close streams still return HTTP `200` SSE with `response.created` followed by `response.failed`.
- Non-transient `invalid_request_error` behavior remains unchanged.
- Terminal retirement/replacement/durable release tests still pass.
- `bunx @fission-ai/openspec validate --specs` passes.
- `ruff`, focused HTTP bridge tests, and full `tests/integration/test_http_responses_bridge.py` pass.
