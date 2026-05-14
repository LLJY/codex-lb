## Why

Recent upstream proxy-stream fixes address compatibility gaps that overlap with codex-lb's bridged Responses path: single `previous_response_id` misses can leak raw upstream 400s, stream startup failures can be flattened into HTTP 200 SSE, transient stream timeouts are not retried like other transient failures, idle SSE clients can hang, and final-only response output can complete without visible text deltas.

## What Changes

- retry direct streaming transient terminal codes `stream_incomplete` and `upstream_request_timeout` with the existing same-account transient retry budget
- mask single previous-response misses as `stream_incomplete` without leaking raw upstream `previous_response_not_found` details
- preserve HTTP error statuses for stream startup failures before the SSE response is committed, while converting post-startup proxy errors to in-band `response.failed`
- inject configurable SSE comment keepalives on client-facing streaming responses
- synthesize public `response.output_text.delta` events when upstream only provides final output text

## Impact

- improves compatibility with upstream proxy-stream behavior without wholesale merging unrelated APIs
- makes retryable startup failures visible as HTTP 5xx/4xx responses before stream commit
- keeps committed streams on the SSE contract, with transparent keepalive comments and terminal `response.failed` events
- introduces `CODEX_LB_SSE_KEEPALIVE_INTERVAL_SECONDS` with default `10.0`; set `0` to disable keepalives
