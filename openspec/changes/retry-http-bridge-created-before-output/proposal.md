## Why

HTTP Responses bridge sessions can lose the upstream websocket after OpenAI has emitted `response.created` but before any downstream-visible output or terminal event is delivered.

In that narrow case, codex-lb currently forwards the created response id, then reports `stream_incomplete`. Clients must then manually continue even though the request can be safely replayed as a fresh first attempt when no output has reached the client.

## What Changes

- delay forwarding bridged HTTP `response.created` until the next downstream event is ready
- retry exactly one pending bridged HTTP request once when upstream disconnects after `response.created` but before output is flushed downstream
- reset the stale upstream response id before replay so it cannot leak to the client
- preserve existing failure behavior after output is emitted, for continuations, for repeated replays, and when multiple requests are pending
- add regression coverage for the transparent retry and the no-retry-after-output case

## Impact

- reduces manual continuation prompts for a common safe `stream_incomplete` case
- keeps the external SSE contract ordered as `response.created` before subsequent events
- avoids changing continuation semantics or multiplexed pending-request behavior
