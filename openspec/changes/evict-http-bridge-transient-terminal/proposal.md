## Why

Bridged HTTP Responses sessions can receive upstream terminal transient errors such as `server_error`, `server_is_overloaded`, or `upstream_error` while the cached session remains reusable.

Recent production logs show repeated terminal upstream errors and repeated upstream request IDs on the same `session_header` bridge key. This matches the upstream issue class where a failed upstream session can become sticky and continue serving failures until the client starts a new conversation.

## What Changes

- treat transient terminal upstream errors on HTTP bridge sessions as session-poisoning events
- evict the affected bridge from in-memory affinity indexes after the terminal error is delivered when no other requests remain pending
- close/release the affected upstream bridge so the next request with the same cache/session key creates a fresh upstream session
- preserve existing behavior for non-transient client/request errors such as `invalid_request_error`
- add regression coverage for both transient eviction and non-transient bridge preservation

## Impact

- reduces sticky repeated upstream failures on long-context bridged HTTP Responses workloads
- sacrifices bridge continuity/prompt-cache reuse only after upstream has already returned a transient terminal failure
- keeps the downstream error contract unchanged for the failing request
