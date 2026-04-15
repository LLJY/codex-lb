### MODIFIED Requirements

### Requirement: HTTP Responses routes preserve upstream websocket session continuity
When serving HTTP `/v1/responses` or HTTP `/backend-api/codex/responses`, the service MUST preserve upstream Responses websocket session continuity on a stable per-session bridge key instead of opening a brand new upstream session for every eligible request. The bridge key MUST use an explicit session/conversation header when present; otherwise it MUST use normalized `prompt_cache_key`, and when the client omits `prompt_cache_key` the service MUST derive a stable key from the same cache-affinity inputs already used for OpenAI prompt-cache routing. While bridged, the service MUST preserve the external HTTP/SSE contract, MUST continue request logging with `transport = "http"`, and MUST keep requests from different bridge keys isolated from one another.

#### Scenario: abandoned bridged request does not leak foreign response events into a later request
- **WHEN** an earlier bridged HTTP request is abandoned or cancelled before upstream finishes streaming
- **AND** that abandoned request may not have reached `response.created` before it was detached
- **THEN** the service MUST retire that bridged upstream websocket session before accepting a later request on the same bridge key
- **AND** any later request on that bridge key MUST use a fresh upstream websocket session
- **AND** the service MUST keep one in-flight bridged response per upstream websocket until a terminal event releases the bridge gate
- **AND** upstream later may emit Responses events for the abandoned request that include either a top-level `response_id` or only item/content/reasoning identifiers
- **THEN** the service MUST match events using top-level `response_id` when present
- **AND** it MUST NOT route response-less stale item/content/reasoning events from the abandoned request into a later pending request on the same bridge key
- **AND** it MUST ignore any explicit `response_id` event when that `response_id` does not belong to any pending bridged request
- **AND** if the abandoned request never received `response.created`, the service MUST retire or recreate the bridged upstream session before reusing it for a later request on the same bridge key
- **AND** if the abandoned request already received `response.created` but not a terminal event, the service MUST retire or recreate the bridged upstream session once that detached request is the only active bridged request on the session
