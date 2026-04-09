### MODIFIED Requirements

### Requirement: HTTP Responses routes preserve upstream websocket session continuity
When serving HTTP `/v1/responses` or HTTP `/backend-api/codex/responses`, the service MUST preserve upstream Responses websocket session continuity on a stable per-session bridge key instead of opening a brand new upstream session for every eligible request. The bridge key MUST use an explicit session/conversation header when present; otherwise it MUST use normalized `prompt_cache_key`, and when the client omits `prompt_cache_key` the service MUST derive a stable key from the same cache-affinity inputs already used for OpenAI prompt-cache routing. While bridged, the service MUST preserve the external HTTP/SSE contract, MUST continue request logging with `transport = "http"`, and MUST keep requests from different bridge keys isolated from one another.

#### Scenario: abandoned bridged request does not leak foreign response events into a later request
- **WHEN** an earlier bridged HTTP request is abandoned or cancelled before upstream finishes streaming
- **AND** upstream later emits a Responses stream event with a top-level `response_id` for that abandoned request
- **THEN** the service matches the event using that top-level `response_id`
- **AND** it MUST ignore the event when that `response_id` does not belong to any pending bridged request
- **AND** it MUST NOT route that event to a later pending request solely because it is the only pending request
