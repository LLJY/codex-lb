## ADDED Requirements

### Requirement: HTTP bridge evicts sessions after terminal transient upstream errors
When a bridged HTTP Responses request receives a terminal upstream error whose normalized error code indicates a transient upstream/session failure, the service MUST mark the bridged upstream session as retiring, remove it from reuse indexes, release durable ownership for that session, deliver the terminal error to the affected request, and then close the upstream transport once no bridged requests remain pending or queued on that session.

Transient upstream/session failure codes include `server_error`, `server_is_overloaded`, `upstream_error`, `stream_incomplete`, and `upstream_request_timeout`, except when `stream_incomplete` is a local rewrite of an upstream `previous_response_not_found` continuity error.

#### Scenario: terminal transient error retires same-key bridge before reuse
- **WHEN** a bridged HTTP `/v1/responses` or `/backend-api/codex/responses` request receives a terminal upstream `response.failed` or `error` event with a transient upstream/session failure code
- **AND** the failing request has been finalized downstream
- **AND** no other requests remain pending or queued on that bridged upstream session
- **THEN** the service MUST remove that session from bridge affinity indexes
- **AND** it MUST close the affected upstream bridge session transport
- **AND** the next request using the same bridge affinity key MUST allocate a fresh upstream bridge session instead of reusing the failed one

#### Scenario: terminal transient retirement releases durable ownership before replacement
- **WHEN** a terminal transient error marks a bridged upstream session as retiring
- **THEN** the service MUST release durable ownership for that retiring session before a same-key replacement can be claimed
- **AND** the retiring session MUST NOT later release durable ownership for a fresh replacement session

#### Scenario: terminal transient retirement blocks stale alias registration
- **WHEN** a bridged upstream session has been marked retiring after a terminal transient error
- **THEN** the service MUST NOT register new turn-state aliases or previous-response aliases for that retiring session
- **AND** it MUST NOT persist durable aliases for that retiring session

#### Scenario: terminal transient retirement drains queued stale holders
- **WHEN** a terminal transient error marks a bridged upstream session as retiring
- **AND** another request already holds or later reaches the retired session object
- **THEN** the service MUST reject that stale session object without reconnecting it or restoring it into bridge affinity indexes
- **AND** it MUST finish closing/releasing the retired bridge after pending and queued requests drain

#### Scenario: terminal transient error preserves downstream failure contract
- **WHEN** a bridged HTTP Responses request receives a terminal upstream transient error
- **THEN** the downstream response for that request MUST preserve the upstream terminal error code and message rather than replacing it with a synthetic bridge error

#### Scenario: early streaming terminal transient errors become HTTP JSON failures
- **WHEN** a bridged HTTP streaming Responses request receives a terminal transient upstream error before any SSE event has been emitted downstream
- **THEN** the service MUST return an HTTP JSON OpenAI error envelope instead of committing a `200` SSE response
- **AND** it MUST map `server_error` to HTTP `500`, `server_is_overloaded` to HTTP `503`, `upstream_error` to HTTP `502`, `stream_incomplete` to HTTP `502`, and `upstream_request_timeout` to HTTP `504`
- **AND** it MUST preserve upstream error `code`, `type`, `message`, and `param` fields when available

#### Scenario: committed streaming terminal transient errors remain in-band SSE
- **WHEN** a bridged HTTP streaming Responses request has already emitted at least one SSE event downstream
- **AND** the same request later receives a terminal transient upstream error or transport failure
- **THEN** the service MUST keep the response as HTTP `200` SSE and emit an in-band terminal `response.failed` event

#### Scenario: non-transient terminal errors do not force bridge eviction
- **WHEN** a bridged HTTP Responses request receives a terminal upstream error with a non-transient client/request error code such as `invalid_request_error`
- **THEN** the service MUST preserve the existing bridge reuse behavior
- **AND** it MUST NOT retire the bridge solely because of that non-transient terminal error

#### Scenario: local previous-response continuity rewrites do not force bridge eviction
- **WHEN** a bridged HTTP Responses request receives an upstream `previous_response_not_found` error that codex-lb rewrites to `stream_incomplete`
- **THEN** the service MUST preserve the existing bridge reuse behavior
- **AND** it MUST NOT retire the bridge solely because of that local continuity rewrite
