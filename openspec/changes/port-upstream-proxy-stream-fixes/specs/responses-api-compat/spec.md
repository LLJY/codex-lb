## ADDED Requirements

### Requirement: Stream transient timeouts participate in same-account retry
Direct upstream Responses streaming MUST treat terminal `stream_incomplete` and `upstream_request_timeout` failures as transient retry candidates with the same retry budget used for `server_error`.

#### Scenario: first stream timeout retries before surfacing downstream
- **WHEN** the first upstream streaming attempt yields a terminal `response.failed` with code `upstream_request_timeout` before any successful terminal event
- **THEN** the proxy MUST retry the same account within the transient retry budget
- **AND** if a retry succeeds, the downstream stream MUST complete without surfacing the transient timeout event

### Requirement: Previous-response misses are masked on public streaming surfaces
Public Responses streaming surfaces MUST mask upstream `previous_response_not_found` continuity misses as `stream_incomplete` bridge failures instead of leaking raw upstream continuity identifiers.

#### Scenario: single bridged previous-response miss is masked
- **WHEN** a bridged HTTP Responses request receives a single upstream `previous_response_not_found` error for the current request
- **THEN** the downstream event or startup error MUST use code `stream_incomplete`
- **AND** the payload MUST NOT contain the raw `previous_response_not_found` code

#### Scenario: unsafe direct websocket previous-response miss fails closed
- **WHEN** a direct websocket request with `previous_response_id` receives a previous-response miss
- **AND** the request has no safe fresh-turn body to replay without that anchor
- **THEN** the proxy MUST fail the request as `stream_incomplete` instead of replaying the stale `previous_response_id`

### Requirement: Stream startup errors preserve HTTP status before SSE commit
Responses and chat streaming endpoints MUST probe for immediate startup errors before committing `text/event-stream` and MUST return an HTTP JSON OpenAI error response when a startup error is observed.

#### Scenario: immediate proxy error returns JSON status
- **WHEN** a streaming request raises a `ProxyResponseError` during the startup probe
- **THEN** the HTTP response MUST use the proxy error status code
- **AND** the body MUST be an OpenAI error envelope

#### Scenario: post-startup proxy errors remain SSE
- **WHEN** no startup error is observed before the stream is returned
- **AND** a later proxy error occurs after the SSE response is committed
- **THEN** the stream MUST remain HTTP `200` SSE
- **AND** the proxy MUST emit an in-band `response.failed` event preserving the error details

### Requirement: Client-facing SSE streams emit keepalive comments while idle
Client-facing SSE responses for Responses and streaming chat MUST emit SSE comment keepalive frames after the configured idle interval. A non-positive interval MUST disable keepalive injection.

#### Scenario: idle stream gets a comment heartbeat
- **WHEN** a client-facing SSE source is idle longer than `sse_keepalive_interval_seconds`
- **THEN** the proxy MUST emit `: keepalive\n\n`
- **AND** it MUST continue streaming upstream events when they arrive

### Requirement: Final-only output text produces public text deltas
Public Responses streams MUST synthesize `response.output_text.delta` events from final output text when upstream provides text only in `response.output_item.done`, `response.completed`, or `response.incomplete` payloads.

#### Scenario: completed output text has no prior delta
- **WHEN** a public Responses stream receives a terminal response whose output contains text
- **AND** no prior text delta is known for that output item
- **THEN** the proxy MUST emit a synthetic `response.output_text.delta` before the terminal event

#### Scenario: existing text delta is not duplicated
- **WHEN** a public Responses stream already emitted a text delta for an output item
- **THEN** the proxy MUST NOT emit a duplicate synthetic delta for the same item when the final output arrives
