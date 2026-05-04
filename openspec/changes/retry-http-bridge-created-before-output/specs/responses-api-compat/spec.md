## ADDED Requirements

### Requirement: HTTP bridge retries created-without-output disconnects safely
When a bridged HTTP Responses request receives `response.created` from upstream and the upstream websocket disconnects before any later event has been emitted downstream, the service MUST retry the request transparently at most once if the request is a first-turn request without `previous_response_id`, has replay count `0`, still has a replayable request payload, and is the only pending or queued request on the bridged upstream session.

#### Scenario: retry suppresses the stale created response id
- **WHEN** a bridged HTTP `/v1/responses` or `/backend-api/codex/responses` stream receives upstream `response.created`
- **AND** that `response.created` has not been emitted downstream
- **AND** upstream disconnects before any downstream-visible output or terminal event
- **AND** there is exactly one pending or queued request with no `previous_response_id`, replay count `0`, and a replayable request payload
- **THEN** the service MUST reconnect the HTTP bridge upstream
- **AND** it MUST replay the request once with the stale upstream response id cleared
- **AND** the downstream stream MUST NOT include the stale `response.created` response id from the failed attempt
- **AND** the downstream stream MUST include only events for the replayed response if the replay succeeds
- **AND** the downstream stream MUST NOT emit `stream_incomplete` for the failed first attempt

#### Scenario: no retry after downstream output
- **WHEN** a bridged HTTP Responses stream has already emitted any event after `response.created` downstream
- **AND** upstream disconnects before `response.completed`
- **THEN** the service MUST NOT replay the request transparently
- **AND** it MUST surface the existing `stream_incomplete` failure downstream

#### Scenario: unsafe created disconnects remain failures
- **WHEN** upstream disconnects after `response.created`
- **AND** the pending bridged HTTP request has `previous_response_id`, replay count greater than `0`, no replayable request payload, more than one pending or queued request exists, or its `response.created` was already emitted downstream
- **THEN** the service MUST NOT replay the request transparently
- **AND** it MUST preserve the existing downstream failure behavior
