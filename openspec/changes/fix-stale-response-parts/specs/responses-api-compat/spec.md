## ADDED Requirements

### Requirement: Full Responses requests include encrypted reasoning state

Full Responses requests sent upstream with stateless storage MUST include `reasoning.encrypted_content` in the `include` array unless it is already present. Existing supported include values MUST be preserved and duplicates MUST NOT be added. Compact requests MUST NOT receive this include unless compact support is explicitly added.

#### Scenario: include is injected once
- **WHEN** a full Responses request is serialized without `reasoning.encrypted_content`
- **THEN** the upstream payload includes `reasoning.encrypted_content` exactly once alongside any existing include values

### Requirement: Stale part-reference errors are retried only before downstream state

The proxy MUST classify raw upstream `reasoning part <id>:<index> not found` and `text part <id> not found` errors separately from `previous_response_id` not-found errors. If such an error occurs before downstream response/output state is emitted, the proxy MAY retry once with a sanitized copy of the request that removes stale partial abandoned items while preserving completed visible messages, function calls, function outputs, request-level reasoning controls, and reasoning items with non-empty `encrypted_content`. If any downstream response/output state has already been emitted, the proxy MUST NOT silently retry.

#### Scenario: pre-state stale part retry
- **WHEN** upstream rejects a response before downstream state with `reasoning part rs_abc:0 not found`
- **THEN** the proxy retries once with stale partial abandoned items removed

#### Scenario: post-state stale part no silent retry
- **WHEN** downstream response/output state has already been emitted
- **THEN** the proxy surfaces a terminal failure instead of silently retrying

### Requirement: Orphan item-scoped events are filtered

The proxy MUST prevent downstream clients from receiving item-scoped reasoning summary, content part, output text, or refusal events that reference an unknown current-response item. Item-id-less deltas MUST be preserved. Unknown item-scoped events SHOULD be quarantined until the matching output item arrives and MUST be dropped if the stream terminates without that item. Terminal events MUST NOT be dropped.

#### Scenario: orphan item-scoped event is not forwarded
- **WHEN** a stream emits `response.reasoning_summary_text.delta` for an unknown `item_id`
- **THEN** the proxy does not forward the event unless the matching output item appears before terminal settlement
