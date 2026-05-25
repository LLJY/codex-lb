# Bug Report: Stop/Reword Resends Replay Stale Response Parts

## Summary

Stopping an OpenCode request and immediately rewording/resending can produce upstream or client-side failures caused by stale partial response parts from the abandoned turn.

Observed errors include:

```text
reasoning part rs_01094052542d5575016a0711749fe0819182f45419d73a183b:0 not found
text part msg_0facdefe9b7eff1d016a0711bfad4c819182ce28c4e9d0a7cf not found
undefined is not an object (evaluating 'i[W.item_id].summaryParts')
```

This is not the same issue as the reverted broad reasoning sanitizer. The reverted sanitizer caused frequent stream incompletes because it pruned valid reasoning continuity state too aggressively. The current bug is narrower: aborted/incomplete output state can be replayed or leaked across the next request.

## Working hypothesis

OpenCode can send full conversation/input history to `codex-lb`, especially after stop/reword/resend. `codex-lb` then uses the HTTP Responses bridge/WebSocket upstream path to keep requests fast by anchoring on upstream state and trimming already-stored prefix when safe.

When a stream is stopped before a clean terminal event, one or both of these can happen:

1. The client or bridge retains partial output items from the aborted turn.
2. The next resend includes references to reasoning or text parts that were never finalized upstream, or were tied to a response state that no longer exists.

Upstream then rejects the request with `reasoning part ... not found` / `text part ... not found`, or the downstream OpenCode UI receives a part event for an `item_id` that was not initialized, causing `summaryParts` to be undefined.

## Important constraints

- Do **not** reapply the broad sanitizer from `6b7b7cce`.
- Do **not** strip normal completed `type: "reasoning"` items on successful requests.
- Preserve encrypted reasoning continuity state.
- Preserve visible completed messages, function calls, and function call outputs.
- Keep the fast bridge/continuity path intact; do not force every request to send full context upstream.
- Avoid silent retry after any downstream response/output state has already been emitted, not just after visible text. Once OpenCode has seen stateful events, retry must fail in-band instead of creating duplicate/orphan client state.
- Before implementation, promote this plan into an OpenSpec change/delta because it changes Responses bridge compatibility behavior.

## Relevant API behavior

OpenAI Responses reasoning docs state that stateless use with `store=false` should include:

```json
"include": ["reasoning.encrypted_content"]
```

Reasoning output items can then include an opaque `encrypted_content` blob that can be passed back in future turns. This matters because `codex-lb` forces `store=false`; reasoning items with only `id`/`summary` may not be self-contained across stop/retry/replay boundaries.

## Proposed fix

### 1. Request portable reasoning state by default

For full Responses requests, append `reasoning.encrypted_content` to `include` when missing.

Rules:

- Apply only to full Responses create/stream requests.
- Preserve existing `include` values and avoid duplicates.
- Do not add unsupported include values.
- Do not apply to compact requests unless the compact endpoint explicitly supports it.
- Cover all full Responses construction paths that can reach upstream, including `/backend-api/codex/responses`, `/v1/responses`, chat-mapped Responses requests, HTTP bridge sends, and upstream WebSocket `response.create` payloads.

Expected effect: future reasoning output items become self-contained enough for stateless replay when OpenCode sends full history after a stop/reword. This is forward-looking only; it will not repair already-stale local histories by itself.

### 2. Classify stale part-reference errors

Add a narrow classifier for upstream errors whose message matches stale part references:

- `reasoning part <id>:<index> not found`
- `text part <id> not found`

This classifier should be separate from `previous_response_id` not-found handling. These errors are about missing output parts/items, not missing response anchors.

Placement:

- Classify raw upstream `error` and `response.failed` payloads before previous-response masking/rewrite logic and before public stream normalization can replace the original message.
- Return structured data such as `{kind, item_id, part_index}` so retry sanitization can target the exact stale item family.
- Keep this account-neutral and distinct from previous-response owner/rebind recovery.

### 3. Retry once only before downstream state is flushed

When a stale part-reference error occurs before any downstream response/output state was emitted:

1. Confirm no downstream state event has been flushed for this request. State events include `response.created`, `response.output_item.*`, `response.content_part.*`, `response.reasoning_summary_*`, `response.output_text.*`, `response.refusal.*`, terminal events, and any event that mutates OpenCode's response item map.
2. Prefer buffering early state events until retry/no-retry is decided. If any state event was already flushed, do **not** retry silently; emit the appropriate terminal failure for the already-started stream.
3. Retry once with a sanitized copy of the incoming `input`.
4. Stay on the same live bridge session when possible. Do not reset/recreate the bridge while keeping a session-local `previous_response_id` anchor.
5. If resetting/recreating is required, only retry a proven-safe full unanchored request:
   - the anchor was proxy-injected,
   - the untrimmed full resend is fingerprint-verified,
   - dropping `previous_response_id` preserves the client's full visible context,
   - and the request has not already emitted downstream state.
6. Remove stale or obviously partial abandoned output items with explicit precedence:
   - If an item `id` matches the failed `rs_...` / `msg_...` and the item is partial/aborted (`status: "in_progress"`, missing terminal/completed status where one is expected, or part of the known abandoned suffix), remove or rewrite it.
   - If the referenced item is a completed visible `message`, do not drop the user-visible text. Prefer rewriting it into a plain assistant message with no stale server-generated item/part IDs.
   - If the referenced item is an encrypted reasoning item, preserve non-empty `encrypted_content`; if the stale id/summary reference is the issue, strip summary-only/stale-id fields rather than dropping encrypted state.
   - Remove all clearly partial/abandoned sibling output items from the same aborted suffix, not only the first referenced id, so the one retry does not fail on the next stale part.
7. Preserve:
   - completed assistant/user messages
   - `function_call`
   - `function_call_output`
   - reasoning items with non-empty `encrypted_content`
   - request-level `reasoning` controls

Do not proactively sanitize normal successful requests beyond adding `reasoning.encrypted_content` to `include`.

### 4. Drop orphan downstream part events

Protect OpenCode from receiving part events for unknown items.

Track output items emitted to the current downstream request by `item_id` after `response.output_item.added` / `response.output_item.done`. Drop or quarantine part/delta events with unknown `item_id` for the current request, including:

- `response.reasoning_summary_part.added`
- `response.reasoning_summary_part.done`
- `response.reasoning_summary_text.delta`
- `response.reasoning_summary_text.done`
- `response.content_part.added`
- `response.content_part.done`
- `response.output_text.delta`
- `response.output_text.done`
- `response.refusal.delta`
- `response.refusal.done`

Ordering rules:

- Track known items by `(response_id, item_id)` and fallback `(response_id, output_index)`.
- Do not drop item-id-less deltas solely because no item is known; valid Responses streams can send deltas without `item_id`.
- If an item-scoped event references an unknown `item_id` / `output_index`, quarantine it briefly and flush it if the matching `response.output_item.added` or `response.output_item.done` arrives.
- Drop only when the event is proven stale/current-response-mismatched, or when the stream reaches a terminal event without the referenced item ever appearing.
- Apply equivalent protection to both HTTP SSE bridge output and direct downstream WebSocket relay paths.

This should prevent client crashes like:

```text
undefined is not an object (evaluating 'i[W.item_id].summaryParts')
```

The filter must not drop terminal events (`response.completed`, `response.incomplete`, `response.failed`, `error`).

## Non-goals

- No broad replay sanitization of all reasoning output.
- No removal of response-side reasoning summaries from completed responses.
- No full rewrite of bridge session affinity or previous-response routing.
- No change to Anthropic/OpenAI client transport outside the Responses bridge path.

## Test plan

### Unit tests

- `ResponsesRequest.to_payload()` appends `reasoning.encrypted_content` to `include` when missing.
- Existing include values are preserved and `reasoning.encrypted_content` is not duplicated.
- Compact request payloads remain unchanged unless compact support is intentionally added.
- Stale part-reference classifier matches:
  - `reasoning part rs_abc:0 not found`
  - `text part msg_abc not found`
- The classifier does not match unrelated `not found` messages.
- Classifier runs on raw upstream `error` and `response.failed` payloads before previous-response rewrite/public normalization.
- Retry sanitizer removes the referenced stale `rs_...` / `msg_...` item and all partial abandoned sibling items from the aborted suffix.
- Retry sanitizer rewrites completed visible `message` items with stale ids into plain assistant messages instead of dropping visible text.
- Retry sanitizer preserves tool calls, tool outputs, request-level `reasoning`, and encrypted reasoning items with non-empty `encrypted_content`.
- Retry gate refuses silent retry once any downstream state event has been flushed, including `response.created` and output item/part events.
- Orphan event filtering drops or quarantines item-scoped reasoning summary/content/text/refusal events for unknown `item_id` / `output_index` while preserving known-item events and valid item-id-less deltas.

### Integration tests

- Simulate an aborted bridge response that emits partial `reasoning` / `message` items, then resend a reworded request.
- Upstream first returns `reasoning part ... not found`; bridge retries once with stale/partial aborted suffix removed while preserving encrypted reasoning state.
- Upstream first returns `text part ... not found`; bridge retries once with stale ids stripped or partial stale message removed while preserving completed visible text.
- Ensure no retry occurs after any downstream state event has been emitted, not just visible text.
- Ensure retry does not reset/recreate the bridge while retaining an unsafe session-local `previous_response_id` anchor.
- Ensure a valid encrypted reasoning item round-trips through compact/full Responses without pruning.
- Ensure OpenCode-facing HTTP SSE and downstream WebSocket streams never receive orphan item-scoped reasoning summary/content/text/refusal events.

## Acceptance criteria

- Stop/reword/resend no longer produces `reasoning part ... not found` or `text part ... not found` for stale aborted items.
- OpenCode UI no longer crashes on orphan `summaryParts` access.
- Normal completed reasoning continuity remains intact.
- Bridge remains fast: full-history client resends can still use upstream continuity/trim where safe.
- The reverted broad sanitizer behavior is not reintroduced.
- Retrying never creates duplicate/orphan OpenCode response state after downstream state was already flushed.

## Verification commands

Use focused checks first:

```bash
uv run pytest tests/unit/test_openai_requests.py tests/unit/test_proxy_utils.py tests/unit/test_proxy_api_responses_contract.py -q
uv run pytest tests/integration/test_http_responses_bridge.py tests/integration/test_proxy_responses.py tests/integration/test_proxy_compact.py -q -k 'reasoning or stale or orphan or compact or previous_response'
uv run ruff check app/core/openai/requests.py app/modules/proxy/api.py app/modules/proxy/service.py tests/unit/test_openai_requests.py tests/unit/test_proxy_utils.py tests/unit/test_proxy_api_responses_contract.py tests/integration/test_http_responses_bridge.py tests/integration/test_proxy_responses.py tests/integration/test_proxy_compact.py
bunx @fission-ai/openspec validate --specs
git diff --check
```
