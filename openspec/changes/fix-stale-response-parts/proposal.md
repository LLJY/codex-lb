# Change: Fix stale Responses part replay after stop/reword

## Why

Stopping a Responses stream before terminal settlement can leave partial output item or part references in replayed client history. A subsequent reword/resend may fail upstream with `reasoning part ... not found` or `text part ... not found`, or emit item-scoped part events for unknown items that can crash OpenCode.

## What Changes

- Request `reasoning.encrypted_content` on full Responses requests so stateless `store=false` reasoning items can be replayed safely.
- Classify stale reasoning/text part-reference upstream errors separately from `previous_response_id` not-found handling.
- Retry once with a narrowly sanitized request only when no downstream response/output state has been emitted.
- Filter/quarantine orphan item-scoped downstream events without dropping terminal events or item-id-less deltas.

## Non-Goals

- No broad reasoning sanitizer or removal of valid completed reasoning items.
- No changes to compact request include support.
- No bridge session affinity rewrite.
