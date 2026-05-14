# Proposal: fix-replayed-reasoning-summary-input

## Why

Some clients replay prior Responses output as the next `/v1/responses` input. When that replay includes reasoning summaries, the proxy can forward output-only shapes such as `summary_text` parts or summary-only `reasoning` items back upstream as input, causing upstream validation errors.

Compact and Responses continuity flows also rely on encrypted reasoning state. The sanitizer must remove unsupported summary-only replay data without pruning valid `type: "reasoning"` input items that carry `encrypted_content`.

## What Changes

- Sanitize replayed Responses input items under `input` so summary-only reasoning output shapes are removed before upstream forwarding.
- Preserve request-level `reasoning` controls and encrypted reasoning input items with non-empty `encrypted_content`.
- Strip unsupported summary fields from preserved encrypted reasoning items.
- Add focused unit and integration coverage for replay sanitization and compact encrypted-reasoning round trips.

## Impact

- Restores replay/continuation compatibility for clients that include prior reasoning summaries in history.
- Preserves compact continuity state required by follow-up Responses calls.
- Keeps the change scoped to request sanitization for Responses-compatible routes.
