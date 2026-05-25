## 1. Specification

- [x] Add a `responses-api-compat` delta for stateless reasoning include, stale part-reference retry behavior, and orphan event filtering.

## 2. Implementation

- [x] Inject `reasoning.encrypted_content` into full Responses payloads without duplicating includes.
- [x] Add stale part-reference classification and one-shot pre-state retry sanitization.
- [x] Add item-scoped event filtering for HTTP SSE normalization and downstream WebSocket relay.

## 3. Verification

- [x] Run focused unit tests.
- [x] Run focused integration tests.
- [x] Run formatting, linting, OpenSpec validation, and diff whitespace checks.
