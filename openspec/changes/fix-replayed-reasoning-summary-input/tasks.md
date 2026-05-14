## 1. Implementation

- [x] 1.1 Sanitize summary-only reasoning replay shapes under Responses `input`.
- [x] 1.2 Preserve encrypted reasoning input items with non-empty `encrypted_content`.
- [x] 1.3 Preserve request-level `reasoning` controls.

## 2. Verification

- [x] 2.1 Add/update unit tests for request payload sanitization.
- [x] 2.2 Add/update integration tests for `/v1/responses` forwarded payload sanitization and compact encrypted reasoning round trips.
- [x] 2.3 Run targeted pytest, ruff, OpenSpec validation, and whitespace checks.
