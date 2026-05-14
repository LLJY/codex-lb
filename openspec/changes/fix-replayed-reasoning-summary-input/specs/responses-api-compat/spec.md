## ADDED Requirements
### Requirement: Responses replay input sanitization
The service SHALL sanitize replayed Responses input before forwarding upstream by removing output-only reasoning summary shapes from `input` while preserving valid request-level Responses controls and encrypted reasoning continuity state.

#### Scenario: summary-only replay items are removed
- **WHEN** a client sends `/v1/responses` or `/backend-api/codex/responses` input containing output-only reasoning summary items such as `type: "summary_text"`, `type: "reasoning_content"`, `type: "reasoning_details"`, or `type: "reasoning"` without non-empty `encrypted_content`
- **THEN** the proxy removes those replay-only items before forwarding the request upstream
- **AND** visible text input/output content remains in the forwarded payload

#### Scenario: encrypted reasoning state is preserved
- **WHEN** a client sends `/v1/responses` or `/backend-api/codex/responses` input containing a `type: "reasoning"` item with non-empty `encrypted_content`
- **THEN** the proxy preserves that encrypted reasoning item in the forwarded payload
- **AND** removes unsupported summary-only fields from that item

#### Scenario: request-level reasoning controls are preserved
- **WHEN** a client sends request-level `reasoning` controls such as `effort` or `summary`
- **THEN** the proxy forwards those controls unchanged except for existing alias normalization
- **AND** input replay sanitization does not remove or rewrite the request-level `reasoning` field
