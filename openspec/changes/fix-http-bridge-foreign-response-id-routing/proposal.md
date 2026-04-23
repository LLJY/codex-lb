## Why

HTTP Responses bridge routing only extracted response ids from `response.id`, but current OpenAI Responses delta events carry the identifier at top-level `response_id`.

When a bridged HTTP request was abandoned or cancelled and a stale upstream delta arrived later, codex-lb could treat that event as response-id-less and misroute it to a newer pending request on the same bridged websocket session.

Current OpenAI Responses streaming can also emit item/content/reasoning events that do not always carry `response_id`, so matching only explicit response ids is not sufficient if an abandoned bridged session remains reusable.

## What Changes

- honor top-level `response_id` when matching bridged/websocket Responses events to pending requests
- ignore foreign `response_id` events instead of falling back to the only pending request
- retire the bridged upstream session before reuse when a request is abandoned before `response.created`
- retire the bridged upstream session when an active in-flight request is abandoned after `response.created`
- keep one in-flight bridged response per upstream websocket until a terminal event releases the bridge gate
- replay a pre-created bridged request once on a fresh upstream account when the original upstream fails with a retryable quota/rate-limit terminal error before `response.created`
- log targeted HTTP bridge diagnostics when upstream disconnects before a terminal event
- add focused regression coverage for top-level `response_id` extraction and abandoned HTTP bridge requests

## Impact

- prevents stale upstream deltas, content parts, and reasoning parts from leaking into later bridged HTTP requests on the same websocket session
- reduces avoidable request failures when the selected bridged account is rate-limited before upstream creates a response and other viable accounts remain
- keeps the existing external HTTP/SSE contract unchanged
