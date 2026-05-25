from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock

import pytest
from starlette.requests import Request
from starlette.responses import StreamingResponse

import app.modules.proxy.api as proxy_api_module
from app.core.clients.proxy import ProxyResponseError
from app.core.errors import openai_error
from app.core.openai.chat_requests import ChatCompletionsRequest
from app.core.openai.requests import ResponsesRequest

pytestmark = pytest.mark.unit


async def _iter_blocks(*blocks: str) -> AsyncIterator[str]:
    for block in blocks:
        yield block


@pytest.mark.asyncio
async def test_collect_responses_payload_returns_contract_error_on_truncated_stream() -> None:
    result = await proxy_api_module._collect_responses_payload(
        _iter_blocks('data: {"type":"response.output_text.delta","delta":"hello"}\n\n')
    )

    body = result.model_dump(mode="json", exclude_none=True)
    assert body["error"]["code"] == "upstream_stream_truncated"


@pytest.mark.asyncio
async def test_collect_responses_payload_normalizes_unknown_output_item_to_message() -> None:
    result = await proxy_api_module._collect_responses_payload(
        _iter_blocks(
            (
                'data: {"type":"response.output_item.done","output_index":0,'
                '"item":{"id":"fa_1","type":"final_answer","text":"hello from final answer"}}\n\n'
            ),
            (
                'data: {"type":"response.completed","response":{"id":"resp_1","object":"response",'
                '"status":"completed","output":[]}}\n\n'
            ),
        )
    )

    body = result.model_dump(mode="json", exclude_none=True)
    assert body["id"] == "resp_1"
    assert body["output"] == [
        {
            "id": "fa_1",
            "type": "message",
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": "hello from final answer"}],
        }
    ]


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_appends_response_failed_on_invalid_json() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(_iter_blocks("data: {not-json}\n\n"))
    ]

    assert len(blocks) == 1
    payload = proxy_api_module._parse_sse_payload(blocks[0])
    assert payload is not None
    assert payload["type"] == "response.failed"
    response = payload["response"]
    assert isinstance(response, dict)
    error = response["error"]
    assert isinstance(error, dict)
    assert error["code"] == "invalid_json"


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_masks_response_failed_previous_response_not_found() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.failed","response":{"object":"response","status":"failed",'
                    '"error":{'
                    '"message":"Previous response with id \'resp_anchor\' not found.",'
                    '"param":"previous_response_id"}}}\n\n'
                )
            )
        )
    ]

    assert len(blocks) == 1
    assert "stream_incomplete" in blocks[0]
    assert "previous_response_not_found" not in blocks[0]


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_closes_source_after_yielded_chunk() -> None:
    class ClosingIterator:
        closed = False
        yielded = False

        def __aiter__(self) -> "ClosingIterator":
            return self

        async def __anext__(self) -> str:
            if not self.yielded:
                self.yielded = True
                return 'data: {"type":"response.output_text.delta","delta":"hello"}\n\n'
            await asyncio.sleep(10.0)
            return 'data: {"type":"response.completed","response":{"id":"resp_1","status":"completed"}}\n\n'

        async def aclose(self) -> None:
            self.closed = True

    source = ClosingIterator()
    stream = proxy_api_module._normalize_public_responses_stream(source)

    first = await anext(stream)
    payload = proxy_api_module._parse_sse_payload(first)
    assert payload is not None
    assert payload["type"] == "response.output_text.delta"
    await stream.aclose()

    assert source.closed is True


@pytest.mark.asyncio
async def test_stream_proxy_errors_as_response_failed_closes_source_after_yielded_chunk() -> None:
    class ClosingIterator:
        closed = False
        yielded = False

        def __aiter__(self) -> "ClosingIterator":
            return self

        async def __anext__(self) -> str:
            if not self.yielded:
                self.yielded = True
                return "data: {}\n\n"
            await asyncio.sleep(10.0)
            return "data: {}\n\n"

        async def aclose(self) -> None:
            self.closed = True

    source = ClosingIterator()
    stream = proxy_api_module._stream_proxy_errors_as_response_failed(source)

    assert await anext(stream) == "data: {}\n\n"
    await stream.aclose()

    assert source.closed is True


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_normalizes_unknown_terminal_output_item() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.completed","response":{"id":"resp_1","object":"response",'
                    '"status":"completed","output":[{"id":"fa_1","type":"final_answer","text":"normalized"}]}}\n\n'
                )
            )
        )
    ]

    assert len(blocks) == 2
    delta_payload = proxy_api_module._parse_sse_payload(blocks[0])
    assert delta_payload is not None
    assert delta_payload["type"] == "response.output_text.delta"
    assert delta_payload["delta"] == "normalized"
    assert "item_id" not in delta_payload
    assert "output_index" not in delta_payload
    payload = proxy_api_module._parse_sse_payload(blocks[1])
    assert payload is not None
    assert payload["type"] == "response.completed"
    response = payload["response"]
    assert isinstance(response, dict)
    output = response["output"]
    assert isinstance(output, list)
    assert output == [
        {
            "id": "fa_1",
            "type": "message",
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": "normalized"}],
        }
    ]


@pytest.mark.asyncio
async def test_collect_responses_payload_does_not_merge_foreign_output_item() -> None:
    result = await proxy_api_module._collect_responses_payload(
        _iter_blocks(
            (
                'data: {"type":"response.output_item.done","response_id":"resp_old","output_index":0,'
                '"item":{"id":"msg_old","type":"message","role":"assistant",'
                '"content":[{"type":"output_text","text":"old"}]}}\n\n'
            ),
            (
                'data: {"type":"response.completed","response":{"id":"resp_new","object":"response",'
                '"status":"completed","output":[]}}\n\n'
            ),
        )
    )

    body = result.model_dump(mode="json", exclude_none=True)
    assert body["id"] == "resp_new"
    assert body["output"] == []


@pytest.mark.asyncio
async def test_collect_responses_payload_merges_matching_response_output_item() -> None:
    result = await proxy_api_module._collect_responses_payload(
        _iter_blocks(
            (
                'data: {"type":"response.output_item.done","response_id":"resp_1","output_index":0,'
                '"item":{"id":"msg_1","type":"message","role":"assistant",'
                '"content":[{"type":"output_text","text":"ok"}]}}\n\n'
            ),
            (
                'data: {"type":"response.completed","response":{"id":"resp_1","object":"response",'
                '"status":"completed","output":[]}}\n\n'
            ),
        )
    )

    body = result.model_dump(mode="json", exclude_none=True)
    assert body["id"] == "resp_1"
    assert body["output"][0]["id"] == "msg_1"


@pytest.mark.asyncio
async def test_collect_responses_payload_ignores_conflicting_output_item_for_known_index() -> None:
    result = await proxy_api_module._collect_responses_payload(
        _iter_blocks(
            (
                'data: {"type":"response.output_item.added","response_id":"resp_1","output_index":0,'
                '"item":{"id":"msg_known","type":"message","role":"assistant",'
                '"content":[{"type":"output_text","text":"known"}]}}\n\n'
            ),
            (
                'data: {"type":"response.output_item.done","response_id":"resp_1","output_index":0,'
                '"item":{"id":"msg_stale","type":"message","role":"assistant",'
                '"content":[{"type":"output_text","text":"stale"}]}}\n\n'
            ),
            (
                'data: {"type":"response.completed","response":{"id":"resp_1","object":"response",'
                '"status":"completed","output":[]}}\n\n'
            ),
        )
    )

    body = result.model_dump(mode="json", exclude_none=True)
    assert body["id"] == "resp_1"
    assert body["output"][0]["id"] == "msg_known"
    assert body["output"][0]["content"][0]["text"] == "known"


@pytest.mark.asyncio
async def test_collect_responses_payload_ignores_foreign_terminal_after_created() -> None:
    result = await proxy_api_module._collect_responses_payload(
        _iter_blocks(
            'data: {"type":"response.created","response":{"id":"resp_current","status":"in_progress"}}\n\n',
            'data: {"type":"response.completed","response":{"id":"resp_stale","status":"completed"}}\n\n',
            'data: {"type":"response.completed","response":{"id":"resp_current","status":"completed"}}\n\n',
        )
    )

    body = result.model_dump(mode="json", exclude_none=True)
    assert body["id"] == "resp_current"


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_synthesizes_delta_from_done_message() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.output_item.done","output_index":0,'
                    '"item":{"id":"msg_1","type":"message","role":"assistant",'
                    '"content":[{"type":"output_text","text":"visible text"}]}}\n\n'
                ),
                (
                    'data: {"type":"response.completed","response":{"id":"resp_1","object":"response",'
                    '"status":"completed","output":[]}}\n\n'
                ),
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    assert payloads[0] is not None
    assert payloads[0]["type"] == "response.output_item.done"
    assert payloads[1] == {
        "type": "response.output_text.delta",
        "output_index": 0,
        "content_index": 0,
        "delta": "visible text",
        "item_id": "msg_1",
    }
    assert payloads[2] is not None
    assert payloads[2]["type"] == "response.completed"


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_synthesizes_delta_from_completed_output() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.completed","response":{"id":"resp_1","object":"response",'
                    '"status":"completed","output":[{"id":"msg_1","type":"message",'
                    '"content":[{"type":"output_text","text":"terminal text"}]}]}}\n\n'
                )
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    assert payloads[0] == {
        "type": "response.output_text.delta",
        "content_index": 0,
        "delta": "terminal text",
    }
    assert payloads[1] is not None
    assert payloads[1]["type"] == "response.completed"


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_does_not_synthesize_delta_from_reasoning_summary() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.completed","response":{"id":"resp_1","object":"response",'
                    '"status":"completed","output":[{"id":"rs_1","type":"reasoning",'
                    '"summary":"internal reasoning"}]}}\n\n'
                )
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    assert [payload["type"] for payload in payloads if payload is not None] == ["response.completed"]
    assert "internal reasoning" in blocks[0]


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_drains_after_terminal_event_for_settlement() -> None:
    settled = False
    closed = False

    async def source() -> AsyncIterator[str]:
        nonlocal settled, closed
        try:
            yield 'data: {"type":"response.completed","response":{"id":"resp_1","status":"completed"}}\n\n'
            settled = True
        finally:
            closed = True

    blocks = [block async for block in proxy_api_module._normalize_public_responses_stream(source())]

    assert len(blocks) == 1
    assert settled is True
    assert closed is True


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_drains_if_closed_after_synthetic_terminal_delta() -> None:
    settled = False

    async def source() -> AsyncIterator[str]:
        nonlocal settled
        yield (
            'data: {"type":"response.completed","response":{"id":"resp_1","object":"response",'
            '"status":"completed","output":[{"id":"msg_1","type":"message",'
            '"content":[{"type":"output_text","text":"terminal text"}]}]}}\n\n'
        )
        settled = True

    stream = proxy_api_module._normalize_public_responses_stream(source())

    first = await anext(stream)
    payload = proxy_api_module._parse_sse_payload(first)
    assert payload is not None
    assert payload["type"] == "response.output_text.delta"
    await stream.aclose()

    assert settled is True


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_does_not_duplicate_existing_delta() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                'data: {"type":"response.output_text.delta","item_id":"msg_1","delta":"already visible"}\n\n',
                (
                    'data: {"type":"response.output_item.done","output_index":0,'
                    '"item":{"id":"msg_1","type":"message","role":"assistant",'
                    '"content":[{"type":"output_text","text":"already visible"}]}}\n\n'
                ),
                (
                    'data: {"type":"response.completed","response":{"id":"resp_1","object":"response",'
                    '"status":"completed","output":[]}}\n\n'
                ),
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    event_types = [payload["type"] for payload in payloads if payload is not None]
    assert event_types == [
        "response.output_item.done",
        "response.output_text.delta",
        "response.completed",
    ]
    assert [
        payload.get("delta") for payload in payloads if payload and payload["type"] == "response.output_text.delta"
    ] == ["already visible"]


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_preserves_multiple_real_deltas_for_same_item() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.output_item.added","output_index":0,'
                    '"item":{"id":"msg_1","type":"message","status":"in_progress",'
                    '"role":"assistant","content":[]}}\n\n'
                ),
                'data: {"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"delta":"hel"}\n\n',
                'data: {"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"delta":"lo"}\n\n',
                'data: {"type":"response.output_text.delta","delta":" anonymous 1"}\n\n',
                'data: {"type":"response.output_text.delta","delta":" anonymous 2"}\n\n',
                'data: {"type":"response.completed","response":{"id":"resp_1","status":"completed"}}\n\n',
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    assert [
        payload.get("delta") for payload in payloads if payload and payload["type"] == "response.output_text.delta"
    ] == [
        "hel",
        "lo",
        " anonymous 1",
        " anonymous 2",
    ]


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_drops_orphan_item_scoped_event_at_terminal() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.reasoning_summary_text.delta",'
                    '"item_id":"rs_missing","summary_index":0,"delta":"orphan"}\n\n'
                ),
                'data: {"type":"response.completed","response":{"id":"resp_1","status":"completed"}}\n\n',
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    assert [payload["type"] for payload in payloads if payload is not None] == ["response.completed"]
    assert "orphan" not in "".join(blocks)


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_quarantines_and_flushes_known_item_scoped_event() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.reasoning_summary_part.added",'
                    '"item_id":"rs_late","output_index":0,"summary_index":0,'
                    '"part":{"type":"summary_text","text":"late summary"}}\n\n'
                ),
                (
                    'data: {"type":"response.output_item.added","output_index":0,'
                    '"item":{"id":"rs_late","type":"reasoning","summary":[]}}\n\n'
                ),
                'data: {"type":"response.completed","response":{"id":"resp_1","status":"completed"}}\n\n',
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    assert [payload["type"] for payload in payloads if payload is not None] == [
        "response.output_item.added",
        "response.reasoning_summary_part.added",
        "response.completed",
    ]


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_replays_quarantined_text_delta_when_item_arrives() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.output_text.delta",'
                    '"item_id":"msg_late","output_index":0,"content_index":0,"delta":"late text"}\n\n'
                ),
                (
                    'data: {"type":"response.output_text.delta",'
                    '"item_id":"msg_late","output_index":0,"content_index":0,"delta":" again"}\n\n'
                ),
                (
                    'data: {"type":"response.output_item.added","output_index":0,'
                    '"item":{"id":"msg_late","type":"message","status":"in_progress",'
                    '"role":"assistant","content":[]}}\n\n'
                ),
                'data: {"type":"response.completed","response":{"id":"resp_1","status":"completed"}}\n\n',
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    assert [payload["type"] for payload in payloads if payload is not None] == [
        "response.output_item.added",
        "response.output_text.delta",
        "response.output_text.delta",
        "response.completed",
    ]
    assert payloads[1] is not None
    assert payloads[1]["delta"] == "late text"
    assert payloads[2] is not None
    assert payloads[2]["delta"] == " again"


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_drops_mismatched_item_id_even_when_output_index_known() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.output_item.added","output_index":0,'
                    '"item":{"id":"msg_known","type":"message","status":"in_progress",'
                    '"role":"assistant","content":[]}}\n\n'
                ),
                (
                    'data: {"type":"response.output_text.delta",'
                    '"item_id":"msg_foreign","output_index":0,"content_index":0,"delta":"orphan"}\n\n'
                ),
                'data: {"type":"response.completed","response":{"id":"resp_1","status":"completed"}}\n\n',
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    assert [payload["type"] for payload in payloads if payload is not None] == [
        "response.output_item.added",
        "response.completed",
    ]
    assert "orphan" not in "".join(blocks)


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_drops_conflicting_output_item_for_known_index() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                'data: {"type":"response.created","response":{"id":"resp_current","status":"in_progress"}}\n\n',
                (
                    'data: {"type":"response.output_item.added","response_id":"resp_current",'
                    '"output_index":0,"item":{"id":"msg_known","type":"message",'
                    '"status":"in_progress","role":"assistant","content":[]}}\n\n'
                ),
                (
                    'data: {"type":"response.output_item.done","response_id":"resp_current",'
                    '"output_index":0,"item":{"id":"msg_stale","type":"message",'
                    '"status":"completed","role":"assistant","content":[]}}\n\n'
                ),
                'data: {"type":"response.completed","response":{"id":"resp_current","status":"completed"}}\n\n',
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    assert [payload["type"] for payload in payloads if payload is not None] == [
        "response.created",
        "response.output_item.added",
        "response.completed",
    ]
    assert "msg_stale" not in "".join(blocks)


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_scopes_known_items_by_response_id() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.output_item.added","response_id":"resp_current",'
                    '"output_index":0,"item":{"id":"msg_known","type":"message",'
                    '"status":"in_progress","role":"assistant","content":[]}}\n\n'
                ),
                (
                    'data: {"type":"response.output_text.delta","response_id":"resp_stale",'
                    '"item_id":"msg_known","output_index":0,"content_index":0,"delta":"stale"}\n\n'
                ),
                'data: {"type":"response.completed","response":{"id":"resp_current","status":"completed"}}\n\n',
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    assert [payload["type"] for payload in payloads if payload is not None] == [
        "response.output_item.added",
        "response.completed",
    ]
    assert "stale" not in "".join(blocks)


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_drops_foreign_output_item_after_created() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                'data: {"type":"response.created","response":{"id":"resp_current","status":"in_progress"}}\n\n',
                (
                    'data: {"type":"response.output_item.added","response_id":"resp_stale",'
                    '"output_index":0,"item":{"id":"msg_stale","type":"message",'
                    '"status":"in_progress","role":"assistant","content":[]}}\n\n'
                ),
                (
                    'data: {"type":"response.function_call_arguments.delta","response_id":"resp_stale",'
                    '"item_id":"msg_stale","output_index":0,"delta":"{}"}\n\n'
                ),
                'data: {"type":"response.completed","response":{"id":"resp_current","status":"completed"}}\n\n',
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    assert [payload["type"] for payload in payloads if payload is not None] == [
        "response.created",
        "response.completed",
    ]
    assert "msg_stale" not in "".join(blocks)


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_drops_foreign_output_item_before_created() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.output_item.added","response_id":"resp_stale",'
                    '"output_index":0,"item":{"id":"msg_stale","type":"message",'
                    '"status":"in_progress","role":"assistant","content":[]}}\n\n'
                ),
                'data: {"type":"response.created","response":{"id":"resp_current","status":"in_progress"}}\n\n',
                'data: {"type":"response.completed","response":{"id":"resp_current","status":"completed"}}\n\n',
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    assert [payload["type"] for payload in payloads if payload is not None] == [
        "response.created",
        "response.completed",
    ]
    assert "msg_stale" not in "".join(blocks)


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_flushes_matching_output_item_after_created() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                (
                    'data: {"type":"response.output_item.added","response_id":"resp_current",'
                    '"output_index":0,"item":{"id":"msg_current","type":"message",'
                    '"status":"in_progress","role":"assistant","content":[]}}\n\n'
                ),
                'data: {"type":"response.created","response":{"id":"resp_current","status":"in_progress"}}\n\n',
                'data: {"type":"response.completed","response":{"id":"resp_current","status":"completed"}}\n\n',
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    assert [payload["type"] for payload in payloads if payload is not None] == [
        "response.created",
        "response.output_item.added",
        "response.completed",
    ]
    assert payloads[1]["item"]["id"] == "msg_current"


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_ignores_foreign_terminal() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                'data: {"type":"response.created","response":{"id":"resp_current","status":"in_progress"}}\n\n',
                'data: {"type":"response.completed","response":{"id":"resp_stale","status":"completed"}}\n\n',
                'data: {"type":"response.completed","response":{"id":"resp_current","status":"completed"}}\n\n',
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    assert [payload["type"] for payload in payloads if payload is not None] == [
        "response.created",
        "response.completed",
    ]
    assert payloads[-1] is not None
    assert payloads[-1]["response"]["id"] == "resp_current"


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_drops_foreign_unscoped_delta() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                'data: {"type":"response.created","response":{"id":"resp_current","status":"in_progress"}}\n\n',
                'data: {"type":"response.output_text.delta","response_id":"resp_stale","delta":"stale"}\n\n',
                'data: {"type":"response.completed","response":{"id":"resp_current","status":"completed"}}\n\n',
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    assert [payload["type"] for payload in payloads if payload is not None] == [
        "response.created",
        "response.completed",
    ]
    assert "stale" not in "".join(blocks)


@pytest.mark.asyncio
async def test_normalize_public_responses_stream_preserves_item_id_less_delta() -> None:
    blocks = [
        block
        async for block in proxy_api_module._normalize_public_responses_stream(
            _iter_blocks(
                'data: {"type":"response.output_text.delta","delta":"anonymous"}\n\n',
                'data: {"type":"response.completed","response":{"id":"resp_1","status":"completed"}}\n\n',
            )
        )
    ]

    payloads = [proxy_api_module._parse_sse_payload(block) for block in blocks]
    assert payloads[0] == {"type": "response.output_text.delta", "delta": "anonymous"}
    assert payloads[1] is not None
    assert payloads[1]["type"] == "response.completed"


@pytest.mark.asyncio
async def test_stream_responses_returns_before_first_upstream_event(monkeypatch: pytest.MonkeyPatch) -> None:
    async def skip_limits(*args: object, **kwargs: object) -> None:
        del args, kwargs
        return None

    async def slow_stream(*args: object, **kwargs: object) -> AsyncIterator[str]:
        del args, kwargs
        await asyncio.sleep(10.0)
        yield 'data: {"type":"response.completed","response":{"id":"resp_slow","status":"completed"}}\n\n'

    monkeypatch.setattr(proxy_api_module, "_enforce_request_limits", skip_limits)
    monkeypatch.setattr(
        proxy_api_module,
        "get_settings",
        lambda: SimpleNamespace(http_responses_session_bridge_enabled=False, sse_keepalive_interval_seconds=0.0),
    )
    context = SimpleNamespace(
        service=SimpleNamespace(
            rate_limit_headers=AsyncMock(return_value={}),
            stream_responses=slow_stream,
        )
    )
    request = Request({"type": "http", "method": "POST", "path": "/v1/responses", "headers": []})
    payload = ResponsesRequest(model="gpt-5.1", instructions="test", input="hello")

    response = await asyncio.wait_for(
        proxy_api_module._stream_responses(
            request,
            payload,
            context=cast(proxy_api_module.ProxyContext, context),
            api_key=None,
        ),
        timeout=0.2,
    )

    assert isinstance(response, StreamingResponse)


@pytest.mark.asyncio
async def test_probe_stream_startup_error_closes_timed_out_first_task() -> None:
    class NeverYieldingStream:
        closed = False
        cancelled = False

        def __aiter__(self) -> "NeverYieldingStream":
            return self

        async def __anext__(self) -> str:
            try:
                await asyncio.sleep(10.0)
            except asyncio.CancelledError:
                self.cancelled = True
                raise
            raise AssertionError("unreachable")

        async def aclose(self) -> None:
            self.closed = True

    source = NeverYieldingStream()

    stream, startup_error = await proxy_api_module._probe_stream_startup_error(source, timeout_seconds=0.01)

    assert startup_error is None
    aclose = getattr(stream, "aclose", None)
    assert callable(aclose)
    await aclose()
    assert source.cancelled is True
    assert source.closed is True


@pytest.mark.asyncio
async def test_probe_stream_startup_error_converts_source_timeout() -> None:
    async def timeout_stream() -> AsyncIterator[str]:
        raise TimeoutError("source timeout")
        yield ""  # pragma: no cover

    stream, startup_error = await proxy_api_module._probe_stream_startup_error(timeout_stream(), timeout_seconds=5.0)

    assert [chunk async for chunk in stream] == []
    assert isinstance(startup_error, ProxyResponseError)
    assert startup_error.status_code == 504
    assert startup_error.payload["error"]["code"] == "upstream_request_timeout"


@pytest.mark.asyncio
async def test_probe_stream_startup_error_drains_after_startup_event_error() -> None:
    settled = False

    async def source() -> AsyncIterator[str]:
        nonlocal settled
        yield (
            'event: error\ndata: {"type":"error","status":504,'
            '"error":{"type":"server_error","code":"upstream_request_timeout",'
            '"message":"Proxy request budget exhausted"}}\n\n'
        )
        settled = True

    stream, startup_error = await proxy_api_module._probe_stream_startup_error(
        source(),
        convert_event_errors=True,
        timeout_seconds=5.0,
    )

    assert [chunk async for chunk in stream] == []
    assert isinstance(startup_error, ProxyResponseError)
    assert startup_error.status_code == 504
    assert settled is True


def test_stream_startup_error_response_can_preserve_internal_bridge_previous_response_error() -> None:
    request = Request({"type": "http", "method": "POST", "path": "/internal/bridge/responses", "headers": []})
    error = ProxyResponseError(
        400,
        openai_error("bridge_previous_response_not_found", "Bridge previous response was not found"),
    )

    response = proxy_api_module._stream_startup_error_response(
        request,
        error,
        headers={},
        mask_previous_response_not_found=False,
    )

    assert response.status_code == 400
    body = response.body.decode()
    assert "bridge_previous_response_not_found" in body
    assert "stream_incomplete" not in body


@pytest.mark.asyncio
async def test_stream_responses_preserves_startup_proxy_error_status(monkeypatch: pytest.MonkeyPatch) -> None:
    async def skip_limits(*args: object, **kwargs: object) -> None:
        del args, kwargs
        return None

    async def failing_stream(*args: object, **kwargs: object) -> AsyncIterator[str]:
        del args, kwargs
        raise ProxyResponseError(
            504,
            openai_error("upstream_request_timeout", "Proxy request budget exhausted"),
        )
        yield ""  # pragma: no cover

    monkeypatch.setattr(proxy_api_module, "_enforce_request_limits", skip_limits)
    monkeypatch.setattr(
        proxy_api_module,
        "get_settings",
        lambda: SimpleNamespace(http_responses_session_bridge_enabled=False, sse_keepalive_interval_seconds=0.0),
    )
    context = SimpleNamespace(
        service=SimpleNamespace(
            rate_limit_headers=AsyncMock(return_value={}),
            stream_responses=failing_stream,
        )
    )
    request = Request({"type": "http", "method": "POST", "path": "/v1/responses", "headers": []})
    payload = ResponsesRequest(model="gpt-5.1", instructions="test", input="hello")

    response = await proxy_api_module._stream_responses(
        request,
        payload,
        context=cast(proxy_api_module.ProxyContext, context),
        api_key=None,
        convert_startup_event_errors=True,
    )

    assert response.status_code == 504
    body = response.body.decode()
    assert "upstream_request_timeout" in body


@pytest.mark.asyncio
async def test_stream_responses_maps_startup_response_failed_timeout_to_504(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def skip_limits(*args: object, **kwargs: object) -> None:
        del args, kwargs
        return None

    async def failing_stream(*args: object, **kwargs: object) -> AsyncIterator[str]:
        del args, kwargs
        yield (
            'event: response.failed\ndata: {"type":"response.failed",'
            '"response":{"object":"response","status":"failed",'
            '"error":{"type":"server_error","code":"upstream_request_timeout",'
            '"message":"Proxy request budget exhausted"}}}\n\n'
        )

    monkeypatch.setattr(proxy_api_module, "_enforce_request_limits", skip_limits)
    monkeypatch.setattr(
        proxy_api_module,
        "get_settings",
        lambda: SimpleNamespace(http_responses_session_bridge_enabled=False, sse_keepalive_interval_seconds=0.0),
    )
    context = SimpleNamespace(
        service=SimpleNamespace(
            rate_limit_headers=AsyncMock(return_value={}),
            stream_responses=failing_stream,
        )
    )
    request = Request({"type": "http", "method": "POST", "path": "/v1/responses", "headers": []})
    payload = ResponsesRequest(model="gpt-5.1", instructions="test", input="hello")

    response = await proxy_api_module._stream_responses(
        request,
        payload,
        context=cast(proxy_api_module.ProxyContext, context),
        api_key=None,
        convert_startup_event_errors=True,
    )

    assert response.status_code == 504
    body = response.body.decode()
    assert "upstream_request_timeout" in body


@pytest.mark.asyncio
async def test_stream_responses_preserves_direct_startup_sse_event_status(monkeypatch: pytest.MonkeyPatch) -> None:
    async def skip_limits(*args: object, **kwargs: object) -> None:
        del args, kwargs
        return None

    async def failing_stream(*args: object, **kwargs: object) -> AsyncIterator[str]:
        del args, kwargs
        yield (
            'event: error\ndata: {"type":"error","status":504,'
            '"error":{"type":"server_error","code":"upstream_request_timeout",'
            '"message":"Proxy request budget exhausted"}}\n\n'
        )

    monkeypatch.setattr(proxy_api_module, "_enforce_request_limits", skip_limits)
    monkeypatch.setattr(
        proxy_api_module,
        "get_settings",
        lambda: SimpleNamespace(http_responses_session_bridge_enabled=False, sse_keepalive_interval_seconds=0.0),
    )
    context = SimpleNamespace(
        service=SimpleNamespace(
            rate_limit_headers=AsyncMock(return_value={}),
            stream_responses=failing_stream,
        )
    )
    request = Request({"type": "http", "method": "POST", "path": "/v1/responses", "headers": []})
    payload = ResponsesRequest(model="gpt-5.1", instructions="test", input="hello")

    response = await proxy_api_module._stream_responses(
        request,
        payload,
        context=cast(proxy_api_module.ProxyContext, context),
        api_key=None,
        convert_startup_event_errors=True,
    )

    assert response.status_code == 504
    body = response.body.decode()
    assert "upstream_request_timeout" in body


@pytest.mark.asyncio
async def test_stream_responses_preserves_bridged_startup_sse_event_status(monkeypatch: pytest.MonkeyPatch) -> None:
    async def skip_limits(*args: object, **kwargs: object) -> None:
        del args, kwargs
        return None

    async def failing_stream(*args: object, **kwargs: object) -> AsyncIterator[str]:
        del args, kwargs
        yield (
            'event: error\ndata: {"type":"error","status":504,'
            '"error":{"type":"server_error","code":"upstream_request_timeout",'
            '"message":"Proxy request budget exhausted"}}\n\n'
        )

    monkeypatch.setattr(proxy_api_module, "_enforce_request_limits", skip_limits)
    monkeypatch.setattr(
        proxy_api_module,
        "get_settings",
        lambda: SimpleNamespace(http_responses_session_bridge_enabled=True, sse_keepalive_interval_seconds=0.0),
    )
    monkeypatch.setattr(
        proxy_api_module.proxy_service_module,
        "get_settings",
        lambda: SimpleNamespace(http_responses_session_bridge_enabled=True),
    )
    context = SimpleNamespace(
        service=SimpleNamespace(
            rate_limit_headers=AsyncMock(return_value={}),
            stream_http_responses=failing_stream,
        )
    )
    request = Request({"type": "http", "method": "POST", "path": "/v1/responses", "headers": []})
    payload = ResponsesRequest(model="gpt-5.1", instructions="test", input="hello")

    response = await proxy_api_module._stream_responses(
        request,
        payload,
        context=cast(proxy_api_module.ProxyContext, context),
        api_key=None,
        prefer_http_bridge=True,
    )

    assert response.status_code == 504
    body = response.body.decode()
    assert "upstream_request_timeout" in body


@pytest.mark.asyncio
async def test_v1_chat_completions_preserves_startup_sse_event_status(monkeypatch: pytest.MonkeyPatch) -> None:
    async def skip_limits(*args: object, **kwargs: object) -> None:
        del args, kwargs
        return None

    async def failing_stream(*args: object, **kwargs: object) -> AsyncIterator[str]:
        del args, kwargs
        yield (
            'event: error\ndata: {"type":"error","status":504,'
            '"error":{"type":"server_error","code":"upstream_request_timeout",'
            '"message":"Proxy request budget exhausted"}}\n\n'
        )

    monkeypatch.setattr(proxy_api_module, "_enforce_request_limits", skip_limits)
    context = SimpleNamespace(
        service=SimpleNamespace(
            rate_limit_headers=AsyncMock(return_value={}),
            stream_responses=failing_stream,
        )
    )
    request = Request({"type": "http", "method": "POST", "path": "/v1/chat/completions", "headers": []})
    payload = ChatCompletionsRequest(
        model="gpt-5.1",
        messages=[{"role": "user", "content": "hello"}],
        stream=True,
    )

    response = await proxy_api_module.v1_chat_completions(
        request,
        payload,
        context=cast(proxy_api_module.ProxyContext, context),
        api_key=None,
    )

    assert response.status_code == 504
    body = response.body.decode()
    assert "upstream_request_timeout" in body


@pytest.mark.asyncio
async def test_v1_chat_completions_closes_upstream_on_stream_close(monkeypatch: pytest.MonkeyPatch) -> None:
    async def skip_limits(*args: object, **kwargs: object) -> None:
        del args, kwargs
        return None

    class ClosingIterator:
        closed = False
        yielded = False

        def __aiter__(self) -> "ClosingIterator":
            return self

        async def __anext__(self) -> str:
            if not self.yielded:
                self.yielded = True
                return 'data: {"type":"response.output_text.delta","delta":"hello"}\n\n'
            await asyncio.sleep(10.0)
            return 'data: {"type":"response.completed","response":{"id":"resp_1","status":"completed"}}\n\n'

        async def aclose(self) -> None:
            self.closed = True

    source = ClosingIterator()

    def stream_responses(*args: object, **kwargs: object) -> AsyncIterator[str]:
        del args, kwargs
        return source

    monkeypatch.setattr(proxy_api_module, "_enforce_request_limits", skip_limits)
    monkeypatch.setattr(
        proxy_api_module,
        "get_settings",
        lambda: SimpleNamespace(http_responses_session_bridge_enabled=False, sse_keepalive_interval_seconds=0.0),
    )
    context = SimpleNamespace(
        service=SimpleNamespace(
            rate_limit_headers=AsyncMock(return_value={}),
            stream_responses=stream_responses,
        )
    )
    request = Request({"type": "http", "method": "POST", "path": "/v1/chat/completions", "headers": []})
    payload = ChatCompletionsRequest(
        model="gpt-5.1",
        messages=[{"role": "user", "content": "hello"}],
        stream=True,
    )

    response = await proxy_api_module.v1_chat_completions(
        request,
        payload,
        context=cast(proxy_api_module.ProxyContext, context),
        api_key=None,
    )

    assert isinstance(response, StreamingResponse)
    first = await anext(response.body_iterator)
    assert "chat.completion.chunk" in (first.decode() if isinstance(first, bytes) else str(first))
    await response.body_iterator.aclose()

    assert source.closed is True


@pytest.mark.asyncio
async def test_stream_responses_disables_keepalives_for_forwarded_internal_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_intervals: list[float] = []

    def capture_keepalive_interval(source: AsyncIterator[str], interval_seconds: float) -> AsyncIterator[str]:
        captured_intervals.append(interval_seconds)
        return source

    async def completed_stream(*args: object, **kwargs: object) -> AsyncIterator[str]:
        del args, kwargs
        yield 'data: {"type":"response.completed","response":{"id":"resp_1","status":"completed"}}\n\n'

    monkeypatch.setattr(proxy_api_module, "inject_sse_keepalives", capture_keepalive_interval)
    monkeypatch.setattr(
        proxy_api_module,
        "get_settings",
        lambda: SimpleNamespace(
            http_responses_session_bridge_enabled=False,
            proxy_request_budget_seconds=5.0,
            sse_keepalive_interval_seconds=5.0,
        ),
    )
    context = SimpleNamespace(
        service=SimpleNamespace(
            rate_limit_headers=AsyncMock(return_value={}),
            stream_responses=completed_stream,
        )
    )
    request = Request({"type": "http", "method": "POST", "path": "/internal/bridge/responses", "headers": []})
    payload = ResponsesRequest(model="gpt-5.1", instructions="test", input="hello")

    response = await proxy_api_module._stream_responses(
        request,
        payload,
        context=cast(proxy_api_module.ProxyContext, context),
        api_key=None,
        skip_limit_enforcement=True,
        forwarded_request=True,
    )

    assert isinstance(response, StreamingResponse)
    assert captured_intervals == [0.0]


@pytest.mark.asyncio
async def test_stream_responses_streams_post_startup_proxy_error_as_sse(monkeypatch: pytest.MonkeyPatch) -> None:
    async def skip_limits(*args: object, **kwargs: object) -> None:
        del args, kwargs
        return None

    async def late_failing_stream(*args: object, **kwargs: object) -> AsyncIterator[str]:
        del args, kwargs
        await asyncio.sleep(0.1)
        error = openai_error("rate_limit_exceeded", "opportunistic burn window closed")
        error["error"]["plan_type"] = "pro"
        error["error"]["resets_in_seconds"] = 42
        raise ProxyResponseError(
            429,
            error,
        )
        yield ""  # pragma: no cover

    monkeypatch.setattr(proxy_api_module, "_enforce_request_limits", skip_limits)
    monkeypatch.setattr(
        proxy_api_module,
        "get_settings",
        lambda: SimpleNamespace(http_responses_session_bridge_enabled=False, sse_keepalive_interval_seconds=0.0),
    )
    context = SimpleNamespace(
        service=SimpleNamespace(
            rate_limit_headers=AsyncMock(return_value={"X-RateLimit-Limit": "1"}),
            stream_responses=late_failing_stream,
        )
    )
    request = Request({"type": "http", "method": "POST", "path": "/v1/responses", "headers": []})
    payload = ResponsesRequest(model="gpt-5.1", instructions="test", input="hello")

    response = await proxy_api_module._stream_responses(
        request,
        payload,
        context=cast(proxy_api_module.ProxyContext, context),
        api_key=None,
    )

    assert isinstance(response, StreamingResponse)
    assert response.status_code == 200
    assert response.headers["X-RateLimit-Limit"] == "1"
    chunks = [chunk async for chunk in response.body_iterator]
    body = "".join(chunk.decode() if isinstance(chunk, bytes) else str(chunk) for chunk in chunks)
    assert "response.failed" in body
    assert "rate_limit_exceeded" in body
    assert '"plan_type":"pro"' in body
    assert '"resets_in_seconds":42' in body


@pytest.mark.asyncio
async def test_collect_responses_payload_preserves_apply_patch_call_output_item() -> None:
    result = await proxy_api_module._collect_responses_payload(
        _iter_blocks(
            (
                'data: {"type":"response.output_item.done","output_index":0,'
                '"item":{"id":"apc_1","type":"apply_patch_call","status":"completed",'
                '"call_id":"call_1","patch":"*** Begin Patch\\n*** End Patch\\n"}}\n\n'
            ),
            (
                'data: {"type":"response.completed","response":{"id":"resp_1","object":"response",'
                '"status":"completed","output":[]}}\n\n'
            ),
        )
    )

    body = result.model_dump(mode="json", exclude_none=True)
    assert body["id"] == "resp_1"
    assert body["output"] == [
        {
            "id": "apc_1",
            "type": "apply_patch_call",
            "status": "completed",
            "call_id": "call_1",
            "patch": "*** Begin Patch\n*** End Patch\n",
        }
    ]


@pytest.mark.asyncio
async def test_collect_responses_payload_preserves_mcp_approval_request_output_item() -> None:
    result = await proxy_api_module._collect_responses_payload(
        _iter_blocks(
            (
                'data: {"type":"response.output_item.done","output_index":0,'
                '"item":{"id":"mcp_1","type":"mcp_approval_request","status":"in_progress",'
                '"request_id":"req_1","server_label":"github","tool_name":"repos/list"}}\n\n'
            ),
            (
                'data: {"type":"response.completed","response":{"id":"resp_2","object":"response",'
                '"status":"completed","output":[]}}\n\n'
            ),
        )
    )

    body = result.model_dump(mode="json", exclude_none=True)
    assert body["id"] == "resp_2"
    assert body["output"] == [
        {
            "id": "mcp_1",
            "type": "mcp_approval_request",
            "status": "in_progress",
            "request_id": "req_1",
            "server_label": "github",
            "tool_name": "repos/list",
        }
    ]


@pytest.mark.asyncio
async def test_collect_responses_payload_preserves_output_image_item() -> None:
    result = await proxy_api_module._collect_responses_payload(
        _iter_blocks(
            (
                'data: {"type":"response.output_item.done","output_index":0,'
                '"item":{"id":"img_1","type":"output_image","image_url":"https://example.com/a.png"}}\n\n'
            ),
            (
                'data: {"type":"response.completed","response":{"id":"resp_3","object":"response",'
                '"status":"completed","output":[]}}\n\n'
            ),
        )
    )

    body = result.model_dump(mode="json", exclude_none=True)
    assert body["id"] == "resp_3"
    assert body["output"] == [
        {
            "id": "img_1",
            "type": "output_image",
            "image_url": "https://example.com/a.png",
        }
    ]
