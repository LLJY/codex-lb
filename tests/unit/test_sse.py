from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import pytest

from app.core.utils.sse import SSE_KEEPALIVE_FRAME, format_sse_event, inject_sse_keepalives

pytestmark = pytest.mark.unit


def test_format_sse_event_serializes_payload():
    payload = {"type": "response.completed", "response": {"id": "resp_1"}}
    result = format_sse_event(payload)
    assert result == 'event: response.completed\ndata: {"type":"response.completed","response":{"id":"resp_1"}}\n\n'


async def _agen(items: list[str]) -> AsyncIterator[str]:
    for item in items:
        yield item


async def _slow_agen(items: list[str], delay: float) -> AsyncIterator[str]:
    for item in items:
        await asyncio.sleep(delay)
        yield item


@pytest.mark.asyncio
async def test_inject_sse_keepalives_passes_through_when_disabled():
    out = [chunk async for chunk in inject_sse_keepalives(_agen(["a\n\n", "b\n\n"]), 0)]
    assert out == ["a\n\n", "b\n\n"]


@pytest.mark.asyncio
async def test_inject_sse_keepalives_no_pings_when_source_is_fast():
    out = [chunk async for chunk in inject_sse_keepalives(_agen(["a\n\n", "b\n\n"]), 5.0)]
    assert out == ["a\n\n", "b\n\n"]


@pytest.mark.asyncio
async def test_inject_sse_keepalives_emits_pings_on_idle_gap():
    out = [chunk async for chunk in inject_sse_keepalives(_slow_agen(["a\n\n"], delay=0.25), 0.05)]
    assert out[-1] == "a\n\n"
    assert SSE_KEEPALIVE_FRAME in out
    assert out.count(SSE_KEEPALIVE_FRAME) >= 2


@pytest.mark.asyncio
async def test_inject_sse_keepalives_propagates_source_timeout():
    async def timeout_source() -> AsyncIterator[str]:
        raise TimeoutError("source timeout")
        yield ""  # pragma: no cover

    stream = inject_sse_keepalives(timeout_source(), 5.0)

    with pytest.raises(TimeoutError, match="source timeout"):
        await anext(stream)


@pytest.mark.asyncio
async def test_inject_sse_keepalives_closes_source_after_yielded_chunk():
    class ClosingIterator:
        closed = False
        yielded = False

        def __aiter__(self) -> "ClosingIterator":
            return self

        async def __anext__(self) -> str:
            if not self.yielded:
                self.yielded = True
                return "a\n\n"
            await asyncio.sleep(10.0)
            return "b\n\n"

        async def aclose(self) -> None:
            self.closed = True

    source = ClosingIterator()
    stream = inject_sse_keepalives(source, 5.0)

    assert await anext(stream) == "a\n\n"
    await stream.aclose()

    assert source.closed is True


@pytest.mark.asyncio
async def test_inject_sse_keepalives_keepalive_frame_is_sse_comment():
    assert SSE_KEEPALIVE_FRAME.startswith(":")
    assert SSE_KEEPALIVE_FRAME.endswith("\n\n")
