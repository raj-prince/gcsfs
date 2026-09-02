import asyncio
from unittest import mock

import fsspec.asyn
import pytest

from gcsfs.prefetcher import BackgroundPrefetcher, RunningAverageTracker, _fast_slice


@pytest.fixture
def prefetcher_factory():
    prefetchers = []

    def _make_prefetcher(**kwargs):
        if "loop" not in kwargs:
            kwargs["loop"] = fsspec.asyn.get_loop()

        bp = BackgroundPrefetcher(**kwargs)
        prefetchers.append(bp)
        return bp

    yield _make_prefetcher

    for bp in prefetchers:
        bp.is_stopped = False
        bp.close()


class MockFetcher:
    def __init__(self, data, fail_at_call=None, hang_at_call=None):
        self.data = data
        self.calls = []
        self.fail_at_call = fail_at_call
        self.hang_at_call = hang_at_call
        self.call_count = 0

    async def __call__(self, start, size, split_factor=1):
        self.call_count += 1
        self.calls.append({"start": start, "size": size, "split_factor": split_factor})

        await asyncio.sleep(0.001)

        if self.hang_at_call is not None and self.call_count >= self.hang_at_call:
            await asyncio.sleep(1000)

        if self.fail_at_call is not None and self.call_count >= self.fail_at_call:
            raise OSError("Simulated Network Timeout")

        return self.data[start : start + size]


def test_fast_slice_direct():
    src = b"0123456789"
    assert _fast_slice(src, 2, 4) == b"2345"
    assert _fast_slice(src, 5, 0) == b""
    assert _fast_slice(src, 0, 10) == b"0123456789"


@mock.patch("gcsfs.prefetcher.HAS_CPYTHON_API", False)
def test_fast_slice_pypy_fallback():
    src = b"0123456789_pypy_fallback_test"
    assert _fast_slice(src, 11, 13) == b"pypy_fallback"
    assert _fast_slice(src, 5, 0) == b""


def test_running_average_tracker():
    tracker = RunningAverageTracker(maxlen=3)
    assert tracker.average == 1024 * 1024

    tracker.add(512)
    tracker.add(512)
    assert tracker.average == 512

    tracker.add(2048)
    assert tracker.average == 1024

    tracker.clear()
    assert tracker.average == 1024 * 1024


def test_max_prefetch_size_property(prefetcher_factory):
    bp1 = prefetcher_factory(fetcher=MockFetcher(b""), size=10000, concurrency=4)
    assert bp1.max_prefetch_size == bp1.DEFAULT_MAX_PREFETCH

    bp2 = prefetcher_factory(fetcher=MockFetcher(b""), size=1000000000, max_prefetch_size=200 * 1024 * 1024)
    assert bp2.max_prefetch_size == 200 * 1024 * 1024


def test_sequential_read_spanning_blocks(prefetcher_factory):
    data = b"A" * 100 + b"B" * 100 + b"C" * 100
    fetcher = MockFetcher(data)
    bp = prefetcher_factory(fetcher=fetcher, size=300, concurrency=4)

    assert bp.fetch(0, 100) == b"A" * 100
    assert bp.fetch(100, 150) == b"B" * 50
    assert bp.fetch(150, 250) == b"B" * 50 + b"C" * 50
    assert bp.fetch(250, 300) == b"C" * 50
    assert bp.fetch(300, 310) == b""


def test_fetch_default_args_and_out_of_bounds(prefetcher_factory):
    fetcher = MockFetcher(b"12345")
    bp = prefetcher_factory(fetcher=fetcher, size=5, concurrency=4)

    assert bp.fetch(None, None) == b"12345"
    assert bp.fetch(None, 2) == b"12"
    assert bp.fetch(5, 10) == b""
    assert bp.fetch(10, 20) == b""
    assert bp.fetch(2, 2) == b""
    assert bp.fetch(4, 2) == b""


def test_seek_logic(prefetcher_factory):
    data = b"0123456789" * 10
    fetcher = MockFetcher(data)
    bp = prefetcher_factory(fetcher=fetcher, size=100, concurrency=4)

    assert bp.fetch(0, 10) == data[0:10]
    assert bp.fetch(10, 20) == data[10:20]
    assert bp.user_offset == 20
    assert bp.fetch(50, 60) == data[50:60]
    assert bp.user_offset == 60
    assert bp.fetch(10, 20) == data[10:20]
    assert bp.user_offset == 20


def test_error_state_handling(prefetcher_factory):
    bp = prefetcher_factory(fetcher=MockFetcher(b"X" * 100), size=100, concurrency=4)
    bp.set_error(ValueError("Injected Prefetcher Error"))

    with pytest.raises(ValueError, match="Injected Prefetcher Error"):
        bp.fetch(0, 50)


@pytest.mark.asyncio
async def test_async_context_manager():
    data = b"Hello, World!"
    fetcher = MockFetcher(data)
    async with BackgroundPrefetcher(fetcher=fetcher, size=len(data)) as bp:
        res = await bp.afetch(0, 5)
        assert res == b"Hello"
        res_all = await bp.afetch(0, None)
        assert res_all == b"Hello, World!"


@pytest.mark.asyncio
async def test_double_buffering_page_flip(prefetcher_factory):
    data = b"0123456789" * 100
    fetcher = MockFetcher(data)
    bp = prefetcher_factory(fetcher=fetcher, size=1000, concurrency=1)

    # Initial read fills active_data with first 1MB (or total size 1000)
    res1 = await bp.afetch(0, 100)
    assert res1 == data[0:100]

    # Verify active buffer matches range [0, 1000]
    assert bp._active_start == 0
    assert len(bp._active_data) == 1000
