import asyncio
import logging
import weakref
from collections import deque

import fsspec.asyn

logger = logging.getLogger(__name__)

HAS_CPYTHON_API = True


def _fast_slice(src_bytes, offset, read_size):
    """Fast zero-copy slicing using CPython memoryview."""
    if read_size == 0:
        return b""
    if offset < 0 or offset + read_size > len(src_bytes):
        raise ValueError("Slice indices out of bounds")

    return bytes(memoryview(src_bytes)[offset : offset + read_size])


class RunningAverageTracker:
    """Sliding window read tracker for backward-compatibility."""

    def __init__(self, maxlen=10):
        self._history = deque(maxlen=maxlen)
        self._sum = 0

    def add(self, value: int):
        if value <= 0:
            raise ValueError(
                "Internal error, RunningAverageTracker tried inserting negative value"
            )
        if len(self._history) == self._history.maxlen:
            self._sum -= self._history[0]
        self._history.append(value)
        self._sum += value

    @property
    def average(self) -> int:
        count = len(self._history)
        if count == 0:
            return 1024 * 1024
        return self._sum // count

    @property
    def is_variable(self) -> bool:
        if len(self._history) < 2:
            return False
        return len(set(self._history)) > 1

    @property
    def last_value(self) -> int:
        if not self._history:
            raise RuntimeError("No entry found in history")
        return self._history[-1]

    def clear(self):
        self._history.clear()
        self._sum = 0


class BackgroundPrefetcher:
    """Double-Buffered Readahead Engine (Preventing Premature Buffer Eviction)."""

    DEFAULT_INITIAL_PREFETCH = 1 * 1024 * 1024  # 1 MB
    DEFAULT_MAX_PREFETCH = 128 * 1024 * 1024     # 128 MB

    def __init__(
        self, fetcher, size: int, concurrency: int = 1, max_prefetch_size=None, loop=None
    ):
        if max_prefetch_size is not None and max_prefetch_size <= 0:
            raise ValueError(
                "max_prefetch_size should be a positive integer to use adaptive prefetching!"
            )

        self.fetcher = fetcher
        self.size = size
        self.concurrency = concurrency
        self.max_prefetch_size = max_prefetch_size or self.DEFAULT_MAX_PREFETCH
        self.loop = loop
        self.user_offset = 0
        self.is_stopped = False
        self._error = None

        self.read_tracker = RunningAverageTracker(maxlen=10)
        self.current_window = self.DEFAULT_INITIAL_PREFETCH

        # Primary Active Buffer (being read by user)
        self._active_start = 0
        self._active_data = b""

        # Secondary Next Buffer (prefetched asynchronously in background)
        self._next_start = 0
        self._next_data = b""

        self._prefetch_task = None
        self._async_lock = asyncio.Lock()

    def set_error(self, e: Exception):
        logger.error("Global error state set in BackgroundPrefetcher: %s", e)
        self._error = e

    def _trigger_background_prefetch(self, prefetch_start, prefetch_size):
        """Fetches the next block into _next_data WITHOUT touching _active_data."""
        if self.is_stopped or prefetch_start >= self.size or prefetch_size <= 0:
            return

        if self._prefetch_task and not self._prefetch_task.done():
            return

        async def _do_prefetch():
            try:
                fetch_end = min(self.size, prefetch_start + prefetch_size)
                if fetch_end <= prefetch_start:
                    return

                res = await self.fetcher(prefetch_start, fetch_end - prefetch_start)
                if res and not self.is_stopped:
                    async with self._async_lock:
                        # Write to NEXT buffer only (Active buffer remains untouched!)
                        self._next_start = prefetch_start
                        self._next_data = res
            except Exception as e:
                logger.debug("Background prefetch failed gracefully: %s", e)

        try:
            active_loop = self.loop or asyncio.get_running_loop()
            self._prefetch_task = active_loop.create_task(_do_prefetch())
        except RuntimeError:
            pass

    async def afetch(self, start=None, end=None):
        if self.is_stopped:
            raise RuntimeError("Cannot fetch data: BackgroundPrefetcher is stopped or closed.")

        if self._error:
            err = self._error
            self._error = None
            raise err

        # Normalize start and end
        if start is None:
            if end is not None and end <= self.user_offset:
                start = 0
            else:
                start = self.user_offset
        if end is None:
            end = self.size

        start = max(0, start)
        end = min(self.size, max(start, end))
        read_size = end - start

        if read_size == 0 or start >= self.size:
            self.user_offset = start
            return b""

        self.read_tracker.add(read_size)

        async with self._async_lock:
            # 1. Check Primary Active Buffer
            act_len = len(self._active_data)
            act_end = self._active_start + act_len

            if self._active_start <= start and end <= act_end and act_len > 0:
                slice_offset = start - self._active_start
                result = _fast_slice(self._active_data, slice_offset, read_size)
                self.user_offset = end

                # If user cursor approaches end of active buffer, trigger background fetch for NEXT buffer
                if end >= (act_end - (self.current_window // 2)):
                    self._trigger_background_prefetch(act_end, self.current_window)

                return result

            # 2. Check Secondary Next Buffer (Page Flip Opportunity!)
            nxt_len = len(self._next_data)
            nxt_end = self._next_start + nxt_len

            if self._next_start <= start and end <= nxt_end and nxt_len > 0:
                # INSTANT PAGE FLIP: Promote _next_data to _active_data
                self._active_start = self._next_start
                self._active_data = self._next_data
                self._next_data = b""

                slice_offset = start - self._active_start
                result = _fast_slice(self._active_data, slice_offset, read_size)
                self.user_offset = end

                # Double window size for sequential streams
                self.current_window = min(self.max_prefetch_size, self.current_window * 2)

                # Trigger prefetch for the subsequent block
                new_act_end = self._active_start + len(self._active_data)
                self._trigger_background_prefetch(new_act_end, self.current_window)
                return result

            # 3. Cache Miss or Seek Jump: Synchronous Direct Fetch
            if start != self.user_offset:
                self.current_window = self.DEFAULT_INITIAL_PREFETCH  # Reset window on seek

            if self._prefetch_task and not self._prefetch_task.done():
                self._prefetch_task.cancel()

            fetch_size = max(read_size, self.current_window)
            fetch_end = min(self.size, start + fetch_size)

            data = await self.fetcher(start, fetch_end - start)
            self._active_start = start
            self._active_data = data
            self._next_data = b""
            self.user_offset = end

            result = _fast_slice(self._active_data, 0, read_size)

            # Trigger background prefetch into _next_data
            self._trigger_background_prefetch(fetch_end, self.current_window)
            return result

    def fetch(self, start=None, end=None):
        loop = self.loop or fsspec.asyn.get_loop()
        return fsspec.asyn.sync(loop, self.afetch, start, end)

    def close(self):
        self.is_stopped = True
        if self._prefetch_task and not self._prefetch_task.done():
            self._prefetch_task.cancel()
        self._active_data = b""
        self._next_data = b""

    async def aclose(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()
