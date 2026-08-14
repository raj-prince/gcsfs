"""
Zero-Copy Recyclable Slab Prefetcher for GCSFS.

Provides:
- Slab: Individual recyclable memoryview / bytes buffer slot.
- SlabPool: Fixed ring buffer pool of recyclable Slabs.
- ZeroCopySlabPrefetcher: High-throughput prefetcher with zero-copy slicing and synchronous fast-path.
"""

from __future__ import annotations

import asyncio
import logging
import fsspec.asyn

logger = logging.getLogger(__name__)


class Slab:
    """Represents a recyclable slab staged with direct memoryviews or raw bytes."""
    __slots__ = (
        "index",
        "slab_size",
        "_buffer",
        "_master_view",
        "raw_bytes",
        "raw_offset",
        "offset",
        "valid_size",
        "ready_event",
        "error",
        "task",
    )

    def __init__(self, index: int, slab_size: int):
        self.index = index
        self.slab_size = slab_size
        self._buffer: bytearray | None = None
        self._master_view: memoryview | None = None
        self.raw_bytes: bytes | None = None
        self.raw_offset: int = 0
        self.offset = -1
        self.valid_size = 0
        self.ready_event = asyncio.Event()
        self.error: Exception | None = None
        self.task: asyncio.Task | None = None

    @property
    def master_view(self) -> memoryview:
        if self._master_view is None:
            self._buffer = bytearray(self.slab_size)
            self._master_view = memoryview(self._buffer)
        return self._master_view

    def reset(self, offset: int):
        self.offset = offset
        self.valid_size = 0
        self.ready_event.clear()
        self.raw_bytes = None
        self.raw_offset = 0
        self.error = None
        if self.task and not self.task.done():
            self.task.cancel()
        self.task = None

    def slice_data(self, rel_start: int, rel_end: int) -> bytes:
        """Returns bytes with minimum memory copy."""
        if self.raw_bytes is not None:
            return self.raw_bytes[self.raw_offset + rel_start : self.raw_offset + rel_end]
        return bytes(self.master_view[rel_start:rel_end])

    def slice_view(self, rel_start: int, rel_end: int) -> memoryview:
        """Returns a non-copying memoryview slice."""
        if self.raw_bytes is not None:
            return memoryview(self.raw_bytes)[self.raw_offset + rel_start : self.raw_offset + rel_end]
        return self.master_view[rel_start:rel_end]


class SlabPool:
    """Fixed pool of recyclable slabs that eliminates runtime memory allocations."""
    def __init__(self, num_slabs: int, slab_size: int, loop: asyncio.AbstractEventLoop):
        self.num_slabs = num_slabs
        self.slab_size = slab_size
        self.loop = loop
        self.slabs = [Slab(i, slab_size) for i in range(num_slabs)]
        self.free_queue = asyncio.Queue()
        for slab in self.slabs:
            self.free_queue.put_nowait(slab)

    async def acquire(self, offset: int) -> Slab:
        slab: Slab = await self.free_queue.get()
        slab.reset(offset)
        return slab

    def acquire_nowait(self, offset: int) -> Slab | None:
        try:
            slab = self.free_queue.get_nowait()
            slab.reset(offset)
            return slab
        except asyncio.QueueEmpty:
            return None

    def release(self, slab: Slab):
        slab.reset(-1)
        self.free_queue.put_nowait(slab)


class ZeroCopySlabPrefetcher:
    """
    Fixed-Slab Zero-Copy Prefetcher using memoryview for intermediate staging.

    Features:
    1. Pre-allocates a fixed ring buffer pool of recyclable Slabs (`bytearray` + `memoryview`).
    2. Zero heap allocations during sequential streaming: slabs are recycled back to free pool once read.
    3. Intermediate staging via `memoryview`: downloads write directly into pre-allocated memory slices.
    4. Fast synchronous direct slicing: avoids `asyn.sync` thread context switches for in-RAM slabs.
    5. Bounded in-flight lookahead window: prevents memory over-allocation on large read requests.
    6. Instant-hit caching for seeks within prefetched slab ranges.
    7. Dynamic streak-based lookahead scaling ($1 \to 2 \to 4 \to 8$ slabs).
    """
    MIN_SLAB_SIZE = 16 * 1024 * 1024  # 16 MB network request floor for TCP saturation

    def __init__(
        self,
        fetcher,
        size: int,
        slab_size: int = 16 * 1024 * 1024,
        num_slabs: int | None = None,
        max_prefetch_bytes: int = 128 * 1024 * 1024,
        concurrency: int = 1,
        loop: asyncio.AbstractEventLoop | None = None,
    ):
        self.fetcher = fetcher
        self.size = size
        self.slab_size = max(self.MIN_SLAB_SIZE, slab_size)
        self.max_prefetch_bytes = max(self.slab_size, max_prefetch_bytes)
        if num_slabs is None:
            self.num_slabs = max(2, min(8, self.max_prefetch_bytes // self.slab_size))
        else:
            self.num_slabs = max(1, num_slabs)
        self.concurrency = max(1, concurrency)
        self.loop = loop or asyncio.get_running_loop()
        self.slab_pool = SlabPool(self.num_slabs, self.slab_size, self.loop)

        # Map slab start offset -> active Slab object
        self.active_slabs: dict[int, Slab] = {}
        self.current_user_offset = 0
        self.streak = 0
        self.last_read_end = -1
        self._last_scheduled_slab = -1
        self._lookahead_slabs = 2
        self.is_stopped = False
        self._async_lock = asyncio.Lock()

        # Warm initial prefetch window immediately in event loop
        if self.loop.is_running():
            self.loop.call_soon_threadsafe(self._ensure_prefetch_window, 0)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()

    def _update_streak(self, start: int, end: int):
        """Adapts lookahead window using Linux kernel readahead doubling (mm/readahead.c)."""
        req_slabs = max(1, (end - start + self.slab_size - 1) // self.slab_size)
        if self.last_read_end != -1 and start == self.last_read_end:
            self.streak += 1
            # Kernel exponential doubling: 2 -> 4 -> 8 (capped at num_slabs)
            self._lookahead_slabs = min(self.num_slabs, max(req_slabs + 1, self._lookahead_slabs * 2))
        else:
            # Random seek: reset to initial lookahead
            self.streak = 1
            self._lookahead_slabs = min(self.num_slabs, req_slabs + 1)
            self._last_scheduled_slab = -1
        self.last_read_end = end

    def _get_lookahead_slabs(self) -> int:
        return self._lookahead_slabs

    async def _fill_slab_parallel(self, slab: Slab):
        """Downloads a chunk directly into slab.master_view with zero intermediate byte concatenation."""
        try:
            chunk_start = slab.offset
            chunk_len = min(self.slab_size, self.size - chunk_start)

            if chunk_len <= 0:
                slab.valid_size = 0
                slab.ready_event.set()
                return

            if self.concurrency <= 1:
                data = await self.fetcher(chunk_start, chunk_len, split_factor=1)
                data_len = len(data)
                if isinstance(data, (bytes, bytearray)):
                    slab.raw_bytes = data
                else:
                    slab.master_view[:data_len] = data
                slab.valid_size = data_len
                slab.ready_event.set()
                return

            sub_size = (chunk_len + self.concurrency - 1) // self.concurrency
            sub_tasks = []

            async def _download_sub(sub_rel_offset: int, sub_len: int, sub_view: memoryview):
                data = await self.fetcher(chunk_start + sub_rel_offset, sub_len, split_factor=1)
                sub_view[: len(data)] = data

            for i in range(self.concurrency):
                rel_offset = i * sub_size
                if rel_offset >= chunk_len:
                    break
                actual_sub_len = min(sub_size, chunk_len - rel_offset)
                sub_view = slab.master_view[rel_offset : rel_offset + actual_sub_len]
                sub_tasks.append(
                    self.loop.create_task(_download_sub(rel_offset, actual_sub_len, sub_view))
                )

            await asyncio.gather(*sub_tasks)
            slab.valid_size = chunk_len
            slab.ready_event.set()
        except asyncio.CancelledError:
            slab.valid_size = 0
            slab.ready_event.set()
            raise
        except Exception as e:
            slab.error = e
            slab.valid_size = 0
            slab.ready_event.set()

    def _evict_out_of_window_slabs(self, window_start: int, window_end: int):
        """Evicts and recycles slabs outside the prefetch active window."""
        to_evict = []
        for offset, slab in self.active_slabs.items():
            if offset < window_start or offset >= window_end:
                to_evict.append(offset)
        for offset in to_evict:
            slab = self.active_slabs.pop(offset)
            self.slab_pool.release(slab)

    def _ensure_prefetch_window(self, user_start: int):
        """Spawns parallel slab downloads scaled dynamically by sequential streak."""
        if self.is_stopped:
            return

        lookahead_slabs = self._get_lookahead_slabs()
        aligned_user_start = (user_start // self.slab_size) * self.slab_size
        active_window_end = aligned_user_start + (self.num_slabs * self.slab_size)

        # Evict slabs outside active window
        self._evict_out_of_window_slabs(aligned_user_start, active_window_end)

        for i in range(lookahead_slabs):
            target_offset = aligned_user_start + (i * self.slab_size)
            if target_offset >= self.size:
                break
            if target_offset not in self.active_slabs:
                slab = self.slab_pool.acquire_nowait(target_offset)
                if slab is None:
                    break
                self.active_slabs[target_offset] = slab
                slab.task = self.loop.create_task(self._fill_slab_parallel(slab))

    async def _async_fetch(self, start: int, end: int) -> bytes:
        if self.is_stopped:
            raise RuntimeError("The file instance has been closed.")

        start = max(0, start)
        end = min(self.size, max(start, end))
        if start >= end:
            return b""

        self._update_streak(start, end)
        aligned_start = (start // self.slab_size) * self.slab_size
        slab = self.active_slabs.get(aligned_start)
        if slab is not None and slab.ready_event.is_set() and not slab.error:
            rel_start = start - slab.offset
            avail = slab.valid_size - rel_start
            req_len = end - start
            if avail >= req_len:
                data = slab.slice_data(rel_start, rel_start + req_len)
                self.current_user_offset = end
                if aligned_start > self._last_scheduled_slab and rel_start + req_len >= (slab.valid_size // 2):
                    self._last_scheduled_slab = aligned_start
                    self._ensure_prefetch_window(end)
                return data

            next_aligned = aligned_start + self.slab_size
            next_slab = self.active_slabs.get(next_aligned)
            if next_slab is not None and next_slab.ready_event.is_set() and not next_slab.error:
                part1 = slab.slice_data(rel_start, slab.valid_size)
                part2_len = req_len - len(part1)
                if next_slab.valid_size >= part2_len:
                    part2 = next_slab.slice_data(0, part2_len)
                    self.current_user_offset = end
                    if next_aligned > self._last_scheduled_slab:
                        self._last_scheduled_slab = next_aligned
                        self._ensure_prefetch_window(end)
                    return part1 + part2

        async with self._async_lock:
            self._ensure_prefetch_window(start)

            curr = start
            out_chunks: list[bytes] = []

            while curr < end:
                aligned_start = (curr // self.slab_size) * self.slab_size
                if aligned_start not in self.active_slabs:
                    # Make room if needed
                    window_end = aligned_start + (self.num_slabs * self.slab_size)
                    self._evict_out_of_window_slabs(aligned_start, window_end)
                    slab = await self.slab_pool.acquire(aligned_start)
                    self.active_slabs[aligned_start] = slab
                    slab.task = self.loop.create_task(self._fill_slab_parallel(slab))

                slab = self.active_slabs[aligned_start]
                await slab.ready_event.wait()
                if slab.error:
                    raise slab.error

                rel_start = curr - slab.offset
                avail = slab.valid_size - rel_start
                if avail <= 0:
                    break

                take = min(avail, end - curr)
                rel_end = rel_start + take
                out_chunks.append(slab.slice_data(rel_start, rel_end))
                curr += take

            self.current_user_offset = curr
            self._ensure_prefetch_window(curr)

            if not out_chunks:
                return b""
            if len(out_chunks) == 1:
                return out_chunks[0]

            return b"".join(out_chunks)

    async def afetch(self, start: int | None, end: int | None) -> bytes:
        if start is None:
            start = 0
        if end is None:
            end = self.size
        return await self._async_fetch(start, end)

    def fetch(self, start: int | None, end: int | None) -> bytes:
        if start is None:
            start = 0
        if end is None:
            end = self.size

        end = min(end, self.size)
        if start >= self.size or start >= end:
            return b""

        self._update_streak(start, end)
        if not self.is_stopped:
            aligned_start = (start // self.slab_size) * self.slab_size
            slab = self.active_slabs.get(aligned_start)
            if slab is not None and slab.ready_event.is_set() and not slab.error:
                rel_start = start - slab.offset
                avail = slab.valid_size - rel_start
                req_len = end - start
                if avail >= req_len:
                    data = slab.slice_data(rel_start, rel_start + req_len)
                    self.current_user_offset = end
                    if aligned_start > self._last_scheduled_slab and rel_start + req_len >= (slab.valid_size // 2):
                        self._last_scheduled_slab = aligned_start
                        self.loop.call_soon_threadsafe(self._ensure_prefetch_window, end)
                    return data

                next_aligned = aligned_start + self.slab_size
                next_slab = self.active_slabs.get(next_aligned)
                if next_slab is not None and next_slab.ready_event.is_set() and not next_slab.error:
                    part1 = slab.slice_data(rel_start, slab.valid_size)
                    part2_len = req_len - len(part1)
                    if next_slab.valid_size >= part2_len:
                        part2 = next_slab.slice_data(0, part2_len)
                        self.current_user_offset = end
                        if next_aligned > self._last_scheduled_slab:
                            self._last_scheduled_slab = next_aligned
                            self.loop.call_soon_threadsafe(self._ensure_prefetch_window, end)
                        return part1 + part2

        return fsspec.asyn.sync(self.loop, self.afetch, start, end)

    async def aclose(self):
        if self.is_stopped:
            return
        self.is_stopped = True
        for slab in list(self.active_slabs.values()):
            if slab.task and not slab.task.done():
                slab.task.cancel()
            self.slab_pool.release(slab)
        self.active_slabs.clear()

    def close(self):
        fsspec.asyn.sync(self.loop, self.aclose)
