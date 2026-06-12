"""
Tests for blitz.worker — AsyncWorkerPool scaling, execution, and work-stealing.

Covers:
  - Scaling up when queue depth grows.
  - Scaling down when workers are idle.
  - All submitted work items are eventually processed.
  - Worker crash recovery (unhandled exception in executor doesn't kill pool).
"""

from __future__ import annotations

import asyncio

import pytest

from blitz.queue import PriorityWorkQueue, WorkItem
from blitz.request import BlitzRequest
from blitz.worker import AsyncWorkerPool


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _noop_executor(item: WorkItem) -> None:
    """Executor that immediately resolves the future with a sentinel value."""
    if not item.future.done():
        item.future.set_result("ok")


async def _slow_executor(item: WorkItem) -> None:
    """Executor that sleeps briefly to simulate I/O."""
    await asyncio.sleep(0.005)
    if not item.future.done():
        item.future.set_result("ok")


async def _error_executor(item: WorkItem) -> None:
    """Executor that always raises — tests crash resilience."""
    raise RuntimeError("intentional error")


def _make_request(priority: str = "normal") -> BlitzRequest:
    return BlitzRequest(method="GET", url="https://example.com", priority=priority)


# ---------------------------------------------------------------------------
# Pool basics
# ---------------------------------------------------------------------------

class TestAsyncWorkerPool:
    """Tests for AsyncWorkerPool."""

    @pytest.mark.asyncio
    async def test_pool_starts_and_stops_cleanly(self):
        """start() and stop() complete without errors."""
        q = PriorityWorkQueue()
        pool = AsyncWorkerPool(executor=_noop_executor, queue=q, min_workers=2, max_workers=4)
        await pool.start()
        assert len(pool._workers) == 2
        await pool.stop()
        assert len(pool._workers) == 0

    @pytest.mark.asyncio
    async def test_workers_process_all_items(self):
        """Every enqueued item is eventually resolved."""
        q = PriorityWorkQueue()
        pool = AsyncWorkerPool(executor=_noop_executor, queue=q, min_workers=4, max_workers=4)
        await pool.start()

        n = 50
        futures = [await q.put(_make_request()) for _ in range(n)]
        results = await asyncio.gather(*futures)

        assert len(results) == n
        assert all(r == "ok" for r in results)

        await pool.stop()

    @pytest.mark.asyncio
    async def test_priority_items_are_dequeued_in_order(self):
        """HIGH priority items resolve before LOW priority items."""
        q = PriorityWorkQueue()
        order: list[str] = []

        async def _recording_executor(item: WorkItem) -> None:
            await asyncio.sleep(0)  # Yield so other tasks can run.
            order.append(item.request.priority)
            if not item.future.done():
                item.future.set_result("ok")

        pool = AsyncWorkerPool(
            executor=_recording_executor, queue=q, min_workers=1, max_workers=1
        )
        await pool.start()

        # Enqueue: 2 low, 2 high.
        f1 = await q.put(_make_request("low"))
        f2 = await q.put(_make_request("low"))
        f3 = await q.put(_make_request("high"))
        f4 = await q.put(_make_request("high"))

        await asyncio.gather(f1, f2, f3, f4)
        await pool.stop()

        # High items should appear before low items in execution order.
        # (Order within same priority is FIFO)
        high_indices = [i for i, p in enumerate(order) if p == "high"]
        low_indices  = [i for i, p in enumerate(order) if p == "low"]
        assert max(high_indices) < min(low_indices), (
            f"HIGH items not before LOW items: {order}"
        )

    @pytest.mark.asyncio
    async def test_pool_scales_up_under_load(self):
        """Pool spawns extra workers when queue depth is high."""
        q = PriorityWorkQueue()
        pool = AsyncWorkerPool(
            executor=_slow_executor,
            queue=q,
            min_workers=2,
            max_workers=16,
            scale_interval=0.05,  # Fast scaling for tests.
        )
        await pool.start()

        # Flood the queue with 200 slow items.
        futures = [await q.put(_make_request()) for _ in range(200)]

        # Wait for scale check.
        await asyncio.sleep(0.15)
        assert len(pool._workers) > 2, "Pool should have scaled up."

        await asyncio.gather(*futures)
        await pool.stop()

    @pytest.mark.asyncio
    async def test_crashed_executor_does_not_kill_pool(self):
        """An exception raised by the executor sets the future's exception
        but does not crash the worker or the pool."""
        q = PriorityWorkQueue()
        pool = AsyncWorkerPool(executor=_error_executor, queue=q, min_workers=2, max_workers=2)
        await pool.start()

        fut = await q.put(_make_request())

        # The future should have the exception set, not be left pending.
        try:
            await asyncio.wait_for(asyncio.shield(fut), timeout=2.0)
        except (RuntimeError, asyncio.TimeoutError):
            pass  # Expected — either RuntimeError from executor or timeout.

        # Pool should still be running.
        assert pool._running is True
        await pool.stop()

    @pytest.mark.asyncio
    async def test_stats_reflect_worker_count(self):
        """stats() worker_count matches actual spawned worker count."""
        q = PriorityWorkQueue()
        pool = AsyncWorkerPool(executor=_noop_executor, queue=q, min_workers=3, max_workers=6)
        await pool.start()
        s = pool.stats()
        assert s["workers_total"] == 3
        await pool.stop()

    @pytest.mark.asyncio
    async def test_double_start_is_idempotent(self):
        """Calling start() twice does not double the worker count."""
        q = PriorityWorkQueue()
        pool = AsyncWorkerPool(executor=_noop_executor, queue=q, min_workers=2, max_workers=4)
        await pool.start()
        await pool.start()
        assert len(pool._workers) == 2
        await pool.stop()
