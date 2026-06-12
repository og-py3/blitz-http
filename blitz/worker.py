"""
blitz.worker — AsyncWorkerPool with dynamic scaling and work-stealing.

Each worker is an asyncio.Task that repeatedly dequeues items from the shared
PriorityWorkQueue and executes them.  The pool monitors queue depth and worker
utilisation to scale the number of workers between min_workers and max_workers.

Work-stealing: each worker maintains a local deque of items it pulled ahead of
time.  When a worker's local deque is empty it first checks the shared queue,
then attempts to steal items from a randomly chosen peer's local deque.
"""

from __future__ import annotations

import asyncio
import collections
import os
import random
import time
from collections import deque
from typing import Awaitable, Callable, Optional

from blitz.queue import PriorityWorkQueue, WorkItem


# Type alias for the async function that actually executes a single request.
RequestExecutor = Callable[[WorkItem], Awaitable[None]]


class WorkerState:
    """Mutable state for a single worker coroutine."""

    def __init__(self, worker_id: int) -> None:
        self.worker_id = worker_id
        self.local_queue: deque[WorkItem] = deque()
        self.busy: bool = False
        self.task: Optional[asyncio.Task] = None
        self.last_idle_at: float = time.monotonic()
        self.items_processed: int = 0


class AsyncWorkerPool:
    """Dynamically-scaled async worker pool with work-stealing.

    Args:
        executor: Async callable that takes a WorkItem and executes the request,
                  setting ``item.future``'s result or exception.
        queue: The shared priority queue to pull work from.
        min_workers: Minimum number of always-running workers.
        max_workers: Upper bound on worker count.
        scale_interval: Seconds between auto-scaling checks.
        idle_timeout: Seconds a worker must be idle before being eligible for removal.
    """

    def __init__(
        self,
        executor: RequestExecutor,
        queue: PriorityWorkQueue,
        min_workers: int = 0,
        max_workers: int = 0,
        scale_interval: float = 1.0,
        idle_timeout: float = 5.0,
    ) -> None:
        cpu_count = os.cpu_count() or 4
        self._executor = executor
        self._queue = queue
        self._min_workers = min_workers or cpu_count
        self._max_workers = max_workers or cpu_count * 16
        self._scale_interval = scale_interval
        self._idle_timeout = idle_timeout

        self._workers: list[WorkerState] = []
        self._running: bool = False
        self._scale_lock: asyncio.Lock = asyncio.Lock()
        self._scaler_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the initial pool of min_workers workers and the scaler."""
        if self._running:
            return
        self._running = True
        for _ in range(self._min_workers):
            await self._spawn_worker()
        self._scaler_task = asyncio.create_task(self._auto_scaler())

    async def stop(self) -> None:
        """Gracefully drain in-flight requests and stop all workers."""
        self._running = False

        # Cancel the scaler first so it doesn't fight us.
        if self._scaler_task and not self._scaler_task.done():
            self._scaler_task.cancel()
            try:
                await self._scaler_task
            except asyncio.CancelledError:
                pass

        # Wait for in-flight work to finish.
        await self._queue.join()

        # Cancel all worker tasks.
        tasks = [w.task for w in self._workers if w.task and not w.task.done()]
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._workers.clear()

    # ------------------------------------------------------------------
    # Worker loop
    # ------------------------------------------------------------------

    async def _worker_loop(self, state: WorkerState) -> None:
        """Main loop for a single worker coroutine."""
        while self._running:
            item = await self._get_work(state)
            if item is None:
                # Nothing to steal, nothing in queue — brief yield.
                await asyncio.sleep(0.001)
                continue

            state.busy = True
            state.items_processed += 1
            try:
                await self._executor(item)
            except Exception as exc:
                # Last-resort guard: the executor should never raise unhandled.
                if not item.future.done():
                    item.future.set_exception(exc)
            finally:
                state.busy = False
                state.last_idle_at = time.monotonic()
                self._queue.task_done()

    async def _get_work(self, state: WorkerState) -> Optional[WorkItem]:
        """Try to get work: local deque → shared queue → steal from peer."""
        # 1. Own local deque (pre-fetched items).
        if state.local_queue:
            return state.local_queue.popleft()

        # 2. Non-blocking check of shared queue.
        item = await self._queue.get_nowait()
        if item is not None:
            return item

        # 3. Work-stealing: try a random peer's local deque.
        if len(self._workers) > 1:
            peers = [w for w in self._workers if w is not state and w.local_queue]
            if peers:
                victim = random.choice(peers)
                if victim.local_queue:
                    try:
                        stolen = victim.local_queue.pop()  # Steal from tail.
                        # Mark as in-flight in the shared queue accounting.
                        self._queue._in_flight += 1
                        return stolen
                    except IndexError:
                        pass

        # 4. Blocking wait on the shared queue (with timeout for responsiveness).
        try:
            item = await asyncio.wait_for(self._queue.get(), timeout=0.05)
            return item
        except (asyncio.TimeoutError, asyncio.CancelledError):
            return None

    # ------------------------------------------------------------------
    # Auto-scaler
    # ------------------------------------------------------------------

    async def _auto_scaler(self) -> None:
        """Periodically check queue depth and scale workers up or down."""
        while self._running:
            await asyncio.sleep(self._scale_interval)
            try:
                await self._scale()
            except Exception:
                pass  # Scaler must never crash the pool.

    async def _scale(self) -> None:
        """Scale the worker count based on current queue depth and utilisation."""
        async with self._scale_lock:
            active = len(self._workers)
            depth = self._queue.depth

            # Scale up: queue is growing faster than workers can drain it.
            if depth > active * 10 and active < self._max_workers:
                new_count = min(active + max(1, active // 2), self._max_workers)
                for _ in range(new_count - active):
                    await self._spawn_worker()
                return

            # Scale down: excess idle workers.
            if active > self._min_workers:
                now = time.monotonic()
                idle_workers = [
                    w for w in self._workers
                    if not w.busy and (now - w.last_idle_at) > self._idle_timeout
                ]
                excess = max(0, active - self._min_workers)
                to_remove = idle_workers[:excess // 2]
                for w in to_remove:
                    await self._remove_worker(w)

    async def _spawn_worker(self) -> WorkerState:
        """Create a new worker coroutine and register it."""
        state = WorkerState(worker_id=len(self._workers))
        task = asyncio.create_task(self._worker_loop(state))
        state.task = task
        self._workers.append(state)
        return state

    async def _remove_worker(self, state: WorkerState) -> None:
        """Cancel a worker task and remove it from the pool."""
        if state.task and not state.task.done():
            state.task.cancel()
            try:
                await state.task
            except (asyncio.CancelledError, Exception):
                pass
        if state in self._workers:
            self._workers.remove(state)

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        """Return a snapshot of pool state."""
        busy = sum(1 for w in self._workers if w.busy)
        total = len(self._workers)
        return {
            "workers_total": total,
            "workers_busy": busy,
            "workers_idle": total - busy,
            "queue_depth": self._queue.depth,
            "queue_in_flight": self._queue.in_flight,
            "utilisation_pct": round((busy / total * 100) if total else 0, 1),
        }
