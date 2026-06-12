"""
blitz.queue — Async priority work queue supporting HIGH / NORMAL / LOW priorities.

The queue wraps asyncio.PriorityQueue and exposes a typed interface so the
worker pool does not need to know about the underlying priority integers.
Items are dequeued in priority order (HIGH first), with FIFO ordering within
each priority tier.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Optional

from blitz.request import BlitzRequest, PRIORITY_HIGH, PRIORITY_NORMAL, PRIORITY_LOW


@dataclass(order=True)
class WorkItem:
    """An item placed on the priority queue.

    The dataclass ``order=True`` means comparison uses the field order, so
    ``priority_int`` is compared first, then ``seq`` (arrival order), ensuring
    FIFO within each priority tier.

    Attributes:
        priority_int: Numeric priority (lower = higher priority).
        seq: Monotonically increasing arrival counter for FIFO tie-breaking.
        request: The BlitzRequest to execute (not compared).
        future: asyncio.Future that receives the result (not compared).
    """

    priority_int: int
    seq: int
    request: BlitzRequest = field(compare=False)
    future: asyncio.Future = field(compare=False)


class PriorityWorkQueue:
    """Async priority queue for BlitzRequest work items.

    Internally uses an ``asyncio.PriorityQueue`` keyed on ``(priority_int, seq)``
    so that all HIGH-priority items are dequeued before NORMAL, and all NORMAL
    before LOW, with FIFO ordering within each tier.

    Args:
        maxsize: Maximum number of items allowed in the queue (0 = unlimited).
    """

    def __init__(self, maxsize: int = 0) -> None:
        self._q: asyncio.PriorityQueue[WorkItem] = asyncio.PriorityQueue(maxsize=maxsize)
        self._seq: int = 0
        self._in_flight: int = 0

    # ------------------------------------------------------------------
    # Producer API
    # ------------------------------------------------------------------

    async def put(self, request: BlitzRequest) -> asyncio.Future:
        """Enqueue a request and return a Future that resolves to the response.

        The caller should ``await`` the returned Future to get the result.

        Args:
            request: The request to enqueue.

        Returns:
            An asyncio.Future that will hold the BlitzResponse when complete.
        """
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        seq = self._seq
        self._seq += 1
        item = WorkItem(
            priority_int=request._priority_int,
            seq=seq,
            request=request,
            future=fut,
        )
        await self._q.put(item)
        return fut

    def put_nowait(self, request: BlitzRequest) -> asyncio.Future:
        """Non-blocking enqueue.  Raises QueueFull if the queue is at maxsize.

        Args:
            request: The request to enqueue.

        Returns:
            An asyncio.Future that will hold the BlitzResponse when complete.

        Raises:
            asyncio.QueueFull: If the queue is full.
        """
        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        seq = self._seq
        self._seq += 1
        item = WorkItem(
            priority_int=request._priority_int,
            seq=seq,
            request=request,
            future=fut,
        )
        self._q.put_nowait(item)
        return fut

    # ------------------------------------------------------------------
    # Consumer API
    # ------------------------------------------------------------------

    async def get(self) -> WorkItem:
        """Dequeue the highest-priority WorkItem, blocking if the queue is empty.

        Returns:
            The next WorkItem to process.
        """
        item = await self._q.get()
        self._in_flight += 1
        return item

    async def get_nowait(self) -> Optional[WorkItem]:
        """Dequeue without blocking; returns None if the queue is empty."""
        try:
            item = self._q.get_nowait()
            self._in_flight += 1
            return item
        except asyncio.QueueEmpty:
            return None

    def task_done(self) -> None:
        """Mark a previously dequeued item as processed.

        Must be called by the consumer after handling each item.
        """
        self._q.task_done()
        self._in_flight = max(0, self._in_flight - 1)

    async def join(self) -> None:
        """Block until all items have been processed (task_done called for each)."""
        await self._q.join()

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def depth(self) -> int:
        """Current number of items waiting in the queue (not yet dequeued)."""
        return self._q.qsize()

    @property
    def in_flight(self) -> int:
        """Number of items that have been dequeued but not yet task_done'd."""
        return self._in_flight

    @property
    def total_pending(self) -> int:
        """Total items: queued + in-flight."""
        return self.depth + self._in_flight

    def __repr__(self) -> str:
        return (
            f"PriorityWorkQueue(depth={self.depth}, in_flight={self._in_flight})"
        )
