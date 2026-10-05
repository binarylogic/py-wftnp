"""Bounded, ordered delivery that never blocks the protocol reader."""

import asyncio
import inspect
from collections.abc import AsyncIterator, Awaitable, Callable
from uuid import UUID

from .exceptions import CallbackError, SubscriptionError, SubscriptionOverflow
from .models import Notification

NotificationCallback = Callable[[Notification], Awaitable[None] | None]


class Subscription(AsyncIterator[Notification]):
    """One characteristic subscription. Use a callback OR one async iterator consumer."""

    def __init__(
        self,
        characteristic: UUID,
        callback: NotificationCallback | None,
        buffer_size: int,
        unsubscribe: Callable[["Subscription"], Awaitable[None]],
    ) -> None:
        self.characteristic = characteristic
        self._callback = callback
        self._unsubscribe = unsubscribe
        self._queue: asyncio.Queue[Notification | None] = asyncio.Queue(maxsize=buffer_size)
        self._closed = asyncio.Event()
        self._error: SubscriptionError | None = None
        self._consumer: asyncio.Task[None] | None = None
        if callback is not None:
            self._consumer = asyncio.create_task(self._deliver(), name="wftnp-subscription")

    @property
    def closed(self) -> bool:
        return self._closed.is_set()

    @property
    def error(self) -> SubscriptionError | None:
        return self._error

    async def wait_closed(self) -> None:
        """Wait for termination, raising the recorded failure if any."""
        await self._closed.wait()
        if self._error is not None:
            raise self._error

    def _clear(self) -> None:
        while not self._queue.empty():
            self._queue.get_nowait()

    def _finish(self, error: SubscriptionError | None = None) -> None:
        if self.closed:
            return
        self._error = error
        self._closed.set()
        self._clear()
        self._queue.put_nowait(None)
        if self._consumer is not None and self._consumer is not asyncio.current_task():
            self._consumer.cancel()

    def _push(self, notification: Notification) -> None:
        if self.closed:
            return
        try:
            self._queue.put_nowait(notification)
        except asyncio.QueueFull:
            self._finish(SubscriptionOverflow(f"Notification buffer full for {self.characteristic}"))

    async def _next(self) -> Notification:
        if self.closed:
            if self._error is not None:
                raise self._error
            raise StopAsyncIteration
        value = await self._queue.get()
        if value is None:
            if self._error is not None:
                raise self._error
            raise StopAsyncIteration
        return value

    async def __anext__(self) -> Notification:
        if self._callback is not None:
            raise RuntimeError("A callback subscription cannot also be iterated")
        return await self._next()

    async def _deliver(self) -> None:
        assert self._callback is not None
        try:
            while True:
                value = await self._next()
                result = self._callback(value)
                if inspect.isawaitable(result):
                    await result
        except StopAsyncIteration:
            pass
        except asyncio.CancelledError:
            if not self.closed:
                self._finish(CallbackError("Notification callback was cancelled"))
        except Exception as exc:
            error = CallbackError(f"Notification callback failed: {exc}")
            error.__cause__ = exc
            self._finish(error)

    async def close(self) -> None:
        """Remove local intent even if disconnected; disable on the current peer if possible."""
        self._finish()
        try:
            await self._unsubscribe(self)
        finally:
            await self._wait_consumer()

    async def _wait_consumer(self) -> None:
        if self._consumer is not None and self._consumer is not asyncio.current_task():
            await asyncio.gather(self._consumer, return_exceptions=True)

    async def __aenter__(self) -> "Subscription":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()
