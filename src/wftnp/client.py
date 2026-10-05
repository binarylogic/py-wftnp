"""Public client and the sole owner of reconnect/subscription recovery policy."""

import asyncio
import contextlib
import logging
import math
import random
import time
from collections.abc import Callable
from functools import partial
from uuid import UUID

from .connection import Connection
from .exceptions import NotConnected, OperationRejected, SubscriptionError, WftnpError
from .models import Characteristic, ClientState, Endpoint, Notification, Service
from .protocol import Opcode, decode_characteristics, decode_services, decode_value
from .subscription import NotificationCallback, Subscription

_LOGGER = logging.getLogger(__name__)
StateListener = Callable[[ClientState], None]


def _positive(name: str, value: float) -> None:
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be finite and positive")


class WftnpClient:
    """A long-lived async WFTNP client. Use from a single event loop."""

    def __init__(
        self,
        endpoint: Endpoint,
        *,
        connect_timeout: float = 5.0,
        request_timeout: float = 5.0,
        close_timeout: float = 2.0,
        heartbeat_interval: float | None = 20.0,
        reconnect_initial: float = 1.0,
        reconnect_max: float = 30.0,
        auto_reconnect: bool = True,
    ) -> None:
        for name, value in (
            ("connect_timeout", connect_timeout),
            ("request_timeout", request_timeout),
            ("close_timeout", close_timeout),
            ("reconnect_initial", reconnect_initial),
            ("reconnect_max", reconnect_max),
        ):
            _positive(name, value)
        if heartbeat_interval is not None:
            _positive("heartbeat_interval", heartbeat_interval)
        if reconnect_initial > reconnect_max:
            raise ValueError("reconnect_initial must not exceed reconnect_max")
        self.endpoint = endpoint
        self._connect_timeout = connect_timeout
        self._request_timeout = request_timeout
        self._close_timeout = close_timeout
        self._heartbeat_interval = heartbeat_interval
        self._reconnect_initial = reconnect_initial
        self._reconnect_max = reconnect_max
        self._auto_reconnect = auto_reconnect
        self._state = ClientState.STOPPED
        self._changed = asyncio.Event()
        self._listeners: set[StateListener] = set()
        self._lifecycle_lock = asyncio.Lock()
        self._subscription_lock = asyncio.Lock()
        self._connection: Connection | None = None
        self._connection_id = 0
        self._task: asyncio.Task[None] | None = None
        self._subscriptions: dict[UUID, Subscription] = {}
        self._running = False
        self._last_error: WftnpError | None = None

    @property
    def state(self) -> ClientState:
        return self._state

    @property
    def connection_id(self) -> int:
        """Monotonically increasing identity for each established socket."""
        return self._connection_id

    @property
    def last_error(self) -> WftnpError | None:
        return self._last_error

    def add_state_listener(self, listener: StateListener) -> Callable[[], None]:
        """Register a brief synchronous listener; return its removal function."""
        self._listeners.add(listener)
        return lambda: self._listeners.discard(listener)

    def _set_state(self, state: ClientState) -> None:
        if state == self._state:
            return
        self._state = state
        previous = self._changed
        self._changed = asyncio.Event()
        previous.set()
        for listener in tuple(self._listeners):
            try:
                listener(state)
            except Exception:
                _LOGGER.exception("WFTNP state listener failed")

    async def start(self) -> None:
        """Start the connection supervisor; does not wait for a device to be online."""
        async with self._lifecycle_lock:
            if self._task is not None and not self._task.done():
                return
            self._running = True
            self._last_error = None
            self._set_state(ClientState.CONNECTING)
            self._task = asyncio.create_task(self._supervise(), name="wftnp-client")

    async def wait_ready(self, timeout: float = 10.0) -> None:
        """Wait for a usable connection; timeout does not stop background recovery."""
        _positive("timeout", timeout)
        async with asyncio.timeout(timeout):
            while (
                self._state != ClientState.READY
                or self._connection is None
                or self._connection.closed.is_set()
            ):
                if not self._running or self._state == ClientState.STOPPED:
                    raise NotConnected("Client is stopped") from self._last_error
                changed = self._changed
                await changed.wait()

    async def stop(self) -> None:
        """Finish cleanup before propagating cancellation of the stopping caller."""
        async with self._lifecycle_lock:
            self._running = False
            self._set_state(ClientState.STOPPED)
            subscriptions = tuple(self._subscriptions.values())
            self._subscriptions.clear()
            for subscription in subscriptions:
                subscription._finish()
            caller = asyncio.current_task()

            async def cleanup() -> None:
                if self._task is not None:
                    self._task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await self._task
                    self._task = None
                for subscription in subscriptions:
                    await subscription._wait_consumer(exclude=caller)

            # A callback may itself stop the client. Do not cancel/join that caller.
            task = asyncio.create_task(cleanup(), name="wftnp-cleanup")
            cancelled = False
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    cancelled = True
            task.result()
            if cancelled:
                raise asyncio.CancelledError

    async def __aenter__(self) -> "WftnpClient":
        await self.start()
        try:
            await self.wait_ready()
        except BaseException:
            await self.stop()
            raise
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.stop()

    def _ready_connection(self) -> Connection:
        connection = self._connection
        if self._state != ClientState.READY or connection is None or connection.closed.is_set():
            raise NotConnected("Client is not ready; use wait_ready() explicitly")
        return connection

    async def discover_services(self) -> tuple[Service, ...]:
        response = await self._ready_connection().request(Opcode.SERVICES, b"", timeout=self._request_timeout)
        return decode_services(response.payload)

    async def discover_characteristics(self, service: UUID) -> tuple[Characteristic, ...]:
        response = await self._ready_connection().request(
            Opcode.CHARACTERISTICS, service.bytes, timeout=self._request_timeout
        )
        return decode_characteristics(response.payload, service)

    async def read_characteristic(self, characteristic: UUID) -> bytes:
        response = await self._ready_connection().request(
            Opcode.READ, characteristic.bytes, timeout=self._request_timeout
        )
        return decode_value(response.payload, characteristic)

    async def write_characteristic(self, characteristic: UUID, value: bytes) -> None:
        """Write opaque bytes once. A lost response does not imply the write failed."""
        await self._ready_connection().request(
            Opcode.WRITE, characteristic.bytes + value, timeout=self._request_timeout
        )

    async def subscribe(
        self,
        characteristic: UUID,
        callback: NotificationCallback | None = None,
        *,
        buffer_size: int = 64,
    ) -> Subscription:
        """Subscribe once per UUID; omit callback to consume with async iteration."""
        if buffer_size < 1:
            raise ValueError("buffer_size must be positive")
        async with self._subscription_lock:
            connection = self._ready_connection()
            existing = self._subscriptions.get(characteristic)
            if existing is not None and not existing.closed:
                raise ValueError(f"Already subscribed to {characteristic}")
            subscription = Subscription(characteristic, callback, buffer_size, self._unsubscribe)
            self._subscriptions[characteristic] = subscription
            try:
                await connection.request(
                    Opcode.SUBSCRIBE, characteristic.bytes + b"\x01", timeout=self._request_timeout
                )
            except BaseException:
                self._subscriptions.pop(characteristic, None)
                subscription._finish()
                await subscription._wait_consumer()
                raise
            return subscription

    async def _unsubscribe(self, subscription: Subscription) -> None:
        characteristic = subscription.characteristic
        async with self._subscription_lock:
            if self._subscriptions.get(characteristic) is not subscription:
                return
            del self._subscriptions[characteristic]
            connection = self._connection
            if connection is not None and not connection.closed.is_set():
                # Intent is removed first, so failure cannot resurrect this subscription.
                await connection.request(
                    Opcode.SUBSCRIBE, characteristic.bytes + b"\x00", timeout=self._request_timeout
                )

    def _notify(self, connection_id: int, characteristic: UUID, value: bytes) -> None:
        if connection_id != self._connection_id:
            return
        subscription = self._subscriptions.get(characteristic)
        if subscription is not None:
            subscription._push(Notification(characteristic, value, connection_id))

    async def _restore(self, connection: Connection) -> None:
        async with self._subscription_lock:
            for characteristic, subscription in tuple(self._subscriptions.items()):
                if subscription.closed:
                    del self._subscriptions[characteristic]
                    continue
                try:
                    await connection.request(
                        Opcode.SUBSCRIBE, characteristic.bytes + b"\x01", timeout=self._request_timeout
                    )
                except OperationRejected as exc:
                    error = SubscriptionError(f"Cannot restore {characteristic}: {exc}")
                    error.__cause__ = exc
                    subscription._finish(error)
                    del self._subscriptions[characteristic]

    async def _watch(self, connection: Connection) -> None:
        interval = self._heartbeat_interval
        while not connection.closed.is_set():
            if interval is None:
                await connection.closed.wait()
                break
            try:
                async with asyncio.timeout(interval):
                    await connection.closed.wait()
            except TimeoutError:
                if time.monotonic() - connection.last_received >= interval:
                    await connection.request(Opcode.SERVICES, b"", timeout=self._request_timeout)
        if connection.error is not None:
            raise connection.error

    async def _supervise(self) -> None:
        delay = self._reconnect_initial
        try:
            while self._running:
                connection: Connection | None = None
                ready_at: float | None = None
                try:
                    self._set_state(ClientState.CONNECTING)
                    generation = self._connection_id + 1
                    connection = await Connection.open(
                        self.endpoint,
                        partial(self._notify, generation),
                        timeout=self._connect_timeout,
                        close_timeout=self._close_timeout,
                    )
                    self._connection = connection
                    self._connection_id = generation
                    self._set_state(ClientState.RESTORING)
                    # Prove that the peer speaks WFTNP even when no subscriptions exist.
                    await connection.request(Opcode.SERVICES, b"", timeout=self._request_timeout)
                    await self._restore(connection)
                    self._last_error = None
                    self._set_state(ClientState.READY)
                    ready_at = time.monotonic()
                    await self._watch(connection)
                except WftnpError as exc:
                    self._last_error = exc
                    _LOGGER.debug("WFTNP connection lost: %s", exc)
                finally:
                    self._connection = None
                    if self._running:
                        self._set_state(ClientState.BACKOFF)
                    if connection is not None:
                        await connection.close()
                    for subscription in self._subscriptions.values():
                        if not subscription.closed:
                            subscription._clear()
                if not self._running or not self._auto_reconnect:
                    break
                if ready_at is not None and time.monotonic() - ready_at >= self._reconnect_max:
                    delay = self._reconnect_initial
                await asyncio.sleep(random.uniform(delay * 0.8, delay))
                delay = min(delay * 2, self._reconnect_max)
        finally:
            self._running = False
            self._set_state(ClientState.STOPPED)
            for subscription in self._subscriptions.values():
                subscription._finish()
