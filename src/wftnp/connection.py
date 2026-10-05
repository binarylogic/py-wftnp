"""One socket lifetime. No retries, profile parsing, or application callbacks."""

import asyncio
import contextlib
import socket
import time
from collections.abc import Callable
from uuid import UUID

from .exceptions import (
    ConnectionLost,
    NotConnected,
    OperationRejected,
    ProtocolError,
    RequestTimeout,
    WftnpError,
)
from .models import Endpoint
from .protocol import HEADER, Frame, Opcode, decode_header, encode, validate_response


class Connection:
    """Internal single-connection request dispatcher; calls are serialized."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        on_notification: Callable[[UUID, bytes], None],
        close_timeout: float,
    ) -> None:
        self._reader = reader
        self._writer = writer
        self._on_notification = on_notification
        self._close_timeout = close_timeout
        self._lock = asyncio.Lock()
        self._sequence = 0
        self._pending: tuple[Frame, asyncio.Future[Frame]] | None = None
        self.closed = asyncio.Event()
        self.error: WftnpError | None = None
        self.last_received = time.monotonic()
        self._reader_task = asyncio.create_task(self._read_loop(), name="wftnp-reader")

    @classmethod
    async def open(
        cls,
        endpoint: Endpoint,
        on_notification: Callable[[UUID, bytes], None],
        *,
        timeout: float,
        close_timeout: float,
    ) -> "Connection":
        try:
            async with asyncio.timeout(timeout):
                reader, writer = await asyncio.open_connection(endpoint.host, endpoint.port)
        except TimeoutError as exc:
            raise RequestTimeout("Connection attempt timed out") from exc
        except OSError as exc:
            raise ConnectionLost(f"Cannot connect to {endpoint.host}:{endpoint.port}: {exc}") from exc
        sock = writer.get_extra_info("socket")
        if sock is not None:
            with contextlib.suppress(OSError):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return cls(reader, writer, on_notification, close_timeout)

    def fail(self, error: WftnpError) -> None:
        """Atomically retire this connection and wake its outstanding caller."""
        if self.closed.is_set():
            return
        self.error = error
        self.closed.set()
        self._writer.close()
        if self._pending is not None:
            future = self._pending[1]
            if not future.done():
                future.set_exception(error)

    async def close(self) -> None:
        self.fail(ConnectionLost("Connection closed"))
        self._reader_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._reader_task
        with contextlib.suppress(OSError, TimeoutError):
            async with asyncio.timeout(self._close_timeout):
                await self._writer.wait_closed()

    async def request(self, opcode: Opcode, payload: bytes, *, timeout: float) -> Frame:
        # Validate before acquiring the lock or touching the socket.
        encode(Frame(opcode, 0, payload=payload))
        sent = False
        future: asyncio.Future[Frame] | None = None
        try:
            async with asyncio.timeout(timeout):
                async with self._lock:
                    if self.closed.is_set():
                        raise NotConnected("Connection is closed")
                    self._sequence = (self._sequence + 1) % 256
                    frame = Frame(opcode, self._sequence, payload=payload)
                    future = asyncio.get_running_loop().create_future()
                    self._pending = (frame, future)
                    try:
                        sent = True
                        self._writer.write(encode(frame))
                        await self._writer.drain()
                        response = await future
                        if response.status:
                            raise OperationRejected(opcode, response.status)
                        return response
                    finally:
                        self._pending = None
        except TimeoutError as exc:
            error = RequestTimeout(f"WFTNP operation {opcode.name} timed out")
            if sent:
                self.fail(error)
            raise error from exc
        except asyncio.CancelledError:
            if sent:
                self.fail(ConnectionLost("Transmitted request was cancelled"))
            raise
        except OSError as exc:
            error = ConnectionLost(f"Socket write failed: {exc}")
            self.fail(error)
            raise error from exc
        finally:
            if future is not None:
                if not future.done():
                    future.cancel()
                elif not future.cancelled():
                    future.exception()  # Retrieve failure if drain/cancellation won the race.

    async def _read_loop(self) -> None:
        try:
            while True:
                header = decode_header(await self._reader.readexactly(HEADER.size))
                payload = await self._reader.readexactly(header.length)
                frame = Frame(header.opcode, header.sequence, header.status, payload)
                self.last_received = time.monotonic()
                if frame.opcode == Opcode.NOTIFY:
                    if frame.status or len(payload) < 16:
                        raise ProtocolError("Invalid characteristic notification")
                    self._on_notification(UUID(bytes=payload[:16]), payload[16:])
                    continue
                if self._pending is None:
                    raise ProtocolError("Unsolicited response with no outstanding request")
                request, future = self._pending
                validate_response(frame, request)
                if future.done():
                    raise ProtocolError("Duplicate or late response")
                future.set_result(frame)
        except (OSError, asyncio.IncompleteReadError) as exc:
            self.fail(ConnectionLost(f"Connection ended: {exc}"))
        except WftnpError as exc:
            self.fail(exc)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.fail(ProtocolError(f"Notification dispatch failed: {exc}"))
