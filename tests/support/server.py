"""Independent wire-level peer for deterministic fault injection."""

import asyncio
import contextlib
import struct
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from uuid import UUID

from wftnp import Endpoint

SERVICE = UUID("00001826-0000-1000-8000-00805f9b34fb")
CHARACTERISTIC = UUID("00002ad2-0000-1000-8000-00805f9b34fb")
SECOND = UUID("00002acd-0000-1000-8000-00805f9b34fb")
HEADER = struct.Struct("!BBBBH")


@dataclass
class Request:
    operation: int
    sequence: int
    payload: bytes


class Peer:
    def __init__(self, writer: asyncio.StreamWriter) -> None:
        self.writer = writer

    async def respond(self, request: Request, payload: bytes = b"", status: int = 0) -> None:
        await self.send(HEADER.pack(1, request.operation, request.sequence, status, len(payload)) + payload)

    async def notify(self, value: bytes, characteristic: UUID = CHARACTERISTIC) -> None:
        payload = characteristic.bytes + value
        await self.send(HEADER.pack(1, 6, 0, 0, len(payload)) + payload)

    async def send(self, data: bytes) -> None:
        self.writer.write(data)
        await self.writer.drain()

    def drop(self) -> None:
        self.writer.close()


Handler = Callable[[Peer, Request], Awaitable[bool]]


class FakeServer:
    def __init__(self) -> None:
        self.handler: Handler | None = None
        self.requests: list[Request] = []
        self.peers: list[Peer] = []
        self._tasks: set[asyncio.Task[None]] = set()
        self.server: asyncio.Server | None = None
        self.endpoint = Endpoint("127.0.0.1")

    async def start(self) -> None:
        self.server = await asyncio.start_server(self._accept, "127.0.0.1", 0)
        self.endpoint = Endpoint("127.0.0.1", self.server.sockets[0].getsockname()[1])

    async def stop(self) -> None:
        assert self.server is not None
        self.server.close()
        await self.server.wait_closed()
        for peer in self.peers:
            peer.drop()
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.create_task(self._serve(reader, writer))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = Peer(writer)
        self.peers.append(peer)
        try:
            while True:
                version, op, seq, status, length = HEADER.unpack(await reader.readexactly(6))
                assert version == 1 and status == 0
                request = Request(op, seq, await reader.readexactly(length))
                self.requests.append(request)
                if self.handler is not None and await self.handler(peer, request):
                    continue
                if op == 1:
                    await peer.respond(request, SERVICE.bytes)
                elif op == 2:
                    await peer.respond(request, request.payload + CHARACTERISTIC.bytes + b"\x07")
                elif op == 3:
                    await peer.respond(request, request.payload + b"value")
                else:
                    await peer.respond(request, request.payload[:16])
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()


async def eventually(predicate: Callable[[], bool], timeout: float = 2.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.001)
