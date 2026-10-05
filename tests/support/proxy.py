"""Read-only WFTNP forwarding proxy for real socket interruption tests."""

import asyncio
import contextlib
import struct

from wftnp import Endpoint

HEADER = struct.Struct("!BBBBH")


class ReadOnlyProxy:
    def __init__(self, target: Endpoint) -> None:
        self.target = target
        self.endpoint = Endpoint("127.0.0.1")
        self.errors: list[Exception] = []
        self.connections = 0
        self.requests: list[tuple[int, bytes]] = []
        self._server: asyncio.Server | None = None
        self._writers: set[asyncio.StreamWriter] = set()
        self._tasks: set[asyncio.Task[None]] = set()

    async def __aenter__(self) -> "ReadOnlyProxy":
        self._server = await asyncio.start_server(self._accept, "127.0.0.1", 0)
        self.endpoint = Endpoint("127.0.0.1", self._server.sockets[0].getsockname()[1])
        return self

    async def __aexit__(self, *_: object) -> None:
        assert self._server is not None
        self._server.close()
        await self._server.wait_closed()
        self.interrupt()
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    def interrupt(self) -> None:
        """Close only sockets belonging to this proxy, keeping its listener available."""
        for writer in tuple(self._writers):
            writer.close()

    def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.create_task(self._serve(reader, writer), name="hardware-proxy")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _requests(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while True:
            header = await reader.readexactly(HEADER.size)
            version, opcode, sequence, status, length = HEADER.unpack(header)
            payload = await reader.readexactly(length)
            # Fail closed: never forward writes, unknown operations, or malformed requests.
            valid = (
                (opcode == 1 and length == 0)
                or (opcode in (2, 3) and length == 16)
                or (opcode == 5 and length == 17 and payload[-1] in (0, 1))
            )
            if version != 1 or status != 0 or not valid:
                raise ValueError(f"Hardware proxy blocked operation {opcode}")
            self.requests.append((opcode, payload))
            writer.write(header + payload)
            await writer.drain()

    async def _responses(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writers = [writer]
        pumps: list[asyncio.Task[None]] = []
        self._writers.add(writer)
        try:
            async with asyncio.timeout(5):
                upstream, remote = await asyncio.open_connection(self.target.host, self.target.port)
            writers.append(remote)
            self._writers.add(remote)
            self.connections += 1
            pumps = [
                asyncio.create_task(self._requests(reader, remote)),
                asyncio.create_task(self._responses(upstream, writer)),
            ]
            done, _ = await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        except (OSError, asyncio.IncompleteReadError):
            pass  # Expected when either end closes or an interruption is injected.
        except Exception as exc:
            self.errors.append(exc)
        finally:
            for task in pumps:
                task.cancel()
            await asyncio.gather(*pumps, return_exceptions=True)
            for stream in writers:
                self._writers.discard(stream)
                stream.close()
            for stream in writers:
                with contextlib.suppress(OSError, TimeoutError):
                    async with asyncio.timeout(2):
                        await stream.wait_closed()
