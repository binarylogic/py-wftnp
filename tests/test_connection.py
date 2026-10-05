import asyncio
import struct

import pytest

from wftnp import ConnectionLost, NotConnected, OperationRejected, ProtocolError, RequestTimeout
from wftnp.connection import Connection
from wftnp.protocol import Opcode

from .support.server import CHARACTERISTIC, HEADER, SERVICE, eventually


async def connect(server, callback=lambda *_: None):
    return await Connection.open(server.endpoint, callback, timeout=0.2, close_timeout=0.2)


async def test_fragmentation_and_coalescing(server):
    seen = []
    connection = await connect(server, lambda *args: seen.append(args))

    async def handler(peer, req):
        frame = HEADER.pack(1, req.operation, req.sequence, 0, 16) + SERVICE.bytes
        for byte in frame:
            await peer.send(bytes([byte]))
            await asyncio.sleep(0)
        notification = HEADER.pack(1, 6, 0, 0, 17) + CHARACTERISTIC.bytes + b"x"
        await peer.send(notification * 3)
        return True

    server.handler = handler
    try:
        assert (await connection.request(Opcode.SERVICES, b"", timeout=1)).payload == SERVICE.bytes
        await eventually(lambda: len(seen) == 3)
        assert seen == [(CHARACTERISTIC, b"x")] * 3
    finally:
        await connection.close()


async def test_serialized_concurrency_and_sequence_wrap(server):
    connection = await connect(server)
    try:
        responses = await asyncio.gather(
            *[connection.request(Opcode.READ, CHARACTERISTIC.bytes, timeout=5) for _ in range(270)]
        )
        assert len(responses) == 270
        assert server.requests[254].sequence == 255
        assert server.requests[255].sequence == 0
        assert server.requests[256].sequence == 1
    finally:
        await connection.close()


async def test_peer_eof_fails_outstanding_and_queued_requests(server):
    async def handler(peer, req):
        peer.drop()
        return True

    server.handler = handler
    connection = await connect(server)
    try:
        results = await asyncio.gather(
            *[connection.request(Opcode.SERVICES, b"", timeout=1) for _ in range(3)], return_exceptions=True
        )
        assert isinstance(results[0], ConnectionLost)
        assert all(isinstance(result, (ConnectionLost, NotConnected)) for result in results)
        assert connection.closed.is_set()
    finally:
        await connection.close()
        await connection.close()


@pytest.mark.parametrize("cancel", [False, True])
async def test_lost_response_retires_socket(server, cancel):
    async def silent(peer, req):
        return True

    server.handler = silent
    connection = await connect(server)
    request = asyncio.create_task(connection.request(Opcode.WRITE, CHARACTERISTIC.bytes + b"x", timeout=0.03))
    await eventually(lambda: bool(server.requests))
    try:
        if cancel:
            request.cancel()
            with pytest.raises(asyncio.CancelledError):
                await request
        else:
            with pytest.raises(RequestTimeout):
                await request
        assert connection.closed.is_set()
        assert connection._pending is None
    finally:
        await connection.close()


async def test_queued_timeout_does_not_break_active_request(server):
    release = asyncio.Event()

    async def hold(peer, req):
        await release.wait()
        return False

    server.handler = hold
    connection = await connect(server)
    first = asyncio.create_task(connection.request(Opcode.SERVICES, b"", timeout=1))
    await eventually(lambda: bool(server.requests))
    try:
        with pytest.raises(RequestTimeout):
            await connection.request(Opcode.SERVICES, b"", timeout=0.02)
        assert not connection.closed.is_set()
        release.set()
        await first
    finally:
        release.set()
        await connection.close()


async def test_rejected_operation_keeps_connection_usable(server):
    async def reject(peer, req):
        await peer.respond(req, status=5)
        return True

    connection = await connect(server)
    server.handler = reject
    try:
        with pytest.raises(OperationRejected) as error:
            await connection.request(Opcode.READ, CHARACTERISTIC.bytes, timeout=1)
        assert error.value.code == 5
        assert not connection.closed.is_set()
        server.handler = None
        await connection.request(Opcode.SERVICES, b"", timeout=1)
    finally:
        await connection.close()


@pytest.mark.parametrize(
    "kind", ["version", "opcode", "sequence", "uuid", "truncated", "notification", "extra"]
)
async def test_malformed_peer_is_retired(server, kind):
    async def malformed(peer, req):
        if kind == "truncated":
            await peer.send(b"\x01\x01")
            peer.drop()
        elif kind == "notification":
            await peer.send(HEADER.pack(1, 6, 0, 0, 1) + b"x")
        else:
            version = 2 if kind == "version" else 1
            op = 99 if kind == "opcode" else 3
            seq = (req.sequence + 1) % 256 if kind == "sequence" else req.sequence
            payload = SERVICE.bytes if kind == "uuid" else CHARACTERISTIC.bytes + b"x"
            if kind == "extra":
                op = 4
            await peer.send(struct.pack("!BBBBH", version, op, seq, 0, len(payload)) + payload)
        return True

    server.handler = malformed
    connection = await connect(server)
    try:
        with pytest.raises((ConnectionLost, ProtocolError)):
            await connection.request(Opcode.READ, CHARACTERISTIC.bytes, timeout=1)
        assert connection.closed.is_set()
    finally:
        await connection.close()


async def test_unsolicited_response_closes_socket(server):
    connection = await connect(server)
    await eventually(lambda: bool(server.peers))
    try:
        await server.peers[0].send(HEADER.pack(1, 1, 0, 0, 0))
        await asyncio.wait_for(connection.closed.wait(), 1)
        assert isinstance(connection.error, ProtocolError)
    finally:
        await connection.close()
