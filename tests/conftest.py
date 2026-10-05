from collections.abc import AsyncIterator

import pytest

from wftnp import WftnpClient

from .support.server import FakeServer


@pytest.fixture
async def server() -> AsyncIterator[FakeServer]:
    server = FakeServer()
    await server.start()
    try:
        yield server
    finally:
        await server.stop()


@pytest.fixture
async def client(server: FakeServer) -> AsyncIterator[WftnpClient]:
    client = WftnpClient(
        server.endpoint,
        request_timeout=0.15,
        connect_timeout=0.15,
        reconnect_initial=0.01,
        reconnect_max=0.02,
        heartbeat_interval=None,
    )
    await client.start()
    await client.wait_ready()
    try:
        yield client
    finally:
        await client.stop()
