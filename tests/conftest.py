from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from wftnp import WftnpClient

from .support.server import FakeServer


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--hardware", action="store_true", help="Enable explicitly configured real-device tests")
    parser.addoption("--hardware-config", default=".hardware.toml", help="Local hardware configuration path")


def pytest_ignore_collect(collection_path: Path, config: pytest.Config) -> bool | None:
    if collection_path.name == "hardware" and not config.getoption("--hardware"):
        return True
    return None


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
