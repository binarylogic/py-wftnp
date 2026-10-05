import asyncio

import pytest

from wftnp import ConnectionLost, WftnpClient

from .support.proxy import ReadOnlyProxy
from .support.server import CHARACTERISTIC


async def test_hardware_proxy_forwards_reads_but_blocks_writes(server):
    async with ReadOnlyProxy(server.endpoint) as proxy:
        async with WftnpClient(proxy.endpoint, auto_reconnect=False) as client:
            assert await client.read_characteristic(CHARACTERISTIC) == b"value"
            with pytest.raises(ConnectionLost):
                await client.write_characteristic(CHARACTERISTIC, b"must not reach device")
    assert not any(request.operation == 4 for request in server.requests)
    assert len(proxy.errors) == 1
    assert "blocked operation 4" in str(proxy.errors[0])
    assert not [task for task in asyncio.all_tasks() if task.get_name() == "hardware-proxy"]
