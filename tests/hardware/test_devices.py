import asyncio

import pytest

from wftnp import CharacteristicProperties, ClientState, WftnpClient

from ..support.proxy import ReadOnlyProxy
from .conftest import Device

pytestmark = pytest.mark.hardware


async def interrupt_and_wait(client: WftnpClient, proxy: ReadOnlyProxy) -> int:
    previous = client.connection_id
    restored = asyncio.Event()

    def state_changed(state: ClientState) -> None:
        if state == ClientState.READY and client.connection_id > previous:
            restored.set()

    remove_listener = client.add_state_listener(state_changed)
    try:
        proxy.interrupt()
        await asyncio.wait_for(restored.wait(), 20)
    finally:
        remove_listener()
    return previous


async def test_live_protocol_and_recovery(device: Device, record_property) -> None:
    """One sequential scenario per device; no private client state or device control."""
    received = 0
    async with ReadOnlyProxy(device.endpoint) as proxy:
        async with WftnpClient(proxy.endpoint, reconnect_initial=0.2, reconnect_max=1) as client:
            services = await client.discover_services()
            assert device.service in {service.uuid for service in services}
            characteristics = {}
            for service in services:
                found = await client.discover_characteristics(service.uuid)
                if service.uuid == device.service:
                    characteristics = {item.uuid: item for item in found}
            assert characteristics[device.readable].properties & CharacteristicProperties.READ
            assert characteristics[device.notify].properties & CharacteristicProperties.NOTIFY
            assert await client.read_characteristic(device.readable)

            async with await client.subscribe(device.notify) as subscription:
                deadline = asyncio.get_running_loop().time() + device.stream_seconds
                while asyncio.get_running_loop().time() < deadline:
                    notification = await asyncio.wait_for(anext(subscription), 15)
                    assert notification.characteristic == device.notify and notification.value
                    assert notification.connection_id == client.connection_id
                    received += 1
                    # Verify response correlation while notifications are being received.
                    assert await client.read_characteristic(device.readable)
                assert received >= 3

                for _ in range(3):
                    previous = await interrupt_and_wait(client, proxy)
                    notification = await asyncio.wait_for(anext(subscription), 15)
                    assert notification.connection_id > previous
                    assert notification.value
                    assert await client.read_characteristic(device.readable)

            # close() only returns after a valid disable acknowledgement.
            assert (5, device.notify.bytes + b"\x00") in proxy.requests
            # Verify enabling again after disable and cleanly closing another subscription.
            async with await client.subscribe(device.notify) as replacement:
                assert (await asyncio.wait_for(anext(replacement), 15)).value
        assert client.state == ClientState.STOPPED
    assert not proxy.errors
    assert proxy.connections >= 4
    record_property("notifications_during_stream", received)
    record_property("connections", proxy.connections)
    record_property("stream_seconds", device.stream_seconds)
