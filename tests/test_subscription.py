import asyncio

import pytest

from wftnp import CallbackError, SubscriptionOverflow

from .support.server import CHARACTERISTIC, eventually


async def test_ordered_iteration_and_close(client, server):
    async with await client.subscribe(CHARACTERISTIC) as subscription:
        for value in [b"a", b"b", b"c"]:
            await server.peers[-1].notify(value)
        received = [await asyncio.wait_for(anext(subscription), 1) for _ in range(3)]
        assert [n.value for n in received] == [b"a", b"b", b"c"]
        assert all(n.connection_id == client.connection_id for n in received)
    await subscription.wait_closed()
    await subscription.close()
    with pytest.raises(StopAsyncIteration):
        await anext(subscription)
    assert server.requests[-1].payload == CHARACTERISTIC.bytes + b"\x00"


async def test_close_unblocks_iterator(client):
    sub = await client.subscribe(CHARACTERISTIC)
    consumer = asyncio.create_task(anext(sub))
    await asyncio.sleep(0)
    await sub.close()
    with pytest.raises(StopAsyncIteration):
        await consumer


async def test_ordered_async_callback_and_request_from_callback(client, server):
    values = []

    async def callback(notification):
        await client.read_characteristic(CHARACTERISTIC)
        values.append(notification.value)

    sub = await client.subscribe(CHARACTERISTIC, callback)
    for value in [b"1", b"2", b"3"]:
        await server.peers[-1].notify(value)
    await eventually(lambda: len(values) == 3)
    assert values == [b"1", b"2", b"3"]
    with pytest.raises(RuntimeError):
        await anext(sub)


async def test_sync_callback_failure_is_observable(client, server):
    def callback(notification):
        raise ValueError("bad consumer")

    sub = await client.subscribe(CHARACTERISTIC, callback)
    await server.peers[-1].notify(b"x")
    with pytest.raises(CallbackError):
        await asyncio.wait_for(sub.wait_closed(), 1)
    assert isinstance(sub.error.__cause__, ValueError)
    assert await client.read_characteristic(CHARACTERISTIC) == b"value"
    await sub.close()


async def test_buffer_overflow_is_explicit(client, server):
    sub = await client.subscribe(CHARACTERISTIC, buffer_size=1)
    await server.peers[-1].notify(b"a")
    await server.peers[-1].notify(b"b")
    with pytest.raises(SubscriptionOverflow):
        await asyncio.wait_for(sub.wait_closed(), 1)
    with pytest.raises(SubscriptionOverflow):
        await anext(sub)
    assert await client.read_characteristic(CHARACTERISTIC) == b"value"


async def test_slow_callback_never_blocks_response_reader(client, server):
    entered = asyncio.Event()
    release = asyncio.Event()

    async def callback(notification):
        entered.set()
        await release.wait()

    sub = await client.subscribe(CHARACTERISTIC, callback, buffer_size=1)
    await server.peers[-1].notify(b"first")
    await asyncio.wait_for(entered.wait(), 1)
    assert await client.read_characteristic(CHARACTERISTIC) == b"value"
    await server.peers[-1].notify(b"second")
    await server.peers[-1].notify(b"third")
    with pytest.raises(SubscriptionOverflow):
        await asyncio.wait_for(sub.wait_closed(), 1)
    await sub.close()
    assert sub._consumer.done()


async def test_callback_can_close_itself(client, server):
    async def callback(notification):
        await sub.close()

    sub = await client.subscribe(CHARACTERISTIC, callback)
    await server.peers[-1].notify(b"x")
    await asyncio.wait_for(sub.wait_closed(), 1)
    await eventually(lambda: sub._consumer.done())


async def test_callback_can_stop_client(client, server):
    stopped = asyncio.Event()

    async def callback(notification):
        await client.stop()
        stopped.set()

    sub = await client.subscribe(CHARACTERISTIC, callback)
    await server.peers[-1].notify(b"x")
    await asyncio.wait_for(sub.wait_closed(), 1)
    await eventually(lambda: sub._consumer.done())
    assert stopped.is_set()


async def test_cancel_close_removes_intent_and_propagates(client, server):
    sub = await client.subscribe(CHARACTERISTIC, lambda _: None)

    async def silent_disable(peer, req):
        return req.operation == 5 and req.payload[-1] == 0

    server.handler = silent_disable
    task = asyncio.create_task(sub.close())
    await eventually(lambda: server.requests[-1].payload[-1] == 0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert sub.closed and sub._consumer.done()
    assert not client._subscriptions
