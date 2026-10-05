import asyncio

import pytest

from wftnp import ClientState, ConnectionLost, NotConnected, RequestTimeout, SubscriptionError, WftnpClient

from .support.server import CHARACTERISTIC, SECOND, eventually


async def test_disconnect_restores_subscriptions_and_clears_old_buffer(client, server):
    states = []
    client.add_state_listener(states.append)
    sub = await client.subscribe(CHARACTERISTIC)
    old_id = client.connection_id
    await server.peers[-1].notify(b"stale")
    server.peers[-1].drop()
    await eventually(lambda: client.connection_id > old_id and client.state == ClientState.READY)
    assert states[:4] == [
        ClientState.BACKOFF,
        ClientState.CONNECTING,
        ClientState.RESTORING,
        ClientState.READY,
    ]
    await server.peers[-1].notify(b"fresh")
    value = await asyncio.wait_for(anext(sub), 1)
    assert value.value == b"fresh" and value.connection_id == old_id + 1
    assert len([r for r in server.requests if r.operation == 5 and r.payload[-1] == 1]) == 2


async def test_write_is_never_replayed_after_lost_response(client, server):
    async def drop_write(peer, req):
        if req.operation == 4:
            peer.drop()
            return True
        return False

    server.handler = drop_write
    old_id = client.connection_id
    with pytest.raises(ConnectionLost):
        await client.write_characteristic(CHARACTERISTIC, b"command")
    await eventually(lambda: client.connection_id > old_id and client.state == ClientState.READY)
    assert len([r for r in server.requests if r.operation == 4]) == 1


async def test_restoration_rejection_only_terminates_affected_subscription(client, server):
    first = await client.subscribe(CHARACTERISTIC)
    second = await client.subscribe(SECOND)

    async def reject_one(peer, req):
        if req.operation == 5 and req.payload[:16] == CHARACTERISTIC.bytes:
            await peer.respond(req, status=4)
            return True
        return False

    server.handler = reject_one
    old_id = client.connection_id
    server.peers[-1].drop()
    with pytest.raises(SubscriptionError):
        await asyncio.wait_for(first.wait_closed(), 1)
    await eventually(lambda: client.connection_id > old_id and client.state == ClientState.READY)
    await server.peers[-1].notify(b"ok", SECOND)
    assert (await asyncio.wait_for(anext(second), 1)).value == b"ok"


async def test_unsubscribe_during_backoff_is_not_restored(client, server):
    sub = await client.subscribe(CHARACTERISTIC)
    client._reconnect_initial = 0.1
    server.peers[-1].drop()
    await eventually(lambda: client.state == ClientState.BACKOFF)
    await sub.close()
    await client.wait_ready()
    assert len([r for r in server.requests if r.operation == 5 and r.payload[-1] == 1]) == 1


async def test_stop_interrupts_backoff_and_allows_explicit_restart(client, server):
    client._reconnect_max = 100
    client._reconnect_initial = 100
    server.peers[-1].drop()
    await eventually(lambda: client.state == ClientState.BACKOFF)
    async with asyncio.timeout(0.5):
        await client.stop()
        await client.stop()
    peer_count = len(server.peers)
    await asyncio.sleep(0.03)
    assert len(server.peers) == peer_count
    await client.start()
    await client.wait_ready()
    assert len(server.peers) == peer_count + 1


async def test_wait_ready_timeout_does_not_stop_recovery(server):
    gate = asyncio.Event()

    async def stall(peer, req):
        await gate.wait()
        return False

    server.handler = stall
    client = WftnpClient(server.endpoint, request_timeout=1)
    await client.start()
    try:
        with pytest.raises(TimeoutError):
            await client.wait_ready(0.02)
        assert client.state != ClientState.STOPPED
        gate.set()
        await client.wait_ready()
    finally:
        await client.stop()


async def test_stop_during_request_and_wait_ready(client, server):
    async def silent(peer, req):
        return True

    server.handler = silent
    request = asyncio.create_task(client.read_characteristic(CHARACTERISTIC))
    await eventually(lambda: server.requests[-1].operation == 3)
    await client.stop()
    with pytest.raises(ConnectionLost):
        await request
    assert client._connection is None
    assert client._task is None


async def test_heartbeat_detects_silent_peer(server):
    client = WftnpClient(
        server.endpoint,
        heartbeat_interval=0.01,
        request_timeout=0.02,
        reconnect_initial=0.01,
        reconnect_max=0.02,
    )
    await client.start()
    await client.wait_ready()
    old_id = client.connection_id

    async def silent(peer, req):
        return True

    server.handler = silent
    try:
        await eventually(lambda: isinstance(client.last_error, RequestTimeout))
        server.handler = None
        await eventually(lambda: client.connection_id > old_id and client.state == ClientState.READY)
    finally:
        await client.stop()


async def test_auto_reconnect_disabled_reports_stopped(server):
    client = WftnpClient(server.endpoint, auto_reconnect=False)
    await client.start()
    await client.wait_ready()
    sub = await client.subscribe(CHARACTERISTIC)
    server.peers[-1].drop()
    await eventually(lambda: client.state == ClientState.STOPPED)
    assert sub.closed
    assert isinstance(client.last_error, ConnectionLost)
    with pytest.raises(NotConnected):
        await client.wait_ready()
    await client.stop()
    assert len(server.peers) == 1


async def test_stop_before_supervisor_runs(server):
    client = WftnpClient(server.endpoint)
    await client.start()
    await client.stop()
    assert client.state == ClientState.STOPPED
    assert not server.peers


async def test_no_library_tasks_after_shutdown(client, server):
    await client.subscribe(CHARACTERISTIC, lambda _: None)
    await client.stop()
    tasks = [task for task in asyncio.all_tasks() if task.get_name().startswith("wftnp-")]
    assert tasks == []


async def test_cancelled_stop_finishes_cleanup_and_propagates_cancellation(client, monkeypatch):
    sub = await client.subscribe(CHARACTERISTIC, lambda _: None)
    connection = client._connection
    original_close = connection.close
    entered = asyncio.Event()
    release = asyncio.Event()

    async def delayed_close():
        entered.set()
        await release.wait()
        await original_close()

    monkeypatch.setattr(connection, "close", delayed_close)
    stop = asyncio.create_task(client.stop())
    await entered.wait()
    stop.cancel()
    await asyncio.sleep(0)
    assert not stop.done()
    stop.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await stop
    assert sub.closed and sub._consumer.done()
    assert connection.closed.is_set()
    assert client._task is None and client._connection is None
    assert not [t for t in asyncio.all_tasks() if t.get_name().startswith("wftnp-")]
