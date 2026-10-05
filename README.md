# wftnp

An asynchronous, typed Python client for WFTNP (Wahoo Direct Connect).

The library discovers endpoints, inspects services and characteristics, exchanges
opaque characteristic bytes, and receives notifications. It manages connections
and restores subscriptions after network interruptions. It does not interpret
fitness metrics, control workouts, or depend on Home Assistant.

Requires Python 3.11 or newer. The core client has no runtime dependencies.

```sh
pip install wftnp
# Optional mDNS discovery:
pip install 'wftnp[discovery]'
```

```python
import asyncio
from wftnp import Endpoint, WftnpClient


async def main():
    async with WftnpClient(Endpoint("trainer.local", 36866)) as client:
        for service in await client.discover_services():
            print(service.uuid)
            for characteristic in await client.discover_characteristics(service.uuid):
                print(characteristic.uuid, characteristic.properties)


asyncio.run(main())
```

See [architecture and lifecycle](docs/architecture.md) for the behavioral contract,
and [examples](examples/) for notification streaming and read-only hardware checks.

## Notifications

```python
from uuid import UUID


async def stream(client: WftnpClient, characteristic: UUID):
    async with await client.subscribe(characteristic) as subscription:
        async for notification in subscription:
            print(notification.connection_id, notification.value.hex())
```

Subscriptions recover across reconnects. Requests and writes are never replayed.
Disconnected calls fail promptly; use `await client.wait_ready()` when recovery should
be awaited. Notification buffers are bounded and report overflow explicitly.
Values remain raw bytes: decoding speed, cadence, power, or treadmill metrics belongs
in a fitness-profile layer above this library.

## Discovery

```python
from wftnp.discovery import discover


async def find_devices():
    for advertisement in await discover(timeout=3):
        print(advertisement.name, advertisement.endpoint)
```

## Development

```sh
uv sync --all-extras
task check
task build
```

Tests use a local TCP simulator and require no hardware. Real-device checks are
explicitly opted into and perform only discovery, reads, and notification subscriptions.

## Protocol basis

WFTNP is a vendor-originated protocol with an unofficial public description:
[elfrances/wahoo-fitness-tnp](https://github.com/elfrances/wahoo-fitness-tnp).
This implementation is independently written. It is not affiliated with Wahoo.
Characteristic payload semantics belong to higher-level libraries.
