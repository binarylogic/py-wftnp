"""Optional mDNS discovery. Core protocol use has no third-party dependencies."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Mapping
from typing import TYPE_CHECKING
from uuid import UUID

from .models import Advertisement, Endpoint

if TYPE_CHECKING:
    from zeroconf.asyncio import AsyncZeroconf

SERVICE_TYPE = "_wahoo-fitness-tnp._tcp.local."


def parse_advertisement(
    name: str,
    host: str,
    port: int,
    properties: Mapping[bytes, bytes | None],
    addresses: tuple[str, ...] = (),
) -> Advertisement:
    """Normalize advertised metadata; ignore unrecognized/malformed optional UUID entries."""

    def text(key: bytes) -> str | None:
        value = properties.get(key)
        return value.decode("utf-8", errors="replace") if value else None

    services: list[UUID] = []
    for item in (text(b"ble-service-uuids") or "").split(","):
        value = item.strip()
        if not value:
            continue
        try:
            if value.lower().startswith("0x"):
                number = int(value, 16)
                if not 0 <= number <= 0xFFFFFFFF:
                    continue
                parsed = UUID(f"{number:08x}-0000-1000-8000-00805f9b34fb")
            else:
                parsed = UUID(value)
        except ValueError:
            continue
        if parsed not in services:
            services.append(parsed)
    return Advertisement(
        name=name,
        endpoint=Endpoint(host, port),
        addresses=addresses,
        serial_number=text(b"serial-number"),
        mac_address=text(b"mac-address"),
        services=tuple(services),
    )


async def discover(
    *, timeout: float = 3.0, zeroconf: AsyncZeroconf | None = None
) -> tuple[Advertisement, ...]:
    """Browse for a bounded time. A supplied AsyncZeroconf remains owned by the caller."""
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be finite and positive")
    try:
        from zeroconf import ServiceStateChange, Zeroconf
        from zeroconf.asyncio import AsyncServiceBrowser, AsyncServiceInfo, AsyncZeroconf
    except ImportError as exc:
        raise ImportError("Install wftnp[discovery] to use mDNS discovery") from exc

    owned = zeroconf is None
    azc = zeroconf if zeroconf is not None else AsyncZeroconf()
    found: dict[str, Advertisement] = {}
    pending: dict[str, asyncio.Task[None]] = {}
    tasks: set[asyncio.Task[None]] = set()

    async def resolve(name: str) -> None:
        info = AsyncServiceInfo(SERVICE_TYPE, name)
        if await info.async_request(azc.zeroconf, timeout * 1000):
            addresses = tuple(info.parsed_scoped_addresses())
            host = addresses[0] if addresses else info.server
            if host and info.port:
                found[name] = parse_advertisement(name, host, info.port, info.properties, addresses)

    def changed(zeroconf: Zeroconf, service_type: str, name: str, state_change: ServiceStateChange) -> None:
        old = pending.pop(name, None)
        if old is not None:
            old.cancel()
        if state_change == ServiceStateChange.Removed:
            found.pop(name, None)
        else:
            task = asyncio.create_task(resolve(name), name="wftnp-discovery")
            pending[name] = task
            tasks.add(task)

    browser = AsyncServiceBrowser(azc.zeroconf, SERVICE_TYPE, handlers=[changed])
    try:
        await asyncio.sleep(timeout)
    finally:
        await browser.async_cancel()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if owned:
            await azc.async_close()
    return tuple(found[name] for name in sorted(found))
