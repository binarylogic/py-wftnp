import asyncio
import builtins
from unittest.mock import MagicMock
from uuid import UUID

import pytest
import zeroconf.asyncio
from zeroconf import ServiceStateChange

from wftnp.discovery import SERVICE_TYPE, discover, parse_advertisement

from .support.server import SERVICE


def test_metadata_parsing_preserves_identity_and_ignores_bad_optional_values():
    ad = parse_advertisement(
        "Device." + SERVICE_TYPE,
        "fe80::1%en0",
        36866,
        {
            b"serial-number": b"serial",
            b"mac-address": b"mac",
            b"ble-service-uuids": (
                b"0x1826, bad, 0x100000000, 0xno, ,"
                b"00001826-0000-1000-8000-00805f9b34fb,"
                b"12345678-1234-5678-1234-567812345678"
            ),
        },
        ("fe80::1%en0",),
    )
    assert ad.serial_number == "serial" and ad.mac_address == "mac"
    assert ad.services == (SERVICE, UUID("12345678-1234-5678-1234-567812345678"))
    assert ad.addresses == ("fe80::1%en0",)
    assert ad.endpoint.host == "fe80::1%en0"
    assert parse_advertisement("empty", "host", 1, {}).services == ()


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf")])
async def test_invalid_timeout(timeout):
    with pytest.raises(ValueError):
        await discover(timeout=timeout)


async def test_missing_optional_dependency(monkeypatch):
    original = builtins.__import__

    def import_without_zeroconf(name, *args, **kwargs):
        if name == "zeroconf":
            raise ImportError("missing")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_zeroconf)
    with pytest.raises(ImportError, match=r"wftnp\[discovery\]"):
        await discover()


@pytest.fixture
def mdns(monkeypatch):
    instances = []
    browsers = []
    resolutions = []

    class Zeroconf:
        def __init__(self):
            self.zeroconf = object()
            self.closed = False
            instances.append(self)

        async def async_close(self):
            self.closed = True

    class Info:
        def __init__(self, service_type, name):
            assert service_type == SERVICE_TYPE
            self.name = name
            self.server = "device.local."
            self.port = 36866
            self.properties = {b"ble-service-uuids": b"0x1826"}

        async def async_request(self, zc, timeout):
            resolutions.append(self.name)
            if self.name == "pending":
                await asyncio.Event().wait()
            return self.name != "missing"

        def parsed_scoped_addresses(self):
            return [] if self.name == "hostname" else ["192.0.2.1"]

    class Browser:
        def __init__(self, zc, service_type, handlers):
            self.handler = handlers[0]
            self.cancelled = False
            browsers.append(self)

        def change(self, name, state=ServiceStateChange.Added):
            self.handler(None, SERVICE_TYPE, name, state)

        async def async_cancel(self):
            self.cancelled = True

    monkeypatch.setattr(zeroconf.asyncio, "AsyncZeroconf", Zeroconf)
    monkeypatch.setattr(zeroconf.asyncio, "AsyncServiceBrowser", Browser)
    monkeypatch.setattr(zeroconf.asyncio, "AsyncServiceInfo", Info)
    return MagicMock(instances=instances, browsers=browsers, resolutions=resolutions, zeroconf=Zeroconf)


async def test_browse_resolve_update_remove_and_cleanup(mdns):
    task = asyncio.create_task(discover(timeout=0.02))
    await asyncio.sleep(0)
    browser = mdns.browsers[0]
    for name in ["device", "hostname", "missing", "pending", "removed"]:
        browser.change(name)
    await asyncio.sleep(0)
    browser.change("device", ServiceStateChange.Updated)
    browser.change("removed", ServiceStateChange.Removed)
    browser.change("pending", ServiceStateChange.Removed)
    result = await task
    assert [ad.name for ad in result] == ["device", "hostname"]
    assert result[0].endpoint.host == "192.0.2.1"
    assert result[1].endpoint.host == "device.local."
    assert mdns.resolutions.count("device") == 2
    assert browser.cancelled and mdns.instances[0].closed


@pytest.mark.parametrize("owned", [True, False])
async def test_cancellation_releases_only_owned_resources(mdns, owned):
    shared = None if owned else mdns.zeroconf()
    task = asyncio.create_task(discover(timeout=60, zeroconf=shared))
    await asyncio.sleep(0)
    mdns.browsers[0].change("pending")
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert mdns.browsers[0].cancelled
    assert mdns.instances[0].closed == owned
    assert not [t for t in asyncio.all_tasks() if t.get_name() == "wftnp-discovery"]


async def test_browser_creation_failure_closes_owned_zeroconf(mdns, monkeypatch):
    def fail(*args, **kwargs):
        raise OSError("Cannot browse")

    monkeypatch.setattr(zeroconf.asyncio, "AsyncServiceBrowser", fail)
    with pytest.raises(OSError, match="Cannot browse"):
        await discover()
    assert mdns.instances[0].closed


async def test_resolution_failure_is_logged_and_resources_are_closed(mdns, monkeypatch, caplog):
    async def fail(*args, **kwargs):
        raise OSError("Cannot resolve advertisement")

    monkeypatch.setattr(zeroconf.asyncio.AsyncServiceInfo, "async_request", fail)
    caplog.set_level("DEBUG", logger="wftnp.discovery")
    task = asyncio.create_task(discover(timeout=0.01))
    await asyncio.sleep(0)
    mdns.browsers[0].change("device")
    assert await task == ()
    assert "Cannot resolve advertisement" in caplog.text
    assert mdns.instances[0].closed
