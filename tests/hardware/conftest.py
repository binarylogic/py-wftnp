import math
import tomllib
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

import pytest

from wftnp import Endpoint


@dataclass(frozen=True)
class Device:
    name: str
    endpoint: Endpoint
    service: UUID
    readable: UUID
    notify: UUID
    stream_seconds: float


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "device" not in metafunc.fixturenames:
        return
    config = metafunc.config
    if not config.getoption("--hardware"):
        raise pytest.UsageError("Real-device tests require --hardware")
    if getattr(config.option, "numprocesses", None):
        raise pytest.UsageError("Hardware tests must run in one serial lane, without xdist")
    path = Path(config.getoption("--hardware-config"))
    try:
        data = tomllib.loads(path.read_text())
        seconds = float(data.get("stream_seconds", 30))
        if not math.isfinite(seconds) or not 1 <= seconds <= 3600:
            raise ValueError("stream_seconds must be between 1 and 3600")
        devices = [
            Device(
                name=entry["name"],
                endpoint=Endpoint(entry["host"], entry.get("port", 36866)),
                service=UUID(entry["service"]),
                readable=UUID(entry["read"]),
                notify=UUID(entry["notify"]),
                stream_seconds=seconds,
            )
            for entry in data["devices"]
        ]
        if not devices or len({d.name for d in devices}) != len(devices):
            raise ValueError("Provide at least one device, with unique names")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        raise pytest.UsageError(f"Invalid hardware configuration {path}: {exc}") from exc
    metafunc.parametrize("device", devices, ids=[device.name for device in devices])
