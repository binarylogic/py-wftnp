# Hardware validation

## Repeatable hardware lane

```sh
cp hardware.example.toml .hardware.toml
# Edit .hardware.toml with reachable endpoints and known safe characteristic UUIDs.
task test:hardware
# An alternate configuration can be selected explicitly:
task test:hardware -- --hardware-config=/path/to/devices.toml
```

This is one serial lane using the local Python environment. It is excluded from normal
test collection and hosted CI, even when `.hardware.toml` exists. Explicitly opting in
with missing or invalid configuration fails collection; it never silently skips an
unreachable or unconfigured device. Parallel xdist execution is rejected.

For each configured device the scenario discovers services and characteristics, reads
a configured characteristic, receives notifications for 30 seconds while issuing reads,
and closes the test's TCP connection three times. After each interruption it requires
automatic reconnect, restored notifications from the new connection, and a successful
read. It then disables and re-enables notifications and checks clean client shutdown.
`stream_seconds` can be increased in the local configuration for longer observation.

A local forwarding proxy rejects every request except service/characteristic discovery,
reads, and notification enable/disable. This prevents accidental characteristic writes
from reaching hardware. The proxy only interrupts its own sockets. A normal simulator
test independently verifies the write block. Tests never touch the device's network
settings or another application's connections, and use no private client attributes.

Local results are written to `.artifacts/hardware.xml`, with notification counts,
connection counts, and stream durations. This report and `.hardware.toml` are gitignored
and excluded from package distributions. Configure only characteristic reads and
subscriptions that are safe for your hardware.

This lane deliberately checks the TCP protocol using explicit endpoints. Optional mDNS
discovery has isolated automated tests and the separate observations below; multicast
reachability is not a prerequisite for the TCP hardware lane.

## Observations

The 0.1.2 hardware lane passed on both devices after the shutdown fixes:

| Device | Stream duration | Notifications during stream | TCP connections | Result |
| --- | --- | --- | --- | --- |
| KICKR BIKE | 30 seconds | 121 | 4 (three interruptions) | Passed |
| KICKR RUN | 30 seconds | 61 | 4 (three interruptions) | Passed |

Both scenarios completed in about 70 seconds total, including discovery, reads,
reconnects, and subscription disable/re-enable. Counts are observations from that run,
not fixed notification-rate assertions.

Read-only checks performed on October 5, 2026 using the library against a Wahoo
KICKR BIKE and KICKR RUN on their local TCP endpoints.

| Check | KICKR BIKE | KICKR RUN |
| --- | --- | --- |
| Discover services and characteristics | Passed | Passed |
| Read Fitness Machine Feature (`2acc`) | Passed | Passed |
| Enable and receive notifications | Passed (`2ad2`) | Passed (`2acd`) |
| Disable notifications and receive acknowledgement | Passed | Passed |
| Recover after three deliberately closed local sockets | Passed | Passed |
| Restore subscription and receive data on each new connection | Passed | Passed |

UUID abbreviations above use the Bluetooth base UUID. Sample idle payloads were
`4400000000000000` for the bike and `0c0000000000000000ff7f` for the treadmill.
These are raw protocol observations, not a promise of fitness-profile decoding.

Optional mDNS discovery found both devices. Initial bounded searches returned no
results; subsequent searches succeeded. Discovery depends on multicast delivery
and network configuration, so an empty result is not proof that equipment is offline.
Directly configured endpoints worked throughout validation.

The checks did not write control characteristics, start or stop equipment, or change
resistance, speed, or incline. Devices were idle. Concurrent operation with ROUVY,
Zwift, or another controlling application has not been validated. This is evidence
for these two devices, not certification for every firmware or WFTNP implementation.

Reproduce the read-only checks with an explicit endpoint:

```sh
uv run python examples/inspect_device.py DEVICE_HOST \
  --read 00002acc-0000-1000-8000-00805f9b34fb \
  --subscribe 00002ad2-0000-1000-8000-00805f9b34fb --count 5
# For KICKR RUN, use 00002acd-0000-1000-8000-00805f9b34fb instead.
uv run python examples/discover_devices.py
```

Disconnect recovery and write semantics are covered deterministically by the local
TCP simulator; the opt-in lane also validates real peer reconnect/subscription behavior.
Physical network outages and long-duration workout sessions require
additional hardware observation beyond this initial release validation.
