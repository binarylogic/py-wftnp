# Hardware validation

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
TCP simulator. Physical network outages and long-duration workout sessions require
additional hardware observation beyond this initial release validation.
