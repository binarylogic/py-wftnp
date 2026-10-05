# Changelog

## 0.1.2

- Add a single opt-in, serial hardware test lane covering real notification streams,
  interleaved reads, subscription disable/re-enable, and three TCP interruptions per device.
- Join callback cleanup even after a failed subscription is replaced or removed.
- Prevent deadlock when callback cleanup calls `stop()` during an existing shutdown.
- Log mDNS resolution failures at debug level instead of silently discarding them.

## 0.1.1

- Update the PyPI publishing action to support Core Metadata 2.5 emitted by Hatchling.
- First PyPI publication; no changes to the protocol library from 0.1.0.

## 0.1.0

Initial release of the standalone WFTNP v1 protocol library.

- Typed async service/characteristic discovery, reads, writes, and raw notifications.
- Managed reconnect with backoff, read-only health probes, and subscription restoration.
- Explicit deadlines, failure/cancellation behavior, and no replay of application requests.
- Bounded, ordered callback or iterator delivery with observable consumer failures.
- Optional mDNS discovery with caller-owned Zeroconf support.
- Dependency-free core, Python 3.11+, Apache-2.0 license, and typed package marker.
- Local TCP simulator tests and read-only validation against KICKR BIKE and KICKR RUN.

Fitness-profile decoding, device control policy, and Home Assistant integration are
outside this package. See `docs/hardware-validation.md` for observed compatibility limits.
