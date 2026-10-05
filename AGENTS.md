# py-wftnp

This library implements WFTNP, not fitness profiles or Home Assistant behavior.

- Keep the codec pure, one-connection state in `connection.py`, and recovery policy in `client.py`.
- Use immutable public models, explicit errors, bounded operations, and one asyncio event loop.
- Never replay application requests after disconnects. Restore notification subscriptions only.
- No packet decoding for FTMS, device-specific commands, cloud access, or equipment control in tests against real hardware.
- Use `uv` and `task check`; test failure paths with the local TCP simulator.
- `task test:hardware` is the single serial real-device lane. Opt in explicitly, configure `.hardware.toml`, and keep device addresses and reports out of git. Never run hardware tests in the hosted CI matrix.
- Preserve the documented lifecycle and cancellation contracts when changing code.
- Use conventional commits. Release only after CI, package validation, and release notes are complete.
- Publish through the GitHub release workflow; keep the package version and release tag identical.
