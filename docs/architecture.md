# Architecture and lifecycle

The public abstraction is one `WftnpClient` for one TCP endpoint. WFTNP carries
UUID-addressed characteristic bytes; their interpretation belongs above this library.
There are no device subclasses, FTMS parsers, equipment commands, cloud clients,
or Home Assistant dependencies. Generic characteristic writes are supported, but
the library does not know what those bytes mean.

## Files and ownership

| Module | Responsibility |
| --- | --- |
| `models.py` | Immutable endpoint, advertisement, service, characteristic, notification, and state values. |
| `exceptions.py` | Explicit protocol, connection, operation, and consumer errors. |
| `protocol.py` | Pure WFTNP v1 framing and response validation. No I/O or retry policy. |
| `connection.py` | Internal single socket lifetime, serialized requests, sequence correlation, reader, deadlines. |
| `client.py` | Public operations, connection supervisor, reconnect policy, subscription intent. |
| `subscription.py` | Bounded notification delivery, callback/iterator lifecycle, consumer errors. |
| `discovery.py` | Optional bounded mDNS discovery, independent of the client. |

`Connection`, codec objects, and underscore-prefixed members are internal.
Public classes are exported from `wftnp`; discovery functions are exported from
`wftnp.discovery`. Use a client and its subscriptions from a single asyncio event loop.

## Connection lifecycle

```text
STOPPED -> CONNECTING -> RESTORING -> READY
               |             |         |
               +-------------+---------+-> BACKOFF -> CONNECTING
```

`start()` starts one supervisor and returns without waiting for the network.
Repeated calls do not create more tasks. `wait_ready(timeout=10)` waits for a
validated WFTNP connection and restored subscriptions. It raises `TimeoutError`
on deadline or `NotConnected` if stopped; its timeout does not stop recovery.
The async context manager starts the client and waits up to 10 seconds for readiness.
For another startup deadline, use `start()`, `wait_ready()`, and `stop()` explicitly.

On loss, the supervisor closes the socket, clears buffered notifications from that
connection, and retries with exponential backoff and jitter. Defaults are 1–30 seconds.
The delay resets after a connection has stayed ready for at least `reconnect_max`
seconds, so peers that repeatedly accept then fail do not cause a tight retry loop.
`auto_reconnect=False` stops the client and terminates subscriptions on connection loss.

Each connection must answer service discovery before becoming ready. On a quiet
connection, service discovery also acts as a read-only health probe (default interval
20 seconds; `heartbeat_interval=None` disables it). TCP keepalive is enabled too.
Readiness describes the connection and subscriptions; it does not imply fresh metrics.

`state` is the current `ClientState`. `connection_id` increments for each established
socket. `last_error` records the latest connection failure and clears on readiness.
`add_state_listener(callback)` returns a removal function. Listeners run synchronously
and must be brief; exceptions are logged and isolated. They must schedule any async
work themselves and remove their listener when no longer needed.

`stop()` stops retries, closes the socket, terminates subscriptions, and joins owned
tasks. It is idempotent. Once shutdown begins, cancellation of its caller is propagated
only after cleanup finishes. A callback may close its subscription or stop its client.
An explicit `start()` after stopping creates a new lifecycle; old subscriptions stay closed.

## Request contract

All five request operations are supported: service discovery, characteristic discovery,
read, write, and notification enable/disable. Notifications are unsolicited inbound frames.
UUIDs are `uuid.UUID`; values are `bytes`. Unknown characteristic property bits are preserved.

- Disconnected calls raise `NotConnected` immediately. The caller explicitly waits for readiness.
- One request is outstanding per socket. The request deadline includes waiting for that slot.
- Requests validate version, operation, sequence, UUID, and payload shape before completing.
- Default connect and request deadlines are 5 seconds; socket close is bounded to 2 seconds.
- A rejected operation raises `OperationRejected`, retaining the numeric response code.
- A malformed, mismatched, duplicate, or unsolicited response retires the connection.
- EOF or a socket failure fails pending work. Requests are never replayed on a new connection.
- Cancelling/timing out before transmission leaves the connection usable. After transmission,
  it retires the socket, preventing a late response from satisfying another request.
- A failed or timed-out write has an **unknown outcome**: the device may have applied it.
  The caller must decide how to reconcile that outcome; the library never retries the write.

Applications should catch `WftnpError` for library failures. `RequestTimeout` also derives
from `TimeoutError`. Caller cancellation remains `asyncio.CancelledError`.

## Subscriptions

`await client.subscribe(uuid)` returns a `Subscription` for async iteration. Alternatively,
pass a synchronous or asynchronous notification callback. Choose one delivery mode and
one consumer per subscription. One active subscription is allowed per UUID per client.

The protocol reader only queues notifications. Each callback subscription owns a separate
task that delivers them sequentially. Async callbacks may issue client requests. All
callbacks must cooperate with asyncio: do not block the event loop or suppress cancellation
indefinitely. Shutdown cannot impose a deadline on arbitrary uncooperative application code.

Each subscription has a bounded queue (default 64). Overflow terminates that subscription
with `SubscriptionOverflow`; there is no silent dropping while it remains active. Callback
failure terminates it with `CallbackError`. Iterator consumption and `wait_closed()` expose
stored errors; `closed` and `error` are also available. `wait_closed()` reports logical
termination; `close()` additionally joins the callback task and removes subscription intent.
Use a subscription context manager or explicitly close failed subscriptions to disable
remote notifications on the current connection. Closing may raise a protocol/connection
error when disabling remotely fails; local intent is still removed.

Subscription intent survives reconnects. Restoration completes before `READY` is emitted.
A rejected restore terminates only the affected subscription with `SubscriptionError`.
Reconnects do not promise continuity: old queued notifications are discarded, and already
running callbacks are allowed to finish. Every notification carries its `connection_id`
so applications can distinguish generations. No metric cache or stale-value policy exists.

## Discovery

Install `wftnp[discovery]` and use `await discover(timeout=3)`. This is a finite browse
of `_wahoo-fitness-tnp._tcp.local.`, returning immutable advertisements with endpoints,
addresses, advertised UUIDs, and optional identity metadata. It does not connect to devices.
When provided an existing `AsyncZeroconf`, the caller retains its ownership. Otherwise,
the function creates and closes its own instance, including on cancellation.

The client reconnects to its configured endpoint; it does not secretly rediscover or select
another device. Use a stable hostname, or let the application resolve identity and construct
a replacement client when network addresses change. No Bluetooth or cloud fallback is provided.

## Verification and release

The tests use an independent local TCP simulator for wire framing, fragmentation, coalescing,
sequence wrap, malformed peers, lost responses, cancellation, bounded delivery, reconnection,
and subscription recovery. Discovery tests use an isolated mDNS test double. CI checks
Python 3.11–3.14 on Linux plus Python 3.12 on macOS and Windows, builds distributions,
checks metadata, and imports the installed wheel without optional dependencies.

Hardware compatibility observations belong in [hardware validation](hardware-validation.md).
No automated test sends equipment-control commands to real devices.
