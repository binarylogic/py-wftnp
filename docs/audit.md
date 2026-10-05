# Repository audit — October 5, 2026

Scope: protocol framing and response matching, deadlines and cancellation, lifecycle
ownership, subscription recovery and delivery, discovery cleanup, hardware test isolation,
package contents, documentation, and release workflow/repository consistency.

## Findings addressed in 0.1.2

1. **Shutdown could miss a retired callback.** Replacing a failed subscription removed
   the old subscription from the intent registry while its callback could still be
   performing async cleanup. A regression test reproduced `stop()` returning early.
   The client now tracks callback task lifetime independently from subscription intent.
2. **Reentrant callback shutdown could deadlock.** A callback's `finally` block calling
   `stop()` during an externally initiated shutdown waited on a lock held by the shutdown
   that was joining that callback. A regression test reproduced the timeout. A callback
   now recognizes an existing shutdown and lets that shutdown complete the join.
3. **Hardware checks were not a durable test lane.** Earlier checks were real, but used
   manual scripts. The repository now has one opt-in serial pytest lane, explicit local
   configuration, a write-blocking forwarding proxy, and a local JUnit report.
4. **Discovery resolution failures lacked diagnostics.** Resolver task exceptions were
   collected and discarded. They now appear in debug logs while preserving the bounded,
   best-effort discovery API and resource cleanup.

## Boundaries retained

- Protocol framing stays separate from connection lifetime and reconnect policy.
- Public models remain immutable; UUID payloads remain opaque.
- Application requests/writes are never replayed. Only subscription intent is restored.
- Notification queues are bounded and failures observable.
- No Home Assistant, fitness-profile, cloud, or device-control behavior was added.
- Hardware configuration and reports remain local; ordinary CI needs no device access.

## Remaining validation limits

The hardware lane exercises idle KICKR BIKE and KICKR RUN equipment and simulated loss
of the test TCP connection. It does not certify all firmware, real router outages,
simultaneous ROUVY/Zwift sessions, equipment-control commands, or hours-long workouts.
Callback functions must remain cooperative with asyncio and cancellation, as documented.
These are explicit limits of the evidence, not claims of universal compatibility.

## Verification

- 91 simulator/unit tests pass; combined statement/branch coverage remains 96%.
- Both real-device scenarios pass, including three socket interruptions each; detailed
  observations are in [hardware validation](hardware-validation.md).
- Formatting, lint, and source type checks pass.
- Wheel and source metadata pass strict checks; the wheel contains the typed marker.
- The source archive includes the hardware test lane but excludes local configuration
  and test reports. The wheel contains only library/package metadata.
- Default test collection excludes hardware, and an explicit hardware run with missing
  configuration fails rather than reporting an empty success.

No additional blocking finding was identified within this audit's scope.
