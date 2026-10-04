# 5. Idle connections are kept alive with a periodic CLOSE

## Status

Accepted

## Context

Relays drop connections that send nothing for too long; innis/nostr-relay does after 300 seconds. A subscriber that only reads is exactly such a connection, and a drop costs a reconnect, a backoff penalty and a replay of stored events. A WebSocket ping does not reset a relay's idle timer, and a keep-alive `REQ` or `EVENT` would be rate-limited or leave state on the relay. innis/nostr-client sends a periodic `CLOSE` (its ADR-0013).

## Decision

Every open socket sends `["CLOSE", "keepalive"]` every `heartbeatIntervalMs` (default 30000); `0` disables it. The heartbeat starts when the socket opens and stops when the socket closes, is replaced, or the pool is disposed. It only sends on an open socket and never triggers a reconnect; the socket's close handling owns that.

## Consequences

- An idle subscriber survives a relay's idle timeout whenever that timeout is longer than the interval.
- The heartbeat does not count as activity for the pool's own idle-socket close, so an unused socket still closes after `idleSocketTimeoutMs`.
- The id `keepalive` is shared with the `ping` reply (ADR-0004).
