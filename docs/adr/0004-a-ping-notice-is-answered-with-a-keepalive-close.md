# 4. A `ping` NOTICE is answered with a keepalive CLOSE

## Status

Accepted

## Context

NIP-01 defines no rules for how `NOTICE` messages are sent or treated, and Nostr has no application-level ping. Some relays send `["NOTICE", "ping"]` and drop a client that does not send a frame back; a WebSocket control pong does not count, because they watch application messages. Sending a `CLOSE` for a subscription that was never opened looks like a bug. innis/nostr-client answers the same way (its ADR-0003).

## Decision

When a `NOTICE`'s text is `ping`, ignoring surrounding whitespace and case, the pool sends `["CLOSE", "keepalive"]` on that connection. It is the smallest well-formed client frame, needs no signing, and a relay ignores a `CLOSE` for an unknown id. The id is shared with the heartbeat (ADR-0005). The pool does not surface `NOTICE` messages to callers.

## Consequences

- Connections to relays that probe liveness this way stay open.
- The pool never opens a subscription named `keepalive`; its own ids are `pool-<n>`.
