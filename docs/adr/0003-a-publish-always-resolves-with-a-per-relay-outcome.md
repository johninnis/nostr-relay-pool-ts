# 3. A publish always resolves with a per-relay outcome

## Status

Accepted

## Context

A relay answers an `EVENT` with `OK`, and a refusal (`duplicate:`, `rate-limited:`, `blocked:`, `auth-required:`) is a normal answer, not a fault. Some outcomes never produce an `OK`: the socket cannot be opened, drops before the reply, the pool is disposed, or the relay never answers. innis/nostr-client returns a relay's refusal as a value and reserves errors for a broken connection (its ADR-0009).

## Decision

`publish` never rejects. It resolves once with `{ from, ok, message }`: the relay's `OK` when one arrives, or `{ ok: false }` with a pool message (`"failed to connect"`, `"invalid url"`, `"disconnected"`, `"disposed"`, `"timeout"`) when none can. A publish waits at most `publishTimeoutMs` (default 8000) for its `OK`. While it is parked for authentication that timeout is suspended and the auth timeout bounds it instead, settling it as `auth-required: auth timed out` (ADR-0001); the resend after the relay accepts the `AUTH` starts `publishTimeoutMs` again.

## Consequences

- Callers inspect the outcome instead of catching errors, and every publish settles.
- Unlike innis/nostr-client, a dropped connection is also a resolved outcome. A parked publish is settled only by the relay's verdict on the `AUTH`, a decline, the auth timeout or a dropped connection, never by `timeout` while a slow auth handler is still answering.
- The pool's own messages carry no reason prefix, so they are never mistaken for a relay's machine-readable reason.
