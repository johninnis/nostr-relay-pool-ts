# 10. A read the relay narrowed until AUTH is sent again once the relay accepts AUTH

## Status

Accepted

## Context

A relay can answer a `REQ` that asks for more than a guest may read by serving the guest-readable part and ending its stored events with NIP-67's hint, `["EOSE", <subscription_id>, ["auth"]]`: "the relay may have more stored events matching the subscription's filters if the user performs `AUTH`". NIP-42 points to the hint for this case, and the relay sends its `AUTH` challenge before that `EOSE`. NIP-01 makes a `REQ` on an open subscription id replace that subscription, and NIP-42's flow shows the client sending its `REQ` again once the relay has accepted the `AUTH`.

The pool did not read the hint. It relied on the relay re-streaming the newly visible stored events on the open subscription after `AUTH`, ending with a second `EOSE`. That breaks what `EOSE` means, since events after it are new real-time events to every client that follows NIP-01, and a relay that keeps `EOSE`'s meaning replays nothing: the subscription keeps the narrowed scope for good, although the pool authenticated. Shared ADR-0029 therefore decides that a relay replays nothing after `AUTH`, and shared ADR-0030 that the client sends the narrowed subscription again, once, when the relay accepts an `AUTH` before or after that `EOSE`; innis/nostr-client follows the same record.

The obvious alternatives each fail:

- Parking the subscription, as an `auth-required` `CLOSED` is (ADR-0001), stops a subscription the relay is serving, and lets the auth timeout close it when nothing authenticates.
- Sending the `REQ` again at every hinted `EOSE` loops against a relay that still narrows the read for the key that signed: the replacing `REQ`'s `EOSE` carries the hint again.
- Sending it again only for a `REQ` put on the wire before the relay accepted the `AUTH` misses the relay that judged a `REQ` before verifying the `AUTH` but answered the `AUTH` first; the pool cannot see which the relay did.

## Decision

- An `EOSE` whose hints include `"auth"`, exactly, marks its subscription as narrowed. Any other hint means nothing here. The subscription stays open with its listeners, its `onEose` fires as for any `EOSE`, nothing is parked, the hint answers no challenge and starts no auth timer.
- When the relay has accepted the pool's `AUTH` on the connection, before or after that `EOSE`, the narrowed subscription's `REQ` is sent again, once, on the same subscription id with the same filters: at the hinted `EOSE` when the relay had already accepted, otherwise at the relay's `OK true` for the `AUTH`, together with the parked work ADR-0001 releases.
- Once: an `"auth"` hint on the `EOSE` of a `REQ` the pool sent again to widen the read is not acted on. A subscription the pool sends again for any other reason, on a reconnect or after it was parked, is a new `REQ`, and its own hint is read afresh.
- The relay answers the replacing `REQ` as a new subscription: events already delivered may arrive again, and its `EOSE` fires `onEose` again, with the `EOSE` latency measured from the replacing `REQ`.
- A subscription closed before the relay accepts the `AUTH`, by its caller or by the relay, is not sent again, save one the relay closed with `auth-required:` while a handler can still authenticate: that one is parked (ADR-0001) and re-issued once, as parked work is. When the `AUTH` is refused, declined or times out, a narrowed subscription stays open at the scope the relay gave it.

## Consequences

- A live subscription reaches the scope its key earns without the relay replaying anything after its `EOSE`.
- A one-shot fetch (`subscribeMany`) closes its leg at the first `EOSE`, so it keeps the narrowed answer; a caller wanting the wider one subscribes live or fetches again once the connection has authenticated.
- `onEose` can fire more than once on one subscription, and events can arrive twice; de-duplication stays the caller's event store's job.
- Do not park a narrowed subscription, and do not send it again at every hinted `EOSE`.
- Shared decisions: nostr-adrs ADR-0030 and ADR-0091.
