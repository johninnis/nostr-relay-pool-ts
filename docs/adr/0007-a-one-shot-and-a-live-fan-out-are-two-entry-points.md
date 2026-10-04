# 7. A one-shot and a live fan-out are two entry points, not a flag

## Status

Accepted

## Context

`subscribeMany(rawUrls, filters, { onEvent, onRelayEose?, onRelayClosed?, persistent? })` took one object holding three callbacks and a mode switch. The callbacks are the subscription's NIP-01 `EVENT`, `EOSE` and `CLOSED` messages, per relay; `persistent` decided whether each relay's leg closes at its EOSE (a fetch of stored events, with an adaptive soft timeout and a hard timeout) or stays open for live events. The object existed to keep the argument count down, and bundling a mode switch with an observer to dodge the count is what CODING-CONVENTIONS §3 forbids.

The callers split cleanly by that switch. The app's query path fetches stored events once (`onEvent`, `onRelayEose`); its live feeds stay open (`onEvent`, `persistent: true`, `syncUrls`); `@innis/nostr-signer`'s NIP-46 transport stays open and reports status from all three callbacks. No caller chooses the mode at run time.

Moving the per-relay notifications onto the returned handle was considered and rejected: a leg the pool cannot open (a relay in backoff, a gated or disposed pool) reports its end of stored events synchronously, before `subscribeMany` returns, so a listener attached to the handle afterwards would miss it, and a caller counting relays to completion would wait for ever.

## Decision

- `subscribeMany(rawUrls, filters, callbacks)` fetches stored events: each relay's leg closes at its EOSE or its timeout.
- `subscribeManyLive(rawUrls, filters, callbacks)` keeps each relay's leg open past its EOSE until `unsubscribe()` or the relay closes it.
- Both take `SubscribeManyCallbacks` — `{ onEvent, onRelayEose?, onRelayClosed? }` — and return the same `PoolSubscription` handle. The callbacks are one concept, the subscription's `EVENT` / `EOSE` / `CLOSED` per relay, and the multi-relay counterpart of `subscribe`'s `RelaySubscribeCallbacks`; nothing else travels with them.

## Consequences

- Neither entry point takes more than three arguments, and neither takes a settings object.
- A caller states the kind of subscription by the name it calls, not by a boolean.
- Do not add a mode flag or a settings field to `SubscribeManyCallbacks`: a new kind of fan-out is a new entry point.
