# 6. A subscription that can match nothing is answered locally

## Status

Accepted

## Context

A caller can hand `subscribe` filters that select nothing: an `authors` list built from an empty follow list, an empty `ids` list, a `since` after its `until`. NIP-01 lists hold "one or more values", so a relay reads such a filter by its own rules, and a relay that skips an empty list streams its whole store to a subscription meant to select nothing. `@innis/nostr-core` therefore never serialises a filter that can match nothing, and `serialiseReqMessage` returns `null` when no filter is left.

The pool still owes the caller an answer. Opening a socket to send nothing wastes a connection; returning an inactive handle would tell the caller the relay could not be reached, which is untrue; never calling `onEose` would leave a caller that waits for the end of stored events waiting for ever.

## Decision

- `subscribe` asks `canFilterMatch` of `@innis/nostr-core` whether any filter can match an event. When none can, it opens no connection, records no subscription history and sends nothing; it returns an active handle and calls `onEose` once, on a microtask, unless the handle is closed first. No event is ever delivered to it.
- `reissueReq` sends the `REQ` that `serialiseReqMessage` returns and nothing when it returns `null`, so every `REQ` the pool puts on the wire has passed the same test.

## Consequences

- A caller need not guard its own empty lists before subscribing: the answer is an empty backlog, the same a relay would give if it read the filter as NIP-01 intends.
- A subscription whose filters partly match nothing sends only the filters that can match; the local predicate already matches nothing for the others.
- Do not open a connection for such a subscription, or mark it inactive: nothing failed, and there is nothing to ask a relay.
