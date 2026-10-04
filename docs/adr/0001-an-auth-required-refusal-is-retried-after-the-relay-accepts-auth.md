# 1. An `auth-required` refusal is retried after the relay accepts AUTH, and only when a handler can answer

## Status

Accepted

## Context

NIP-42 lets a relay refuse an `EVENT` with `OK false` or a `REQ` with `CLOSED`, both prefixed `auth-required:`. Its protocol flow shows the client authenticating, the relay answering the `AUTH` with `OK true`, and the client then sending the `EVENT` or `REQ` again. NIP-42 also says `AUTH` messages sent by clients MUST be answered with an `OK` message, so the relay's verdict on the `AUTH` always arrives.

Handing that sequence to every caller forces retry code into all of them and races: the challenge and the refusal arrive in an order the caller cannot control. innis/nostr-client handles it the same way as this pool (its ADR-0014): refused publishes and subscriptions are parked on one path and released by the relay's answer to the client's `AUTH`. This pool used to handle the same situation three ways: it parked every refused publish even with no auth handler, so the publish only ever settled at its timeout and lost the relay's reason; it resent parked events and subscriptions as soon as it had sent `AUTH`, before the relay had judged it; and it re-parked a subscription on every `auth-required` `CLOSED`, clearing its own authenticated flag, which can loop against a relay that keeps refusing.

A relay that receives an `AUTH` it has no challenge for issues a fresh challenge and replies `OK false` `auth-required:` to the `AUTH` event itself (innis/nostr-relay `ProcessAuthUseCase`). That reply asks for the fresh challenge to be answered; it is not a final refusal.

## Decision

There is one path for auth-parked work on a connection, shared by publishes and subscriptions.

- An `OK false` or `CLOSED` whose reason prefix is `auth-required` is parked only when an auth handler is set, the handler has not declined the relay's current challenge, and the relay has not yet accepted an `AUTH` on this connection. Parking keeps the already-signed event so the resend is byte-identical, and keeps the subscription's filters and listeners.
- Otherwise the refusal is the outcome: the publish resolves with the relay's `OK`, and the subscription is closed with the relay's message through `onClosed`. This matches innis/nostr-client.
- Parking also answers the relay's stored challenge if no handler has answered it yet (ADR-0002).
- The relay's `OK` for the pool's `AUTH` event decides the parked work:
  - `OK true`: the connection is authenticated; every parked event is resent and every parked subscription's `REQ` is re-issued on the same subscription id.
  - `OK false` with `auth-required:`: the pool answers the fresh challenge, now or when it arrives, and the work stays parked.
  - `OK false` with any other reason: parked publishes resolve `{ ok: false }` and parked subscriptions close, both with `auth-required: auth rejected: ` followed by the relay's message.
- A handler that declines the challenge (returns `null`) means the connection cannot authenticate until the relay sends a new challenge: parked publishes resolve `{ ok: false }` and parked subscriptions close, both with `auth-required: auth declined`.
- One auth timer per connection, `authTimeoutMs` (default 60000), runs while an answer is in flight or work is parked, and restarts with each answer (ADR-0002). When it fires, parked publishes resolve `{ ok: false }` and parked subscriptions close, both with `auth-required: auth timed out`. A parked publish's own `publishTimeoutMs` is suspended while it is parked (ADR-0003).
- Nothing else releases parked work, except that a dropped socket settles a parked publish as `disconnected` and carries a parked subscription to the reconnect, where its `REQ` is re-issued like any other subscription's, or it is unsubscribed.

## Consequences

- Callers write no retry code; a caller with no handler, or whose handler declines, gets an `auth-required:` outcome at once, and one whose relay never completes the handshake gets `auth-required: auth timed out`, instead of a `timeout` or a subscription that never ends.
- Nothing is resent before the relay has accepted the `AUTH`, and a relay that keeps refusing after accepting an `AUTH` ends the work instead of looping.
- Subscriptions are parked and re-issued, following the NIP-42 flow in which the client sends the `REQ` again after the relay's `OK`, because consumers hold long-lived subscriptions to relays that require authentication for reads.
- The synthetic rejection reason starts with `auth-required:` so it parses under the shared `OK`/`CLOSED` reason vocabulary.
- `RelayPoolStateEntry.authed` means the relay answered the pool's `AUTH` with `OK true`, not that an `AUTH` was sent.
