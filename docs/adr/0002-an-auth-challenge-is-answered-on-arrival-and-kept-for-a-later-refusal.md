# 2. An AUTH challenge is answered on arrival and kept for a later refusal

## Status

Accepted

## Context

NIP-42 says the client MAY send its `AUTH` event at any point, and that the client must have a stored challenge associated with the relay so it can act on it in response to an `auth-required` `CLOSED`. A challenge is valid for the duration of the connection or until the relay sends another.

innis/nostr-client answers every challenge as it arrives when an auth handler is registered. Answering only when a refusal arrives would delay every restricted request by a round trip; answering on arrival alone misses the case the NIP names, where the challenge came before a handler was set or while another answer was in flight.

Auth handlers often have a person in the loop (a browser extension prompt, a remote signer), so they can take a long time or never settle, and a relay may never send the `OK` NIP-42 requires for the `AUTH` event.

## Decision

- The pool stores the relay's latest challenge on the connection and answers it on arrival when a handler is set, the relay has not accepted an `AUTH`, and no answer is in flight. A repeated challenge frame is answered again.
- When an `auth-required` refusal parks work (ADR-0001), or the relay answers the pool's `AUTH` with `auth-required:`, the stored challenge is answered if it has not been answered yet.
- One answer is in flight per connection. It spans the handler producing the event and the relay's `OK` for it, and is abandoned when the auth timer fires (`authTimeoutMs`, default 60000, restarted with each answer and kept running while work is parked), which settles parked work as `auth-required: auth timed out` (ADR-0001) and lets the next challenge be answered. A timeout is an anticipated outcome, not a fault, so it is not reported as an error. A handler that returns `null` ends the attempt without sending anything and declines the challenge (ADR-0001); the next challenge frame clears the decline.
- A new connection starts with no challenge and not authenticated.

## Consequences

- The NIP's stored-challenge flow works whether the handler was set before or after the challenge arrived.
- A declined challenge is not offered to the handler again until the relay sends a new one, so a person who said no is not asked repeatedly.
- A hung handler or a relay that never answers `AUTH` blocks further answers on that connection only until `authTimeoutMs`, and the work waiting on it is settled then.
