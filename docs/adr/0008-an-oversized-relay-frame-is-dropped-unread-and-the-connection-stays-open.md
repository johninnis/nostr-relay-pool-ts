# 8. An oversized relay frame is dropped unread and the connection stays open

## Status

Accepted

## Context

A relay message is parsed whole: `JSON.parse` builds the frame's entire object graph before the pool can look at its type, so a hostile or broken relay that sends one enormous frame costs a multiple of its size in memory. NIP-01 sets no frame size. A browser `WebSocket` offers no way to refuse a frame before it is received: by the time `onmessage` runs the text is already held, and what the pool can still bound is the parse and everything after it.

innis/nostr-client bounds every inbound frame at 256 KiB, and its WebSocket parser closes the connection on a longer one. The obvious copy of that here is to close the socket too. But the pool reconnects a dropped relay and re-sends every open subscription, so a relay that answers a subscription with an oversized stored event would send it again on every reconnect: a close turns one unreadable event into a reconnect loop with a growing backoff penalty, and every other subscription on that relay goes dark with it. The pool already drops a frame that is not a relay message — not text, not JSON, not a known message shape — and reads on.

## Decision

- Every text frame is measured in UTF-8 bytes before it is parsed. A frame longer than `maxMessageBytes` is dropped unread, exactly as a malformed frame is: nothing is delivered, nothing is reported, and the connection stays open.
- `maxMessageBytes` is set in the pool's construction options like its other limits, and defaults to 262144 (256 KiB), the ceiling innis/nostr-client applies.
- The measure is the UTF-8 byte length, the size of the frame on the wire, not the UTF-16 length of the JavaScript string. A string longer than the ceiling in code units is over it in bytes without being encoded; only a string between a third of the ceiling and the ceiling is encoded to be measured.

## Consequences

- An event larger than the ceiling never reaches a subscriber. Its subscription keeps running and its `EOSE` still arrives; the event is simply absent, as if the relay had not sent it.
- An oversized `OK` or `CLOSED` is lost like any malformed one: the publish it answered times out, the subscription it closed stays open until the pool's own timeouts end it.
- A host that reads relays serving larger events, such as long-form articles with inline media or very large contact lists, raises `maxMessageBytes`.
- Do not close the connection on an oversized frame to match innis/nostr-client: here a close is followed by a reconnect that asks for the same frame again.
