# 9. A filter with a search is sent only to a relay whose NIP-11 document, as the host holds it, lists NIP-50

## Status

Accepted

## Context

NIP-50 says clients SHOULD use a relay's NIP-11 `supported_nips` to learn whether it supports `search`, and MAY send a search to any relay only if they are prepared to filter out extraneous responses. A relay that does not support NIP-50 ignores the field and answers the rest of the filter, every event of the right kinds. The pool does not check what a relay sends against a search, because the relay decides what matches it (shared ADR-0082), so it cannot tell that answer from a real one. Shared ADR-0082 therefore decides that a client sends a filter with a `search` only to relays whose NIP-11 `supported_nips` lists 50, and that a relay whose document is unknown or does not list 50 is not sent the filter. The pool used to send it to every relay it subscribed on.

The pool is transport over WebSockets and holds no HTTP client. A NIP-11 document is fetched over HTTP from the relay's own URI, and fetching one means deciding when to fetch, how long to keep the answer and whether a relay at a private address may be reached; `@innis/nostr-core`'s `fetchRelayInformation` leaves the last to the host's choice of `HttpClient`. Those are a host's policies, and a host that shows relay details already holds the documents.

`subscribe` returns its handle at once. Waiting for a document before sending would hold every search on an HTTP round trip, and for ever on a fetch the host never makes.

## Decision

- The host supplies the documents it holds through `relayInformation`, a `RelayInformationLookup` (`(url) => RelayInformation | null`), in the pool's construction options. The pool never fetches a document.
- When a subscription opens on a relay, the pool reads that relay's document once. A filter with a `search` is sent only when the document's `supportedNips` lists 50. A relay whose document is `null` (unknown), lists no NIPs or does not list 50 is sent the subscription's other filters only. With no `relayInformation`, every relay's document is unknown and no relay is sent a search.
- The filters sent are the subscription's filters on that relay for its whole life: its `REQ`, re-issued unchanged on a reconnect and after AUTH, its de-duplication with identical subscriptions and its history all use them. A document the host learns later applies to the subscriptions opened after it.
- What the relay sends is checked against the filters it was sent, each without its `search` (shared ADR-0082), so an event only a withheld filter would admit is dropped.
- A subscription with no filter left to send that can match anything is answered as one that can match nothing (ADR-0006): it opens no connection, returns an active handle and calls `onEose` once, on a microtask.

## Consequences

- A search reaches only relays that say they support it, and a relay that would ignore `search` is never asked, so its answer to the rest of the filter is never taken for a search result.
- A host that wants searches answered supplies `relayInformation`; a pool without it sends no search. A relay the host holds no document for yet is not searched even when it supports NIP-50: fetching documents before searching is the host's job.
- A relay not sent a search gives the caller nothing for it, as a relay that found nothing does. The host knows which relays it reported as supporting NIP-50, so it can tell the two apart. The pool does not close such a subscription through `onClosed`: nothing failed, and a subscription that mixes a search with other filters still receives their events from that relay.
- Do not send a search to a relay whose document is unknown in the hope that it supports NIP-50, and do not fetch documents inside the pool: the first takes a relay's answer to the rest of the filter for a search result, the second puts HTTP and its policies in a WebSocket transport.
- Shared decision: nostr-adrs ADR-0082.
