import type { NostrEvent, RelayUrl } from "@innis/nostr-core"

/**
 * Host-supplied NIP-42 challenge responder. Given the relay and its `challenge` string, return a
 * signed kind-22242 AUTH event to authenticate, or `null` to decline. The pool sends the event
 * optimistically and re-issues any auth-parked subscriptions once it is on the wire.
 */
export type AuthHandler = (url: RelayUrl, challenge: string) => Promise<NostrEvent | null>
