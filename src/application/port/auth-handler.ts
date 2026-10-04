import type { AuthChallenge, NostrEvent, RelayUrl } from "@innis/nostr-core"

/**
 * Host-supplied NIP-42 challenge responder. Given the relay and its non-empty `challenge`, return a
 * signed kind-22242 AUTH event to authenticate, or `null` to decline. Once the relay accepts the AUTH
 * event with `OK true`, the pool resends every publish and re-issues every subscription the relay had
 * refused with `auth-required:` on that connection.
 */
export type AuthHandler = (url: RelayUrl, challenge: AuthChallenge) => Promise<NostrEvent | null>
