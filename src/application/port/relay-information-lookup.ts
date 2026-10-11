import type { RelayInformation, RelayUrl } from "@innis/nostr-core"

/**
 * Host-supplied reader of the NIP-11 relay information documents the host holds, such as those it fetched with
 * `@innis/nostr-core`'s `fetchRelayInformation`. Returns the relay's document, or `null` when the host holds none. The
 * pool reads it as a subscription opens and sends a filter with a NIP-50 `search` only to a relay whose document lists
 * 50 in `supportedNips`; a relay whose document is unknown is not sent one.
 */
export type RelayInformationLookup = (url: RelayUrl) => RelayInformation | null
