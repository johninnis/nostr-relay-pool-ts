import type { NostrFilter, RelayInformation } from "@innis/nostr-core"

const SEARCH_NIP = 50

const supportsSearch = (information: RelayInformation | null): boolean =>
  information?.supportedNips?.includes(SEARCH_NIP) ?? false

// Deliberate: a search goes only to a relay whose NIP-11 document lists NIP-50, never to one whose document is unknown — see shared ADR-0082 and ADR-0009
export const filtersSentTo = (
  information: RelayInformation | null,
  filters: ReadonlyArray<NostrFilter>,
): ReadonlyArray<NostrFilter> =>
  supportsSearch(information) ? filters : filters.filter(({ search }) => search === undefined)

// Deliberate: a relay answers a NIP-50 search by its own reading, so what it sends is checked against the rest — see shared ADR-0082
export const withoutSearch = ({ search: _search, ...conditions }: NostrFilter): NostrFilter => conditions
