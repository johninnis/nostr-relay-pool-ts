import type { RelayInformation } from "@innis/nostr-core"
import { parseRelayInformation } from "@innis/nostr-core"

export const relayInformationOf = (document: Readonly<Record<string, unknown>>): RelayInformation => {
  const information = parseRelayInformation(document)
  if (information === null) throw new Error("expected a NIP-11 relay information document")
  return information
}

export const SEARCH_RELAY_INFORMATION = relayInformationOf({ supported_nips: [1, 11, 50] })

export const PLAIN_RELAY_INFORMATION = relayInformationOf({ supported_nips: [1, 11] })
