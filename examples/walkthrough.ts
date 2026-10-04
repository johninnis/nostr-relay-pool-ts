/**
 * Walkthrough of the main features of @innis/nostr-relay-pool.
 *
 * Run with: `deno run --allow-net examples/walkthrough.ts` — the relay is the in-memory relay from
 * `@innis/nostr-relay-pool/testing`, served locally and reached over loopback, so nothing leaves the
 * machine. Each step asserts what it shows.
 *
 * @module
 */

import { assert, assertEquals } from "@std/assert"
import { buildTextNote, createLocalSigner, generateSecretKey, KIND_TEXT_NOTE } from "@innis/nostr-core"
import type { NostrEvent } from "@innis/nostr-core"
import { createRelayPool } from "../mod.ts"
import { createInMemoryRelay } from "../testing.ts"

const relay = createInMemoryRelay()
await relay.start()
const pool = createRelayPool()

const signer = createLocalSigner(generateSecretKey())
const signed = await signer.signEvent(buildTextNote("hello relay", 1700000000))
assert(signed.success)
const note = signed.value

const published = await pool.publish(relay.url, note)
assertEquals([published.from, published.ok], [relay.url, true])
assertEquals(relay.getStoredEvents().map((event) => event.id), [note.id])

const received = await new Promise<ReadonlyArray<NostrEvent>>((resolve) => {
  const events: Array<NostrEvent> = []
  const subscription = pool.subscribe(relay.url, [{ kinds: [KIND_TEXT_NOTE], authors: [note.pubkey] }], {
    onEvent: (event) => events.push(event),
    onEose: () => {
      subscription.unsubscribe()
      resolve(events)
    },
  })
})
assertEquals(received.map((event) => event.id), [note.id])
assertEquals(pool.getConnectedRelayUrls(), [relay.url])

const unsatisfiable = await new Promise<number>((resolve) => {
  let delivered = 0
  pool.subscribe(relay.url, [{ authors: [] }], { onEvent: () => delivered++, onEose: () => resolve(delivered) })
})
assertEquals(unsatisfiable, 0)

pool.dispose()
await relay.stop()
