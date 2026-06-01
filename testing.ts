/**
 * `@innis/nostr-relay-pool/testing` — an in-memory Nostr relay for end-to-end pool tests.
 *
 * Spins up a real Deno WebSocket server speaking the NIP-01 wire protocol (REQ/EVENT/CLOSE/OK/EOSE
 * plus tag-filter matching). Hosts use it to drive the pool against an actual socket instead of
 * a mock. The pool is treated as the system under test; the relay is the dependency.
 *
 * @module
 */
import type { NostrEvent, NostrFilter, RelayUrl } from "@innis/nostr-core"
import { isRecord, matchesAnyFilter, parseNostrEvent, parseRelayUrl, tryParseJson } from "@innis/nostr-core"

// Self-contained close so this reference relay reaches into no package internals. close() throws on
// some platforms once the socket is already torn down; nothing is recoverable, so swallow it.
const closeSocket = (ws: WebSocket): void => {
  if (ws.readyState === WebSocket.CLOSED) return
  try {
    ws.close()
  } catch {
    // already closed — nothing to clean up
  }
}

interface StoredEvent {
  readonly event: NostrEvent
  readonly receivedAt: number
}

interface ClientSubscription {
  readonly subId: string
  readonly filters: ReadonlyArray<NostrFilter>
}

/** A running in-memory relay. Returned by {@link createInMemoryRelay}. */
export interface InMemoryRelay {
  /** The relay's `ws://127.0.0.1:<port>` URL, valid once {@link InMemoryRelay.start} has resolved. */
  readonly url: RelayUrl
  /** The bound TCP port (the OS-assigned port when constructed with `port: 0`). */
  readonly port: number
  /** Start listening; resolves once the port is bound and `url`/`port` are valid. */
  readonly start: () => Promise<void>
  /** Close every client socket and shut the server down. */
  readonly stop: () => Promise<void>
  /** Store an event and broadcast it to every matching subscription, as if a client had sent it. */
  readonly inject: (event: NostrEvent) => void
  /** Every event currently stored, in insertion order. */
  readonly getStoredEvents: () => ReadonlyArray<NostrEvent>
  /** Number of currently connected client sockets. */
  readonly getConnectionCount: () => number
  /** Forcibly drop every client socket without stopping the server (simulates a network cut). */
  readonly dropConnections: () => void
  /** Discard all stored events; leaves connections and subscriptions intact. */
  readonly clear: () => void
}

/** Options for {@link createInMemoryRelay}. */
export interface InMemoryRelayOptions {
  /** Port to bind; `0` (the default) lets the OS pick a free port. */
  readonly port?: number
  /** Delay (ms) before each `EOSE` is sent, to simulate stored-event latency. Default: 0. */
  readonly eoseDelayMs?: number
  /** When `true`, REQs are answered with a NIP-42 auth challenge until the socket authenticates. */
  readonly requireAuth?: boolean
}

/**
 * Create an in-memory Nostr relay backed by a real Deno WebSocket server speaking the NIP-01 wire
 * protocol (REQ/EVENT/CLOSE/OK/EOSE plus tag-filter matching). Drives the pool against an actual
 * socket in end-to-end tests. Call {@link InMemoryRelay.start} before connecting.
 */
export const createInMemoryRelay = (options: InMemoryRelayOptions = {}): InMemoryRelay => {
  const { port = 0, eoseDelayMs = 0, requireAuth = false } = options
  const events: Map<string, StoredEvent> = new Map()
  // Keyed by socket, then by subscription id: a relay holds at most one live sub per (socket, subId),
  // so a repeat REQ replaces rather than accumulates a duplicate that would double-broadcast.
  const subscriptions: Map<WebSocket, Map<string, ClientSubscription>> = new Map()
  let server: Deno.HttpServer | null = null
  let actualPort = port
  const sockets: Set<WebSocket> = new Set()
  const authedSockets: Set<WebSocket> = new Set()

  const send = (ws: WebSocket, data: unknown): void => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(data))
    }
  }

  // Fan one event out to every matching subscription, optionally skipping the socket that supplied
  // it. `send` already gates on the socket being open. Shared by client EVENTs and test injection.
  const broadcast = (event: NostrEvent, exclude?: WebSocket): void => {
    for (const [ws, subs] of subscriptions) {
      if (ws === exclude) continue
      for (const sub of subs.values()) {
        if (matchesAnyFilter(event, sub.filters)) send(ws, ["EVENT", sub.subId, event])
      }
    }
  }

  const handleReq = (ws: WebSocket, subId: string, filters: ReadonlyArray<NostrFilter>): void => {
    if (requireAuth && !authedSockets.has(ws)) {
      // Challenge only in response to a REQ (not proactively on connect): a reconnect that fails to
      // re-issue an auth-parked sub then receives no fresh challenge, so the sub strands.
      send(ws, ["CLOSED", subId, "auth-required: authentication required"])
      send(ws, ["AUTH", "challenge"])
      return
    }
    let subs = subscriptions.get(ws)
    if (!subs) {
      subs = new Map()
      subscriptions.set(ws, subs)
    }
    subs.set(subId, { subId, filters })

    for (const { event } of events.values()) {
      if (matchesAnyFilter(event, filters)) {
        send(ws, ["EVENT", subId, event])
      }
    }

    if (eoseDelayMs > 0) {
      setTimeout(() => send(ws, ["EOSE", subId]), eoseDelayMs)
    } else {
      send(ws, ["EOSE", subId])
    }
  }

  const handleEvent = (ws: WebSocket, event: NostrEvent): void => {
    const existed = events.has(event.id)
    events.set(event.id, { event, receivedAt: Date.now() })
    send(ws, ["OK", event.id, true, ""])

    if (!existed) broadcast(event, ws)
  }

  const handleClose = (subId: string, ws: WebSocket): void => {
    subscriptions.get(ws)?.delete(subId)
  }

  const handleMessage = (ws: WebSocket, raw: string): void => {
    const data = tryParseJson(raw)
    if (!Array.isArray(data) || data.length < 2) return

    const type = data[0]
    if (typeof type !== "string") return

    if (type === "REQ" && data.length >= 3 && typeof data[1] === "string") {
      // Wire filters are untyped JSON; drop any non-object entries before trusting them. matchesFilter
      // reads every field defensively, so narrowing the surviving records to NostrFilter is sound.
      // deno-lint-ignore innis/no-type-assertions
      const filters = data.slice(2).filter(isRecord) as ReadonlyArray<NostrFilter>
      handleReq(ws, data[1], filters)
      return
    }

    if (type === "EVENT" && data[1]) {
      const event = parseNostrEvent(data[1])
      if (event) handleEvent(ws, event)
      return
    }

    if (type === "CLOSE" && typeof data[1] === "string") {
      handleClose(data[1], ws)
      return
    }

    if (type === "AUTH" && data[1]) {
      authedSockets.add(ws)
      return
    }
  }

  const handleConnection = (ws: WebSocket): void => {
    sockets.add(ws)
    ws.onmessage = (msg: MessageEvent): void => {
      if (typeof msg.data !== "string") return
      handleMessage(ws, msg.data)
    }
    ws.onclose = (): void => {
      sockets.delete(ws)
      authedSockets.delete(ws)
      subscriptions.delete(ws)
    }
  }

  const start = (): Promise<void> =>
    new Promise((resolve) => {
      server = Deno.serve(
        {
          port: actualPort,
          onListen: ({ port: p }) => {
            actualPort = p
            resolve()
          },
        },
        (req) => {
          if (req.headers.get("upgrade") === "websocket") {
            const { socket, response } = Deno.upgradeWebSocket(req)
            handleConnection(socket)
            return response
          }
          return new Response("Not a WebSocket request", { status: 400 })
        },
      )
    })

  const stop = async (): Promise<void> => {
    for (const ws of sockets) {
      closeSocket(ws)
    }
    sockets.clear()
    subscriptions.clear()
    if (server) {
      await server.shutdown()
      server = null
    }
  }

  const inject = (event: NostrEvent): void => {
    events.set(event.id, { event, receivedAt: Date.now() })
    broadcast(event)
  }

  const dropConnections = (): void => {
    for (const ws of sockets) closeSocket(ws)
    sockets.clear()
    authedSockets.clear()
    subscriptions.clear()
  }

  return {
    get url() {
      return parseRelayUrl(`ws://127.0.0.1:${actualPort}`)
    },
    get port() {
      return actualPort
    },
    start,
    stop,
    inject,
    getStoredEvents: () => [...events.values()].map((s) => s.event),
    getConnectionCount: () => sockets.size,
    dropConnections,
    clear: () => {
      events.clear()
    },
  }
}
