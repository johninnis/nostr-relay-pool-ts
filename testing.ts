/**
 * `@innis/nostr-relay-pool/testing` — an in-memory Nostr relay for end-to-end pool tests.
 *
 * Spins up a real Deno WebSocket server speaking the NIP-01 wire protocol (REQ/EVENT/CLOSE/OK/EOSE
 * plus tag-filter matching). Hosts use it to drive the pool against an actual socket instead of
 * a mock. The pool is treated as the system under test; the relay is the dependency.
 *
 * @module
 */
import type { CompiledFilter, NostrEvent, NostrFilter, RelayUrl } from "@innis/nostr-core"
import { compileFilters, isOk, isRecord, parseJson, parseNostrEvent, parseRelayUrl } from "@innis/nostr-core"

interface StoredEvent {
  readonly event: NostrEvent
  readonly receivedAt: number
}

interface ClientSubscription {
  readonly subId: string
  readonly filter: CompiledFilter
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
  /**
   * When `true`, REQs are refused with `auth-required:` and a NIP-42 challenge until the socket sends
   * an `AUTH` event, which is accepted with `OK true` without checking it.
   */
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

  const broadcast = (event: NostrEvent, sender?: WebSocket): void => {
    for (const [ws, subs] of subscriptions) {
      if (ws === sender) continue
      for (const sub of subs.values()) {
        if (sub.filter.matches(event)) send(ws, ["EVENT", sub.subId, event])
      }
    }
  }

  const handleReq = (ws: WebSocket, subId: string, filters: ReadonlyArray<NostrFilter>): void => {
    if (requireAuth && !authedSockets.has(ws)) {
      send(ws, ["CLOSED", subId, "auth-required: authentication required"])
      send(ws, ["AUTH", "challenge"])
      return
    }
    let subs = subscriptions.get(ws)
    if (!subs) {
      subs = new Map()
      subscriptions.set(ws, subs)
    }
    const filter = compileFilters(filters)
    subs.set(subId, { subId, filter })

    for (const { event } of events.values()) {
      if (filter.matches(event)) {
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
    const parsed = parseJson(raw)
    if (!isOk(parsed)) return
    const data = parsed.value
    if (!Array.isArray(data) || data.length < 2) return

    const type = data[0]
    if (typeof type !== "string") return

    if (type === "REQ" && data.length >= 3 && typeof data[1] === "string") {
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
      const event = parseNostrEvent(data[1])
      if (!event) return
      authedSockets.add(ws)
      send(ws, ["OK", event.id, true, ""])
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
    for (const ws of sockets) ws.close()
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
    for (const ws of sockets) ws.close()
    sockets.clear()
    authedSockets.clear()
    subscriptions.clear()
  }

  return {
    get url() {
      const url = parseRelayUrl(`ws://127.0.0.1:${actualPort}`)
      if (url === null) throw new Error(`In-memory relay has no URL before start() binds a port (port ${actualPort})`)
      return url
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
