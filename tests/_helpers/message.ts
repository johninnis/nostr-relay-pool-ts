import type { NostrEvent } from "@innis/nostr-core"
import { eventIdFixture, publicKeyFixture, relayUrlFixture, sigFixture } from "@innis/nostr-core/testing"
import { systemWallClock } from "../../src/infrastructure/time/system-wall-clock.ts"
import { systemScheduler } from "../../src/infrastructure/time/system-scheduler.ts"
import type { MessageContext } from "../../src/infrastructure/web-socket/relay-message-handler.ts"
import type { RelayState } from "../../src/infrastructure/web-socket/relay-state.ts"
import type { WireSub } from "../../src/infrastructure/web-socket/wire-sub.ts"
import { stubWireSub } from "./relay-state.ts"
import { FakeWebSocket } from "./fake-web-socket.ts"

export const URL = relayUrlFixture("wss://relay.example.com")

export const event = (overrides: Partial<NostrEvent> = {}): NostrEvent => ({
  id: eventIdFixture("a".repeat(64)),
  pubkey: publicKeyFixture("b".repeat(64)),
  created_at: 1700000000,
  kind: 1,
  tags: [],
  content: "hello",
  sig: sigFixture("c".repeat(128)),
  ...overrides,
})

export const authEvent = (idChar: string): NostrEvent =>
  event({ id: eventIdFixture(idChar.repeat(64)), kind: 22242, tags: [["challenge", "challenge"]] })

export const context = (state: RelayState, partial: Partial<MessageContext> = {}): MessageContext => ({
  state,
  url: URL,
  subHistory: new Map(),
  authHandler: () => null,
  authTimeoutMs: 60_000,
  maxMessageBytes: 262_144,
  clock: systemWallClock,
  scheduler: systemScheduler,
  onEventReceived: () => {},
  onEoseLatency: () => {},
  onStateChange: () => {},
  ...partial,
})

export const wireSubWith = (listeners: WireSub["listeners"]): WireSub => stubWireSub({ listeners })

export const message = (payload: unknown): MessageEvent =>
  new MessageEvent("message", { data: JSON.stringify(payload) })

export const openSocket = (sent: string[]): WebSocket => new FakeWebSocket(WebSocket.OPEN, sent)

export const flushMicrotasks = (): Promise<void> => new Promise((resolve) => setTimeout(resolve, 0))

export const hangingHandler = (): MessageContext["authHandler"] => () => () => new Promise<NostrEvent | null>(() => {})
