import { assertEquals } from "@std/assert"
import { handleRelayMessage } from "../src/infrastructure/web-socket/relay-message-handler.ts"
import { stubRelayState } from "./_helpers/relay-state.ts"
import { context, message, openSocket } from "./_helpers/message.ts"

Deno.test("handleRelayMessage - a ping NOTICE is answered with a keepalive CLOSE", () => {
  const sent: string[] = []
  const state = stubRelayState({ ws: openSocket(sent) })
  handleRelayMessage(context(state), message(["NOTICE", " PING "]))
  assertEquals(sent, [JSON.stringify(["CLOSE", "keepalive"])])
})

Deno.test("handleRelayMessage - any other NOTICE sends nothing", () => {
  const sent: string[] = []
  const state = stubRelayState({ ws: openSocket(sent) })
  handleRelayMessage(context(state), message(["NOTICE", "ping me later"]))
  assertEquals(sent, [])
})

Deno.test("handleRelayMessage - a ping NOTICE wrapped in a vertical tab, a NUL and a line feed is answered with a keepalive CLOSE", () => {
  const sent: string[] = []
  const state = stubRelayState({ ws: openSocket(sent) })
  handleRelayMessage(context(state), message(["NOTICE", "\u000b\u0000ping\r\n\t"]))
  assertEquals(sent, [JSON.stringify(["CLOSE", "keepalive"])])
})

Deno.test("handleRelayMessage - a ping NOTICE followed by a no-break space sends nothing", () => {
  const sent: string[] = []
  const state = stubRelayState({ ws: openSocket(sent) })
  handleRelayMessage(context(state), message(["NOTICE", "ping "]))
  assertEquals(sent, [])
})

Deno.test("handleRelayMessage - a ping NOTICE preceded by a form feed sends nothing", () => {
  const sent: string[] = []
  const state = stubRelayState({ ws: openSocket(sent) })
  handleRelayMessage(context(state), message(["NOTICE", "\u000cping"]))
  assertEquals(sent, [])
})
