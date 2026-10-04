import type { WallClock } from "../../application/port/clock.ts"

/** Default {@link WallClock} backed by `Date.now()` — milliseconds since the Unix epoch. */
export const systemWallClock: WallClock = (): number => Date.now()
