# relay

Signaling + fallback relay for the online mode.

- `deno/main.ts` — Deno Deploy app (entrypoint `relay/deno/main.ts`), used in production.
- `src/index.js` + `wrangler.toml` — the same thing as a Cloudflare Worker (workers.dev is blocked in RU, kept as a spare).

Optional env vars: `TURN_KEY_ID`, `TURN_TOKEN` (Cloudflare Realtime TURN) — enables `/turn` credentials.
