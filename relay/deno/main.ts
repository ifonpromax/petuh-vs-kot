// Petuh vs Kot — relay + signaling server for Deno Deploy.
//
//   GET  /turn                      -> { iceServers, turn } (Cloudflare TURN creds if TURN_KEY_ID/TURN_TOKEN env vars are set, else STUN)
//   GET  /room/<code>/check         -> { host: bool, guest: bool }
//   WS   /room/<code>?role=host     -> opens a room (one host per room)
//   WS   /room/<code>?role=guest    -> joins it; every message is forwarded verbatim to the other side
//
// Presence lives in Deno KV (so it is shared across isolates); messages hop between isolates over
// BroadcastChannel. "hb" frames are heartbeats: answered with "hb" and used to detect dead hosts.

const CODE_RE = /^[a-z0-9]{5}$/;
const STALE_MS = 45_000;
const CORS = { "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, OPTIONS", "Access-Control-Allow-Headers": "*" };
const STUN = [{ urls: ["stun:stun.cloudflare.com:3478", "stun:stun.l.google.com:19302"] }];
const json = (o: unknown, status = 200) =>
  new Response(JSON.stringify(o), { status, headers: { "content-type": "application/json", ...CORS } });

type Role = "host" | "guest";
type Presence = { host?: number; guest?: number };
// Deno KV / BroadcastChannel are used when the platform offers them; otherwise everything is
// kept in this isolate's memory (fine as long as both players land on the same instance).
let kv: Deno.Kv | null = null;
try { kv = await Deno.openKv(); } catch (e) { console.warn("KV unavailable, using memory:", (e as Error)?.message); }
const memPresence = new Map<string, Presence>();
const local = new Map<string, { host?: WebSocket; guest?: WebSocket }>();
const channels = new Map<string, BroadcastChannel | null>();

// Without KV, presence is asked from every isolate over a shared BroadcastChannel ("who holds room X?")
const askBc = (() => { try { return new BroadcastChannel("rooms-presence"); } catch { return null; } })();
const askWaiters = new Map<string, (p: Presence) => void>();
if (askBc) askBc.onmessage = (e) => {
  const m = e.data as { q?: string; a?: string; code: string; id: string; p?: Presence };
  if (m.q === "who") {
    const mine = memPresence.get(m.code);
    const live: Presence = {};
    const s = local.get(m.code);
    if (s?.host && s.host.readyState === WebSocket.OPEN) live.host = mine?.host || Date.now();
    if (s?.guest && s.guest.readyState === WebSocket.OPEN) live.guest = mine?.guest || Date.now();
    if (live.host || live.guest) askBc.postMessage({ a: "here", code: m.code, id: m.id, p: live });
  } else if (m.a === "here") {
    const w = askWaiters.get(m.id); if (w && m.p) w(m.p);
  }
};
async function presence(code: string): Promise<Presence> {
  let p: Presence = {};
  if (kv) { try { p = (await kv.get<Presence>(["room", code])).value || {}; } catch { p = memPresence.get(code) || {}; } }
  else {
    p = { ...(memPresence.get(code) || {}) };
    // only trust local memory for sockets that are actually ours and open; merge what other isolates report
    const s = local.get(code);
    if (p.host && !(s?.host && s.host.readyState === WebSocket.OPEN)) delete p.host;
    if (p.guest && !(s?.guest && s.guest.readyState === WebSocket.OPEN)) delete p.guest;
    if (askBc && (!p.host || !p.guest)) {
      const id = Math.random().toString(36).slice(2);
      const remote = await new Promise<Presence>((resolve) => {
        const acc: Presence = {};
        const t = setTimeout(() => { askWaiters.delete(id); resolve(acc); }, 350);
        askWaiters.set(id, (r) => { Object.assign(acc, r); if (acc.host && acc.guest) { clearTimeout(t); askWaiters.delete(id); resolve(acc); } });
        askBc.postMessage({ q: "who", code, id });
      });
      p = { ...remote, ...p };
    }
  }
  const now = Date.now();
  if (p.host && now - p.host > STALE_MS) delete p.host;
  if (p.guest && now - p.guest > STALE_MS) delete p.guest;
  return p;
}
async function touch(code: string, role: Role, on: boolean) {
  const p = await presence(code);
  if (on) p[role] = Date.now(); else delete p[role];
  memPresence.set(code, p);
  if (kv) { try { await kv.set(["room", code], p, { expireIn: 6 * 3600_000 }); } catch { /* memory copy already updated */ } }
}

function channel(code: string): BroadcastChannel | null {
  if (channels.has(code)) return channels.get(code)!;
  let bc: BroadcastChannel | null = null;
  try {
    bc = new BroadcastChannel("room-" + code);
    bc.onmessage = (e) => {
      const { to, data } = e.data as { to: Role; data: string };
      const ws = local.get(code)?.[to];
      if (ws && ws.readyState === WebSocket.OPEN) try { ws.send(data); } catch { /* gone */ }
    };
  } catch (e) { console.warn("BroadcastChannel unavailable:", (e as Error)?.message); }
  channels.set(code, bc);
  return bc;
}
// deliver to the other side: same isolate directly, otherwise via BroadcastChannel
function forward(code: string, from: Role, data: string) {
  const to: Role = from === "host" ? "guest" : "host";
  const ws = local.get(code)?.[to];
  if (ws && ws.readyState === WebSocket.OPEN) { try { ws.send(data); } catch { /* gone */ } return; }
  const bc = channel(code);
  if (bc) bc.postMessage({ to, data });
}

async function turnCreds() {
  const id = Deno.env.get("TURN_KEY_ID"), token = Deno.env.get("TURN_TOKEN");
  if (!id || !token) return { iceServers: STUN, turn: false };
  try {
    const r = await fetch(`https://rtc.live.cloudflare.com/v1/turn/keys/${id}/credentials/generate-ice-servers`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ ttl: 3 * 3600 }),
    });
    if (!r.ok) throw new Error("turn api " + r.status);
    const j = await r.json();
    const servers = Array.isArray(j.iceServers) ? j.iceServers : [j.iceServers];
    return { iceServers: [...STUN, ...servers], turn: true };
  } catch (e) {
    return { iceServers: STUN, turn: false, error: String((e as Error)?.message || e) };
  }
}

const ISO_ID = Math.random().toString(36).slice(2, 8);
let bcOk = true; try { new BroadcastChannel("probe").close(); } catch { bcOk = false; }

Deno.serve(async (req) => {
  const url = new URL(req.url);
  if (req.method === "OPTIONS") return new Response(null, { headers: CORS });
  if (url.pathname === "/health") return json({ kv: !!kv, bc: bcOk, iso: ISO_ID, region: Deno.env.get("DENO_REGION") || null, rooms: local.size });
  if (url.pathname === "/turn") return json(await turnCreds());

  const m = url.pathname.match(/^\/room\/([a-z0-9]{5})(\/check)?$/);
  if (!m || !CODE_RE.test(m[1])) return new Response("petuh-vs-kot relay: ok", { headers: { "content-type": "text/plain", ...CORS } });
  const code = m[1];
  const p = await presence(code);
  if (m[2]) return json({ host: !!p.host, guest: !!p.guest });

  if (req.headers.get("upgrade")?.toLowerCase() !== "websocket") return new Response("expected websocket", { status: 426, headers: CORS });
  const role = url.searchParams.get("role") as Role;
  if (role !== "host" && role !== "guest") return new Response("bad role", { status: 400 });
  // room rules are enforced at the WebSocket level too (close codes the client understands)
  const reject = (p.host && role === "host") ? 4409 : (!p.host && role === "guest") ? 4404 : (p.guest && role === "guest") ? 4409 : 0;

  const { socket, response } = Deno.upgradeWebSocket(req);
  socket.onopen = async () => {
    if (reject) { try { socket.close(reject, reject === 4404 ? "no such room" : "room taken"); } catch { /* ignore */ } return; }
    const slot = local.get(code) || {};
    slot[role] = socket; local.set(code, slot);
    channel(code);
    await touch(code, role, true);
    if (role === "guest") forward(code, "guest", JSON.stringify({ t: "joined" }));
  };
  socket.onmessage = (e) => {
    if (typeof e.data !== "string" || e.data.length > 16384) return;
    if (e.data === "hb") { try { socket.send("hb"); } catch { /* gone */ } touch(code, role, true); return; }
    forward(code, role, e.data);
  };
  const gone = async () => {
    if (reject) return;
    const slot = local.get(code);
    if (slot && slot[role] === socket) { delete slot[role]; if (!slot.host && !slot.guest) local.delete(code); }
    await touch(code, role, false);
    forward(code, role, JSON.stringify({ t: "left" }));
  };
  socket.onclose = gone;
  socket.onerror = gone;
  return response;
});
