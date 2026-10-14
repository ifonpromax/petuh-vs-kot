// Petuh vs Kot — relay + signaling server (Cloudflare Worker + Durable Object).
//
//   GET  /turn                      -> { iceServers: [...] } (Cloudflare TURN creds if secrets are set, else STUN)
//   GET  /room/<code>/check         -> { host: bool, guest: bool }
//   WS   /room/<code>?role=host     -> opens a room (one host per room)
//   WS   /room/<code>?role=guest    -> joins it; every message is forwarded verbatim to the other side
//
// The Durable Object is one instance per room code, so both sockets always land in the same place.

const CODE_RE = /^[a-z0-9]{5}$/;
const CORS = { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'GET, OPTIONS', 'Access-Control-Allow-Headers': '*' };
const json = (o, status = 200) => new Response(JSON.stringify(o), { status, headers: { 'content-type': 'application/json', ...CORS } });
const STUN = [{ urls: ['stun:stun.cloudflare.com:3478', 'stun:stun.l.google.com:19302'] }];
const HB_STALE_MS = 45000; // a host whose heartbeat is older than this is considered gone

export class Room {
  constructor(state) {
    this.state = state;
    // heartbeats are answered without waking the object; their timestamp tells us who is still alive
    this.state.setWebSocketAutoResponse(new WebSocketRequestResponsePair('hb', 'hb'));
  }

  sockets() {
    const out = { host: null, guest: null };
    for (const ws of this.state.getWebSockets()) {
      const a = ws.deserializeAttachment() || {};
      if (a.role === 'host' || a.role === 'guest') {
        if (out[a.role]) { try { ws.close(1000, 'duplicate'); } catch (e) {} continue; }
        out[a.role] = ws;
      }
    }
    return out;
  }

  isStale(ws) {
    const a = ws.deserializeAttachment() || {};
    const last = this.state.getWebSocketAutoResponseTimestamp(ws);
    const seen = last ? last.getTime() : a.since || 0;
    return Date.now() - seen > HB_STALE_MS;
  }

  async fetch(req) {
    const url = new URL(req.url);
    const role = url.searchParams.get('role');
    const s = this.sockets();
    // drop hosts that silently vanished (killed app, dead network)
    if (s.host && this.isStale(s.host)) { try { s.host.close(1000, 'stale'); } catch (e) {} s.host = null; }

    if (url.pathname.endsWith('/check')) return json({ host: !!s.host, guest: !!s.guest });

    if (req.headers.get('Upgrade') !== 'websocket') return new Response('expected websocket', { status: 426, headers: CORS });
    if (role === 'host' && s.host) return new Response('room taken', { status: 409 });
    if (role === 'guest' && !s.host) return new Response('no such room', { status: 404 });
    if (role === 'guest' && s.guest) return new Response('room full', { status: 409 });
    if (role !== 'host' && role !== 'guest') return new Response('bad role', { status: 400 });

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.state.acceptWebSocket(server);
    server.serializeAttachment({ role, since: Date.now() });
    if (role === 'guest') { try { s.host.send(JSON.stringify({ t: 'joined' })); } catch (e) {} }
    return new Response(null, { status: 101, webSocket: client });
  }

  webSocketMessage(ws, msg) {
    if (typeof msg !== 'string' || msg.length > 16384) return;
    const a = ws.deserializeAttachment() || {};
    const s = this.sockets();
    const other = a.role === 'host' ? s.guest : s.host;
    if (other) { try { other.send(msg); } catch (e) {} }
  }

  webSocketClose(ws) { this.gone(ws); }
  webSocketError(ws) { this.gone(ws); }

  gone(ws) {
    const a = ws.deserializeAttachment() || {};
    try { ws.close(); } catch (e) {}
    const s = this.sockets();
    const other = a.role === 'host' ? s.guest : s.host;
    if (other && other !== ws) {
      try { other.send(JSON.stringify({ t: 'left' })); } catch (e) {}
      // the room dies with its host; a leaving guest just frees the slot
      if (a.role === 'host') { try { other.close(1000, 'host left'); } catch (e) {} }
    }
  }
}

async function turnCreds(env) {
  if (!env.TURN_KEY_ID || !env.TURN_TOKEN) return { iceServers: STUN, turn: false };
  try {
    const r = await fetch(`https://rtc.live.cloudflare.com/v1/turn/keys/${env.TURN_KEY_ID}/credentials/generate-ice-servers`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${env.TURN_TOKEN}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ ttl: 3 * 3600 }),
    });
    if (!r.ok) throw new Error('turn api ' + r.status);
    const j = await r.json();
    const servers = Array.isArray(j.iceServers) ? j.iceServers : [j.iceServers];
    return { iceServers: [...STUN, ...servers], turn: true };
  } catch (e) {
    return { iceServers: STUN, turn: false, error: String(e && e.message || e) };
  }
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (req.method === 'OPTIONS') return new Response(null, { headers: CORS });
    if (url.pathname === '/turn') return json(await turnCreds(env));
    const m = url.pathname.match(/^\/room\/([a-z0-9]{5})(\/check)?$/);
    if (m && CODE_RE.test(m[1])) {
      const id = env.ROOM.idFromName(m[1]);
      return env.ROOM.get(id).fetch(req);
    }
    return new Response('petuh-vs-kot relay: ok', { headers: { 'content-type': 'text/plain', ...CORS } });
  },
};
