const http = require("http");
const { WebSocketServer } = require("ws");
const srv = http.createServer((q, s) => { s.writeHead(200); s.end("fish server ok"); });
const wss = new WebSocketServer({ server: srv, maxPayload: 16384 });
const peers = new Map();
let n = 0;
function bc(o, except) {
  const s = JSON.stringify(o);
  for (const v of peers.values()) if (v.ws !== except && v.ws.readyState === 1) v.ws.send(s);
}
wss.on("connection", (ws) => {
  if (peers.size >= 40) { ws.close(); return; }
  const id = "p" + (++n).toString(36) + Math.random().toString(36).slice(2, 5);
  const me = { ws, p: null, cnt: 0, win: Date.now() };
  peers.set(id, me);
  const others = [...peers].filter(([k, v]) => k !== id && v.p).map(([k, v]) => ({ peer: k, presence: v.p }));
  ws.send(JSON.stringify({ t: "hello", id, peers: others }));
  ws.on("message", (raw) => {
    const now = Date.now();
    if (now - me.win > 1000) { me.win = now; me.cnt = 0; }
    if (++me.cnt > 120) return;
    let m; try { m = JSON.parse(raw); } catch { return; }
    if (m.t === "p" && m.d && typeof m.d === "object") { me.p = m.d; bc({ t: "p", id, d: m.d }, ws); }
    else if (m.t === "e" && typeof m.topic === "string" && m.topic.length < 20) bc({ t: "e", from: id, topic: m.topic, data: m.data }, ws);
  });
  ws.on("close", () => { peers.delete(id); bc({ t: "left", id }); });
});
srv.listen(process.env.PORT || 8080, () => console.log("listening"));
