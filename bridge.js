// bridge.js — OpenAI-compatible bridge agar 9Router bisa memakai Muse sebagai provider.
// Jalankan:  node bridge.js   (butuh file .token berisi API key, atau env BRIDGE_TOKEN)
// 9Router -> http://127.0.0.1:8765/v1  (API key = isi .token)
//
// Cara kerja: tiap request chat ditulis ke queue/pending/<id>.json,
// lalu bridge menunggu jawaban di queue/done/<id>.json (maks 240 detik).
// Penjawab (poller) mengambil antrean lewat endpoint /q/* .

const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const DIR = __dirname;
const PORT = parseInt(process.env.PORT || "8765", 10);
const PENDING = path.join(DIR, "queue", "pending");
const DONE = path.join(DIR, "queue", "done");
fs.mkdirSync(PENDING, { recursive: true });
fs.mkdirSync(DONE, { recursive: true });

function loadToken() {
  if (process.env.BRIDGE_TOKEN) return process.env.BRIDGE_TOKEN.trim();
  const f = path.join(DIR, ".token");
  if (fs.existsSync(f)) return fs.readFileSync(f, "utf8").trim();
  const t = crypto.randomBytes(32).toString("hex");
  fs.writeFileSync(f, t, { mode: 0o600 });
  console.log("Token baru dibuat dan disimpan di .token. Token ini = API key untuk 9Router.");
  return t;
}
const TOKEN = loadToken();
if (!TOKEN) { console.error("Token kosong."); process.exit(1); }

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function readBody(req) {
  return new Promise((resolve, reject) => {
    let b = "";
    req.on("data", (c) => { b += c; if (b.length > 2 * 1024 * 1024) req.destroy(); });
    req.on("end", () => resolve(b));
    req.on("error", reject);
  });
}
function send(res, code, obj) {
  const body = JSON.stringify(obj);
  res.writeHead(code, { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body) });
  res.end(body);
}
function needAuth(req, res) {
  const h = req.headers["authorization"] || "";
  if (h !== "Bearer " + TOKEN) { send(res, 401, { error: "unauthorized" }); return false; }
  return true;
}
function openAIResponse(id, content, model) {
  return {
    id: "chatcmpl-" + id,
    object: "chat.completion",
    created: Math.floor(Date.now() / 1000),
    model: model || "muse",
    choices: [{ index: 0, message: { role: "assistant", content }, finish_reason: "stop" }],
    usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
  };
}
async function waitDone(id, timeoutMs) {
  const f = path.join(DONE, id + ".json");
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    if (fs.existsSync(f)) {
      try {
        const d = JSON.parse(fs.readFileSync(f, "utf8"));
        fs.unlinkSync(f);
        return d.content || "";
      } catch { /* coba lagi */ }
    }
    await sleep(500);
  }
  return null;
}
function pendingIds() {
  return fs.readdirSync(PENDING)
    .filter((n) => n.endsWith(".json"))
    .map((n) => ({ n, t: fs.statSync(path.join(PENDING, n)).mtimeMs }))
    .sort((a, b) => a.t - b.t)
    .map((x) => x.n.slice(0, -5));
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, "http://x");
    const p = url.pathname;

    if (req.method === "GET" && p === "/health") return send(res, 200, { ok: true });
    if (req.method === "GET" && p === "/v1/models") {
      if (!needAuth(req, res)) return;
      return send(res, 200, { object: "list", data: [{ id: "muse", object: "model", owned_by: "muse" }] });
    }

    // ---- Queue API (dipakai poller penjawab) ----
    if (p === "/q/pending" && req.method === "GET") {
      if (!needAuth(req, res)) return;
      return send(res, 200, { ids: pendingIds() });
    }
    let m = p.match(/^\/q\/pending\/([A-Za-z0-9_-]+)$/);
    if (m) {
      if (!needAuth(req, res)) return;
      const id = m[1], f = path.join(PENDING, id + ".json");
      if (req.method === "GET") {
        if (!fs.existsSync(f)) return send(res, 404, { error: "not found" });
        return send(res, 200, JSON.parse(fs.readFileSync(f, "utf8")));
      }
      if (req.method === "DELETE") {
        if (fs.existsSync(f)) fs.unlinkSync(f);
        return send(res, 200, { ok: true });
      }
    }
    m = p.match(/^\/q\/done\/([A-Za-z0-9_-]+)$/);
    if (m && req.method === "POST") {
      if (!needAuth(req, res)) return;
      const body = JSON.parse(await readBody(req));
      fs.writeFileSync(path.join(DONE, m[1] + ".json"), JSON.stringify({ content: body.content || "" }));
      const pf = path.join(PENDING, m[1] + ".json");
      if (fs.existsSync(pf)) fs.unlinkSync(pf);
      return send(res, 200, { ok: true });
    }

    // ---- OpenAI-compatible chat ----
    if (p === "/v1/chat/completions" && req.method === "POST") {
      if (!needAuth(req, res)) return;
      const body = JSON.parse(await readBody(req));
      const messages = body.messages || [];
      const model = body.model || "muse";
      const stream = !!body.stream;

      // Probe dashboard 9Router ("hi") -> jawab instan
      if (messages.length === 1 && messages[0].role === "user" &&
          typeof messages[0].content === "string" && messages[0].content.trim() === "hi") {
        const r = openAIResponse("probe", "ok", model);
        if (stream) {
          res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache", "Connection": "keep-alive" });
          res.end(`data: ${JSON.stringify({ choices: [{ delta: { content: "ok" } }] })}\n\ndata: [DONE]\n\n`);
          return;
        }
        return send(res, 200, r);
      }

      const id = crypto.randomBytes(8).toString("hex");
      fs.writeFileSync(path.join(PENDING, id + ".json"),
        JSON.stringify({ id, received_at: Math.floor(Date.now() / 1000), request: body }));

      if (stream) {
        res.writeHead(200, { "Content-Type": "text/event-stream", "Cache-Control": "no-cache", "Connection": "keep-alive" });
        const t0 = Date.now();
        let answered = false;
        const iv = setInterval(async () => {
          const f = path.join(DONE, id + ".json");
          if (fs.existsSync(f)) {
            clearInterval(iv); answered = true;
            let content = "";
            try { content = JSON.parse(fs.readFileSync(f, "utf8")).content || ""; fs.unlinkSync(f); } catch {}
            res.write(`data: ${JSON.stringify({ choices: [{ delta: { content } }] })}\n\n`);
            res.end("data: [DONE]\n\n");
          } else if (Date.now() - t0 > 240000) {
            clearInterval(iv); answered = true;
            res.end(`data: ${JSON.stringify({ error: "timeout" })}\n\ndata: [DONE]\n\n`);
          } else {
            res.write(": keepalive\n\n");
          }
        }, 1000);
        req.on("close", () => { if (!answered) clearInterval(iv); });
        return;
      }

      const content = await waitDone(id, 240000);
      if (content === null) return send(res, 504, { error: "timeout menunggu jawaban" });
      return send(res, 200, openAIResponse(id, content, model));
    }

    return send(res, 404, { error: "not found" });
  } catch (e) {
    try { send(res, 500, { error: String(e && e.message || e) }); } catch {}
  }
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`bridge jalan di http://127.0.0.1:${PORT}  (Base URL 9Router: http://127.0.0.1:${PORT}/v1)`);
  console.log("PENTING: biarkan jendela ini terbuka selama 9Router dipakai.");
});
