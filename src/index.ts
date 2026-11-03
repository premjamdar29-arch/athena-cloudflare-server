export interface Env {
  ATHENA_DATA: DurableObjectNamespace;
  ATHENA_API_TOKEN: string;
  ATHENA_SESSION_ENCRYPTION_KEY: string;
  ALLOWED_ORIGINS: string;
}

type QualitySource = "LIVE_SIDE_ESTIMATE" | "BROKER_OHLCV" | "OHLCV_ESTIMATE";
type Cluster = [price: number, bidVolume: number, askVolume: number];

interface MarketBar {
  exchangeType: number;
  token: string;
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number | null;
  delta?: number | null;
  clusters?: Cluster[];
  source: QualitySource;
  exchangeTimeMs?: number | null;
  receivedAtMs?: number | null;
}

interface FeedInstrument {
  exchangeType: number;
  token: string;
  priceStep: number;
}

interface FeedSession {
  apiKey: string;
  clientCode: string;
  jwtToken: string;
  feedToken: string;
  instruments: FeedInstrument[];
}

interface LiveBuild {
  bar: MarketBar;
  clusters: Map<number, [number, number]>;
  lastCumulativeVolume: number;
  lastPrice: number;
  tickDirection: number;
  lastSequence: number;
}

const MAX_BODY_BYTES = 1_000_000;
const MAX_BATCH_BARS = 500;
const EXCHANGES = new Set([1, 2, 3, 4, 5, 7, 13]);
const SOURCES = new Set<QualitySource>(["LIVE_SIDE_ESTIMATE", "BROKER_OHLCV", "OHLCV_ESTIMATE"]);
const MAX_FEED_INSTRUMENTS = 180;
const HISTORY_SHARD_COUNT = 10;

function validFeedInstrument(value: unknown): value is FeedInstrument {
  if (!value || typeof value !== "object") return false;
  const instrument = value as Partial<FeedInstrument>;
  return Number.isInteger(instrument.exchangeType) && EXCHANGES.has(instrument.exchangeType!) &&
    typeof instrument.token === "string" && /^\d{1,24}$/.test(instrument.token) &&
    typeof instrument.priceStep === "number" && Number.isFinite(instrument.priceStep) && instrument.priceStep > 0 && instrument.priceStep <= 1000;
}

function json(data: unknown, status = 200, headers?: HeadersInit): Response {
  const mergedHeaders = new Headers(headers);
  mergedHeaders.set("content-type", "application/json; charset=utf-8");
  return new Response(JSON.stringify(data), {
    status,
    headers: mergedHeaders,
  });
}

function relay(response: Response, extraHeaders: Headers): Response {
  const headers = new Headers(response.headers);
  extraHeaders.forEach((value, key) => headers.set(key, value));
  return new Response(response.body, { status: response.status, statusText: response.statusText, headers });
}

function error(code: string, message: string, status: number): Response {
  return json({ error: { code, message } }, status);
}

function originHeaders(request: Request, env: Env): Headers {
  const origin = request.headers.get("Origin");
  const allowed = (env.ALLOWED_ORIGINS || "").split(",").map((s) => s.trim()).filter(Boolean);
  const headers = new Headers({
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Authorization, Content-Type",
    "Access-Control-Max-Age": "600",
    "Vary": "Origin",
  });
  if (allowed.includes("*") && origin) headers.set("Access-Control-Allow-Origin", "*");
  else if (origin && allowed.includes(origin)) headers.set("Access-Control-Allow-Origin", origin);
  return headers;
}

function authorized(request: Request, env: Env): boolean {
  const header = request.headers.get("Authorization") || "";
  return Boolean(env.ATHENA_API_TOKEN) && header === `Bearer ${env.ATHENA_API_TOKEN}`;
}

async function readJson(request: Request): Promise<unknown> {
  const length = Number(request.headers.get("content-length") || 0);
  if (length > MAX_BODY_BYTES) throw new Error("Request body is too large.");
  const text = await request.text();
  if (new TextEncoder().encode(text).byteLength > MAX_BODY_BYTES) throw new Error("Request body is too large.");
  return JSON.parse(text);
}

function validBar(value: unknown): value is MarketBar {
  if (!value || typeof value !== "object") return false;
  const bar = value as Partial<MarketBar>;
  if (!Number.isInteger(bar.exchangeType) || !EXCHANGES.has(bar.exchangeType!)) return false;
  if (typeof bar.token !== "string" || !/^\d{1,24}$/.test(bar.token)) return false;
  if (!Number.isInteger(bar.time) || bar.time! < 946684800 || bar.time! > 4102444800) return false;
  if (![bar.open, bar.high, bar.low, bar.close].every((n) => typeof n === "number" && Number.isFinite(n) && n > 0)) return false;
  if (bar.low! > Math.min(bar.open!, bar.close!) || bar.high! < Math.max(bar.open!, bar.close!) || bar.low! > bar.high!) return false;
  if (bar.volume !== null && bar.volume !== undefined && (!Number.isFinite(bar.volume) || bar.volume < 0)) return false;
  if (bar.delta !== undefined && bar.delta !== null && !Number.isFinite(bar.delta)) return false;
  if (bar.exchangeTimeMs !== undefined && bar.exchangeTimeMs !== null && (!Number.isFinite(bar.exchangeTimeMs) || bar.exchangeTimeMs < 946684800000 || bar.exchangeTimeMs > 4102444800000)) return false;
  if (bar.receivedAtMs !== undefined && bar.receivedAtMs !== null && (!Number.isFinite(bar.receivedAtMs) || bar.receivedAtMs < 946684800000 || bar.receivedAtMs > 4102444800000)) return false;
  if (!SOURCES.has(bar.source as QualitySource)) return false;
  if (bar.clusters !== undefined && (!Array.isArray(bar.clusters) || bar.clusters.length > 500 ||
      !bar.clusters.every((row) => Array.isArray(row) && row.length === 3 && row.every(Number.isFinite) && row[0] > 0 && row[1] >= 0 && row[2] >= 0))) return false;
  return true;
}

function coordinator(env: Env): DurableObjectStub {
  return env.ATHENA_DATA.get(env.ATHENA_DATA.idFromName("primary"));
}

function historyShard(env: Env, index: number): DurableObjectStub {
  return env.ATHENA_DATA.get(env.ATHENA_DATA.idFromName(`history-${index}`));
}

function historyShardIndex(exchangeType: number, token: string): number {
  let hash = 2166136261;
  for (const char of `${exchangeType}:${token}`) hash = Math.imul(hash ^ char.charCodeAt(0), 16777619);
  return (hash >>> 0) % HISTORY_SHARD_COUNT;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const headers = originHeaders(request, env);
    if (request.method === "OPTIONS") return new Response(null, { status: 204, headers });
    const url = new URL(request.url);
    if (url.pathname === "/health" && request.method === "GET") {
      return json({ ok: true, service: "athena-market-data", timeZone: "Asia/Kolkata" }, 200, headers);
    }
    if (!authorized(request, env)) return error("UNAUTHORIZED", "A valid ATHENA server token is required.", 401);

    const store = coordinator(env);
    try {
      if (url.pathname === "/v1/collector/status" && request.method === "GET") {
        const response = await store.fetch("https://internal/status");
        return relay(response, headers);
      }
      if (url.pathname === "/v1/collector/start" && request.method === "POST") {
        const body = await readJson(request) as Partial<FeedSession>;
        if (![body.apiKey, body.clientCode, body.jwtToken, body.feedToken].every((value) => typeof value === "string" && value.length > 0 && value.length <= 4096) ||
            !Array.isArray(body.instruments) || body.instruments.length < 1 || body.instruments.length > MAX_FEED_INSTRUMENTS ||
            !body.instruments.every(validFeedInstrument)) {
          return error("INVALID_COLLECTOR_SESSION", `Supply a valid daily SmartAPI session and 1–${MAX_FEED_INSTRUMENTS} supported instruments.`, 422);
        }
        const response = await store.fetch("https://internal/collector/start", {
          method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body),
        });
        return relay(response, headers);
      }
      if (url.pathname === "/v1/collector/subscriptions" && request.method === "PUT") {
        const body = await readJson(request) as { instruments?: unknown };
        if (!Array.isArray(body?.instruments) || body.instruments.length < 1 || body.instruments.length > MAX_FEED_INSTRUMENTS ||
            !body.instruments.every(validFeedInstrument)) {
          return error("INVALID_INSTRUMENTS", `Supply 1–${MAX_FEED_INSTRUMENTS} supported instruments.`, 422);
        }
        const response = await store.fetch("https://internal/collector/subscriptions", {
          method: "PUT", headers: { "content-type": "application/json" }, body: JSON.stringify(body),
        });
        return relay(response, headers);
      }
      if (url.pathname === "/v1/collector/stop" && request.method === "POST") {
        const response = await store.fetch("https://internal/collector/stop", { method: "POST" });
        return relay(response, headers);
      }
      if (url.pathname === "/v1/time" && request.method === "GET") {
        const now = Date.now();
        return json({ serverTimeMs: now, serverTimeUtc: new Date(now).toISOString(), timeZone: "UTC" }, 200, headers);
      }
      if (url.pathname === "/v1/market/bars" && request.method === "POST") {
        const body = await readJson(request) as { bars?: unknown };
        if (!Array.isArray(body?.bars) || body.bars.length < 1 || body.bars.length > MAX_BATCH_BARS || !body.bars.every(validBar)) {
          return error("INVALID_BARS", `Send 1–${MAX_BATCH_BARS} valid bars per request.`, 422);
        }
        const response = await store.fetch("https://internal/bars", {
          method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body),
        });
        return relay(response, headers);
      }
      if (url.pathname === "/v1/stream-ticket" && request.method === "POST") {
        const response = await store.fetch("https://internal/ticket", { method: "POST" });
        return relay(response, headers);
      }
      if (url.pathname === "/v1/stream" && request.method === "GET") {
        const ticket = url.searchParams.get("ticket") || "";
        if (!/^[0-9a-f-]{36}$/i.test(ticket)) return error("INVALID_TICKET", "Request a short-lived stream ticket first.", 401);
        const internal = new URL("https://internal/stream");
        internal.searchParams.set("ticket", ticket);
        const response = await store.fetch(new Request(internal, request));
        return response;
      }
      if (url.pathname === "/v1/market/history" && request.method === "GET") {
        const exchangeType = Number(url.searchParams.get("exchangeType"));
        const token = url.searchParams.get("token") || "";
        const from = Number(url.searchParams.get("from") || 0);
        const to = Number(url.searchParams.get("to") || 4102444800);
        if (!EXCHANGES.has(exchangeType) || !/^\d{1,24}$/.test(token) || !Number.isInteger(from) || !Number.isInteger(to) || from > to) {
          return error("INVALID_RANGE", "Provide exchangeType, token, and a valid epoch-second range.", 422);
        }
        const internal = new URL("https://internal/history");
        internal.searchParams.set("exchangeType", String(exchangeType));
        internal.searchParams.set("token", token);
        internal.searchParams.set("from", String(from));
        internal.searchParams.set("to", String(to));
        const response = await store.fetch(internal);
        return relay(response, headers);
      }
      return error("NOT_FOUND", "Endpoint not found.", 404);
    } catch (cause) {
      if (cause instanceof SyntaxError) return error("INVALID_JSON", "Request body must be valid JSON.", 400);
      if (cause instanceof Error && cause.message === "Request body is too large.") return error("BODY_TOO_LARGE", "Request body exceeds the 1 MB limit.", 413);
      return error("REQUEST_FAILED", "The request could not be completed.", 500);
    }
  },

  async scheduled(_event: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    const store = coordinator(env);
    ctx.waitUntil(store.fetch("https://internal/collector/ensure", { method: "POST" }));
    if (_event.cron === "30 11 * * *") ctx.waitUntil((async () => {
      const archived = await store.fetch("https://internal/archive", { method: "POST" });
      if (!archived.ok) throw new Error(`Archive failed with HTTP ${archived.status}`);
      const pruned = await store.fetch("https://internal/prune", { method: "POST" });
      if (!pruned.ok) throw new Error(`Retention failed with HTTP ${pruned.status}`);
    })());
  },
};

export class AthenaDataStore {
  private readonly sql: SqlStorage;
  private lastIngestAt: number | null = null;
  private feedSocket: WebSocket | null = null;
  private feedSession: FeedSession | null = null;
  private readonly liveBuilds = new Map<string, LiveBuild>();
  private feedInstruments: FeedInstrument[] = [];
  private lastStatusPersistAt = 0;

  constructor(private readonly state: DurableObjectState, private readonly env: Env) {
    this.sql = state.storage.sql;
    this.sql.exec(`CREATE TABLE IF NOT EXISTS bars (
      exchange_type INTEGER NOT NULL,
      token TEXT NOT NULL,
      start_time INTEGER NOT NULL,
      session_date TEXT NOT NULL,
      source TEXT NOT NULL,
      body TEXT NOT NULL,
      PRIMARY KEY (exchange_type, token, start_time)
    )`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS stream_tickets (
      ticket TEXT PRIMARY KEY,
      expires_at INTEGER NOT NULL
    )`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS server_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS history_sessions (session_date TEXT PRIMARY KEY)`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS history_archives (
      exchange_type INTEGER NOT NULL,
      token TEXT NOT NULL,
      session_date TEXT NOT NULL,
      payload BLOB NOT NULL,
      PRIMARY KEY (exchange_type, token, session_date)
    )`);
    this.sql.exec(`CREATE INDEX IF NOT EXISTS bars_by_session ON bars(session_date, exchange_type, token)`);
    this.sql.exec(`CREATE INDEX IF NOT EXISTS archives_by_contract ON history_archives(exchange_type, token, session_date)`);
    this.state.setWebSocketAutoResponse(new WebSocketRequestResponsePair("ping", "pong"));
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/status") {
      const meta = new Map([...this.sql.exec<{ key: string; value: string }>("SELECT key, value FROM server_meta")].map((row) => [row.key, row.value]));
      const lastIngestAt = Number(meta.get("lastIngestAt") || 0) || null;
      const clientEventAgeMs = meta.has("clientEventAgeMs") ? Number(meta.get("clientEventAgeMs")) : null;
      const serverEventAgeMs = meta.has("serverEventAgeMs") ? Number(meta.get("serverEventAgeMs")) : null;
      const feedState = meta.get("feedState") || "AUTH_REQUIRED";
      return json({ state: feedState === "CONNECTED" ? (lastIngestAt ? "RECEIVING" : "WAITING_FOR_TICKS") : feedState,
        source: feedState === "CONNECTED" ? "ANGELONE_CLOUD_FEED" : null,
        feedState, lastIngestAt, feedLastMessageAt: Number(meta.get("feedLastMessageAt") || 0) || null,
        feedError: meta.get("feedError") || null,
        feedFreshnessMs: Number(meta.get("feedLastMessageAt") || lastIngestAt || 0) ? Date.now() - Number(meta.get("feedLastMessageAt") || lastIngestAt) : null,
        lastClientEventAgeMs: clientEventAgeMs, lastServerEventAgeMs: serverEventAgeMs,
        serverTimeMs: Date.now(), orderExecution: "NOT_SUPPORTED" });
    }
    if (url.pathname === "/collector/start" && request.method === "POST") return this.startCollector(request);
    if (url.pathname === "/collector/subscriptions" && request.method === "PUT") return this.updateSubscriptions(request);
    if (url.pathname === "/collector/stop" && request.method === "POST") return this.stopCollector();
    if (url.pathname === "/collector/ensure" && request.method === "POST") return this.ensureCollector();
    if (url.pathname === "/ticket" && request.method === "POST") {
      this.sql.exec("DELETE FROM stream_tickets WHERE expires_at < ?", Date.now());
      const ticket = crypto.randomUUID();
      this.sql.exec("INSERT INTO stream_tickets(ticket, expires_at) VALUES (?, ?)", ticket, Date.now() + 30_000);
      return json({ ticket, expiresInSeconds: 30, websocketPath: "/v1/stream" }, 201);
    }
    if (url.pathname === "/stream" && request.method === "GET") return this.openStream(request, url);
    if (url.pathname === "/bars" && request.method === "POST") return this.ingest(request);
    if (url.pathname === "/history" && request.method === "GET") return this.history(url);
    if (url.pathname === "/history/archive" && request.method === "POST") return this.storeHistoryArchive(request, url);
    if (url.pathname === "/history/query" && request.method === "GET") return this.readHistoryArchive(url);
    if (url.pathname === "/history/prune" && request.method === "POST") return this.pruneHistoryArchive(url);
    if (url.pathname === "/archive" && request.method === "POST") return this.archive();
    if (url.pathname === "/prune" && request.method === "POST") return this.prune();
    return error("NOT_FOUND", "Internal endpoint not found.", 404);
  }

  async alarm(): Promise<void> {
    await this.ensureCollector();
    if (["AUTH_REQUIRED", "STOPPED"].includes(this.meta("feedState") || "")) {
      await this.state.storage.deleteAlarm();
      return;
    }
    if (this.feedSocket?.readyState === WebSocket.OPEN) {
      this.feedSocket.send("ping");
      await this.state.storage.setAlarm(Date.now() + 15_000);
    } else await this.state.storage.setAlarm(Date.now() + 5_000);
  }

  private meta(key: string): string | null {
    return [...this.sql.exec<{ value: string }>("SELECT value FROM server_meta WHERE key = ?", key)][0]?.value ?? null;
  }

  private setMeta(key: string, value: string): void {
    this.sql.exec(`INSERT INTO server_meta(key, value) VALUES (?, ?)
      ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value);
  }

  private async startCollector(request: Request): Promise<Response> {
    const session = await request.json() as FeedSession;
    this.feedSession = session;
    this.feedInstruments = session.instruments;
    this.setMeta("encryptedFeedSession", await encryptSecret(JSON.stringify(session), this.env.ATHENA_SESSION_ENCRYPTION_KEY));
    this.setMeta("feedInstruments", JSON.stringify(session.instruments));
    this.setMeta("feedError", "");
    this.setMeta("feedState", "CONNECTING");
    this.liveBuilds.clear();
    if (this.feedSocket && this.feedSocket.readyState < WebSocket.CLOSING) this.feedSocket.close(1000, "ATHENA session refreshed");
    this.feedSocket = null;
    await this.connectCollector();
    const state = this.meta("feedState") || "ERROR";
    return json({ state, subscriptions: session.instruments.length, appMayClose: state === "CONNECTED" }, state === "CONNECTED" ? 200 : 202);
  }

  private async updateSubscriptions(request: Request): Promise<Response> {
    const body = await request.json() as { instruments: FeedInstrument[] };
    const encrypted = this.meta("encryptedFeedSession");
    if (!encrypted) return error("SESSION_REQUIRED", "Log in to Angel One in ATHENA first.", 409);
    const session = JSON.parse(await decryptSecret(encrypted, this.env.ATHENA_SESSION_ENCRYPTION_KEY)) as FeedSession;
    session.instruments = body.instruments;
    this.feedSession = session;
    this.feedInstruments = session.instruments;
    this.setMeta("encryptedFeedSession", await encryptSecret(JSON.stringify(session), this.env.ATHENA_SESSION_ENCRYPTION_KEY));
    this.setMeta("feedInstruments", JSON.stringify(session.instruments));
    this.liveBuilds.clear();
    if (this.feedSocket && this.feedSocket.readyState < WebSocket.CLOSING) this.feedSocket.close(1000, "Subscriptions updated");
    this.feedSocket = null;
    await this.connectCollector();
    return json({ state: this.meta("feedState"), subscriptions: session.instruments.length });
  }

  private async stopCollector(): Promise<Response> {
    if (this.feedSocket && this.feedSocket.readyState < WebSocket.CLOSING) this.feedSocket.close(1000, "Collector stopped by ATHENA");
    this.feedSocket = null;
    this.feedSession = null;
    this.feedInstruments = [];
    this.liveBuilds.clear();
    this.sql.exec("DELETE FROM server_meta WHERE key IN ('encryptedFeedSession', 'feedInstruments')");
    this.setMeta("feedState", "STOPPED");
    this.setMeta("feedError", "");
    await this.state.storage.deleteAlarm();
    return json({ state: "STOPPED" });
  }

  private async ensureCollector(): Promise<Response> {
    const encrypted = this.meta("encryptedFeedSession");
    if (!encrypted) {
      this.setMeta("feedState", "AUTH_REQUIRED");
      return json({ state: "AUTH_REQUIRED" });
    }
    if (this.meta("feedState") === "AUTH_REQUIRED") return json({ state: "AUTH_REQUIRED" });
    if (this.feedSocket?.readyState === WebSocket.OPEN) return json({ state: "CONNECTED" });
    try {
      this.feedSession = JSON.parse(await decryptSecret(encrypted, this.env.ATHENA_SESSION_ENCRYPTION_KEY)) as FeedSession;
      this.feedInstruments = JSON.parse(this.meta("feedInstruments") || "[]") as FeedInstrument[];
      await this.connectCollector();
      return json({ state: this.meta("feedState") });
    } catch {
      this.setMeta("feedState", "ERROR");
      this.setMeta("feedError", "Could not restore the encrypted Angel One feed session.");
      return error("FEED_RESTORE_FAILED", "Could not restore the encrypted feed session.", 500);
    }
  }

  private async connectCollector(): Promise<void> {
    const session = this.feedSession;
    if (!session || !this.feedInstruments.length) throw new Error("Angel One session and instrument subscriptions are required.");
    this.setMeta("feedState", "CONNECTING");
    try {
      const response = await fetch("https://smartapisocket.angelone.in/smart-stream", {
        headers: {
          Upgrade: "websocket",
          Authorization: session.jwtToken,
          "x-api-key": session.apiKey,
          "x-client-code": session.clientCode,
          "x-feed-token": session.feedToken,
        },
      });
      if (response.status !== 101 || !response.webSocket) {
        const status = response.status;
        this.setMeta("feedState", status === 401 || status === 403 ? "AUTH_REQUIRED" : "DISCONNECTED");
        this.setMeta("feedError", status === 401 || status === 403 ? "Angel One rejected or expired the feed session. Log in again in ATHENA." : `Angel One feed connection failed (HTTP ${status}).`);
        if (status !== 401 && status !== 403) await this.state.storage.setAlarm(Date.now() + 5_000);
        return;
      }
      const socket = response.webSocket;
      socket.accept();
      socket.binaryType = "arraybuffer";
      this.feedSocket = socket;
      socket.addEventListener("message", (event) => {
        void this.handleFeedMessage(event.data).catch(() => this.setMeta("feedError", "A market-data packet could not be processed."));
      });
      socket.addEventListener("close", () => {
        if (this.feedSocket !== socket) return;
        this.feedSocket = null;
        if (this.meta("feedState") !== "STOPPED") {
          this.setMeta("feedState", "RECONNECTING");
          void this.state.storage.setAlarm(Date.now() + 5_000);
        }
      });
      socket.addEventListener("error", () => {
        if (this.feedSocket === socket) {
          this.setMeta("feedState", "RECONNECTING");
          this.setMeta("feedError", "Angel One feed connection was interrupted; reconnecting.");
          void this.state.storage.setAlarm(Date.now() + 5_000);
        }
      });
      socket.send(JSON.stringify({
        correlationID: "ATHENAFEED1", action: 1,
        params: { mode: 3, tokenList: groupTokens(this.feedInstruments) },
      }));
      this.setMeta("feedState", "CONNECTED");
      this.setMeta("feedError", "");
      this.setMeta("feedConnectedAt", String(Date.now()));
      await this.state.storage.setAlarm(Date.now() + 15_000);
    } catch {
      this.feedSocket = null;
      this.setMeta("feedState", "RECONNECTING");
      this.setMeta("feedError", "Could not connect to Angel One; retrying automatically.");
      await this.state.storage.setAlarm(Date.now() + 5_000);
      await this.state.storage.setAlarm(Date.now() + 5_000);
    }
  }

  private async handleFeedMessage(data: string | ArrayBuffer): Promise<void> {
    const receivedAtMs = Date.now();
    this.setMeta("feedLastMessageAt", String(receivedAtMs));
    if (typeof data === "string") {
      const control = data.trim().toLowerCase();
      if (control === "pong") return;
      if (control === "ping") { this.feedSocket?.send("pong"); return; }
      // Control responses are intentionally not logged; they may contain account or session details.
      return;
    }
    const bytes = new Uint8Array(data);
    if (bytes.byteLength < 51) return;
    const view = new DataView(data);
    const mode = view.getUint8(0);
    const exchangeType = view.getUint8(1);
    if (mode !== 3 || !EXCHANGES.has(exchangeType) || bytes.byteLength < 379) return;
    const token = decodeToken(bytes.subarray(2, 27));
    if (!token) return;
    const instrument = this.feedInstruments.find((item) => item.exchangeType === exchangeType && item.token === token);
    if (!instrument) return;

    const sequence = Number(view.getBigInt64(27, true));
    const exchangeTimeMs = Number(view.getBigInt64(35, true));
    const price = Number(view.getBigInt64(43, true)) / 100;
    const cumulativeVolume = Number(view.getBigInt64(67, true));
    if (!Number.isFinite(exchangeTimeMs) || exchangeTimeMs < 946684800000 || exchangeTimeMs > receivedAtMs + 60_000 ||
        !Number.isFinite(price) || price <= 0 || !Number.isFinite(cumulativeVolume) || cumulativeVolume < 0) return;

    const key = `${exchangeType}:${token}`;
    let build = this.liveBuilds.get(key);
    const minute = Math.floor(exchangeTimeMs / 60_000) * 60;
    if (build && minute < build.bar.time) return;
    let priorVolume: number | undefined;
    let priorPrice: number | undefined;
    let priorSequence: number | undefined;
    let priorDirection = 1;
    if (build && minute > build.bar.time) {
      priorVolume = build.lastCumulativeVolume;
      priorPrice = build.lastPrice;
      priorSequence = build.lastSequence;
      priorDirection = build.tickDirection;
      const completed = this.completedBar(build, receivedAtMs);
      await this.ingestBars([completed]);
      build = undefined;
    }
    if (!build) {
      build = {
        bar: { exchangeType, token, time: minute, open: price, high: price, low: price, close: price,
          volume: 0, delta: 0, clusters: [], source: "LIVE_SIDE_ESTIMATE", exchangeTimeMs, receivedAtMs },
        clusters: new Map(), lastCumulativeVolume: priorVolume ?? cumulativeVolume, lastPrice: priorPrice ?? price,
        tickDirection: priorDirection, lastSequence: priorSequence ?? sequence - 1,
      };
      this.liveBuilds.set(key, build);
    }

    const tradeQuantity = sequence === build.lastSequence ? 0 : Math.max(0, cumulativeVolume - build.lastCumulativeVolume);
    const previousPrice = build.lastPrice;
    build.bar.high = Math.max(build.bar.high, price);
    build.bar.low = Math.min(build.bar.low, price);
    build.bar.close = price;
    build.bar.exchangeTimeMs = exchangeTimeMs;
    build.bar.receivedAtMs = receivedAtMs;
    if (tradeQuantity > 0) {
      let bestBid = 0;
      let bestAsk = 0;
      // SmartAPI V2 Snap Quote depth flags follow the SDK's binary decoder mapping.
      for (let i = 0; i < 10; i++) {
        const offset = 147 + i * 20;
        const flag = view.getUint16(offset, true);
        const depthPrice = Number(view.getBigInt64(offset + 10, true)) / 100;
        if (depthPrice <= 0) continue;
        if (flag === 1) bestBid = Math.max(bestBid, depthPrice);
        else if (flag === 0) bestAsk = bestAsk === 0 ? depthPrice : Math.min(bestAsk, depthPrice);
      }
      if (bestAsk > 0 && price >= bestAsk) build.tickDirection = 1;
      else if (bestBid > 0 && price <= bestBid) build.tickDirection = -1;
      else if (price > previousPrice) build.tickDirection = 1;
      else if (price < previousPrice) build.tickDirection = -1;
      const bucket = Math.round(price / instrument.priceStep) * instrument.priceStep;
      const cluster = build.clusters.get(bucket) || [0, 0];
      cluster[build.tickDirection > 0 ? 1 : 0] += tradeQuantity;
      build.clusters.set(bucket, cluster);
      build.bar.volume = (build.bar.volume || 0) + tradeQuantity;
      build.bar.delta = (build.bar.delta || 0) + tradeQuantity * build.tickDirection;
      build.bar.clusters = [...build.clusters.entries()].sort((a, b) => b[0] - a[0]).map(([p, v]) => [p, v[0], v[1]]);
    }
    build.lastCumulativeVolume = cumulativeVolume;
    build.lastPrice = price;
    build.lastSequence = sequence;
    if (receivedAtMs - this.lastStatusPersistAt >= 30_000) {
      this.setMeta("serverEventAgeMs", String(Math.max(-60_000, receivedAtMs - exchangeTimeMs)));
      this.setMeta("feedState", "CONNECTED");
      this.setMeta("feedError", "");
      this.setMeta("lastIngestAt", String(receivedAtMs));
      this.setMeta("feedLastMessageAt", String(receivedAtMs));
      this.lastStatusPersistAt = receivedAtMs;
    }
    this.broadcast({ type: "MARKET_BARS", receivedAtMs, bars: [this.completedBar(build, receivedAtMs)], provisional: true });
  }

  private completedBar(build: LiveBuild, receivedAtMs: number): MarketBar {
    return { ...build.bar, clusters: build.bar.clusters?.map((row) => [...row] as Cluster), receivedAtMs };
  }

  private async openStream(request: Request, url: URL): Promise<Response> {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") return error("UPGRADE_REQUIRED", "WebSocket upgrade required.", 426);
    const ticket = url.searchParams.get("ticket") || "";
    const found = [...this.sql.exec<{ expires_at: number }>("SELECT expires_at FROM stream_tickets WHERE ticket = ?", ticket)];
    this.sql.exec("DELETE FROM stream_tickets WHERE expires_at < ?", Date.now());
    if (!found.length || found[0].expires_at < Date.now()) return error("INVALID_TICKET", "Stream ticket expired or was already used.", 401);
    this.sql.exec("DELETE FROM stream_tickets WHERE ticket = ?", ticket);
    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    this.state.acceptWebSocket(server);
    return new Response(null, { status: 101, webSocket: client });
  }

  private async ingest(request: Request): Promise<Response> {
    const body = await request.json() as { bars?: MarketBar[] };
    const bars = body.bars || [];
    return this.ingestBars(bars);
  }

  private async ingestBars(bars: MarketBar[]): Promise<Response> {
    const now = Date.now();
    for (const bar of bars) {
      const sessionDate = istDate(bar.time * 1000);
      this.sql.exec(`INSERT INTO bars(exchange_type, token, start_time, session_date, source, body)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(exchange_type, token, start_time) DO UPDATE SET
        session_date=excluded.session_date, source=excluded.source, body=excluded.body`,
        bar.exchangeType, bar.token, bar.time, sessionDate, bar.source, JSON.stringify(bar));
      if (bar.exchangeTimeMs && Number.isFinite(bar.exchangeTimeMs)) {
        const serverAge = now - bar.exchangeTimeMs;
        if (serverAge > -60_000 && serverAge < 86_400_000) {
          this.sql.exec(`INSERT INTO server_meta(key, value) VALUES ('serverEventAgeMs', ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value`, String(serverAge));
          if (bar.receivedAtMs !== undefined && bar.receivedAtMs !== null) {
            this.sql.exec(`INSERT INTO server_meta(key, value) VALUES ('clientEventAgeMs', ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value`, String(bar.receivedAtMs - bar.exchangeTimeMs));
          }
        }
      }
    }
    this.lastIngestAt = now;
    this.sql.exec(`INSERT INTO server_meta(key, value) VALUES ('lastIngestAt', ?)
      ON CONFLICT(key) DO UPDATE SET value=excluded.value`, String(now));
    this.broadcast({ type: "MARKET_BARS", receivedAtMs: now, bars });
    const serverEventAgeMs = [...this.sql.exec<{ value: string }>("SELECT value FROM server_meta WHERE key = 'serverEventAgeMs'")][0]?.value ?? null;
    const clientEventAgeMs = [...this.sql.exec<{ value: string }>("SELECT value FROM server_meta WHERE key = 'clientEventAgeMs'")][0]?.value ?? null;
    return json({ accepted: bars.length, serverReceivedAtMs: now,
      lastClientEventAgeMs: clientEventAgeMs !== null ? Number(clientEventAgeMs) : null,
      lastServerEventAgeMs: serverEventAgeMs !== null ? Number(serverEventAgeMs) : null }, 202);
  }

  private broadcast(message: unknown): void {
    const payload = JSON.stringify(message);
    for (const socket of this.state.getWebSockets()) {
      try { socket.send(payload); } catch { socket.close(1011, "Stream delivery failed"); }
    }
  }

  private async history(url: URL): Promise<Response> {
    const exchangeType = Number(url.searchParams.get("exchangeType"));
    const token = url.searchParams.get("token") || "";
    const from = Number(url.searchParams.get("from"));
    const to = Number(url.searchParams.get("to"));
    const rows: MarketBar[] = [];
    const archiveUrl = new URL("https://internal/history/query");
    archiveUrl.searchParams.set("exchangeType", String(exchangeType));
    archiveUrl.searchParams.set("token", token);
    archiveUrl.searchParams.set("from", String(from));
    archiveUrl.searchParams.set("to", String(to));
    const archivedResponse = await historyShard(this.env, historyShardIndex(exchangeType, token)).fetch(archiveUrl);
    if (!archivedResponse.ok) return error("HISTORY_READ_FAILED", "Could not read archived market history.", 503);
    const archived = await archivedResponse.json() as { bars: MarketBar[] };
    rows.push(...archived.bars);
    const live = this.sql.exec<{ body: string }>(`SELECT body FROM bars
      WHERE exchange_type = ? AND token = ? AND start_time >= ? AND start_time <= ? ORDER BY start_time`,
      exchangeType, token, from, to);
    for (const row of live) rows.push(JSON.parse(row.body) as MarketBar);
    const unique = new Map<string, MarketBar>();
    for (const bar of rows) unique.set(`${bar.exchangeType}:${bar.token}:${bar.time}`, bar);
    const result = [...unique.values()].sort((a, b) => a.time - b.time);
    return json({ timeZone: "Asia/Kolkata", bars: result, count: result.length });
  }

  private async storeHistoryArchive(request: Request, url: URL): Promise<Response> {
    const exchangeType = Number(url.searchParams.get("exchangeType"));
    const token = url.searchParams.get("token") || "";
    const sessionDate = url.searchParams.get("sessionDate") || "";
    if (!EXCHANGES.has(exchangeType) || !/^\d{1,24}$/.test(token) || !/^\d{4}-\d{2}-\d{2}$/.test(sessionDate)) {
      return error("INVALID_ARCHIVE", "Archive identity is invalid.", 422);
    }
    const incoming = JSON.parse(await ungzip(await request.arrayBuffer())) as MarketBar[];
    if (!Array.isArray(incoming) || !incoming.every((bar) => validBar(bar) && bar.exchangeType === exchangeType && bar.token === token && istDate(bar.time * 1000) === sessionDate)) {
      return error("INVALID_ARCHIVE", "Archive contains invalid or mismatched market bars.", 422);
    }
    const prior = [...this.sql.exec<{ payload: ArrayBuffer }>(
      "SELECT payload FROM history_archives WHERE exchange_type = ? AND token = ? AND session_date = ?",
      exchangeType, token, sessionDate)][0];
    const merged = new Map<number, MarketBar>();
    if (prior) for (const bar of JSON.parse(await ungzip(prior.payload)) as MarketBar[]) merged.set(bar.time, bar);
    for (const bar of incoming) merged.set(bar.time, bar);
    const compressed = await gzip(JSON.stringify([...merged.values()].sort((a, b) => a.time - b.time)));
    if (compressed.byteLength > 1_900_000) return error("ARCHIVE_TOO_LARGE", "A compressed contract-day archive exceeds the free SQLite row-size limit.", 413);
    this.sql.exec(`INSERT INTO history_archives(exchange_type, token, session_date, payload)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(exchange_type, token, session_date) DO UPDATE SET payload = excluded.payload`,
      exchangeType, token, sessionDate, copyToArrayBuffer(compressed));
    return json({ archivedBars: incoming.length, sessionDate });
  }

  private async readHistoryArchive(url: URL): Promise<Response> {
    const exchangeType = Number(url.searchParams.get("exchangeType"));
    const token = url.searchParams.get("token") || "";
    const from = Number(url.searchParams.get("from"));
    const to = Number(url.searchParams.get("to"));
    const fromDate = istDate(from * 1000);
    const toDate = istDate(to * 1000);
    const rows = this.sql.exec<{ payload: ArrayBuffer }>(`SELECT payload FROM history_archives
      WHERE exchange_type = ? AND token = ? AND session_date >= ? AND session_date <= ? ORDER BY session_date`,
      exchangeType, token, fromDate, toDate);
    const bars: MarketBar[] = [];
    for (const row of rows) {
      const session = JSON.parse(await ungzip(row.payload)) as MarketBar[];
      for (const bar of session) if (bar.time >= from && bar.time <= to) bars.push(bar);
    }
    return json({ bars, count: bars.length });
  }

  private pruneHistoryArchive(url: URL): Response {
    const cutoff = url.searchParams.get("before") || "";
    if (!/^\d{4}-\d{2}-\d{2}$/.test(cutoff)) return error("INVALID_CUTOFF", "A valid IST session cutoff is required.", 422);
    const result = this.sql.exec("DELETE FROM history_archives WHERE session_date < ?", cutoff);
    return json({ removedArchives: result.rowsWritten });
  }

  private async archive(): Promise<Response> {
    const partialBars = [...this.liveBuilds.values()].map((build) => this.completedBar(build, Date.now()));
    if (partialBars.length) await this.ingestBars(partialBars);
    this.liveBuilds.clear();
    const dates = [...new Set([...this.sql.exec<{ session_date: string }>("SELECT DISTINCT session_date FROM bars")].map((r) => r.session_date))].sort();
    let archivedCount = 0;
    for (const date of dates) {
      const groups = [...this.sql.exec<{ exchange_type: number; token: string }>(
        "SELECT DISTINCT exchange_type, token FROM bars WHERE session_date = ?", date)];
      for (const group of groups) {
        const rows = [...this.sql.exec<{ body: string }>(
          "SELECT body FROM bars WHERE session_date = ? AND exchange_type = ? AND token = ? ORDER BY start_time",
          date, group.exchange_type, group.token)];
        const incoming = rows.map((row) => JSON.parse(row.body) as MarketBar);
        const compressed = await gzip(JSON.stringify(incoming));
        const target = historyShard(this.env, historyShardIndex(group.exchange_type, group.token));
        const archiveUrl = new URL("https://internal/history/archive");
        archiveUrl.searchParams.set("exchangeType", String(group.exchange_type));
        archiveUrl.searchParams.set("token", group.token);
        archiveUrl.searchParams.set("sessionDate", date);
        const archived = await target.fetch(new Request(archiveUrl, { method: "POST", body: copyToArrayBuffer(compressed) }));
        if (!archived.ok) throw new Error(`SQLite history archive failed for ${group.exchange_type}:${group.token} on ${date} (HTTP ${archived.status}).`);
        this.sql.exec("DELETE FROM bars WHERE session_date = ? AND exchange_type = ? AND token = ?", date, group.exchange_type, group.token);
        archivedCount += incoming.length;
      }
      if (groups.length) this.sql.exec("INSERT OR IGNORE INTO history_sessions(session_date) VALUES (?)", date);
    }
    return json({ archivedBars: archivedCount, serverTimeMs: Date.now() });
  }

  private async prune(): Promise<Response> {
    const dates = [...new Set([
      ...[...this.sql.exec<{ session_date: string }>("SELECT session_date FROM history_sessions")].map((row) => row.session_date),
      ...[...this.sql.exec<{ session_date: string }>("SELECT DISTINCT session_date FROM bars")].map((row) => row.session_date),
    ])].sort();
    const keep = dates.slice(-365);
    if (keep.length < dates.length && keep.length) {
      const cutoff = keep[0];
      const results = await Promise.all(Array.from({ length: HISTORY_SHARD_COUNT }, (_, index) => {
        const pruneUrl = new URL("https://internal/history/prune");
        pruneUrl.searchParams.set("before", cutoff);
        return historyShard(this.env, index).fetch(new Request(pruneUrl, { method: "POST" }));
      }));
      if (results.some((result) => !result.ok)) return error("HISTORY_PRUNE_FAILED", "Could not prune every history shard; retention was not completed.", 503);
      this.sql.exec("DELETE FROM bars WHERE session_date < ?", cutoff);
      this.sql.exec("DELETE FROM history_sessions WHERE session_date < ?", cutoff);
    }
    return json({ retainedSessions: keep.length, removedSessions: Math.max(0, dates.length - keep.length), storage: "SHARDED_DURABLE_OBJECT_SQLITE" });
  }

  webSocketMessage(_socket: WebSocket, message: string | ArrayBuffer): void {
    if (message === "ping") _socket.send("pong");
  }
  webSocketClose(socket: WebSocket): void { socket.close(); }
  webSocketError(socket: WebSocket): void { socket.close(1011, "Stream error"); }
}

function istDate(epochMs: number): string {
  const parts = new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Kolkata", year: "numeric", month: "2-digit", day: "2-digit" }).formatToParts(epochMs);
  const part = (type: string) => parts.find((p) => p.type === type)?.value || "";
  return `${part("year")}-${part("month")}-${part("day")}`;
}

function groupTokens(instruments: FeedInstrument[]): { exchangeType: number; tokens: string[] }[] {
  const groups = new Map<number, Set<string>>();
  for (const item of instruments) {
    const tokens = groups.get(item.exchangeType) || new Set<string>();
    tokens.add(item.token);
    groups.set(item.exchangeType, tokens);
  }
  return [...groups.entries()].sort((a, b) => a[0] - b[0])
    .map(([exchangeType, tokens]) => ({ exchangeType, tokens: [...tokens] }));
}

function decodeToken(bytes: Uint8Array): string {
  let end = bytes.indexOf(0);
  if (end < 0) end = bytes.length;
  return String.fromCharCode(...bytes.subarray(0, end));
}

function decodeBase64(value: string): Uint8Array {
  return Uint8Array.from(atob(value), (char) => char.charCodeAt(0));
}

function copyToArrayBuffer(value: Uint8Array): ArrayBuffer {
  const copy = new Uint8Array(value.byteLength);
  copy.set(value);
  return copy.buffer;
}

function encodeBase64(value: Uint8Array): string {
  let binary = "";
  for (let offset = 0; offset < value.length; offset += 0x8000) {
    binary += String.fromCharCode(...value.subarray(offset, offset + 0x8000));
  }
  return btoa(binary);
}

async function encryptionKey(encoded: string): Promise<CryptoKey> {
  const raw = decodeBase64(encoded);
  if (raw.length !== 32) throw new Error("Session encryption key must decode to 32 bytes.");
  return crypto.subtle.importKey("raw", copyToArrayBuffer(raw), "AES-GCM", false, ["encrypt", "decrypt"]);
}

async function encryptSecret(plaintext: string, encodedKey: string): Promise<string> {
  const iv = crypto.getRandomValues(new Uint8Array(12));
  const key = await encryptionKey(encodedKey);
  const cipher = new Uint8Array(await crypto.subtle.encrypt({ name: "AES-GCM", iv: copyToArrayBuffer(iv) }, key,
    copyToArrayBuffer(new TextEncoder().encode(plaintext))));
  return `${encodeBase64(iv)}.${encodeBase64(cipher)}`;
}

async function decryptSecret(encoded: string, encodedKey: string): Promise<string> {
  const [ivText, cipherText] = encoded.split(".");
  if (!ivText || !cipherText) throw new Error("Invalid encrypted session format.");
  const key = await encryptionKey(encodedKey);
  const plain = await crypto.subtle.decrypt({ name: "AES-GCM", iv: copyToArrayBuffer(decodeBase64(ivText)) }, key,
    copyToArrayBuffer(decodeBase64(cipherText)));
  return new TextDecoder().decode(plain);
}

async function gzip(text: string): Promise<Uint8Array> {
  const stream = new Blob([text]).stream().pipeThrough(new CompressionStream("gzip"));
  return new Uint8Array(await new Response(stream).arrayBuffer());
}

async function ungzip(bytes: ArrayBuffer): Promise<string> {
  const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream("gzip"));
  return new Response(stream).text();
}
