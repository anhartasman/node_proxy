const http = require('http');
const https = require('https');
const net = require('net');
const httpProxy = require('http-proxy');

const config = {
  mysqlHost: process.env.MYSQL_HOST,
  mysqlPort: parseInt(process.env.MYSQL_PORT || '3306'),
  targetUrl: process.env.TARGET_URL,
  healthPath: process.env.HEALTH_PATH || '/',
  proxyUrl: process.env.PROXY_URL,
  port: parseInt(process.env.PORT || '3000'),
  wakeTimeoutMs: parseInt(process.env.WAKE_TIMEOUT_MS || '60000'),
  retryIntervalMs: parseInt(process.env.RETRY_INTERVAL_MS || '2000'),
  warmTtlMs: parseInt(process.env.WARM_TTL_MS || '30000'),
};

// ── sanity check ─────────────────────────────────────────────────────────────
if (!config.mysqlHost) throw new Error('MYSQL_HOST is required');
if (!config.targetUrl) throw new Error('TARGET_URL is required');

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ── TCP probe (wakes Railway MySQL) ──────────────────────────────────────────
function probeTCP(host, port) {
  return new Promise((resolve, reject) => {
    const socket = new net.Socket();
    socket.setTimeout(5000);
    socket.connect(port, host, () => { socket.destroy(); resolve(); });
    socket.on('timeout', () => { socket.destroy(); reject(new Error('TCP timeout')); });
    socket.on('error', (err) => { socket.destroy(); reject(err); });
  });
}

async function waitForMySQL() {
  const deadline = Date.now() + config.wakeTimeoutMs;
  let attempt = 0;
  while (Date.now() < deadline) {
    try {
      await probeTCP(config.mysqlHost, config.mysqlPort);
      console.log(`[mysql] up after ${++attempt} attempt(s)`);
      return;
    } catch {
      attempt++;
      await sleep(config.retryIntervalMs);
    }
  }
  throw new Error(`MySQL did not wake within ${config.wakeTimeoutMs}ms`);
}

// ── HTTP probe (wakes target app) ────────────────────────────────────────────
function probeHTTP(url) {
  return new Promise((resolve, reject) => {
    const lib = url.startsWith('https') ? https : http;
    const req = lib.get(url, { timeout: 5000 }, (res) => {
      res.resume();
      // treat anything below 500 as "alive" (404 is fine, service is running)
      res.statusCode < 500 ? resolve() : reject(new Error(`HTTP ${res.statusCode}`));
    });
    req.on('timeout', () => { req.destroy(); reject(new Error('HTTP timeout')); });
    req.on('error', reject);
  });
}

async function waitForTarget() {
  const healthUrl = config.targetUrl.replace(/\/$/, '') + config.healthPath;
  const deadline = Date.now() + config.wakeTimeoutMs;
  let attempt = 0;
  while (Date.now() < deadline) {
    try {
      await probeHTTP(healthUrl);
      console.log(`[app] up after ${++attempt} attempt(s)`);
      return;
    } catch {
      attempt++;
      await sleep(config.retryIntervalMs);
    }
  }
  throw new Error(`App did not wake within ${config.wakeTimeoutMs}ms`);
}

// ── Wake sequencer with deduplication ────────────────────────────────────────
// All concurrent requests share one wake promise so we don't spawn parallel probes.
let wakePromise = null;
let lastWarmAt = 0;

async function ensureReady() {
  // If services were recently confirmed up, skip probing entirely.
  if (Date.now() - lastWarmAt < config.warmTtlMs) return;

  if (!wakePromise) {
    wakePromise = (async () => {
      console.log('[proxy] waking MySQL …');
      await waitForMySQL();

      console.log('[proxy] waking app …');
      await waitForTarget();

      lastWarmAt = Date.now();
      console.log('[proxy] both services ready');
    })().finally(() => {
      wakePromise = null;
    });
  }

  return wakePromise;
}

// ── Proxy ─────────────────────────────────────────────────────────────────────
const proxy = httpProxy.createProxyServer({ changeOrigin: true });

// Override X-Forwarded-* headers so the app builds URLs using the proxy's public origin.
// xfwd:true would otherwise set proto=http and port=80 (Railway's internal port).
proxy.on('proxyReq', (proxyReq) => {
  if (config.proxyUrl) {
    const url = new URL(config.proxyUrl);
    const proto = url.protocol.replace(':', '');
    const port  = url.port || (proto === 'https' ? '443' : '80');
    proxyReq.setHeader('x-forwarded-proto', proto);
    proxyReq.setHeader('x-forwarded-host',  url.hostname);
    proxyReq.setHeader('x-forwarded-port',  port);
  }
});

// Rewrite any Location header pointing to the internal target URL
// so the browser follows redirects back through the proxy.
proxy.on('proxyRes', (proxyRes) => {
  const location = proxyRes.headers['location'];
  if (location && config.proxyUrl) {
    const targetOrigin = config.targetUrl.replace(/\/$/, '');
    proxyRes.headers['location'] = location.replace(targetOrigin, config.proxyUrl);
  }
});

proxy.on('error', (err, _req, res) => {
  console.error('[proxy] error:', err.message);
  if (!res.headersSent) {
    res.writeHead(502, { 'Content-Type': 'application/json' });
  }
  res.end(JSON.stringify({ error: 'bad_gateway', message: err.message }));
});

const server = http.createServer(async (req, res) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
  try {
    await ensureReady();
    proxy.web(req, res, { target: config.targetUrl, xfwd: true });
  } catch (err) {
    console.error('[proxy] wake failed:', err.message);
    res.writeHead(503, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ error: 'service_unavailable', message: err.message }));
  }
});

// Forward WebSocket upgrades too (needed for Next.js HMR, Laravel Reverb, etc.)
server.on('upgrade', async (req, socket, head) => {
  try {
    await ensureReady();
    proxy.ws(req, socket, head, { target: config.targetUrl, xfwd: true });
  } catch (err) {
    socket.destroy();
  }
});

server.listen(config.port, () => {
  console.log(`[proxy] listening on :${config.port}`);
  console.log(`[proxy] MySQL → ${config.mysqlHost}:${config.mysqlPort}`);
  console.log(`[proxy] app   → ${config.targetUrl}`);
});
