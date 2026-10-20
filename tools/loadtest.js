// Load test for the JPEG relay path. Starts the server on a spare port,
// connects one fake sender and N fake TVs in compatibility mode, pushes
// timestamped frames and reports how late and how often frames arrive.
//
// A Wi-Fi TV is slower than loopback, so --slow-mbps throttles how fast the
// slow TVs read their socket. That is where queueing lag shows up.
//
// --acks 0 behaves like a TV page from before frame acks existed.
//
//   node tools/loadtest.js --tvs 4 --slow 1 --slow-mbps 20 --fps 24 --kb 250
'use strict';

const { spawn } = require('child_process');
const path = require('path');
const { performance } = require('perf_hooks');
const WebSocket = require('ws');

const DEFAULTS = { acks: 1, tvs: 4, slow: 1, slowMbps: 20, fps: 24, kb: 250, seconds: 10, port: 18080 };
const THROTTLE_WINDOW_MS = 20;
const ACK = JSON.stringify({ type: 'frame-ack' });

function parseArgs(argv) {
  const opts = { ...DEFAULTS };
  for (let i = 0; i < argv.length; i += 2) {
    const key = argv[i].replace(/^--/, '').replace(/-(\w)/g, (_, c) => c.toUpperCase());
    if (!(key in DEFAULTS)) throw new Error(`unknown flag ${argv[i]}`);
    opts[key] = Number(argv[i + 1]);
  }
  return opts;
}

function startServer(port) {
  const child = spawn(process.execPath, [path.join(__dirname, '..', 'server.js')], {
    env: { ...process.env, PORT: String(port), JPEG_FALLBACK: '1' },
    stdio: ['ignore', 'pipe', 'inherit'],
  });
  return new Promise((resolve, reject) => {
    child.stdout.on('data', (chunk) => {
      if (chunk.toString().includes('running')) resolve(child);
    });
    child.on('exit', (code) => reject(new Error(`server exited with ${code}`)));
  });
}

function openSocket(url) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    ws.once('open', () => resolve(ws));
    ws.once('error', reject);
  });
}

// Reads at most `mbps` from the socket by pausing it once a window's budget
// is spent, the way a TV on a busy Wi-Fi link drains its receive buffer.
function throttleSocket(socket, mbps) {
  const budget = (mbps * 1e6 / 8) * (THROTTLE_WINDOW_MS / 1000);
  let used = 0;
  socket.on('data', (chunk) => {
    used += chunk.length;
    if (used < budget) return;
    socket.pause();
    const waitMs = THROTTLE_WINDOW_MS * (used / budget);
    used = 0;
    setTimeout(() => socket.resume(), waitMs);
  });
}

async function connectTv(url, mbps, acks) {
  const ws = await openSocket(url);
  const stats = { latencies: [], bytes: 0 };
  if (mbps > 0) throttleSocket(ws._socket, mbps);
  ws.on('message', (data, isBinary) => {
    if (!isBinary) return;
    stats.latencies.push(performance.now() - data.readDoubleLE(0));
    stats.bytes += data.length;
    if (acks) ws.send(ACK);
  });
  ws.send(JSON.stringify({ type: 'hello', role: 'receiver', acks }));
  ws.send(JSON.stringify({ type: 'want-jpeg' }));
  return { ws, stats, slow: mbps > 0 };
}

function percentile(sorted, p) {
  if (!sorted.length) return NaN;
  return sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * p))];
}

function summarize(label, tvs, seconds) {
  if (!tvs.length) return null;
  const all = tvs.flatMap((t) => t.stats.latencies).sort((a, b) => a - b);
  const frames = all.length / tvs.length;
  return {
    tvs: label,
    count: tvs.length,
    fps: +(frames / seconds).toFixed(1),
    p50ms: +percentile(all, 0.5).toFixed(1),
    p95ms: +percentile(all, 0.95).toFixed(1),
    p99ms: +percentile(all, 0.99).toFixed(1),
    maxms: +(all[all.length - 1] || NaN).toFixed(1),
  };
}

async function run() {
  const opts = parseArgs(process.argv.slice(2));
  const server = await startServer(opts.port);
  const url = `ws://127.0.0.1:${opts.port}/ws`;
  try {
    const sender = await openSocket(url);
    sender.send(JSON.stringify({ type: 'hello', role: 'sender' }));
    const tvs = [];
    for (let i = 0; i < opts.tvs; i++) {
      tvs.push(await connectTv(url, i < opts.slow ? opts.slowMbps : 0, opts.acks === 1));
    }

    const template = Buffer.alloc(opts.kb * 1024, 0x5a);
    const interval = setInterval(() => {
      // A fresh copy per frame: ws may still hold the previous one in its queue.
      const frame = Buffer.from(template);
      frame.writeDoubleLE(performance.now(), 0);
      sender.send(frame, { binary: true });
    }, 1000 / opts.fps);
    await new Promise((r) => setTimeout(r, opts.seconds * 1000));
    clearInterval(interval);

    const fast = tvs.filter((t) => !t.slow);
    const slow = tvs.filter((t) => t.slow);
    console.log(JSON.stringify(opts));
    console.table([summarize('fast', fast, opts.seconds), summarize(`slow ${opts.slowMbps} Mbps`, slow, opts.seconds)].filter(Boolean));
    for (const t of tvs) t.ws.terminate();
    sender.terminate();
  } finally {
    server.kill();
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
