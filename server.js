// Local screen-share server: the Mac opens /sender, the TV opens /.
// Video goes over WebRTC when the TV supports it, otherwise as JPEG frames
// relayed through this server's WebSocket.
const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = Number(process.env.PORT) || 8080;
const PUBLIC_DIR = path.join(__dirname, 'public');
// Frames above this size are refused; a 4K JPEG is well under it.
const MAX_FRAME_BYTES = 8 * 1024 * 1024;
// Compatibility (JPEG) mode is off unless started with JPEG_FALLBACK=1.
// While off, TVs without working WebRTC show an error instead of falling back.
const JPEG_FALLBACK = process.env.JPEG_FALLBACK === '1';

const ROUTES = {
  '/': 'receiver.html',
  '/tv': 'receiver.html',
  '/sender': 'sender.html',
};

function serveFile(res, fileName) {
  fs.readFile(path.join(PUBLIC_DIR, fileName), (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end('Not found');
      return;
    }
    res.writeHead(200, {
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'no-store',
    });
    res.end(data);
  });
}

const server = http.createServer((req, res) => {
  const route = ROUTES[req.url.split('?')[0]];
  if (!route) {
    res.writeHead(404);
    res.end('Not found');
    return;
  }
  serveFile(res, route);
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: MAX_FRAME_BYTES });

let sender = null;
// id -> { ws, mode: 'webrtc' | 'jpeg', acks: boolean, sending: boolean, pending: Buffer | null }
const receivers = new Map();
let nextReceiverId = 1;

function send(ws, msg) {
  if (ws && ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
}

function broadcastToReceivers(msg) {
  for (const r of receivers.values()) send(r.ws, msg);
}

function sendStatus() {
  send(sender, { type: 'receivers', ids: [...receivers.keys()] });
}

// Each TV has at most one frame in flight. Frames that arrive meanwhile
// replace each other, so a slow TV gets the newest picture as soon as it
// catches up instead of draining a queue of stale ones. TVs that send
// 'frame-ack' release the slot on arrival, which also keeps the kernel socket
// buffers from filling with stale frames; older TV pages release it once the
// frame is written to the socket.
function sendFrame(r, frame) {
  r.sending = true;
  r.ws.send(frame, { binary: true }, () => {
    if (!r.acks) releaseFrame(r);
  });
}

function releaseFrame(r) {
  r.sending = false;
  const next = r.pending;
  r.pending = null;
  if (next && r.ws.readyState === r.ws.OPEN) sendFrame(r, next);
}

function relayFrame(frame) {
  for (const r of receivers.values()) {
    if (r.mode !== 'jpeg' || r.ws.readyState !== r.ws.OPEN) continue;
    if (r.sending) r.pending = frame;
    else sendFrame(r, frame);
  }
}

wss.on('connection', (ws) => {
  let role = null;
  let receiverId = null;

  ws.on('message', (data, isBinary) => {
    if (isBinary) {
      if (ws === sender && JPEG_FALLBACK) relayFrame(data);
      return;
    }

    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      return;
    }

    switch (msg.type) {
      case 'hello':
        if (msg.role === 'sender') {
          if (sender && sender !== ws) sender.close(4000, 'Replaced by a new sender');
          sender = ws;
          role = 'sender';
          console.log('Sender connected');
          send(ws, { type: 'config', jpegFallback: JPEG_FALLBACK });
          sendStatus();
          broadcastToReceivers({ type: 'sender-online' });
        } else {
          role = 'receiver';
          receiverId = String(nextReceiverId++);
          receivers.set(receiverId, {
            ws, mode: 'webrtc', acks: msg.acks === true, sending: false, pending: null,
          });
          console.log(`TV connected (id ${receiverId})`);
          send(ws, {
            type: 'welcome', id: receiverId, senderOnline: Boolean(sender), jpegFallback: JPEG_FALLBACK,
          });
          send(sender, { type: 'receiver-joined', id: receiverId });
          sendStatus();
        }
        break;

      case 'signal':
        if (role === 'sender') {
          const r = receivers.get(msg.to);
          if (r) send(r.ws, { type: 'signal', data: msg.data });
        } else if (role === 'receiver') {
          send(sender, { type: 'signal', from: receiverId, data: msg.data });
        }
        break;

      case 'want-jpeg':
        if (role === 'receiver' && JPEG_FALLBACK) {
          receivers.get(receiverId).mode = 'jpeg';
          console.log(`TV ${receiverId} switched to JPEG mode`);
          send(sender, { type: 'want-jpeg', from: receiverId });
        }
        break;

      case 'frame-ack':
        if (role === 'receiver') releaseFrame(receivers.get(receiverId));
        break;

      case 'ready':
        // A TV that reconnected or reloaded asks the sender to start over.
        if (role === 'receiver') send(sender, { type: 'receiver-joined', id: receiverId });
        break;

      case 'sharing':
        if (role === 'sender') broadcastToReceivers({ type: 'sharing', active: Boolean(msg.active) });
        break;
    }
  });

  ws.on('close', () => {
    if (role === 'sender' && ws === sender) {
      sender = null;
      console.log('Sender disconnected');
      broadcastToReceivers({ type: 'sender-offline' });
    } else if (role === 'receiver') {
      receivers.delete(receiverId);
      console.log(`TV disconnected (id ${receiverId})`);
      send(sender, { type: 'receiver-left', id: receiverId });
      sendStatus();
    }
  });
});

function lanAddresses() {
  const addrs = [];
  for (const list of Object.values(os.networkInterfaces())) {
    for (const a of list || []) {
      if (a.family === 'IPv4' && !a.internal) addrs.push(a.address);
    }
  }
  return addrs;
}

server.listen(PORT, '0.0.0.0', () => {
  console.log('\nScreen share server running.\n');
  console.log(`  1. On this Mac, open in Chrome:  http://localhost:${PORT}/sender`);
  console.log('  2. On the TV browser, open:');
  for (const ip of lanAddresses()) console.log(`       http://${ip}:${PORT}`);
  console.log('\nPress Ctrl+C to stop.\n');
});
