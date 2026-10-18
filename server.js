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
const MAX_BUFFERED_BYTES = 1.5 * 1024 * 1024;

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

const wss = new WebSocketServer({ server, path: '/ws' });

let sender = null;
const receivers = new Map(); // id -> { ws, mode: 'webrtc' | 'jpeg' }
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

function relayFrame(frame) {
  for (const r of receivers.values()) {
    if (r.mode !== 'jpeg' || r.ws.readyState !== r.ws.OPEN) continue;
    // Drop frames for slow TVs instead of queueing them up (keeps lag low).
    if (r.ws.bufferedAmount > MAX_BUFFERED_BYTES) continue;
    r.ws.send(frame, { binary: true });
  }
}

wss.on('connection', (ws) => {
  let role = null;
  let receiverId = null;

  ws.on('message', (data, isBinary) => {
    if (isBinary) {
      if (ws === sender) relayFrame(data);
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
          sendStatus();
          broadcastToReceivers({ type: 'sender-online' });
        } else {
          role = 'receiver';
          receiverId = String(nextReceiverId++);
          receivers.set(receiverId, { ws, mode: 'webrtc' });
          console.log(`TV connected (id ${receiverId})`);
          send(ws, { type: 'welcome', id: receiverId, senderOnline: Boolean(sender) });
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
        if (role === 'receiver') {
          receivers.get(receiverId).mode = 'jpeg';
          console.log(`TV ${receiverId} switched to JPEG mode`);
          send(sender, { type: 'want-jpeg', from: receiverId });
        }
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
