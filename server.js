import express from 'express';
import { WebSocketServer, WebSocket } from 'ws';
import { createServer } from 'node:http';
import { randomBytes } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const app = express();
const server = createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });
const root = path.dirname(fileURLToPath(import.meta.url));
const rooms = new Map();

app.get('/health', (_req, res) => res.json({ ok: true, rooms: rooms.size }));
app.use((req, res, next) => {
  if (['/server.js', '/package.json', '/package-lock.json'].includes(req.path)) return res.sendStatus(404);
  next();
});
app.use(express.static(root, { index: 'index.html' }));

const code = () => {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let value = '';
  do {
    value = Array.from(randomBytes(6), n => chars[n % chars.length]).join('');
  } while (rooms.has(value));
  return value;
};
const send = (ws, message) => ws?.readyState === WebSocket.OPEN && ws.send(JSON.stringify(message));
const cleanName = value => String(value || 'プレイヤー').trim().slice(0, 16);

wss.on('connection', ws => {
  ws.meta = null;
  ws.on('message', raw => {
    let message;
    try { message = JSON.parse(raw); } catch { return send(ws, { type: 'error', message: '不正な通信です。' }); }

    if (message.type === 'create_room') {
      const roomCode = code();
      const room = { code: roomCode, players: [ws, null], names: [cleanName(message.name), null], state: null, createdAt: Date.now() };
      rooms.set(roomCode, room);
      ws.meta = { roomCode, index: 0 };
      return send(ws, { type: 'room_created', roomCode, playerIndex: 0 });
    }

    if (message.type === 'join_room') {
      const roomCode = String(message.roomCode || '').trim().toUpperCase();
      const room = rooms.get(roomCode);
      if (!room) return send(ws, { type: 'error', message: 'ルームが見つかりません。' });
      if (room.players[1]) return send(ws, { type: 'error', message: 'このルームは満員です。' });
      room.players[1] = ws;
      room.names[1] = cleanName(message.name);
      ws.meta = { roomCode, index: 1 };
      room.players.forEach((client, playerIndex) => send(client, { type: 'room_ready', roomCode, playerIndex, names: room.names }));
      return;
    }

    const meta = ws.meta;
    const room = meta && rooms.get(meta.roomCode);
    if (!room) return send(ws, { type: 'error', message: 'ルームとの接続が切れています。' });

    if (message.type === 'game_state') {
      if (!message.state || typeof message.state !== 'object') return;
      if (room.state) {
        const expectedIndex = room.state.phase === 'DEFENSE' ? 1 - room.state.active : room.state.active;
        if (meta.index !== expectedIndex) return send(ws, { type: 'error', message: '現在は相手の操作を待つ時間です。' });
      } else if (meta.index !== 0) return;
      room.state = message.state;
      room.players.forEach(client => send(client, { type: 'game_state', state: room.state }));
    }
  });

  ws.on('close', () => {
    const meta = ws.meta;
    const room = meta && rooms.get(meta.roomCode);
    if (!room) return;
    room.players.forEach(client => client !== ws && send(client, { type: 'peer_left', message: '相手がルームから退出しました。' }));
    rooms.delete(meta.roomCode);
  });
});

setInterval(() => {
  const limit = Date.now() - 6 * 60 * 60 * 1000;
  for (const [roomCode, room] of rooms) if (room.createdAt < limit) rooms.delete(roomCode);
}, 30 * 60 * 1000).unref();

const port = Number(process.env.PORT || 3000);
server.listen(port, () => console.log(`PRIME DUEL is running on http://localhost:${port}`));
