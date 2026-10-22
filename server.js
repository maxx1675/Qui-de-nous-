const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: false },
  maxHttpBufferSize: 1e6,
  pingInterval: 25000,
  pingTimeout: 20000
});
const rooms = new Map();
const MAX_PLAYERS = 12;
app.disable('x-powered-by');
app.use(express.static(path.join(__dirname, 'public')));
app.get('/health', (_req, res) => res.json({ ok: true, service: 'qui-de-nous' }));

function publishPeers(roomCode) {
  const room = rooms.get(roomCode);
  if (!room) return;
  const ids = [...room.members].map(id => room.playerIds?.get(id) || id);
  io.to(roomCode).emit('room-peers', ids);
}
function roomFor(socket) {
  const code = socket.data.roomCode;
  return code ? rooms.get(code) : null;
}

io.on('connection', socket => {
  socket.on('join-room', (roomName, reply = () => {}) => {
    if (typeof roomName !== 'string' || !/^qdn-[a-z0-9]{4}$/.test(roomName)) return reply({ok:false,error:'Code de salon invalide.'});
    const exists = rooms.has(roomName);
    // First connection creates the room; later connections join it.
    if (!exists) rooms.set(roomName, {members:new Set(), playerIds:new Map(), createdAt:Date.now()});
    const room = rooms.get(roomName);
    if (room.members.size >= MAX_PLAYERS) return reply({ok:false,error:'Salon complet.'});
    socket.join(roomName);
    socket.data.roomCode = roomName;
    room.members.add(socket.id);
    reply({ok:true});
    publishPeers(roomName);
  });
  socket.on('room-presence', data => {
    const room = roomFor(socket);
    if (!room || !data || typeof data.id !== 'string') return;
    socket.data.playerId = data.id.slice(0,32);
    room.playerIds.set(socket.id, socket.data.playerId);
    publishPeers(socket.data.roomCode);
  });
  socket.on('room-event', (msg, reply = () => {}) => {
    const room = roomFor(socket);
    if (!room || !msg || typeof msg.topic !== 'string' || !['st','act','pic'].includes(msg.topic)) return reply({ok:false});
    // Keep payloads bounded. Photo chunks are deliberately small in the client.
    let size;
    try { size = JSON.stringify(msg.data).length; } catch { return reply({ok:false}); }
    if (size > 50000) return reply({ok:false});
    io.to(socket.data.roomCode).emit('room-event', {topic:msg.topic,data:msg.data,from:socket.id});
    reply({ok:true});
  });
  socket.on('leave-room', () => leave(socket));
  socket.on('disconnect', () => leave(socket));
});
function leave(socket) {
  const code = socket.data.roomCode;
  if (!code) return;
  const room = rooms.get(code);
  socket.leave(code);
  socket.data.roomCode = null;
  if (room) {
    room.members.delete(socket.id);
    room.playerIds?.delete(socket.id);
    if (!room.members.size) rooms.delete(code);
    else publishPeers(code);
  }
}
const port = Number(process.env.PORT || 3000);
server.listen(port, '0.0.0.0', () => console.log(`Qui de nous ? disponible sur http://localhost:${port}`));
