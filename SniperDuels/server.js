// server.js - Full-Featured Real-Time WebSocket & Static Game Server for SNIPER DUELS
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const MIME_TYPES = {
  '.html': 'text/html',
  '.css': 'text/css',
  '.js': 'text/javascript',
  '.mjs': 'text/javascript',
  '.json': 'application/json',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.glb': 'model/gltf-binary',
  '.gltf': 'model/gltf+json'
};

// 1. Static HTTP Request Handler
function requestHandler(req, res) {
  let reqPath = req.url.split('?')[0];
  if (reqPath === '/') reqPath = '/index.html';

  const filePath = path.join(__dirname, reqPath);
  const ext = path.extname(filePath).toLowerCase();
  const contentType = MIME_TYPES[ext] || 'application/octet-stream';

  fs.readFile(filePath, (err, content) => {
    if (err) {
      if (err.code === 'ENOENT') {
        res.writeHead(404, { 'Content-Type': 'text/plain' });
        res.end('404 Not Found');
      } else {
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        res.end(`500 Server Error: ${err.code}`);
      }
    } else {
      res.writeHead(200, {
        'Content-Type': contentType,
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Cache-Control': 'no-cache, no-store, must-revalidate'
      });
      res.end(content, 'utf-8');
    }
  });
}

// 2. HTTP Server instances (binds port 3000 as primary preview port, and port 8000 as secondary)
const server = http.createServer(requestHandler);
const server8000 = http.createServer(requestHandler);

// 3. Real-Time 60Hz Authoritative WebSocket Game Server
const wss = new WebSocketServer({ server });
const wss8000 = new WebSocketServer({ server: server8000 });

let clients = new Map();
let nextPlayerId = 1;
let queuePlayers = new Set(); // Set of player IDs standing on the Middle 1v1 Queue Mat
let activeMatches = new Map(); // matchId -> matchData

wss.on('connection', handleConnection);
wss8000.on('connection', handleConnection);

function handleConnection(ws) {
  const playerId = `player_${nextPlayerId++}`;
  const playerObj = {
    id: playerId,
    ws: ws,
    team: 'BLUE',
    health: 100,
    position: { x: 0, y: 0.55, z: -100 },
    rotation: { y: 0, pitch: 0 },
    slot: 'SNIPER',
    isAiming: false,
    isSliding: false,
    onQueuePad: false,
    matchId: null
  };

  clients.set(playerId, playerObj);
  console.log(`[WS] ${playerId} connected. Total online: ${clients.size}`);

  // Send initialization info
  ws.send(JSON.stringify({
    type: 'INIT',
    playerId: playerId,
    playerCount: clients.size,
    queueCount: queuePlayers.size
  }));

  // Notify other clients about the new player in lobby
  broadcastExcept(playerId, {
    type: 'PLAYER_JOINED',
    player: {
      id: playerId,
      position: playerObj.position,
      rotation: playerObj.rotation,
      slot: playerObj.slot
    }
  });

  // Send existing players to the newcomer
  for (const [id, other] of clients.entries()) {
    if (id !== playerId) {
      ws.send(JSON.stringify({
        type: 'PLAYER_JOINED',
        player: {
          id: other.id,
          position: other.position,
          rotation: other.rotation,
          slot: other.slot
        }
      }));
    }
  }

  // Broadcast updated player counts
  broadcast({
    type: 'SERVER_STATS',
    onlinePlayers: clients.size,
    queuePlayers: queuePlayers.size
  });

  ws.on('message', (message) => {
    try {
      const data = JSON.parse(message);
      handleClientMessage(playerId, data);
    } catch (e) {
      console.warn(`[WS] Invalid message from ${playerId}:`, e.message);
    }
  });

  ws.on('close', () => {
    console.log(`[WS] ${playerId} disconnected.`);
    if (queuePlayers.has(playerId)) {
      queuePlayers.delete(playerId);
      broadcastQueueStatus();
    }

    // If player was in an active match, notify opponent
    if (playerObj.matchId && activeMatches.has(playerObj.matchId)) {
      const match = activeMatches.get(playerObj.matchId);
      const opponentId = match.p1 === playerId ? match.p2 : match.p1;
      const opponent = clients.get(opponentId);
      if (opponent && opponent.ws.readyState === ws.OPEN) {
        opponent.ws.send(JSON.stringify({
          type: 'MATCH_OVER',
          winnerId: opponentId,
          reason: 'OPPONENT_DISCONNECTED'
        }));
        opponent.matchId = null;
      }
      activeMatches.delete(playerObj.matchId);
    }

    clients.delete(playerId);
    broadcast({
      type: 'PLAYER_LEFT',
      playerId: playerId
    });
    broadcast({
      type: 'SERVER_STATS',
      onlinePlayers: clients.size,
      queuePlayers: queuePlayers.size
    });
  });
}

function handleClientMessage(senderId, data) {
  const player = clients.get(senderId);
  if (!player) return;

  switch (data.type) {
    // 60Hz transform sync
    case 'SYNC_TRANSFORM':
      player.position = data.position;
      player.rotation = data.rotation;
      player.slot = data.slot;
      player.isAiming = data.isAiming;
      player.isSliding = data.isSliding;

      broadcastExcept(senderId, {
        type: 'PLAYER_TRANSFORM',
        id: senderId,
        position: data.position,
        rotation: data.rotation,
        slot: data.slot,
        isAiming: data.isAiming,
        isSliding: data.isSliding
      });
      break;

    // Player steps ON the Middle 1v1 Queue Mat
    case 'QUEUE_PAD_ENTER':
      if (player.matchId) return; // already in match
      player.onQueuePad = true;
      queuePlayers.add(senderId);
      console.log(`[Queue] ${senderId} entered 1v1 Queue Mat. Players on mat: ${queuePlayers.size}`);
      checkMatchmakingQueue();
      break;

    // Player steps OFF the Middle 1v1 Queue Mat
    case 'QUEUE_PAD_LEAVE':
      player.onQueuePad = false;
      queuePlayers.delete(senderId);
      console.log(`[Queue] ${senderId} left 1v1 Queue Mat. Players on mat: ${queuePlayers.size}`);
      broadcastQueueStatus();
      break;

    // Gunshot or knife slash
    case 'FIRE_WEAPON':
      broadcastExcept(senderId, {
        type: 'REMOTE_FIRE',
        id: senderId,
        origin: data.origin,
        direction: data.direction,
        spread: data.spread,
        isAiming: data.isAiming,
        weaponType: data.weaponType
      });
      break;

    // Damage inflicted in 1v1 Duel
    case 'INFLICT_DAMAGE':
      const target = clients.get(data.targetId);
      if (target) {
        target.health = Math.max(0, target.health - data.damage);
        console.log(`[Combat] ${senderId} dealt ${data.damage} dmg to ${data.targetId} (Remaining HP: ${target.health})`);

        // Notify target of damage
        if (target.ws.readyState === target.ws.OPEN) {
          target.ws.send(JSON.stringify({
            type: 'DAMAGE_TAKEN',
            attackerId: senderId,
            damage: data.damage,
            isHeadshot: data.isHeadshot,
            remainingHealth: target.health
          }));
        }

        // Notify attacker of hit confirmation
        if (player.ws.readyState === player.ws.OPEN) {
          player.ws.send(JSON.stringify({
            type: 'HIT_CONFIRMED',
            targetId: data.targetId,
            damage: data.damage,
            isHeadshot: data.isHeadshot,
            remainingHealth: target.health
          }));
        }

        // Check for fatal elimination -> Match Ends Immediately!
        if (target.health <= 0) {
          const matchId = player.matchId;
          console.log(`[Combat] Fatal kill! ${senderId} wins 1v1 Duel against ${data.targetId}!`);

          if (player.ws.readyState === player.ws.OPEN) {
            player.ws.send(JSON.stringify({
              type: 'MATCH_OVER',
              winnerId: senderId,
              loserId: data.targetId,
              isHeadshot: data.isHeadshot,
              isWinner: true
            }));
          }

          if (target.ws.readyState === target.ws.OPEN) {
            target.ws.send(JSON.stringify({
              type: 'MATCH_OVER',
              winnerId: senderId,
              loserId: data.targetId,
              isHeadshot: data.isHeadshot,
              isWinner: false
            }));
          }

          if (matchId && activeMatches.has(matchId)) {
            activeMatches.delete(matchId);
          }
          player.matchId = null;
          target.matchId = null;
        }
      }
      break;

    case 'MATCH_EXIT':
      if (player.matchId) {
        activeMatches.delete(player.matchId);
        player.matchId = null;
      }
      player.onQueuePad = false;
      queuePlayers.delete(senderId);
      broadcastQueueStatus();
      break;
  }
}

// Checks if 2 players are standing on the Middle Queue Mat to launch the match!
function checkMatchmakingQueue() {
  broadcastQueueStatus();

  if (queuePlayers.size >= 2) {
    const queueArray = Array.from(queuePlayers);
    const p1Id = queueArray[0];
    const p2Id = queueArray[1];

    queuePlayers.delete(p1Id);
    queuePlayers.delete(p2Id);

    const p1 = clients.get(p1Id);
    const p2 = clients.get(p2Id);

    if (p1 && p2 && p1.ws.readyState === p1.ws.OPEN && p2.ws.readyState === p2.ws.OPEN) {
      const matchId = `match_${Date.now()}`;
      p1.matchId = matchId;
      p2.matchId = matchId;
      p1.team = 'BLUE';
      p2.team = 'RED';
      p1.health = 100;
      p2.health = 100;

      activeMatches.set(matchId, {
        id: matchId,
        p1: p1Id,
        p2: p2Id,
        startTime: Date.now()
      });

      console.log(`[Matchmaking] MATCH STARTED: ${p1Id} (BLUE) vs ${p2Id} (RED) on 1v1 Queue Mat!`);

      // Send match start to Player 1 (Blue Perch)
      p1.ws.send(JSON.stringify({
        type: 'MATCH_START_2P',
        matchId: matchId,
        opponentId: p2Id,
        myTeam: 'BLUE',
        opponentTeam: 'RED',
        map: 'blockout'
      }));

      // Send match start to Player 2 (Red Perch)
      p2.ws.send(JSON.stringify({
        type: 'MATCH_START_2P',
        matchId: matchId,
        opponentId: p1Id,
        myTeam: 'RED',
        opponentTeam: 'BLUE',
        map: 'blockout'
      }));

      broadcastQueueStatus();
    }
  }
}

function broadcastQueueStatus() {
  const count = queuePlayers.size;
  broadcast({
    type: 'QUEUE_UPDATE',
    playersOnPad: count,
    message: count === 1 ? 'WAITING FOR OPPONENT (1/2 ON PAD)...' : (count >= 2 ? 'OPPONENT DETECTED! STARTING 1v1...' : 'STAND ON PAD TO QUEUE 1v1')
  });
}

function broadcast(msg) {
  const payload = JSON.stringify(msg);
  for (const client of clients.values()) {
    if (client.ws.readyState === client.ws.OPEN) {
      client.ws.send(payload);
    }
  }
}

function broadcastExcept(excludeId, msg) {
  const payload = JSON.stringify(msg);
  for (const [id, client] of clients.entries()) {
    if (id !== excludeId && client.ws.readyState === client.ws.OPEN) {
      client.ws.send(payload);
    }
  }
}

server.listen(3000, '0.0.0.0', () => {
  console.log(`SNIPER DUELS Primary Server live on http://0.0.0.0:3000`);
});

server8000.listen(8000, '0.0.0.0', () => {
  console.log(`SNIPER DUELS Secondary Server live on http://0.0.0.0:8000`);
});
