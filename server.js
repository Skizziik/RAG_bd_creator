const express = require('express');
const http = require('http');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');
const { Store } = require('./lib/store');
const { parseUrl } = require('./lib/wiki');

const app = express();
const server = http.createServer(app);
const PORT = process.env.PORT || 3000;

const store = new Store(path.join(__dirname, 'data'));

// ---- MIDDLEWARE ----
app.use(express.json({ limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ---- SESSION MANAGEMENT ----
const sessions = new Map(); // code → { browsers: Set<ws>, mcpClients: Set<ws>, createdAt }

function generateCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code = '';
  for (let i = 0; i < 6; i++) code += chars[Math.floor(Math.random() * chars.length)];
  return sessions.has(code) ? generateCode() : code;
}

function broadcast(sessionCode, event, data, excludeWs) {
  const session = sessions.get(sessionCode);
  if (!session) return;
  const msg = JSON.stringify({ event, data });
  for (const ws of [...session.browsers, ...session.mcpClients]) {
    if (ws !== excludeWs && ws.readyState === 1) ws.send(msg);
  }
}

function broadcastToBrowsers(sessionCode, event, data) {
  const session = sessions.get(sessionCode);
  if (!session) return;
  const msg = JSON.stringify({ event, data });
  for (const ws of session.browsers) {
    if (ws.readyState === 1) ws.send(msg);
  }
}

// ---- HEALTH ----
app.get('/health', (_req, res) => {
  res.json({ status: 'alive', app: 'Dataset Builder', timestamp: Date.now() });
});

// ---- SESSION API ----
app.get('/api/session', (_req, res) => {
  const code = generateCode();
  sessions.set(code, { browsers: new Set(), mcpClients: new Set(), createdAt: Date.now() });
  res.json({ code });
});

// ---- PROJECT API ----
app.get('/api/projects', (_req, res) => {
  try { res.json(store.listProjects()); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/projects/:name', (req, res) => {
  try { res.json(store.getProject(req.params.name)); }
  catch (e) { res.status(404).json({ error: e.message }); }
});

app.post('/api/projects', (req, res) => {
  try {
    const source = req.body.source || 'browser';
    const result = store.createProject(req.body.name, { source });
    if (req.body.session) broadcastToBrowsers(req.body.session, 'project:created', result);
    res.json(result);
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.delete('/api/projects/:name', (req, res) => {
  try {
    const source = req.query.source || 'browser';
    const result = store.deleteProject(req.params.name, { source });
    if (req.query.session) broadcastToBrowsers(req.query.session, 'project:deleted', result);
    res.json(result);
  } catch (e) { res.status(404).json({ error: e.message }); }
});

app.get('/api/projects/:name/stats', (req, res) => {
  try { res.json(store.getStats(req.params.name)); }
  catch (e) { res.status(404).json({ error: e.message }); }
});

// ---- CATEGORY API ----
app.get('/api/projects/:name/categories', (req, res) => {
  try { res.json(store.listCategories(req.params.name)); }
  catch (e) { res.status(404).json({ error: e.message }); }
});

app.post('/api/projects/:name/categories', (req, res) => {
  try {
    const source = req.body.source || 'browser';
    const result = store.createCategory(req.params.name, req.body.name, { source });
    if (req.body.session) broadcastToBrowsers(req.body.session, 'data:changed', { project: req.params.name });
    res.json(result);
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.put('/api/projects/:name/categories/:catName', (req, res) => {
  try {
    const source = req.body.source || 'browser';
    const result = store.renameCategory(req.params.name, req.params.catName, req.body.newName, { source });
    if (req.body.session) broadcastToBrowsers(req.body.session, 'data:changed', { project: req.params.name });
    res.json(result);
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.delete('/api/projects/:name/categories/:catName', (req, res) => {
  try {
    const source = req.query.source || 'browser';
    const result = store.deleteCategory(req.params.name, req.params.catName, { source });
    if (req.query.session) broadcastToBrowsers(req.query.session, 'data:changed', { project: req.params.name });
    res.json(result);
  } catch (e) { res.status(404).json({ error: e.message }); }
});

app.post('/api/projects/:name/categories/:catId/toggle', (req, res) => {
  try {
    const result = store.toggleCategory(req.params.name, req.params.catId);
    res.json(result);
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// ---- CHUNK API ----
app.post('/api/projects/:name/categories/:catName/chunks', (req, res) => {
  try {
    const source = req.body.source || 'browser';
    const result = store.addChunk(req.params.name, req.params.catName, req.body, { source });
    if (req.body.session) broadcastToBrowsers(req.body.session, 'data:changed', { project: req.params.name });
    res.json(result);
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.post('/api/projects/:name/categories/:catName/chunks/bulk', (req, res) => {
  try {
    const source = req.body.source || 'browser';
    const result = store.bulkAddChunks(req.params.name, req.params.catName, req.body.chunks, { source });
    if (req.body.session) broadcastToBrowsers(req.body.session, 'data:changed', { project: req.params.name });
    res.json(result);
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.post('/api/projects/:name/categories/:catId/chunks/blank', (req, res) => {
  try {
    const source = req.body.source || 'browser';
    const result = store.addBlankChunk(req.params.name, req.params.catId, { source });
    res.json(result);
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.put('/api/projects/:name/categories/:catId/chunks/:uid', (req, res) => {
  try {
    const source = req.body.source || 'browser';
    const result = store.updateChunk(req.params.name, req.params.catId, req.params.uid, req.body, { source });
    if (req.body.session) broadcastToBrowsers(req.body.session, 'data:changed', { project: req.params.name });
    res.json(result);
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.delete('/api/projects/:name/categories/:catId/chunks/:uid', (req, res) => {
  try {
    const source = req.query.source || 'browser';
    const result = store.deleteChunk(req.params.name, req.params.catId, req.params.uid, { source });
    if (req.query.session) broadcastToBrowsers(req.query.session, 'data:changed', { project: req.params.name });
    res.json(result);
  } catch (e) { res.status(404).json({ error: e.message }); }
});

app.post('/api/projects/:name/categories/:catId/chunks/:uid/duplicate', (req, res) => {
  try {
    const source = req.body.source || 'browser';
    const result = store.duplicateChunk(req.params.name, req.params.catId, req.params.uid, { source });
    res.json(result);
  } catch (e) { res.status(400).json({ error: e.message }); }
});

app.post('/api/projects/:name/chunks/:chunkId/move', (req, res) => {
  try {
    const source = req.body.source || 'browser';
    const result = store.moveChunk(req.params.name, req.params.chunkId, req.body.targetCategory, { source });
    if (req.body.session) broadcastToBrowsers(req.body.session, 'data:changed', { project: req.params.name });
    res.json(result);
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// ---- SEARCH ----
app.get('/api/projects/:name/search', (req, res) => {
  try { res.json(store.searchChunks(req.params.name, req.query.q || '')); }
  catch (e) { res.status(404).json({ error: e.message }); }
});

// ---- EXPORT / IMPORT ----
app.get('/api/projects/:name/export', (req, res) => {
  try { res.json(store.exportProject(req.params.name)); }
  catch (e) { res.status(404).json({ error: e.message }); }
});

app.post('/api/projects/:name/import', (req, res) => {
  try {
    const source = req.body.source || 'browser';
    const result = store.importJSON(req.params.name, req.body.data, req.body.category, { source });
    if (req.body.session) broadcastToBrowsers(req.body.session, 'data:changed', { project: req.params.name });
    res.json(result);
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// ---- BULK UPDATE METADATA ----
app.post('/api/projects/:name/bulk-metadata', (req, res) => {
  try {
    const source = req.body.source || 'browser';
    const result = store.bulkUpdateMetadata(req.params.name, req.body.field, req.body.value, req.body.category, { source });
    if (req.body.session) broadcastToBrowsers(req.body.session, 'data:changed', { project: req.params.name });
    res.json(result);
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// ---- MERGE PROJECTS ----
app.post('/api/projects/:name/merge', (req, res) => {
  try {
    const source = req.body.source || 'browser';
    const result = store.mergeProjects(req.params.name, req.body.target, { source });
    if (req.body.session) broadcastToBrowsers(req.body.session, 'data:changed', { project: req.body.target });
    res.json(result);
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// ---- EXPORT CATEGORY ----
app.get('/api/projects/:name/categories/:catName/export', (req, res) => {
  try { res.json(store.exportCategory(req.params.name, req.params.catName)); }
  catch (e) { res.status(404).json({ error: e.message }); }
});

// ---- HISTORY API ----
app.get('/api/projects/:name/history', (req, res) => {
  try { res.json(store.getHistory(req.params.name)); }
  catch (e) { res.status(404).json({ error: e.message }); }
});

app.get('/api/projects/:name/history/:commitId', (req, res) => {
  try { res.json(store.getCommit(req.params.name, req.params.commitId)); }
  catch (e) { res.status(404).json({ error: e.message }); }
});

app.post('/api/projects/:name/history/:commitId/rollback', (req, res) => {
  try {
    const source = req.body.source || 'browser';
    const result = store.rollback(req.params.name, req.params.commitId, source);
    if (req.body.session) broadcastToBrowsers(req.body.session, 'data:changed', { project: req.params.name });
    res.json(result);
  } catch (e) { res.status(400).json({ error: e.message }); }
});

// ---- WIKI IMPORT ----

app.post('/api/wiki/parse', async (req, res) => {
  try {
    const { url } = req.body;
    if (!url) return res.status(400).json({ error: 'URL is required' });
    try { new URL(url); } catch { return res.status(400).json({ error: 'Invalid URL' }); }
    const result = await parseUrl(url);
    res.json(result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ---- SPA FALLBACK ----
app.get('*', (_req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ---- WEBSOCKET SERVER ----
const wss = new WebSocketServer({ server, path: '/ws' });

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const sessionCode = url.searchParams.get('session');
  const clientType = url.searchParams.get('type') || 'browser'; // 'browser' or 'mcp'

  if (!sessionCode || !sessions.has(sessionCode)) {
    ws.send(JSON.stringify({ event: 'error', data: { message: 'Invalid session code' } }));
    ws.close();
    return;
  }

  const session = sessions.get(sessionCode);

  if (clientType === 'mcp') {
    session.mcpClients.add(ws);
    // Notify browsers that MCP connected
    broadcastToBrowsers(sessionCode, 'mcp:connected', { timestamp: Date.now() });
  } else {
    session.browsers.add(ws);
  }

  ws.sessionCode = sessionCode;
  ws.clientType = clientType;

  ws.send(JSON.stringify({ event: 'connected', data: { session: sessionCode, type: clientType } }));

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      // MCP sends data changes — broadcast to browsers
      if (msg.event === 'data:changed') {
        broadcastToBrowsers(sessionCode, 'data:changed', msg.data);
      }
    } catch {}
  });

  ws.on('close', () => {
    if (clientType === 'mcp') {
      session.mcpClients.delete(ws);
      broadcastToBrowsers(sessionCode, 'mcp:disconnected', { timestamp: Date.now() });
    } else {
      session.browsers.delete(ws);
    }
    // Clean up empty sessions
    if (session.browsers.size === 0 && session.mcpClients.size === 0) {
      sessions.delete(sessionCode);
    }
  });
});

// Clean up stale sessions every 30 minutes
setInterval(() => {
  const now = Date.now();
  for (const [code, session] of sessions) {
    if (session.browsers.size === 0 && session.mcpClients.size === 0 && now - session.createdAt > 30 * 60 * 1000) {
      sessions.delete(code);
    }
  }
}, 30 * 60 * 1000);

// ---- START ----
server.listen(PORT, () => {
  console.log(`Dataset Builder by Tryll Engine — running on port ${PORT}`);
});
