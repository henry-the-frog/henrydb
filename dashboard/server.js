#!/usr/bin/env node
// dashboard/server.js — Dashboard webhook server
// Receives task updates from work sessions, serves dashboard state to browser.
// Endpoints:
//   POST /api/task-update   — task start/complete/session-ended events
//   POST /api/queue-update  — full queue replacement
//   POST /api/archive       — archive current day to history/
//   GET  /api/dashboard     — current dashboard state (light: schedule + current + stats)
//   GET  /api/rich          — rich dashboard data (projects, blog, PRs, etc.)
//   GET  /api/history/:date — historical day data
// Auth: POST requests require Authorization: Bearer <DASHBOARD_TOKEN>

'use strict';

const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = parseInt(process.env.DASHBOARD_PORT || '3000', 10);
const DATA_DIR = path.join(__dirname, 'data');
const HISTORY_DIR = path.join(__dirname, 'history');

// Load token from env or .env file
function loadToken() {
  if (process.env.DASHBOARD_TOKEN) return process.env.DASHBOARD_TOKEN;
  try {
    const envFile = fs.readFileSync(path.join(process.env.HOME || '/tmp', '.openclaw', '.env'), 'utf8');
    const lines = envFile.split('\n');
    for (const line of lines) {
      const m = line.match(/^DASHBOARD_TOKEN=(.+)/);
      if (m) return m[1].trim();
    }
  } catch {}
  return null;
}

const TOKEN = loadToken();

// Ensure directories exist
for (const dir of [DATA_DIR, HISTORY_DIR]) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

// --- State ---
// Current state is the live dashboard data. On startup, load from data/dashboard.json if it exists.
let state = loadState();

function loadState() {
  try {
    const raw = fs.readFileSync(path.join(DATA_DIR, 'dashboard.json'), 'utf8');
    return JSON.parse(raw);
  } catch {
    return {
      generated: new Date().toISOString(),
      current: { status: 'idle', mode: 'THINK', task: 'No data' },
      schedule: { date: today(), blocks: [], backlog: [] },
      stats: { blocksCompleted: 0, blocksTotal: 0, modeDistribution: {} },
    };
  }
}

function saveState() {
  try {
    fs.writeFileSync(path.join(DATA_DIR, 'dashboard.json'), JSON.stringify(state, null, 2));
  } catch (e) {
    console.error('Failed to save state:', e.message);
  }
}

function loadRich() {
  try {
    return JSON.parse(fs.readFileSync(path.join(DATA_DIR, 'rich.json'), 'utf8'));
  } catch {
    return null;
  }
}

function today() {
  // Use Denver time
  const now = new Date();
  const denver = new Date(now.toLocaleString('en-US', { timeZone: 'America/Denver' }));
  const y = denver.getFullYear();
  const m = String(denver.getMonth() + 1).padStart(2, '0');
  const d = String(denver.getDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

// --- Auth ---
function checkAuth(req) {
  if (!TOKEN) return true; // No token configured = no auth
  const auth = req.headers['authorization'] || '';
  return auth === `Bearer ${TOKEN}`;
}

// --- Body parser ---
function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    req.on('data', c => chunks.push(c));
    req.on('end', () => {
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString()));
      } catch (e) {
        reject(new Error('Invalid JSON'));
      }
    });
    req.on('error', reject);
  });
}

// --- CORS ---
function setCors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
}

// --- Route handlers ---

async function handleTaskUpdate(req, res) {
  const body = await readBody(req);
  const { action, task } = body;

  if (!action || !task) {
    return sendJSON(res, 400, { error: 'Missing action or task' });
  }

  if (action === 'start') {
    state.current = {
      status: 'in-progress',
      mode: task.mode || 'BUILD',
      task: task.description || task.task || '',
      startedAt: new Date().toISOString(),
      taskId: task.id,
    };
  } else if (action === 'complete') {
    state.current = {
      status: 'done',
      mode: state.current?.mode || 'BUILD',
      task: state.current?.task || '',
      completedAt: new Date().toISOString(),
      taskId: task.id,
      summary: task.summary || '',
      durationMs: task.duration_ms || 0,
    };
    // Update stats
    if (!state.stats) state.stats = { blocksCompleted: 0, blocksTotal: 0, modeDistribution: {} };
    state.stats.blocksCompleted = (state.stats.blocksCompleted || 0) + 1;
    const mode = state.current.mode;
    if (!state.stats.modeDistribution) state.stats.modeDistribution = {};
    state.stats.modeDistribution[mode] = (state.stats.modeDistribution[mode] || 0) + 1;
  } else if (action === 'session-ended') {
    state.current = {
      status: 'session-ended',
      mode: state.current?.mode || '',
      task: 'Session ended',
      endedAt: new Date().toISOString(),
    };
  } else {
    return sendJSON(res, 400, { error: `Unknown action: ${action}` });
  }

  state.generated = new Date().toISOString();
  saveState();
  sendJSON(res, 200, { ok: true, action });
}

async function handleQueueUpdate(req, res) {
  const body = await readBody(req);

  if (body.queue) {
    if (!state.schedule) state.schedule = {};
    state.schedule.queue = body.queue;
    state.schedule.date = body.date || today();
    // Recalculate stats
    const queue = body.queue;
    const done = queue.filter(t => t.status === 'done').length;
    const dist = {};
    for (const t of queue.filter(t => t.status === 'done')) {
      dist[t.mode] = (dist[t.mode] || 0) + 1;
    }
    state.stats = {
      blocksCompleted: done,
      blocksTotal: queue.length,
      modeDistribution: dist,
    };
  }

  state.generated = new Date().toISOString();
  saveState();
  sendJSON(res, 200, { ok: true });
}

async function handleArchive(req, res) {
  const body = await readBody(req);
  const date = body.date || today();

  // Save current state as history
  const histFile = path.join(HISTORY_DIR, `${date}.json`);
  try {
    fs.writeFileSync(histFile, JSON.stringify(state, null, 2));
  } catch (e) {
    return sendJSON(res, 500, { error: `Failed to write history: ${e.message}` });
  }

  // Reset state for new day
  state = {
    generated: new Date().toISOString(),
    current: { status: 'idle', mode: 'THINK', task: 'New day' },
    schedule: { date: today(), blocks: [], backlog: [] },
    stats: { blocksCompleted: 0, blocksTotal: 0, modeDistribution: {} },
  };
  saveState();

  sendJSON(res, 200, { ok: true, archived: date, file: histFile });
}

function handleGetDashboard(req, res) {
  // Light response: current + schedule + stats
  const light = {
    generated: state.generated,
    current: state.current,
    schedule: state.schedule,
    stats: state.stats,
  };
  sendJSON(res, 200, light);
}

function handleGetRich(req, res) {
  // Rich data from generate.cjs output
  const rich = loadRich();
  if (rich) {
    sendJSON(res, 200, rich);
  } else {
    sendJSON(res, 200, { generated: state.generated, note: 'No rich data yet. Run generate.cjs.' });
  }
}

function handleGetHistory(req, res, date) {
  const histFile = path.join(HISTORY_DIR, `${date}.json`);
  try {
    const data = JSON.parse(fs.readFileSync(histFile, 'utf8'));
    sendJSON(res, 200, data);
  } catch {
    sendJSON(res, 404, { error: `No history for ${date}` });
  }
}

function handleHealthCheck(req, res) {
  sendJSON(res, 200, {
    status: 'ok',
    uptime: process.uptime(),
    date: today(),
    state: state.current?.status || 'unknown',
  });
}

// --- Helpers ---
function sendJSON(res, code, data) {
  setCors(res);
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(data));
}

function send404(res) {
  sendJSON(res, 404, { error: 'Not found' });
}

// --- Server ---
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  const pathname = url.pathname;

  // CORS preflight
  if (req.method === 'OPTIONS') {
    setCors(res);
    res.writeHead(204);
    res.end();
    return;
  }

  try {
    // POST endpoints (auth required)
    if (req.method === 'POST') {
      if (!checkAuth(req)) {
        return sendJSON(res, 401, { error: 'Unauthorized' });
      }

      if (pathname === '/api/task-update') return await handleTaskUpdate(req, res);
      if (pathname === '/api/queue-update') return await handleQueueUpdate(req, res);
      if (pathname === '/api/archive') return await handleArchive(req, res);
      return send404(res);
    }

    // GET endpoints (no auth)
    if (req.method === 'GET') {
      if (pathname === '/api/dashboard') return handleGetDashboard(req, res);
      if (pathname === '/api/rich') return handleGetRich(req, res);
      if (pathname === '/api/health') return handleHealthCheck(req, res);

      // /api/history/:date
      const histMatch = pathname.match(/^\/api\/history\/(\d{4}-\d{2}-\d{2})$/);
      if (histMatch) return handleGetHistory(req, res, histMatch[1]);

      return send404(res);
    }

    send404(res);
  } catch (e) {
    console.error(`Error handling ${req.method} ${pathname}:`, e.message);
    sendJSON(res, 500, { error: e.message });
  }
});

server.listen(PORT, () => {
  console.log(`Dashboard server listening on http://localhost:${PORT}`);
  console.log(`Auth: ${TOKEN ? 'enabled' : 'disabled (no token found)'}`);
  console.log(`Data dir: ${DATA_DIR}`);
  console.log(`History dir: ${HISTORY_DIR}`);
});

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('SIGTERM received, saving state...');
  saveState();
  process.exit(0);
});

process.on('SIGINT', () => {
  console.log('SIGINT received, saving state...');
  saveState();
  process.exit(0);
});
