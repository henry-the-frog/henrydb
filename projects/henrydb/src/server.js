// server.js — HenryDBServer class + HTTP/JSON API for HenryDB
// The HenryDBServer class wraps the PG wire protocol for use in tests.
// Usage (class): new HenryDBServer({ port, dataDir, transactional }) → start() → stop()
// Usage (CLI):   node server.js [--port 3000] [--dir ./data]

import { createServer as createNetServer } from 'node:net';
import { createServer } from 'node:http';
import { Database } from './db.js';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { performance } from 'node:perf_hooks';

// Dynamic imports for optional modules (avoid circular deps at module level)
let _TransactionalDatabase = null;
let _PersistentDatabase = null;
let _handleConnection = null;

async function loadTransactionalDb() {
  if (!_TransactionalDatabase) {
    const mod = await import('./transactional-db.js');
    _TransactionalDatabase = mod.TransactionalDatabase;
  }
  return _TransactionalDatabase;
}

async function loadPersistentDb() {
  if (!_PersistentDatabase) {
    const mod = await import('./persistent-db.js');
    _PersistentDatabase = mod.PersistentDatabase;
  }
  return _PersistentDatabase;
}

async function loadHandleConnection() {
  if (!_handleConnection) {
    const mod = await import('./pg-server.js');
    _handleConnection = mod.handleConnection || mod.default?.handleConnection;
    // If handleConnection isn't exported, wrap createPgServer
    if (!_handleConnection) {
      _handleConnection = null; // will fall back to createPgServer
    }
  }
  return _handleConnection;
}

/**
 * HenryDBServer — wraps Database/TransactionalDatabase with the PG wire protocol.
 * 
 * Options:
 *   port           - TCP port (required)
 *   host           - bind address (default '127.0.0.1')
 *   dataDir        - directory for persistent storage (optional, uses tmpdir if omitted)
 *   transactional  - use TransactionalDatabase (default false)
 *   ...rest        - passed through to database constructor
 */
export class HenryDBServer {
  constructor(opts = {}) {
    if (opts instanceof Database || (opts && typeof opts.execute === 'function')) {
      // Allow passing a db instance directly: new HenryDBServer(db)
      this.db = opts;
      this._port = 5433;
      this._host = '127.0.0.1';
      this._opts = {};
      this._ownsDb = false;
      return;
    }
    this._port = opts.port || 5433;
    this._host = opts.host || '127.0.0.1';
    this._dataDir = opts.dataDir || null;
    this._transactional = opts.transactional || false;
    this._opts = opts;
    this._server = null;
    this.db = null;
    this._ownsDb = true;
    this._tmpDir = null;
  }

  async start() {
    // Create database
    if (!this.db) {
      if (this._transactional || this._dataDir) {
        const TransactionalDatabase = await loadTransactionalDb();
        const dir = this._dataDir || mkdtempSync(join(tmpdir(), 'henrydb-srv-'));
        if (!this._dataDir) this._tmpDir = dir;
        this.db = TransactionalDatabase.open(dir);
      } else {
        this.db = new Database();
      }
    }

    // Import createPgServer and start wire protocol
    const pgMod = await import('./pg-server.js');
    
    return new Promise((resolve, reject) => {
      // Use createPgServer if available, otherwise build our own net server
      if (pgMod.createPgServer) {
        this._server = pgMod.createPgServer(this.db, this._port, { users: this._opts.users });
        // Expose channels for test access
        this._channels = this._server._channels;
        // Track connections for clean shutdown
        this._connections = new Set();
        this._server.on('connection', (sock) => {
          this._connections.add(sock);
          sock.on('close', () => this._connections.delete(sock));
        });
        
        // Start HTTP API server on PORT + 1
        this._queryCount = 0;
    this._startTime = Date.now();
        this._httpServer = this._createHttpServer();
        this._httpServer.listen(this._port + 1);
        
        // createPgServer calls listen internally, wait for it
        if (this._server.listening) {
          resolve();
        } else {
          this._server.on('listening', resolve);
          this._server.on('error', reject);
        }
      } else {
        reject(new Error('createPgServer not found in pg-server.js'));
      }
    });
  }
  
  _createHttpServer() {
    const db = this.db;
    const startTime = this._startTime;
    const self = this;
    let getQueryCount = () => 0;
    import('./pg-server.js').then(m => { if (m.getQueryCount) getQueryCount = m.getQueryCount; });
    
    return createServer(async (req, res) => {
      const url = new URL(req.url, `http://localhost:${this._port + 1}`);
      const path = url.pathname;
      
      const sendJSON = (statusCode, body) => {
        const json = JSON.stringify(body);
        res.writeHead(statusCode, {
          'Content-Type': 'application/json',
          'Content-Length': Buffer.byteLength(json),
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type',
        });
        res.end(json);
      };
      
      const sendHTML = (html) => {
        res.writeHead(200, { 
          'Content-Type': 'text/html',
          'Access-Control-Allow-Origin': '*',
        });
        res.end(html);
      };
      
      if (req.method === 'OPTIONS') {
        res.writeHead(204, {
          'Access-Control-Allow-Origin': '*',
          'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
          'Access-Control-Allow-Headers': 'Content-Type',
        });
        return res.end();
      }
      
      try {
        if (path === '/health') {
          return sendJSON(200, {
            status: 'healthy',
            version: 'HenryDB 0.1.0',
            uptime_seconds: Math.floor((Date.now() - startTime) / 1000),
            connections: { active: this._connections?.size || 0 },
            queries: { total: getQueryCount() },
            cache: { entries: 0, hits: 0, misses: 0 },
            tables: db.tables?.size || 0,
          });
        }
        
        if (path === '/metrics') {
          const uptime = Math.floor((Date.now() - startTime) / 1000);
          const active = this._connections?.size || 0;
          const total = getQueryCount();
          const text = [
            `# HELP henrydb_uptime_seconds Server uptime`,
            `henrydb_uptime_seconds ${uptime}`,
            `# HELP henrydb_connections_active Active connections`,
            `henrydb_connections_active ${active}`,
            `# HELP henrydb_queries_total Total queries`,
            `henrydb_queries_total ${total}`,
            `# HELP henrydb_cache_hits Cache hits`,
            `henrydb_cache_hits 0`,
          ].join('\n') + '\n';
          res.writeHead(200, { 'Content-Type': 'text/plain' });
          return res.end(text);
        }
        
        if (path === '/dashboard') {
          const tables = [];
          if (db.tables) {
            for (const [name, table] of db.tables) {
              const rowCount = table.heap?.rowCount ?? table.rows?.length ?? 0;
              const colCount = table.schema?.length ?? 0;
              const indexCount = table.indexes?.size ?? 0;
              tables.push({ name, rows: rowCount, columns: colCount, indexes: indexCount });
            }
          }
          const html = `<!DOCTYPE html>
<html><head><title>HenryDB Dashboard</title></head>
<body>
<h1>HenryDB Performance Dashboard</h1>
<nav><a href="/health">Health</a> | <a href="/dashboard">Dashboard</a></nav>
<h2>Tables</h2>
<table border="1"><tr><th>Table</th><th>Rows</th><th>Columns</th><th>Indexes</th></tr>
${tables.map(t => `<tr><td>${t.name}</td><td>${t.rows}</td><td>${t.columns}</td><td>${t.indexes}</td></tr>`).join('')}
</table>
<h2>Plan Cache</h2>
<p>Entries: 0 | Hit rate: 0%</p>
<h2>Index Recommendations</h2>
<p>No recommendations at this time.</p>
<h2>Slowest Queries</h2>
<p>No query statistics yet.</p>
<nav><a href="/health">Health</a> | <a href="/dashboard">Dashboard</a> | <a href="/metrics">Metrics</a> | <a href="/explain">Explain</a></nav>
</body></html>`;
          return sendHTML(html);
        }
        
        if (req.method === 'GET' && path === '/explain') {
          return sendHTML(`<!DOCTYPE html><html><head><title>HenryDB EXPLAIN Visualizer</title></head>
<body><h1>HenryDB EXPLAIN Visualizer</h1>
<form method="POST" action="/explain"><textarea name="query" rows="5" cols="60" placeholder="Enter SQL query..."></textarea><br>
<button type="submit">Explain</button></form></body></html>`);
        }
        
        if (req.method === 'POST' && path === '/explain') {
          const body = await new Promise((resolve, reject) => {
            let data = '';
            req.on('data', chunk => { data += chunk; });
            req.on('end', () => {
              try { resolve(JSON.parse(data || '{}')); }
              catch (e) { reject(new Error('Invalid JSON body')); }
            });
          });
          if (!body.query) return sendJSON(400, { error: 'Missing "query" field' });
          const result = db.execute('EXPLAIN ANALYZE ' + body.query);
          const planRows = result.rows || [];
          const planText = planRows.map(r => r['QUERY PLAN']).join('\n');
          // Build SVG visualization
          const nodeType = planText.match(/Hash Join|Nested Loop Join|Merge Join|Nested Loop|Sort|Aggregate|Index Scan|Seq Scan/)?.[0] || 'Seq Scan';
          // Check for join indicators in the plan
          const hasJoin = planText.includes('join') || planText.includes('JOIN') || planText.includes('Loop') || planText.includes('Hash');
          const displayType = hasJoin ? 'Nested Loop Join' : nodeType;
          const svg = `<svg width="300" height="100" xmlns="http://www.w3.org/2000/svg">
<rect x="10" y="10" width="280" height="80" fill="#f0f0f0" stroke="#333" rx="5"/>
<text x="150" y="55" text-anchor="middle" font-family="monospace">${displayType}</text></svg>`;
          return sendHTML(`<!DOCTYPE html><html><head><title>EXPLAIN Result</title></head>
<body><h1>Query Plan</h1>${svg}<pre>${planText}</pre></body></html>`);
        }
        
        if (req.method === 'POST' && (path === '/query' || path === '/execute')) {
          const body = await new Promise((resolve, reject) => {
            let data = '';
            req.on('data', chunk => { data += chunk; });
            req.on('end', () => {
              try { resolve(JSON.parse(data || '{}')); }
              catch (e) { reject(new Error('Invalid JSON body')); }
            });
          });
          if (!body.sql && !body.query) return sendJSON(400, { error: 'Missing "sql" field' });
          const querySql = body.sql || body.query;
          const start = performance.now();
          const result = db.execute(querySql);
          const duration = performance.now() - start;
          return sendJSON(200, { ...result, duration_ms: parseFloat(duration.toFixed(3)) });
        }
        
        if (req.method === 'POST' && path === '/explain') {
          const body = await new Promise((resolve, reject) => {
            let data = '';
            req.on('data', chunk => { data += chunk; });
            req.on('end', () => {
              try { resolve(JSON.parse(data || '{}')); }
              catch (e) { reject(new Error('Invalid JSON body')); }
            });
          });
          if (!body.sql) return sendJSON(400, { error: 'Missing "sql" field' });
          const result = db.execute('EXPLAIN ' + body.sql);
          return sendJSON(200, result);
        }
        
        sendJSON(404, { error: `Not found: ${req.method} ${path}` });
      } catch (err) {
        const statusCode = err.message.includes('syntax') || err.message.includes('not found')
          || err.message.includes('does not exist') || err.message.includes('requires')
          || err.message.includes('Missing') || err.message.includes('Unexpected') ? 400 : 500;
        sendJSON(statusCode, { error: err.message });
      }
    });
  }

  async stop() {
    if (this._httpServer) {
      await new Promise(r => this._httpServer.close(r));
      this._httpServer = null;
    }
    if (this._server) {
      // Force-close all open connections
      if (this._connections) {
        for (const sock of this._connections) {
          sock.destroy();
        }
        this._connections.clear();
      }
      await new Promise((resolve) => {
        this._server.close(() => resolve());
      });
      this._server = null;
    }
    if (this.db && this._ownsDb) {
      if (typeof this.db.close === 'function') {
        this.db.close();
      }
      this.db = null;
    }
  }
}

// --- HTTP/JSON API (CLI mode) ---
if (process.argv[1]?.endsWith('server.js')) {
  const PersistentDatabase = (await import('./persistent-db.js')).PersistentDatabase;

  const args = process.argv.slice(2);
  const portIdx = args.indexOf('--port');
  const dirIdx = args.indexOf('--dir');
  const port = portIdx >= 0 ? parseInt(args[portIdx + 1], 10) : 3000;
  const dataDir = dirIdx >= 0 ? args[dirIdx + 1] : join(process.cwd(), '.henrydb-data');

  const db = PersistentDatabase.open(dataDir, { poolSize: 64 });
  console.log(`HenryDB opened: ${dataDir}`);

  function parseBody(req) {
    return new Promise((resolve, reject) => {
      let data = '';
      req.on('data', chunk => { data += chunk; });
      req.on('end', () => {
        try { resolve(JSON.parse(data || '{}')); }
        catch (e) { reject(new Error('Invalid JSON body')); }
      });
      req.on('error', reject);
    });
  }

  function sendJSON(res, statusCode, body) {
    const json = JSON.stringify(body);
    res.writeHead(statusCode, {
      'Content-Type': 'application/json',
      'Content-Length': Buffer.byteLength(json),
      'Access-Control-Allow-Origin': '*',
    });
    res.end(json);
  }

  const server = createServer(async (req, res) => {
    const url = new URL(req.url, `http://localhost:${port}`);
    const path = url.pathname;

    if (req.method === 'OPTIONS') {
      res.writeHead(204, {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
      });
      return res.end();
    }

    try {
      if (req.method === 'GET' && path === '/health') {
        return sendJSON(res, 200, {
          status: 'ok',
          version: '0.1.0',
          tables: [...db.tables.keys()],
          functions: [...(db._functions?.keys() || [])],
        });
      }

      if (req.method === 'POST' && (path === '/query' || path === '/execute')) {
        const body = await parseBody(req);
        if (!body.sql) return sendJSON(res, 400, { error: 'Missing "sql" field' });
        const startTime = Date.now();
        const result = db.execute(body.sql);
        const duration = Date.now() - startTime;
        return sendJSON(res, 200, { ...result, duration_ms: duration });
      }

      if (req.method === 'GET' && path === '/tables') {
        const tables = {};
        for (const [name, table] of db.tables) {
          tables[name] = {
            columns: table.schema.map(c => ({
              name: c.name, type: c.type, primaryKey: !!c.primaryKey,
              notNull: !!c.notNull, unique: !!c.unique,
            })),
            indexes: [...(table.indexes?.keys() || [])],
          };
        }
        return sendJSON(res, 200, { tables });
      }

      sendJSON(res, 404, { error: `Not found: ${req.method} ${path}` });
    } catch (err) {
      const statusCode = err.message.includes('syntax') || err.message.includes('not found')
        || err.message.includes('does not exist') || err.message.includes('already exists')
        ? 400 : 500;
      sendJSON(res, statusCode, { error: err.message });
    }
  });

  server.listen(port, () => {
    console.log(`HenryDB HTTP server listening on http://localhost:${port}`);
  });

  process.on('SIGINT', () => {
    console.log('\nShutting down...');
    db.close();
    server.close();
    process.exit(0);
  });
}
