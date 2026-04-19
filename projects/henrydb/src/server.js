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
        // createPgServer calls listen internally, wait for it
        // Check if already listening
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

  async stop() {
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
