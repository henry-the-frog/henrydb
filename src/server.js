// server.js — HTTP API server for HenryDB
// No external dependencies — uses native Node http module.

import { createServer } from 'node:http';
import { Database } from './db.js';

/**
 * HenryDBServer — HTTP interface for the database engine.
 * 
 * Endpoints:
 *   POST /sql       — Execute SQL query (body: { sql: "..." })
 *   GET  /tables    — List all tables
 *   GET  /stats     — Database statistics
 *   GET  /health    — Health check
 */
export class HenryDBServer {
  constructor(options = {}) {
    this._db = options.database || new Database(options.dbOptions || {});
    this._port = options.port || 0; // 0 = random available port
    this._server = null;
    this._requestCount = 0;
    this._startTime = null;
  }

  /** Start the server. Returns the actual port. */
  async start() {
    return new Promise((resolve, reject) => {
      this._server = createServer((req, res) => this._handleRequest(req, res));
      this._server.listen(this._port, () => {
        this._port = this._server.address().port;
        this._startTime = Date.now();
        resolve(this._port);
      });
      this._server.on('error', reject);
    });
  }

  /** Stop the server. */
  async stop() {
    return new Promise((resolve) => {
      if (this._server) {
        this._server.close(resolve);
      } else {
        resolve();
      }
    });
  }

  /** Get the database instance. */
  get database() { return this._db; }
  
  /** Get the port number. */
  get port() { return this._port; }

  // ---- Request handling ----

  async _handleRequest(req, res) {
    this._requestCount++;
    
    // CORS headers
    res.setHeader('Access-Control-Allow-Origin', '*');
    res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
    res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
    
    if (req.method === 'OPTIONS') {
      res.writeHead(204);
      res.end();
      return;
    }

    try {
      const url = new URL(req.url, `http://localhost:${this._port}`);
      const path = url.pathname;

      if (path === '/sql' && req.method === 'POST') {
        return this._handleSQL(req, res);
      } else if (path === '/tables' && req.method === 'GET') {
        return this._handleTables(req, res);
      } else if (path === '/stats' && req.method === 'GET') {
        return this._handleStats(req, res);
      } else if (path === '/health' && req.method === 'GET') {
        return this._json(res, 200, { status: 'ok', uptime: Date.now() - this._startTime });
      } else {
        return this._json(res, 404, { error: 'Not found', endpoints: ['/sql', '/tables', '/stats', '/health'] });
      }
    } catch (e) {
      return this._json(res, 500, { error: 'Internal server error', message: e.message });
    }
  }

  async _handleSQL(req, res) {
    const body = await this._readBody(req);
    let parsed;
    try {
      parsed = JSON.parse(body);
    } catch (e) {
      return this._json(res, 400, { error: 'Invalid JSON body. Expected: { "sql": "..." }' });
    }

    const { sql } = parsed;
    if (!sql || typeof sql !== 'string') {
      return this._json(res, 400, { error: 'Missing "sql" field in request body' });
    }

    const startTime = performance.now();
    try {
      const result = this._db.execute(sql);
      const elapsed = performance.now() - startTime;
      
      return this._json(res, 200, {
        success: true,
        result: {
          rows: result.rows || [],
          count: result.count ?? result.rows?.length ?? 0,
          type: result.type || 'OK',
          message: result.message,
        },
        timing: { ms: parseFloat(elapsed.toFixed(2)) },
      });
    } catch (e) {
      const elapsed = performance.now() - startTime;
      return this._json(res, 400, {
        success: false,
        error: e.message,
        timing: { ms: parseFloat(elapsed.toFixed(2)) },
      });
    }
  }

  _handleTables(req, res) {
    const tables = [];
    for (const [name, table] of this._db.tables) {
      tables.push({
        name,
        columns: table.schema.map(c => ({ name: c.name, type: c.type })),
        rowCount: table.heap ? table.heap.rowCount : 0,
      });
    }
    return this._json(res, 200, { tables });
  }

  _handleStats(req, res) {
    return this._json(res, 200, {
      tables: this._db.tables.size,
      views: this._db.views?.size || 0,
      indexes: this._db.indexCatalog?.size || 0,
      requests: this._requestCount,
      uptime: Date.now() - this._startTime,
      mvcc: this._db._mvcc ? this._db._mvcc.getStats() : null,
    });
  }

  // ---- Helpers ----

  _json(res, status, data) {
    res.writeHead(status, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(data));
  }

  _readBody(req) {
    return new Promise((resolve, reject) => {
      let body = '';
      req.on('data', chunk => body += chunk);
      req.on('end', () => resolve(body));
      req.on('error', reject);
    });
  }
}

// CLI: run as standalone server
const isMain = process.argv[1] && process.argv[1].endsWith('server.js');
if (isMain) {
  const port = parseInt(process.argv[2] || '3456');
  const server = new HenryDBServer({ port });
  server.start().then(p => {
    console.log(`HenryDB server listening on http://localhost:${p}`);
    console.log('Endpoints: POST /sql, GET /tables, GET /stats, GET /health');
  });
}
