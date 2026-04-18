// server.js — HTTP/JSON API for HenryDB
// Usage: node server.js [--port 3000] [--dir ./data]

import { createServer } from 'node:http';
import { PersistentDatabase } from './persistent-db.js';
import { join } from 'node:path';

const args = process.argv.slice(2);
const portIdx = args.indexOf('--port');
const dirIdx = args.indexOf('--dir');
const port = portIdx >= 0 ? parseInt(args[portIdx + 1], 10) : 3000;
const dataDir = dirIdx >= 0 ? args[dirIdx + 1] : join(process.cwd(), '.henrydb-data');

// Initialize database
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

  // CORS preflight
  if (req.method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    });
    return res.end();
  }

  try {
    // GET /health — health check
    if (req.method === 'GET' && path === '/health') {
      return sendJSON(res, 200, {
        status: 'ok',
        version: '0.1.0',
        tables: [...db.tables.keys()],
        functions: [...(db._functions?.keys() || [])],
      });
    }

    // POST /query — execute SQL and return results
    if (req.method === 'POST' && path === '/query') {
      const body = await parseBody(req);
      if (!body.sql) return sendJSON(res, 400, { error: 'Missing "sql" field' });

      const startTime = Date.now();
      const result = db.execute(body.sql);
      const duration = Date.now() - startTime;

      return sendJSON(res, 200, {
        ...result,
        duration_ms: duration,
      });
    }

    // POST /execute — execute SQL (alias for /query, for clarity)
    if (req.method === 'POST' && path === '/execute') {
      const body = await parseBody(req);
      if (!body.sql) return sendJSON(res, 400, { error: 'Missing "sql" field' });

      const startTime = Date.now();
      const result = db.execute(body.sql);
      const duration = Date.now() - startTime;

      return sendJSON(res, 200, {
        ...result,
        duration_ms: duration,
      });
    }

    // GET /tables — list all tables with schema info
    if (req.method === 'GET' && path === '/tables') {
      const tables = {};
      for (const [name, table] of db.tables) {
        tables[name] = {
          columns: table.schema.map(c => ({
            name: c.name,
            type: c.type,
            primaryKey: !!c.primaryKey,
            notNull: !!c.notNull,
            unique: !!c.unique,
          })),
          indexes: [...(table.indexes?.keys() || [])],
        };
      }
      return sendJSON(res, 200, { tables });
    }

    // 404 for unknown routes
    sendJSON(res, 404, { error: `Not found: ${req.method} ${path}` });
  } catch (err) {
    // SQL errors return 400, internal errors return 500
    const statusCode = err.message.includes('syntax') || err.message.includes('not found')
      || err.message.includes('does not exist') || err.message.includes('already exists')
      ? 400 : 500;
    sendJSON(res, statusCode, { error: err.message });
  }
});

server.listen(port, () => {
  console.log(`HenryDB HTTP server listening on http://localhost:${port}`);
  console.log('Endpoints:');
  console.log('  GET  /health  — server status');
  console.log('  POST /query   — execute SQL (body: {"sql": "SELECT ..."})');
  console.log('  POST /execute — execute SQL (alias)');
  console.log('  GET  /tables  — list tables with schema');
});

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\nShutting down...');
  db.close();
  server.close();
  process.exit(0);
});

export { server, db };
