// server.test.js — HTTP/JSON API tests for HenryDB
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { createServer, request as httpRequest } from 'node:http';
import { Database } from './db.js';

// In-memory test server (no persistence needed)
let server, port, db;

function request(method, path, body = null) {
  return new Promise((resolve, reject) => {
    const options = {
      hostname: 'localhost',
      port,
      path,
      method,
      headers: body ? { 'Content-Type': 'application/json' } : {},
    };
    const req = httpRequest(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          resolve({ status: res.statusCode, body: JSON.parse(data) });
        } catch {
          resolve({ status: res.statusCode, body: data });
        }
      });
    });
    req.on('error', reject);
    if (body) req.write(JSON.stringify(body));
    req.end();
  });
}

// Create a minimal test server inline (avoid importing server.js which opens PersistentDatabase)
function createTestServer() {
  db = new Database(); // In-memory
  const srv = createServer(async (req, res) => {
    const url = new URL(req.url, `http://localhost:${port}`);
    const path = url.pathname;
    
    function sendJSON(code, body) {
      const json = JSON.stringify(body);
      res.writeHead(code, { 'Content-Type': 'application/json' });
      res.end(json);
    }
    
    function parseBody() {
      return new Promise((resolve, reject) => {
        let data = '';
        req.on('data', c => data += c);
        req.on('end', () => { try { resolve(JSON.parse(data || '{}')); } catch { reject(new Error('Bad JSON')); } });
      });
    }
    
    try {
      if (req.method === 'GET' && path === '/health') {
        return sendJSON(200, { status: 'ok', version: '0.1.0', tables: [...db.tables.keys()] });
      }
      if (req.method === 'POST' && (path === '/query' || path === '/execute')) {
        const body = await parseBody();
        if (!body.sql) return sendJSON(400, { error: 'Missing "sql" field' });
        const t0 = Date.now();
        const result = db.execute(body.sql);
        return sendJSON(200, { ...result, duration_ms: Date.now() - t0 });
      }
      if (req.method === 'GET' && path === '/tables') {
        const tables = {};
        for (const [name, table] of db.tables) {
          tables[name] = { columns: table.schema.map(c => ({ name: c.name, type: c.type, primaryKey: !!c.primaryKey })) };
        }
        return sendJSON(200, { tables });
      }
      sendJSON(404, { error: `Not found: ${req.method} ${path}` });
    } catch (err) {
      sendJSON(400, { error: err.message });
    }
  });
  return srv;
}

describe('HenryDB HTTP Server', () => {
  before(async () => {
    server = createTestServer();
    await new Promise(resolve => {
      server.listen(0, () => {
        port = server.address().port;
        resolve();
      });
    });
  });
  
  after(() => {
    server.close();
  });

  describe('GET /health', () => {
    it('returns status ok', async () => {
      const r = await request('GET', '/health');
      assert.equal(r.status, 200);
      assert.equal(r.body.status, 'ok');
      assert.ok(Array.isArray(r.body.tables));
    });
  });

  describe('POST /query', () => {
    it('creates a table', async () => {
      const r = await request('POST', '/query', { sql: 'CREATE TABLE test (id INT PRIMARY KEY, name TEXT)' });
      assert.equal(r.status, 200);
      assert.equal(r.body.type, 'OK');
    });

    it('inserts rows', async () => {
      const r = await request('POST', '/query', { sql: "INSERT INTO test VALUES (1, 'Alice')" });
      assert.equal(r.status, 200);
      assert.equal(r.body.count, 1);
    });

    it('selects rows', async () => {
      await request('POST', '/query', { sql: "INSERT INTO test VALUES (2, 'Bob')" });
      const r = await request('POST', '/query', { sql: 'SELECT * FROM test ORDER BY id' });
      assert.equal(r.status, 200);
      assert.equal(r.body.type, 'ROWS');
      assert.equal(r.body.rows.length, 2);
      assert.equal(r.body.rows[0].name, 'Alice');
      assert.equal(r.body.rows[1].name, 'Bob');
    });

    it('includes duration_ms', async () => {
      const r = await request('POST', '/query', { sql: 'SELECT 1' });
      assert.equal(r.status, 200);
      assert.ok(typeof r.body.duration_ms === 'number');
    });
  });

  describe('POST /execute', () => {
    it('works as alias for /query', async () => {
      const r = await request('POST', '/execute', { sql: 'SELECT * FROM test' });
      assert.equal(r.status, 200);
      assert.ok(r.body.rows);
    });
  });

  describe('error handling', () => {
    it('returns 400 for missing sql field', async () => {
      const r = await request('POST', '/query', {});
      assert.equal(r.status, 400);
      assert.ok(r.body.error.includes('Missing'));
    });

    it('returns 400 for SQL syntax error', async () => {
      const r = await request('POST', '/query', { sql: 'SELECTT * FROM test' });
      assert.equal(r.status, 400);
      assert.ok(r.body.error);
    });

    it('returns 400 for non-existent table', async () => {
      const r = await request('POST', '/query', { sql: 'SELECT * FROM nonexistent' });
      assert.equal(r.status, 400);
      assert.ok(r.body.error);
    });
  });

  describe('GET /tables', () => {
    it('returns table schema', async () => {
      const r = await request('GET', '/tables');
      assert.equal(r.status, 200);
      assert.ok(r.body.tables.test);
      assert.equal(r.body.tables.test.columns[0].name, 'id');
      assert.equal(r.body.tables.test.columns[0].primaryKey, true);
    });
  });

  describe('404 handling', () => {
    it('returns 404 for unknown routes', async () => {
      const r = await request('GET', '/nonexistent');
      assert.equal(r.status, 404);
      assert.ok(r.body.error);
    });
  });

  describe('UDF support', () => {
    it('can create and call UDFs via HTTP', async () => {
      await request('POST', '/query', { sql: "CREATE FUNCTION double_it(x INT) RETURNS INT AS $$ SELECT x * 2 $$" });
      await request('POST', '/query', { sql: 'CREATE TABLE nums (id INT PRIMARY KEY, val INT)' });
      await request('POST', '/query', { sql: 'INSERT INTO nums VALUES (1, 21)' });
      
      const r = await request('POST', '/query', { sql: 'SELECT double_it(val) as result FROM nums' });
      assert.equal(r.status, 200);
      assert.equal(r.body.rows[0].result, 42);
    });
  });
});
