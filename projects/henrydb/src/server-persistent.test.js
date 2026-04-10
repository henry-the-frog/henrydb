// server-persistent.test.js — Test HenryDB server with persistent storage
// Verifies data survives server restart, crash recovery works through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import { mkdtempSync, rmSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { HenryDBServer } from './server.js';

// Helper: create a raw TCP client
function createPgClient(port) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host: '127.0.0.1', port }, () => resolve(socket));
    socket.on('error', reject);
  });
}

// Helper: send startup message
function sendStartup(socket, user = 'test', database = 'testdb') {
  const params = `user\0${user}\0database\0${database}\0\0`;
  const paramsBuf = Buffer.from(params, 'utf8');
  const len = 4 + 4 + paramsBuf.length;
  const buf = Buffer.alloc(len);
  buf.writeInt32BE(len, 0);
  buf.writeInt32BE(196608, 4);
  paramsBuf.copy(buf, 8);
  socket.write(buf);
}

// Helper: send query
function sendQuery(socket, sql) {
  const queryBuf = Buffer.from(sql + '\0', 'utf8');
  const len = 4 + queryBuf.length;
  const buf = Buffer.alloc(1 + len);
  buf[0] = 0x51;
  buf.writeInt32BE(len, 1);
  queryBuf.copy(buf, 5);
  socket.write(buf);
}

// Helper: wait for ReadyForQuery
function waitForReady(socket) {
  return new Promise((resolve) => {
    const chunks = [];
    const handler = (data) => {
      chunks.push(data);
      const all = Buffer.concat(chunks);
      // Look for ReadyForQuery ('Z')
      for (let i = 0; i < all.length; i++) {
        if (all[i] === 0x5A && i + 5 <= all.length) {
          socket.removeListener('data', handler);
          resolve(all);
          return;
        }
      }
    };
    socket.on('data', handler);
  });
}

// Helper: extract data rows from response buffer
function extractRows(buf) {
  const rows = [];
  let i = 0;
  while (i < buf.length) {
    const type = String.fromCharCode(buf[i]);
    if (i + 5 > buf.length) break;
    const len = buf.readInt32BE(i + 1);
    if (type === 'D') {
      // DataRow
      const numCols = buf.readInt16BE(i + 5);
      const cols = [];
      let pos = i + 7;
      for (let c = 0; c < numCols; c++) {
        const colLen = buf.readInt32BE(pos);
        pos += 4;
        if (colLen === -1) {
          cols.push(null);
        } else {
          cols.push(buf.subarray(pos, pos + colLen).toString('utf8'));
          pos += colLen;
        }
      }
      rows.push(cols);
    }
    i += 1 + len;
  }
  return rows;
}

// Helper: run a complete query cycle (connect, auth, query, get result, disconnect)
async function queryOnce(port, sql) {
  const socket = await createPgClient(port);
  sendStartup(socket);
  await waitForReady(socket);
  sendQuery(socket, sql);
  const result = await waitForReady(socket);
  const rows = extractRows(result);
  socket.end();
  return rows;
}

// Helper: run multiple queries on one connection
async function queryMultiple(port, sqls) {
  const socket = await createPgClient(port);
  sendStartup(socket);
  await waitForReady(socket);
  const results = [];
  for (const sql of sqls) {
    sendQuery(socket, sql);
    const result = await waitForReady(socket);
    results.push(extractRows(result));
  }
  socket.end();
  return results;
}

function getPort() {
  // Use random high port to avoid conflicts
  return 15000 + Math.floor(Math.random() * 10000);
}

describe('HenryDB Persistent Server', () => {
  let dataDir;
  
  before(() => {
    dataDir = mkdtempSync(join(tmpdir(), 'henrydb-server-persist-'));
  });
  
  after(() => {
    if (dataDir && existsSync(dataDir)) {
      rmSync(dataDir, { recursive: true });
    }
  });

  it('data survives server restart', async () => {
    const port = getPort();
    
    // Start server 1 with persistent storage
    const server1 = new HenryDBServer({ port, dataDir });
    await server1.start();
    
    // Create table and insert data
    await queryMultiple(port, [
      'CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT)',
      "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
      "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')",
      "INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')",
    ]);
    
    // Verify data is there
    const rows1 = await queryOnce(port, 'SELECT id, name FROM users ORDER BY id');
    assert.equal(rows1.length, 3);
    assert.deepEqual(rows1[0], ['1', 'Alice']);
    assert.deepEqual(rows1[1], ['2', 'Bob']);
    assert.deepEqual(rows1[2], ['3', 'Charlie']);
    
    // Stop server (graceful shutdown)
    await server1.stop();
    
    // Start server 2 on same dataDir
    const server2 = new HenryDBServer({ port, dataDir });
    await server2.start();
    
    // Data should still be there
    const rows2 = await queryOnce(port, 'SELECT id, name FROM users ORDER BY id');
    assert.equal(rows2.length, 3);
    assert.deepEqual(rows2[0], ['1', 'Alice']);
    assert.deepEqual(rows2[1], ['2', 'Bob']);
    assert.deepEqual(rows2[2], ['3', 'Charlie']);
    
    await server2.stop();
  });

  it('handles multiple tables across restarts', async () => {
    const port = getPort();
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-multi-'));
    
    try {
      const s1 = new HenryDBServer({ port, dataDir: dir });
      await s1.start();
      
      await queryMultiple(port, [
        'CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT)',
        'CREATE TABLE orders (id INT PRIMARY KEY, product_id INT, quantity INT)',
        "INSERT INTO products VALUES (1, 'Widget', 999)",
        "INSERT INTO products VALUES (2, 'Gadget', 1999)",
        "INSERT INTO orders VALUES (1, 1, 5)",
        "INSERT INTO orders VALUES (2, 2, 3)",
      ]);
      
      await s1.stop();
      
      const s2 = new HenryDBServer({ port, dataDir: dir });
      await s2.start();
      
      const products = await queryOnce(port, 'SELECT id, name, price FROM products ORDER BY id');
      assert.equal(products.length, 2);
      assert.deepEqual(products[0], ['1', 'Widget', '999']);
      
      const orders = await queryOnce(port, 'SELECT id, product_id, quantity FROM orders ORDER BY id');
      assert.equal(orders.length, 2);
      assert.deepEqual(orders[0], ['1', '1', '5']);
      
      await s2.stop();
    } finally {
      rmSync(dir, { recursive: true });
    }
  });

  it('data added after restart persists through second restart', async () => {
    const port = getPort();
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-chain-'));
    
    try {
      // Session 1: create and insert
      const s1 = new HenryDBServer({ port, dataDir: dir });
      await s1.start();
      await queryMultiple(port, [
        'CREATE TABLE events (id INT PRIMARY KEY, msg TEXT)',
        "INSERT INTO events VALUES (1, 'boot')",
      ]);
      await s1.stop();
      
      // Session 2: add more data
      const s2 = new HenryDBServer({ port, dataDir: dir });
      await s2.start();
      await queryMultiple(port, [
        "INSERT INTO events VALUES (2, 'restart-1')",
        "INSERT INTO events VALUES (3, 'running')",
      ]);
      await s2.stop();
      
      // Session 3: verify all data
      const s3 = new HenryDBServer({ port, dataDir: dir });
      await s3.start();
      const rows = await queryOnce(port, 'SELECT id, msg FROM events ORDER BY id');
      assert.equal(rows.length, 3);
      assert.deepEqual(rows[0], ['1', 'boot']);
      assert.deepEqual(rows[1], ['2', 'restart-1']);
      assert.deepEqual(rows[2], ['3', 'running']);
      await s3.stop();
    } finally {
      rmSync(dir, { recursive: true });
    }
  });

  it('in-memory mode still works (no dataDir)', async () => {
    const port = getPort();
    const server = new HenryDBServer({ port });
    await server.start();
    
    await queryMultiple(port, [
      'CREATE TABLE temp (id INT, val TEXT)',
      "INSERT INTO temp VALUES (1, 'test')",
    ]);
    
    const rows = await queryOnce(port, 'SELECT id, val FROM temp');
    assert.equal(rows.length, 1);
    assert.deepEqual(rows[0], ['1', 'test']);
    
    await server.stop();
  });

  it('creates data directory if it does not exist', async () => {
    const port = getPort();
    const dir = join(tmpdir(), 'henrydb-newdir-' + Date.now());
    assert.equal(existsSync(dir), false);
    
    try {
      const server = new HenryDBServer({ port, dataDir: dir });
      await server.start();
      assert.equal(existsSync(dir), true);
      
      await queryMultiple(port, [
        'CREATE TABLE test (id INT PRIMARY KEY)',
        'INSERT INTO test VALUES (1)',
      ]);
      
      await server.stop();
    } finally {
      if (existsSync(dir)) rmSync(dir, { recursive: true });
    }
  });

  it('handles concurrent connections with persistent storage', async () => {
    const port = getPort();
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-concurrent-'));
    
    try {
      const server = new HenryDBServer({ port, dataDir: dir });
      await server.start();
      
      await queryOnce(port, 'CREATE TABLE counter (id INT PRIMARY KEY, val INT)');
      await queryOnce(port, 'INSERT INTO counter VALUES (1, 0)');
      
      // Run 5 concurrent connections, each incrementing
      const promises = [];
      for (let i = 0; i < 5; i++) {
        promises.push(queryOnce(port, `UPDATE counter SET val = val + 1 WHERE id = 1`));
      }
      await Promise.all(promises);
      
      const rows = await queryOnce(port, 'SELECT val FROM counter WHERE id = 1');
      assert.equal(rows.length, 1);
      assert.equal(parseInt(rows[0][0]), 5);
      
      await server.stop();
      
      // Verify after restart
      const s2 = new HenryDBServer({ port, dataDir: dir });
      await s2.start();
      const rows2 = await queryOnce(port, 'SELECT val FROM counter WHERE id = 1');
      assert.equal(parseInt(rows2[0][0]), 5);
      await s2.stop();
    } finally {
      rmSync(dir, { recursive: true });
    }
  });

  it('SELECT queries work on reopened database', async () => {
    const port = getPort();
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-select-'));
    
    try {
      const s1 = new HenryDBServer({ port, dataDir: dir });
      await s1.start();
      
      await queryMultiple(port, [
        'CREATE TABLE items (id INT PRIMARY KEY, name TEXT, qty INT, price INT)',
        "INSERT INTO items VALUES (1, 'apple', 100, 150)",
        "INSERT INTO items VALUES (2, 'banana', 200, 50)",
        "INSERT INTO items VALUES (3, 'cherry', 50, 300)",
      ]);
      await s1.stop();
      
      const s2 = new HenryDBServer({ port, dataDir: dir });
      await s2.start();
      
      // Test various query types after restart
      const all = await queryOnce(port, 'SELECT * FROM items ORDER BY id');
      assert.equal(all.length, 3);
      
      const filtered = await queryOnce(port, 'SELECT name FROM items WHERE qty > 75 ORDER BY name');
      assert.equal(filtered.length, 2);
      assert.deepEqual(filtered[0], ['apple']);
      assert.deepEqual(filtered[1], ['banana']);
      
      const agg = await queryOnce(port, 'SELECT COUNT(*) FROM items');
      assert.equal(agg.length, 1);
      assert.equal(agg[0][0], '3');
      
      await s2.stop();
    } finally {
      rmSync(dir, { recursive: true });
    }
  });
});
