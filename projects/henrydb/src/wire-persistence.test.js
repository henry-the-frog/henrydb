// wire-persistence.test.js — End-to-end: pg client → wire protocol → transactional DB → disk → reopen
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { HenryDBServer } from './server.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import pg from 'pg';
const { Client } = pg;

const makeDir = () => mkdtempSync(join(tmpdir(), 'henrydb-wire-persist-'));

async function withServer(dir, port, fn) {
  const server = new HenryDBServer({ port, dataDir: dir, transactional: true });
  await server.start();
  try {
    await fn(server);
  } finally {
    await server.stop();
  }
}

async function withClient(port, fn) {
  const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
  await client.connect();
  try {
    await fn(client);
  } finally {
    await client.end();
  }
}

describe('Wire Protocol + Persistence E2E', () => {
  const dirs = [];
  afterEach(() => {
    for (const d of dirs) {
      try { rmSync(d, { recursive: true }); } catch {}
    }
    dirs.length = 0;
  });

  it('data survives server restart', async () => {
    const dir = makeDir(); dirs.push(dir);
    const port = 15450 + Math.floor(Math.random() * 100);
    
    // Phase 1: Insert data
    await withServer(dir, port, async () => {
      await withClient(port, async (c) => {
        await c.query('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT)');
        await c.query("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')");
        await c.query("INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')");
        await c.query("INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')");
        
        const r = await c.query('SELECT COUNT(*) as cnt FROM users');
        assert.strictEqual(String(r.rows[0].cnt), '3');
      });
    });
    
    // Phase 2: Restart server and verify
    await withServer(dir, port, async () => {
      await withClient(port, async (c) => {
        const r = await c.query('SELECT COUNT(*) as cnt FROM users');
        assert.strictEqual(String(r.rows[0].cnt), '3');
        
        const alice = await c.query('SELECT * FROM users WHERE id = 1');
        assert.strictEqual(alice.rows[0].name, 'Alice');
        assert.strictEqual(alice.rows[0].email, 'alice@example.com');
      });
    });
  });

  it('transaction commit survives restart', async () => {
    const dir = makeDir(); dirs.push(dir);
    const port = 15450 + Math.floor(Math.random() * 100);
    
    await withServer(dir, port, async () => {
      await withClient(port, async (c) => {
        await c.query('CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)');
        await c.query('INSERT INTO accounts VALUES (1, 1000)');
        await c.query('INSERT INTO accounts VALUES (2, 2000)');
        
        await c.query('BEGIN');
        await c.query('UPDATE accounts SET balance = 500 WHERE id = 1');
        await c.query('UPDATE accounts SET balance = 2500 WHERE id = 2');
        await c.query('COMMIT');
      });
    });
    
    await withServer(dir, port, async () => {
      await withClient(port, async (c) => {
        const r = await c.query('SELECT * FROM accounts ORDER BY id');
        assert.strictEqual(String(r.rows[0].balance), '500');
        assert.strictEqual(String(r.rows[1].balance), '2500');
        
        const sum = await c.query('SELECT SUM(balance) as total FROM accounts');
        assert.strictEqual(String(sum.rows[0].total), '3000');
      });
    });
  });

  it('rollback does NOT survive restart', async () => {
    const dir = makeDir(); dirs.push(dir);
    const port = 15450 + Math.floor(Math.random() * 100);
    
    await withServer(dir, port, async () => {
      await withClient(port, async (c) => {
        await c.query('CREATE TABLE items (id INT PRIMARY KEY, val TEXT)');
        await c.query("INSERT INTO items VALUES (1, 'permanent')");
        
        await c.query('BEGIN');
        await c.query("INSERT INTO items VALUES (2, 'rolled_back')");
        await c.query('ROLLBACK');
      });
    });
    
    await withServer(dir, port, async () => {
      await withClient(port, async (c) => {
        const r = await c.query('SELECT COUNT(*) as cnt FROM items');
        assert.strictEqual(String(r.rows[0].cnt), '1');
      });
    });
  });

  it('multi-cycle: add data across restarts', async () => {
    const dir = makeDir(); dirs.push(dir);
    const port = 15450 + Math.floor(Math.random() * 100);
    
    // Cycle 1: Create + initial data
    await withServer(dir, port, async () => {
      await withClient(port, async (c) => {
        await c.query('CREATE TABLE log (id INT PRIMARY KEY, msg TEXT, cycle INT)');
        for (let i = 1; i <= 5; i++) {
          await c.query(`INSERT INTO log VALUES (${i}, 'msg_${i}', 1)`);
        }
      });
    });
    
    // Cycle 2: Add more
    await withServer(dir, port, async () => {
      await withClient(port, async (c) => {
        const existing = await c.query('SELECT COUNT(*) as cnt FROM log');
        assert.strictEqual(String(existing.rows[0].cnt), '5');
        
        for (let i = 6; i <= 10; i++) {
          await c.query(`INSERT INTO log VALUES (${i}, 'msg_${i}', 2)`);
        }
      });
    });
    
    // Cycle 3: Verify all
    await withServer(dir, port, async () => {
      await withClient(port, async (c) => {
        const total = await c.query('SELECT COUNT(*) as cnt FROM log');
        assert.strictEqual(String(total.rows[0].cnt), '10');
        
        const byCycle = await c.query('SELECT cycle, COUNT(*) as cnt FROM log GROUP BY cycle ORDER BY cycle');
        assert.strictEqual(String(byCycle.rows[0].cnt), '5');
        assert.strictEqual(String(byCycle.rows[1].cnt), '5');
      });
    });
  });
});
