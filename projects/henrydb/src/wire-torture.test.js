// wire-torture.test.js — Heavy E2E: many operations across multiple restart cycles
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { HenryDBServer } from './server.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import pg from 'pg';
const { Client } = pg;

const makeDir = () => mkdtempSync(join(tmpdir(), 'henrydb-wire-torture-'));

async function withServer(dir, port, fn) {
  const server = new HenryDBServer({ port, dataDir: dir, transactional: true });
  await server.start();
  try { await fn(server); } finally { await server.stop(); }
}

async function withClient(port, fn) {
  const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
  await client.connect();
  try { return await fn(client); } finally { await client.end(); }
}

describe('Wire Protocol Torture Test', () => {
  const dirs = [];
  afterEach(() => {
    for (const d of dirs) { try { rmSync(d, { recursive: true }); } catch {} }
    dirs.length = 0;
  });

  it('100 operations across 5 restart cycles', async () => {
    const dir = makeDir(); dirs.push(dir);
    const port = 15550 + Math.floor(Math.random() * 100);
    
    // Cycle 1: Create table + bulk insert
    await withServer(dir, port, async () => {
      await withClient(port, async (c) => {
        await c.query('CREATE TABLE torture (id INT PRIMARY KEY, val INT, tag TEXT)');
        for (let i = 0; i < 50; i++) {
          await c.query(`INSERT INTO torture VALUES (${i}, ${i * 10}, 'cycle1')`);
        }
        const cnt = await c.query('SELECT COUNT(*) as cnt FROM torture');
        assert.strictEqual(Number(cnt.rows[0].cnt), 50);
      });
    });
    
    // Cycle 2: Insert more + update some
    await withServer(dir, port, async () => {
      await withClient(port, async (c) => {
        // Verify previous data
        const cnt = await c.query('SELECT COUNT(*) as cnt FROM torture');
        assert.strictEqual(Number(cnt.rows[0].cnt), 50);
        
        // Insert more
        for (let i = 50; i < 80; i++) {
          await c.query(`INSERT INTO torture VALUES (${i}, ${i * 10}, 'cycle2')`);
        }
        
        // Update first 10
        for (let i = 0; i < 10; i++) {
          await c.query(`UPDATE torture SET val = 999, tag = 'updated' WHERE id = ${i}`);
        }
        
        const total = await c.query('SELECT COUNT(*) as cnt FROM torture');
        assert.strictEqual(Number(total.rows[0].cnt), 80);
      });
    });
    
    // Cycle 3: Delete some + transactional insert
    await withServer(dir, port, async () => {
      await withClient(port, async (c) => {
        const cnt = await c.query('SELECT COUNT(*) as cnt FROM torture');
        assert.strictEqual(Number(cnt.rows[0].cnt), 80);
        
        // Check updates survived
        const updated = await c.query("SELECT COUNT(*) as cnt FROM torture WHERE tag = 'updated'");
        assert.strictEqual(Number(updated.rows[0].cnt), 10);
        
        // Delete some
        for (let i = 70; i < 80; i++) {
          await c.query(`DELETE FROM torture WHERE id = ${i}`);
        }
        
        // Transactional insert (should persist)
        await c.query('BEGIN');
        for (let i = 80; i < 90; i++) {
          await c.query(`INSERT INTO torture VALUES (${i}, ${i * 10}, 'cycle3_tx')`);
        }
        await c.query('COMMIT');
        
        // Transactional insert (should NOT persist)
        await c.query('BEGIN');
        for (let i = 90; i < 100; i++) {
          await c.query(`INSERT INTO torture VALUES (${i}, ${i * 10}, 'cycle3_rollback')`);
        }
        await c.query('ROLLBACK');
        
        const final = await c.query('SELECT COUNT(*) as cnt FROM torture');
        assert.strictEqual(Number(final.rows[0].cnt), 80);
      });
    });
    
    // Cycle 4: Verify everything
    await withServer(dir, port, async () => {
      await withClient(port, async (c) => {
        const total = await c.query('SELECT COUNT(*) as cnt FROM torture');
        assert.strictEqual(Number(total.rows[0].cnt), 80, 'Total should be 80');
        
        // Verify updates
        const upd = await c.query("SELECT COUNT(*) as cnt FROM torture WHERE tag = 'updated'");
        assert.strictEqual(Number(upd.rows[0].cnt), 10, 'Should have 10 updated rows');
        
        // Verify deletes
        const deleted = await c.query('SELECT * FROM torture WHERE id >= 70 AND id < 80');
        assert.strictEqual(deleted.rows.length, 0, 'Deleted rows should be gone');
        
        // Verify committed tx
        const tx = await c.query("SELECT COUNT(*) as cnt FROM torture WHERE tag = 'cycle3_tx'");
        assert.strictEqual(Number(tx.rows[0].cnt), 10, 'Committed tx rows should exist');
        
        // Verify rolled back tx
        const rb = await c.query("SELECT COUNT(*) as cnt FROM torture WHERE tag = 'cycle3_rollback'");
        assert.strictEqual(Number(rb.rows[0].cnt), 0, 'Rolled back rows should not exist');
        
        // Aggregation
        const sum = await c.query('SELECT SUM(val) as s FROM torture');
        assert.ok(Number(sum.rows[0].s) > 0, 'SUM should be positive');
      });
    });
    
    // Cycle 5: Modify + verify
    await withServer(dir, port, async () => {
      await withClient(port, async (c) => {
        // Mass update
        await c.query('UPDATE torture SET val = val + 1 WHERE id < 50');
        
        const cnt = await c.query('SELECT COUNT(*) as cnt FROM torture');
        assert.strictEqual(Number(cnt.rows[0].cnt), 80, 'Count should still be 80 after update');
      });
    });
  });

  it('bank transfer integrity across 10 restart cycles', async () => {
    const dir = makeDir(); dirs.push(dir);
    const port = 15550 + Math.floor(Math.random() * 100);
    
    const numAccounts = 10;
    const initialBalance = 1000;
    const totalExpected = numAccounts * initialBalance;
    
    // Setup
    await withServer(dir, port, async () => {
      await withClient(port, async (c) => {
        await c.query('CREATE TABLE bank (id INT PRIMARY KEY, balance INT)');
        for (let i = 0; i < numAccounts; i++) {
          await c.query(`INSERT INTO bank VALUES (${i}, ${initialBalance})`);
        }
      });
    });
    
    // 10 transfer cycles
    for (let cycle = 0; cycle < 10; cycle++) {
      await withServer(dir, port, async () => {
        await withClient(port, async (c) => {
          // Transfer
          const from = cycle % numAccounts;
          const to = (from + 3) % numAccounts;
          
          await c.query('BEGIN');
          await c.query(`UPDATE bank SET balance = balance - 50 WHERE id = ${from}`);
          await c.query(`UPDATE bank SET balance = balance + 50 WHERE id = ${to}`);
          await c.query('COMMIT');
          
          // Verify invariant
          const sum = await c.query('SELECT SUM(balance) as total FROM bank');
          assert.strictEqual(Number(sum.rows[0].total), totalExpected, `Cycle ${cycle}: invariant`);
        });
      });
    }
    
    // Final verification
    await withServer(dir, port, async () => {
      await withClient(port, async (c) => {
        const sum = await c.query('SELECT SUM(balance) as total FROM bank');
        assert.strictEqual(Number(sum.rows[0].total), totalExpected, 'Final invariant');
        
        const cnt = await c.query('SELECT COUNT(*) as cnt FROM bank');
        assert.strictEqual(Number(cnt.rows[0].cnt), numAccounts, 'Account count');
      });
    });
  });
});
