// server-prepared-cache.test.js — Tests for prepared statement caching behavior
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15438;

describe('Prepared Statement Caching', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('pg driver reuses prepared statement for repeated parameterized queries', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE cache_test (id INTEGER, val TEXT)');
    for (let i = 0; i < 20; i++) {
      await client.query('INSERT INTO cache_test VALUES ($1, $2)', [i, `val_${i}`]);
    }

    // Execute the same parameterized query many times
    for (let i = 0; i < 20; i++) {
      const r = await client.query('SELECT val FROM cache_test WHERE id = $1', [i]);
      assert.strictEqual(r.rows.length, 1);
      assert.strictEqual(r.rows[0].val, `val_${i}`);
    }

    await client.end();
  });

  it('named prepared statements persist across executions', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // pg driver supports named prepared statements
    const config = {
      name: 'find-by-id',
      text: 'SELECT val FROM cache_test WHERE id = $1',
    };

    // First execution creates the prepared statement
    const r1 = await client.query({ ...config, values: [5] });
    assert.strictEqual(r1.rows[0].val, 'val_5');

    // Second execution reuses it
    const r2 = await client.query({ ...config, values: [10] });
    assert.strictEqual(r2.rows[0].val, 'val_10');

    // Third execution
    const r3 = await client.query({ ...config, values: [15] });
    assert.strictEqual(r3.rows[0].val, 'val_15');

    await client.end();
  });

  it('different connections share data but not prepared statements', async () => {
    const c1 = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    const c2 = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await c1.connect();
    await c2.connect();

    // Both can see the same data
    const r1 = await c1.query('SELECT COUNT(*) AS c FROM cache_test');
    const r2 = await c2.query('SELECT COUNT(*) AS c FROM cache_test');
    assert.strictEqual(r1.rows[0].c, r2.rows[0].c);

    // Named statement on c1 doesn't affect c2
    await c1.query({ name: 'conn1-stmt', text: 'SELECT val FROM cache_test WHERE id = $1', values: [1] });
    
    // c2 can create same-named statement independently
    await c2.query({ name: 'conn1-stmt', text: 'SELECT val FROM cache_test WHERE id = $1', values: [2] });

    await c1.end();
    await c2.end();
  });

  it('prepared statement with multiple parameters', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const config = {
      name: 'range-query',
      text: 'SELECT val FROM cache_test WHERE id >= $1 AND id <= $2',
    };

    const r1 = await client.query({ ...config, values: [5, 10] });
    assert.strictEqual(r1.rows.length, 6);

    const r2 = await client.query({ ...config, values: [0, 2] });
    assert.strictEqual(r2.rows.length, 3);

    await client.end();
  });

  it('prepared INSERT, UPDATE, DELETE', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Prepared INSERT
    const insertConfig = { name: 'insert-val', text: 'INSERT INTO cache_test VALUES ($1, $2)' };
    await client.query({ ...insertConfig, values: [100, 'hundred'] });
    await client.query({ ...insertConfig, values: [101, 'hundred-one'] });

    // Prepared UPDATE
    await client.query({
      name: 'update-val',
      text: 'UPDATE cache_test SET val = $1 WHERE id = $2',
      values: ['UPDATED', 100],
    });

    const r = await client.query('SELECT val FROM cache_test WHERE id = $1', [100]);
    assert.strictEqual(r.rows[0].val, 'UPDATED');

    // Prepared DELETE
    await client.query({
      name: 'delete-val',
      text: 'DELETE FROM cache_test WHERE id = $1',
      values: [101],
    });

    const r2 = await client.query('SELECT val FROM cache_test WHERE id = $1', [101]);
    assert.strictEqual(r2.rows.length, 0);

    await client.end();
  });

  it('benchmark: prepared vs ad-hoc queries', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const iterations = 50;

    // Ad-hoc queries (no preparation)
    const startAdHoc = Date.now();
    for (let i = 0; i < iterations; i++) {
      await client.query(`SELECT val FROM cache_test WHERE id = ${i % 20}`);
    }
    const adHocMs = Date.now() - startAdHoc;

    // Prepared queries
    const startPrepared = Date.now();
    for (let i = 0; i < iterations; i++) {
      await client.query({
        name: 'bench-query',
        text: 'SELECT val FROM cache_test WHERE id = $1',
        values: [i % 20],
      });
    }
    const preparedMs = Date.now() - startPrepared;

    console.log(`Ad-hoc: ${adHocMs}ms, Prepared: ${preparedMs}ms`);
    // Both should complete (no assertion on speed since it depends on system)
    assert.ok(adHocMs < 5000, 'Ad-hoc too slow');
    assert.ok(preparedMs < 5000, 'Prepared too slow');

    await client.end();
  });
});
