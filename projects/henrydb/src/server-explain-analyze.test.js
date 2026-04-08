// server-explain-analyze.test.js — Tests for EXPLAIN ANALYZE
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15493;

describe('EXPLAIN ANALYZE', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    await client.query('CREATE TABLE explain_test (id INTEGER, name TEXT, score INTEGER)');
    for (let i = 1; i <= 50; i++) {
      await client.query(`INSERT INTO explain_test VALUES (${i}, 'item_${i}', ${i * 10})`);
    }
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('EXPLAIN ANALYZE returns execution stats', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('EXPLAIN ANALYZE SELECT * FROM explain_test WHERE score > 200');
    assert.ok(result.rows.length > 0, 'Expected EXPLAIN ANALYZE output');

    // Should contain execution time
    const output = result.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(output.includes('Execution Time'), `Missing execution time in: ${output}`);
    assert.ok(output.includes('Rows Returned'), `Missing row count in: ${output}`);
    assert.ok(output.includes('Engine'), `Missing engine in: ${output}`);

    await client.end();
  });

  it('EXPLAIN ANALYZE shows correct row count', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('EXPLAIN ANALYZE SELECT * FROM explain_test');
    const output = result.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(output.includes('Rows Returned: 50'), `Expected 50 rows, got: ${output}`);

    await client.end();
  });

  it('EXPLAIN ANALYZE with WHERE clause', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('EXPLAIN ANALYZE SELECT name FROM explain_test WHERE id = 1');
    const output = result.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(output.includes('Rows Returned: 1'));

    await client.end();
  });

  it('EXPLAIN ANALYZE with aggregate', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('EXPLAIN ANALYZE SELECT COUNT(*) FROM explain_test');
    const output = result.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(output.includes('Rows Returned: 1'));
    assert.ok(output.includes('Execution Time'));

    await client.end();
  });

  it('EXPLAIN ANALYZE reports table scan info', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('EXPLAIN ANALYZE SELECT * FROM explain_test WHERE score > 400');
    const output = result.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(output.includes('explain_test'), `Expected table name in: ${output}`);

    await client.end();
  });

  it('EXPLAIN (non-ANALYZE) still works', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('EXPLAIN SELECT * FROM explain_test');
    assert.ok(result.rows.length > 0);

    await client.end();
  });

  it('EXPLAIN ANALYZE with JOIN', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE explain_orders (id INTEGER, user_id INTEGER, amount INTEGER)');
    await client.query('INSERT INTO explain_orders VALUES (1, 1, 100)');
    await client.query('INSERT INTO explain_orders VALUES (2, 2, 200)');

    const result = await client.query(`
      EXPLAIN ANALYZE SELECT e.name, o.amount 
      FROM explain_test e 
      JOIN explain_orders o ON e.id = o.user_id
    `);
    const output = result.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(output.includes('Execution Time'));
    assert.ok(output.includes('Rows Returned: 2'));

    await client.end();
  });
});
