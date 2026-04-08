// server-explain-formats.test.js — Tests for EXPLAIN with different output formats
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15506;

describe('EXPLAIN Output Formats', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    await client.query('CREATE TABLE fmt_test (id INTEGER, name TEXT, score INTEGER)');
    for (let i = 1; i <= 10; i++) {
      await client.query(`INSERT INTO fmt_test VALUES (${i}, 'item_${i}', ${i * 10})`);
    }
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('EXPLAIN (FORMAT JSON) returns valid JSON', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('EXPLAIN (FORMAT JSON) SELECT * FROM fmt_test WHERE score > 50');
    assert.ok(result.rows.length > 0);
    
    const jsonStr = result.rows[0]['QUERY PLAN'];
    const plan = JSON.parse(jsonStr);
    assert.ok(Array.isArray(plan), 'Should be an array');
    assert.ok(plan[0].Plan, 'Should have Plan node');
    assert.ok(plan[0].Plan['Node Type'], 'Should have Node Type');
    assert.ok(plan[0]['Execution Time'] >= 0);

    await client.end();
  });

  it('EXPLAIN (FORMAT YAML) returns YAML-like output', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('EXPLAIN (FORMAT YAML) SELECT * FROM fmt_test');
    const yaml = result.rows[0]['QUERY PLAN'];
    assert.ok(yaml.includes('Node Type'), 'YAML should contain Node Type');
    assert.ok(yaml.includes('Actual Rows'), 'YAML should contain Actual Rows');

    await client.end();
  });

  it('EXPLAIN (FORMAT DOT) returns Graphviz DOT', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('EXPLAIN (FORMAT DOT) SELECT * FROM fmt_test');
    const dot = result.rows[0]['QUERY PLAN'];
    assert.ok(dot.includes('digraph'), 'Should be a DOT graph');
    assert.ok(dot.includes('QueryPlan'), 'Should be named QueryPlan');
    assert.ok(dot.includes('->') || dot.includes('n0'), 'Should have nodes');

    await client.end();
  });

  it('JSON format includes timing info', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('EXPLAIN (FORMAT JSON) SELECT COUNT(*) FROM fmt_test');
    const plan = JSON.parse(result.rows[0]['QUERY PLAN']);
    assert.ok(plan[0].Plan['Actual Total Time'] >= 0);
    assert.strictEqual(plan[0].Plan['Actual Rows'], 1);

    await client.end();
  });
});
