// server-stat-statements.test.js — Tests for pg_stat_statements
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15503;

describe('pg_stat_statements', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('tracks query patterns', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE stat_test (id INTEGER, val TEXT)');
    await client.query("INSERT INTO stat_test VALUES (1, 'a')");
    await client.query("INSERT INTO stat_test VALUES (2, 'b')");
    await client.query('SELECT * FROM stat_test');
    await client.query('SELECT * FROM stat_test');

    const result = await client.query('SELECT * FROM pg_stat_statements');
    assert.ok(result.rows.length > 0, 'Should have statement stats');

    // The SELECT should show at least 2 calls (since we ran it twice)
    const selectStat = result.rows.find(r => r.query?.toLowerCase().includes('select * from stat_test'));
    assert.ok(selectStat, 'Should find SELECT statement stats');
    assert.ok(selectStat.calls >= 2, `Expected at least 2 calls, got ${selectStat.calls}`);
    assert.ok(selectStat.total_time_ms >= 0);
    assert.ok(selectStat.mean_time_ms >= 0);

    await client.end();
  });

  it('shows timing stats', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('SELECT * FROM pg_stat_statements');
    
    for (const row of result.rows) {
      assert.ok(typeof row.calls === 'number' || !isNaN(parseInt(row.calls)));
      assert.ok(typeof row.total_time_ms === 'number' || !isNaN(parseFloat(row.total_time_ms)));
      assert.ok(typeof row.min_time_ms === 'number' || !isNaN(parseFloat(row.min_time_ms)));
      assert.ok(typeof row.max_time_ms === 'number' || !isNaN(parseFloat(row.max_time_ms)));
    }

    await client.end();
  });

  it('sorted by total time descending', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('SELECT * FROM pg_stat_statements');
    if (result.rows.length >= 2) {
      const times = result.rows.map(r => parseFloat(r.total_time_ms));
      for (let i = 1; i < times.length; i++) {
        assert.ok(times[i - 1] >= times[i], 'Should be sorted by total time DESC');
      }
    }

    await client.end();
  });
});
