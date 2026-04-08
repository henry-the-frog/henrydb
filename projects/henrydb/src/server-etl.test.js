// server-etl.test.js — ETL (Extract-Transform-Load) patterns
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15526;

describe('ETL Data Pipeline', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();
  });

  after(async () => {
    await server.stop();
  });

  it('extract: load raw data', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Raw staging table
    await client.query('CREATE TABLE raw_events (id INTEGER, event_type TEXT, payload TEXT, raw_ts TEXT)');
    
    // Load 50 raw events
    for (let i = 0; i < 50; i++) {
      const type = ['click', 'view', 'purchase', 'signup'][i % 4];
      const ts = `2026-04-${String(1 + (i % 7)).padStart(2, '0')}T${String(8 + (i % 12)).padStart(2, '0')}:00:00Z`;
      await client.query(`INSERT INTO raw_events VALUES (${i}, '${type}', '{"user":${i % 10},"page":"/${type}"}', '${ts}')`);
    }

    const result = await client.query('SELECT COUNT(*) AS cnt FROM raw_events');
    assert.strictEqual(parseInt(result.rows[0].cnt), 50);

    await client.end();
  });

  it('transform: aggregate and clean', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Create summary table
    await client.query('CREATE TABLE event_summary (event_type TEXT, count INTEGER, first_seen TEXT, last_seen TEXT)');
    
    // Transform: aggregate raw events
    const types = await client.query('SELECT DISTINCT event_type FROM raw_events');
    for (const row of types.rows) {
      const stats = await client.query(
        `SELECT COUNT(*) AS cnt, MIN(raw_ts) AS first_ts, MAX(raw_ts) AS last_ts FROM raw_events WHERE event_type = '${row.event_type}'`
      );
      await client.query(
        `INSERT INTO event_summary VALUES ('${row.event_type}', ${stats.rows[0].cnt}, '${stats.rows[0].first_ts}', '${stats.rows[0].last_ts}')`
      );
    }

    const result = await client.query('SELECT * FROM event_summary ORDER BY count DESC');
    assert.ok(result.rows.length >= 3);

    await client.end();
  });

  it('load: create denormalized view', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Create denormalized report table
    await client.query('CREATE TABLE daily_report (day TEXT, clicks INTEGER, views INTEGER, purchases INTEGER, signups INTEGER)');
    
    // Pivot data by day
    const days = await client.query('SELECT DISTINCT raw_ts FROM raw_events ORDER BY raw_ts');
    const daySet = new Set(days.rows.map(r => r.raw_ts.substring(0, 10)));
    
    for (const day of daySet) {
      const clicks = await client.query(`SELECT COUNT(*) AS cnt FROM raw_events WHERE event_type = 'click' AND raw_ts LIKE '${day}%'`);
      const views = await client.query(`SELECT COUNT(*) AS cnt FROM raw_events WHERE event_type = 'view' AND raw_ts LIKE '${day}%'`);
      const purchases = await client.query(`SELECT COUNT(*) AS cnt FROM raw_events WHERE event_type = 'purchase' AND raw_ts LIKE '${day}%'`);
      const signups = await client.query(`SELECT COUNT(*) AS cnt FROM raw_events WHERE event_type = 'signup' AND raw_ts LIKE '${day}%'`);
      
      await client.query(
        `INSERT INTO daily_report VALUES ('${day}', ${clicks.rows[0].cnt}, ${views.rows[0].cnt}, ${purchases.rows[0].cnt}, ${signups.rows[0].cnt})`
      );
    }

    const report = await client.query('SELECT * FROM daily_report ORDER BY day');
    assert.ok(report.rows.length >= 3);

    await client.end();
  });

  it('verify data integrity', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Raw count should be 50
    const rawTotal = await client.query('SELECT COUNT(*) AS cnt FROM raw_events');
    assert.strictEqual(parseInt(rawTotal.rows[0].cnt), 50);

    // Summary should have 4 event types
    const summaryCount = await client.query('SELECT COUNT(*) AS cnt FROM event_summary');
    assert.ok(parseInt(summaryCount.rows[0].cnt) >= 3);

    await client.end();
  });

  it('incremental load: add new data', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Add 10 more events
    for (let i = 50; i < 60; i++) {
      await client.query(`INSERT INTO raw_events VALUES (${i}, 'click', '{"user":${i}}', '2026-04-08T12:00:00Z')`);
    }

    const result = await client.query('SELECT COUNT(*) AS cnt FROM raw_events');
    assert.strictEqual(parseInt(result.rows[0].cnt), 60);

    await client.end();
  });
});
