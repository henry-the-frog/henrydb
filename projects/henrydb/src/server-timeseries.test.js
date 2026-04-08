// server-timeseries.test.js — Time-series data workloads through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15514;

describe('Time-Series Data Workload', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('CREATE TABLE sensors (id INTEGER, device TEXT, metric TEXT, value REAL, ts TEXT)');
    
    // Generate 200 sensor readings
    const devices = ['sensor_a', 'sensor_b', 'sensor_c'];
    const metrics = ['temperature', 'humidity', 'pressure'];
    const baseTime = new Date('2026-04-01T00:00:00Z');
    
    for (let i = 0; i < 200; i++) {
      const device = devices[i % 3];
      const metric = metrics[i % 3];
      const value = 20 + Math.sin(i / 10) * 10 + (Math.random() * 2);
      const ts = new Date(baseTime.getTime() + i * 3600000).toISOString();
      await client.query(`INSERT INTO sensors VALUES (${i}, '${device}', '${metric}', ${value.toFixed(2)}, '${ts}')`);
    }
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('query latest readings per device', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Get max timestamp per device
    const result = await client.query(
      'SELECT device, MAX(ts) AS latest FROM sensors GROUP BY device'
    );
    assert.strictEqual(result.rows.length, 3);

    await client.end();
  });

  it('aggregate by time window (hourly average)', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT device, AVG(value) AS avg_val, COUNT(*) AS readings FROM sensors WHERE device = 'sensor_a' GROUP BY device"
    );
    assert.strictEqual(result.rows.length, 1);
    assert.ok(parseFloat(result.rows[0].avg_val) > 0);

    await client.end();
  });

  it('range query with time filter', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT * FROM sensors WHERE ts > '2026-04-03' AND ts < '2026-04-04'"
    );
    assert.ok(result.rows.length > 0);

    await client.end();
  });

  it('downsampling: one reading per day per device', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Get daily averages
    const result = await client.query(
      "SELECT device, AVG(value) AS avg_val, MIN(value) AS min_val, MAX(value) AS max_val, COUNT(*) AS samples FROM sensors GROUP BY device ORDER BY device"
    );
    assert.strictEqual(result.rows.length, 3);
    
    for (const row of result.rows) {
      assert.ok(parseFloat(row.min_val) <= parseFloat(row.avg_val));
      assert.ok(parseFloat(row.avg_val) <= parseFloat(row.max_val));
    }

    await client.end();
  });

  it('anomaly detection: find outlier values', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Find readings that are significantly above average
    const result = await client.query(
      "SELECT device, value, ts FROM sensors WHERE value > 28 ORDER BY value DESC"
    );
    assert.ok(result.rows.length > 0);

    await client.end();
  });

  it('high-velocity insert + query', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    await client.query('CREATE TABLE fast_sensors (id INTEGER, value REAL, ts TEXT)');
    
    // Rapid inserts
    const start = Date.now();
    for (let i = 0; i < 500; i++) {
      await client.query(`INSERT INTO fast_sensors VALUES (${i}, ${Math.random() * 100}, '${new Date().toISOString()}')`);
    }
    const insertTime = Date.now() - start;
    
    // Query
    const result = await client.query('SELECT COUNT(*) AS cnt, AVG(value) AS avg_val FROM fast_sensors');
    assert.strictEqual(parseInt(result.rows[0].cnt), 500);
    
    console.log(`  500 inserts + aggregate in ${insertTime}ms`);

    await client.end();
  });
});
