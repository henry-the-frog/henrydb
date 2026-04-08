// server-logs.test.js — Log analysis through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15587;

describe('Log Analysis', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('CREATE TABLE access_logs (id INTEGER, ip TEXT, method TEXT, path TEXT, status INTEGER, response_ms INTEGER, user_agent TEXT, ts TEXT)');
    
    // 100 log entries
    const paths = ['/api/users', '/api/products', '/api/orders', '/health', '/login', '/api/search'];
    const methods = ['GET', 'GET', 'POST', 'GET', 'POST', 'GET'];
    const statuses = [200, 200, 201, 200, 200, 200, 404, 500, 200, 200];
    const ips = ['192.168.1.10', '192.168.1.20', '10.0.0.1', '192.168.1.10', '172.16.0.5'];
    
    for (let i = 0; i < 100; i++) {
      const path = paths[i % paths.length];
      const method = methods[i % methods.length];
      const status = statuses[i % statuses.length];
      const ip = ips[i % ips.length];
      const ms = 5 + (i % 50) + (status === 500 ? 2000 : 0);
      const hour = String(8 + (i % 12)).padStart(2, '0');
      await client.query(`INSERT INTO access_logs VALUES (${i}, '${ip}', '${method}', '${path}', ${status}, ${ms}, 'Mozilla/5.0', '2026-04-08T${hour}:${String(i % 60).padStart(2, '0')}:00Z')`);
    }
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('status code distribution', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query('SELECT status, COUNT(*) AS cnt FROM access_logs GROUP BY status ORDER BY cnt DESC');
    assert.ok(result.rows.length >= 3);

    await client.end();
  });

  it('error rate (4xx + 5xx)', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const errors = await client.query('SELECT COUNT(*) AS cnt FROM access_logs WHERE status >= 400');
    const total = await client.query('SELECT COUNT(*) AS cnt FROM access_logs');
    assert.ok(parseInt(errors.rows[0].cnt) > 0);
    assert.ok(parseInt(total.rows[0].cnt) === 100);

    await client.end();
  });

  it('top endpoints by traffic', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT path, COUNT(*) AS hits, AVG(response_ms) AS avg_ms FROM access_logs GROUP BY path ORDER BY hits DESC'
    );
    assert.ok(result.rows.length >= 4);

    await client.end();
  });

  it('slow requests', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT path, status, response_ms, ts FROM access_logs WHERE response_ms > 1000 ORDER BY response_ms DESC'
    );
    assert.ok(result.rows.length >= 1); // 500 errors have high response times

    await client.end();
  });

  it('top IPs', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT ip, COUNT(*) AS requests FROM access_logs GROUP BY ip ORDER BY requests DESC'
    );
    assert.ok(result.rows.length >= 3);

    await client.end();
  });
});
