// datetime-functions.test.js — Date/time functions test suite
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;

describe('Date/Time Functions', () => {
  let server, port, c;
  
  before(async () => {
    port = 35600 + Math.floor(Math.random() * 2000);
    server = new HenryDBServer({ port });
    await server.start();
    c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
  });
  
  after(async () => {
    if (c) await c.end();
    if (server) await server.stop();
  });

  it('NOW() returns current timestamp', async () => {
    const r = await c.query('SELECT NOW() as ts');
    const ts = r.rows[0].ts || r.rows[0]['NOW(...)'];
    assert.ok(ts);
    assert.ok(new Date(ts).getFullYear() >= 2024);
  });

  it('CURRENT_TIMESTAMP returns ISO string', async () => {
    const r = await c.query('SELECT CURRENT_TIMESTAMP as ts');
    const ts = r.rows[0].ts || r.rows[0].CURRENT_TIMESTAMP;
    assert.ok(ts);
    assert.ok(ts.includes('T'));
  });

  it('CURRENT_DATE returns date only', async () => {
    const r = await c.query('SELECT CURRENT_DATE as d');
    const d = r.rows[0].d || r.rows[0].CURRENT_DATE;
    assert.ok(d);
    assert.ok(/^\d{4}-\d{2}-\d{2}/.test(d));
  });

  it('EXTRACT YEAR', async () => {
    const r = await c.query("SELECT EXTRACT(YEAR FROM '2024-06-15') as y");
    assert.equal(String(r.rows[0].y), '2024');
  });

  it('EXTRACT MONTH', async () => {
    const r = await c.query("SELECT EXTRACT(MONTH FROM '2024-06-15') as m");
    assert.equal(String(r.rows[0].m), '6');
  });

  it('EXTRACT DAY', async () => {
    const r = await c.query("SELECT EXTRACT(DAY FROM '2024-06-15') as d");
    assert.equal(String(r.rows[0].d), '15');
  });

  it('EXTRACT QUARTER', async () => {
    const r = await c.query("SELECT EXTRACT(QUARTER FROM '2024-09-01') as q");
    assert.equal(String(r.rows[0].q), '3');
  });

  it('EXTRACT EPOCH', async () => {
    const r = await c.query("SELECT EXTRACT(EPOCH FROM '2024-01-01T00:00:00Z') as e");
    assert.equal(String(r.rows[0].e), '1704067200');
  });

  it('DATE_PART function', async () => {
    const r = await c.query("SELECT DATE_PART('year', '2024-12-25') as y");
    assert.equal(String(r.rows[0].y), '2024');
  });

  it('CURRENT_DATE + INTERVAL days', async () => {
    const r = await c.query("SELECT CURRENT_DATE + INTERVAL '30 days' as future");
    const future = new Date(r.rows[0].future);
    const now = new Date();
    assert.ok(future > now);
  });

  it('CURRENT_DATE - INTERVAL months', async () => {
    const r = await c.query("SELECT CURRENT_DATE - INTERVAL '6 months' as past");
    const past = new Date(r.rows[0].past);
    const now = new Date();
    assert.ok(past < now);
  });

  it('CURRENT_DATE + INTERVAL year', async () => {
    const r = await c.query("SELECT CURRENT_DATE + INTERVAL '1 year' as next_year");
    const nextYear = new Date(r.rows[0].next_year);
    const thisYear = new Date().getFullYear();
    assert.equal(nextYear.getFullYear(), thisYear + 1);
  });

  it('NOW() + INTERVAL hours', async () => {
    const r = await c.query("SELECT NOW() + INTERVAL '3 hours' as later");
    const later = new Date(r.rows[0].later);
    const now = new Date();
    assert.ok(later > now);
  });

  it('EXTRACT with expressions', async () => {
    const r = await c.query("SELECT EXTRACT(YEAR FROM '2024-03-15') as y1, EXTRACT(MONTH FROM '2024-07-20') as m2, EXTRACT(DAY FROM '2024-11-05') as d3");
    assert.equal(String(r.rows[0].y1), '2024');
    assert.equal(String(r.rows[0].m2), '7');
    assert.equal(String(r.rows[0].d3), '5');
  });

  it('GREATEST and LEAST', async () => {
    const r1 = await c.query('SELECT GREATEST(1, 5, 3) as mx');
    assert.equal(String(r1.rows[0].mx), '5');
    
    const r2 = await c.query('SELECT LEAST(10, 2, 7) as mn');
    assert.equal(String(r2.rows[0].mn), '2');
  });

  it('MOD function', async () => {
    const r = await c.query('SELECT MOD(17, 5) as remainder');
    assert.equal(String(r.rows[0].remainder), '2');
  });
});
