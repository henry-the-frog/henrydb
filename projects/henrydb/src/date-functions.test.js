// date-functions.test.js — Date/time function tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Date/Time Functions', () => {
  it('NOW() returns ISO timestamp', () => {
    const db = new Database();
    const r = db.execute('SELECT NOW() as ts');
    assert.ok(r.rows[0].ts.includes('T'), 'should be ISO format');
    assert.ok(r.rows[0].ts.includes('Z') || r.rows[0].ts.includes('+'), 'should have timezone');
  });

  it('CURRENT_TIMESTAMP returns ISO timestamp', () => {
    const db = new Database();
    const r = db.execute('SELECT CURRENT_TIMESTAMP as ts');
    assert.ok(r.rows[0].ts.includes('T'));
  });

  it('CURRENT_DATE returns YYYY-MM-DD', () => {
    const db = new Database();
    const r = db.execute('SELECT CURRENT_DATE as d');
    assert.ok(/^\d{4}-\d{2}-\d{2}$/.test(r.rows[0].d), `Expected YYYY-MM-DD, got ${r.rows[0].d}`);
  });

  it('CURRENT_TIME returns HH:MM:SS', () => {
    const db = new Database();
    const r = db.execute('SELECT CURRENT_TIME as t');
    assert.ok(/^\d{2}:\d{2}:\d{2}/.test(r.rows[0].t), `Expected HH:MM:SS, got ${r.rows[0].t}`);
  });

  it('DATE() extracts date from timestamp', () => {
    const db = new Database();
    const r = db.execute("SELECT DATE('2026-04-19T15:30:00Z') as d");
    assert.equal(r.rows[0].d, '2026-04-19');
  });

  it('DATE_PART extracts year', () => {
    const db = new Database();
    const r = db.execute("SELECT DATE_PART('year', '2026-04-19') as yr");
    assert.equal(r.rows[0].yr, 2026);
  });

  it('DATE_PART extracts month', () => {
    const db = new Database();
    const r = db.execute("SELECT DATE_PART('month', '2026-04-19') as mo");
    assert.equal(r.rows[0].mo, 4);
  });

  it('DATE_PART extracts day', () => {
    const db = new Database();
    const r = db.execute("SELECT DATE_PART('day', '2026-04-19') as dy");
    assert.equal(r.rows[0].dy, 19);
  });

  it('EXTRACT YEAR', () => {
    const db = new Database();
    const r = db.execute("SELECT EXTRACT(YEAR FROM '2026-04-19') as yr");
    assert.equal(r.rows[0].yr, 2026);
  });

  it('EXTRACT MONTH', () => {
    const db = new Database();
    const r = db.execute("SELECT EXTRACT(MONTH FROM '2026-04-19') as mo");
    assert.equal(r.rows[0].mo, 4);
  });

  it('DATE_TRUNC month', () => {
    const db = new Database();
    const r = db.execute("SELECT DATE_TRUNC('month', '2026-04-19') as d");
    assert.equal(r.rows[0].d, '2026-04-01');
  });

  it('DATE_TRUNC year', () => {
    const db = new Database();
    const r = db.execute("SELECT DATE_TRUNC('year', '2026-04-19') as d");
    assert.equal(r.rows[0].d, '2026-01-01');
  });

  it('AGE between dates', () => {
    const db = new Database();
    const r = db.execute("SELECT AGE('2026-04-19', '2025-04-19') as a");
    assert.ok(r.rows[0].a.includes('1'), 'should contain 1 year');
  });

  it('date functions in WHERE clause', () => {
    const db = new Database();
    db.execute('CREATE TABLE events (name TEXT, event_date TEXT)');
    db.execute("INSERT INTO events VALUES ('early', '2026-01-15')");
    db.execute("INSERT INTO events VALUES ('mid', '2026-06-15')");
    db.execute("INSERT INTO events VALUES ('late', '2026-11-15')");
    
    const r = db.execute(`
      SELECT name FROM events 
      WHERE DATE_PART('month', event_date) > 3
      ORDER BY event_date
    `);
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'mid');
    assert.equal(r.rows[1].name, 'late');
  });

  it('date functions in GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE logs (ts TEXT, val INT)');
    db.execute("INSERT INTO logs VALUES ('2026-01-15', 10),('2026-01-20', 20),('2026-02-10', 30),('2026-02-15', 40)");
    
    const r = db.execute(`
      SELECT DATE_TRUNC('month', ts) as month, SUM(val) as total
      FROM logs
      GROUP BY DATE_TRUNC('month', ts)
      ORDER BY month
    `);
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].total, 30); // Jan: 10+20
    assert.equal(r.rows[1].total, 70); // Feb: 30+40
  });
});
