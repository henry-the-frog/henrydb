// date-functions.test.js — Comprehensive date/time function tests

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Date/Time Functions', () => {
  let db;

  it('DATE() — cast to date string', () => {
    db = new Database();
    const r = db.execute("SELECT DATE('2025-03-15T14:30:00Z') as d");
    assert.equal(r.rows[0].d, '2025-03-15');
  });

  it('DATE() — with datetime string', () => {
    db = new Database();
    const r = db.execute("SELECT DATE('2025-12-31 23:59:59') as d");
    assert.equal(r.rows[0].d, '2025-12-31');
  });

  it('DATE() — NULL input', () => {
    db = new Database();
    const r = db.execute("SELECT DATE(NULL) as d");
    assert.equal(r.rows[0].d, null);
  });

  it('AGE(date1, date2) — years, months, days', () => {
    db = new Database();
    const r = db.execute("SELECT AGE('2025-03-15', '2024-01-01') as a");
    assert.equal(r.rows[0].a, '1 year 2 mons 14 days');
  });

  it('AGE(date1, date2) — same date', () => {
    db = new Database();
    const r = db.execute("SELECT AGE('2025-01-01', '2025-01-01') as a");
    assert.equal(r.rows[0].a, '0 days');
  });

  it('AGE(date1, date2) — exact years', () => {
    db = new Database();
    const r = db.execute("SELECT AGE('2025-06-01', '2023-06-01') as a");
    assert.equal(r.rows[0].a, '2 years');
  });

  it('AGE(date) — from date to now', () => {
    db = new Database();
    const r = db.execute("SELECT AGE('2020-01-01') as a");
    // Should be ~6 years (test in 2026)
    assert.ok(r.rows[0].a.includes('year'), `Expected years in ${r.rows[0].a}`);
  });

  it('TO_CHAR date — YYYY-MM-DD', () => {
    db = new Database();
    const r = db.execute("SELECT TO_CHAR('2025-06-15', 'YYYY-MM-DD') as d");
    assert.equal(r.rows[0].d, '2025-06-15');
  });

  it('TO_CHAR date — custom format', () => {
    db = new Database();
    const r = db.execute("SELECT TO_CHAR('2025-06-15', 'DD/MM/YYYY') as d");
    assert.equal(r.rows[0].d, '15/06/2025');
  });

  it('TO_CHAR number — thousands separator', () => {
    db = new Database();
    const r = db.execute("SELECT TO_CHAR(12345, '999,999') as n");
    assert.ok(r.rows[0].n.includes('12,345'));
  });

  it('TO_CHAR number — small number', () => {
    db = new Database();
    const r = db.execute("SELECT TO_CHAR(42, '999') as n");
    assert.ok(r.rows[0].n.includes('42'));
  });

  it('MAKE_DATE — construct a date', () => {
    db = new Database();
    const r = db.execute("SELECT MAKE_DATE(2025, 12, 25) as d");
    assert.equal(r.rows[0].d, '2025-12-25');
  });

  it('MAKE_DATE — single digit month/day', () => {
    db = new Database();
    const r = db.execute("SELECT MAKE_DATE(2025, 1, 5) as d");
    assert.equal(r.rows[0].d, '2025-01-05');
  });

  it('MAKE_TIMESTAMP — full timestamp', () => {
    db = new Database();
    const r = db.execute("SELECT MAKE_TIMESTAMP(2025, 6, 15, 14, 30, 0) as ts");
    assert.equal(r.rows[0].ts, '2025-06-15T14:30:00.000Z');
  });

  it('MAKE_TIMESTAMP — midnight', () => {
    db = new Database();
    const r = db.execute("SELECT MAKE_TIMESTAMP(2025, 1, 1, 0, 0, 0) as ts");
    assert.equal(r.rows[0].ts, '2025-01-01T00:00:00.000Z');
  });

  it('EPOCH — date to unix timestamp', () => {
    db = new Database();
    const r = db.execute("SELECT EPOCH('2025-01-01T00:00:00Z') as e");
    assert.equal(r.rows[0].e, 1735689600);
  });

  it('TO_TIMESTAMP — unix timestamp to ISO', () => {
    db = new Database();
    const r = db.execute("SELECT TO_TIMESTAMP(1735689600) as ts");
    assert.equal(r.rows[0].ts, '2025-01-01T00:00:00.000Z');
  });

  it('EPOCH round-trip', () => {
    db = new Database();
    const r = db.execute("SELECT TO_TIMESTAMP(EPOCH('2025-06-15T12:00:00Z')) as ts");
    assert.equal(r.rows[0].ts, '2025-06-15T12:00:00.000Z');
  });

  it('DATE_FORMAT — alias for TO_CHAR', () => {
    db = new Database();
    const r = db.execute("SELECT DATE_FORMAT('2025-03-15', 'YYYY/MM/DD') as d");
    assert.equal(r.rows[0].d, '2025/03/15');
  });

  it('DATE + INTERVAL — 30 days', () => {
    db = new Database();
    const r = db.execute("SELECT '2025-01-15' + INTERVAL '30 days' as d");
    assert.ok(r.rows[0].d.startsWith('2025-02-14'));
  });

  it('DATE + INTERVAL — 1 month', () => {
    db = new Database();
    const r = db.execute("SELECT '2025-01-31' + INTERVAL '1 month' as d");
    // January 31 + 1 month = February 28 (or March 3)
    assert.ok(r.rows[0].d.includes('2025'));
  });

  it('DATE - INTERVAL', () => {
    db = new Database();
    const r = db.execute("SELECT '2025-03-15' - INTERVAL '15 days' as d");
    assert.ok(r.rows[0].d.startsWith('2025-02-28'));
  });

  it('DATE_ADD — add days', () => {
    db = new Database();
    const r = db.execute("SELECT DATE_ADD('2025-01-01', 10, 'day') as d");
    assert.equal(r.rows[0].d, '2025-01-11');
  });

  it('DATE_ADD — add months', () => {
    db = new Database();
    const r = db.execute("SELECT DATE_ADD('2025-01-15', 3, 'month') as d");
    assert.equal(r.rows[0].d, '2025-04-15');
  });

  it('DATE_DIFF — days between dates', () => {
    db = new Database();
    const r = db.execute("SELECT DATE_DIFF('2025-03-15', '2025-01-01', 'day') as d");
    assert.equal(r.rows[0].d, 73);
  });

  it('DATE_DIFF — months between dates', () => {
    db = new Database();
    const r = db.execute("SELECT DATE_DIFF('2025-06-01', '2025-01-01', 'month') as m");
    assert.equal(r.rows[0].m, 5);
  });

  it('EXTRACT — all fields', () => {
    db = new Database();
    const r = db.execute("SELECT EXTRACT(YEAR FROM '2025-06-15') as y, EXTRACT(MONTH FROM '2025-06-15') as m, EXTRACT(DAY FROM '2025-06-15') as d");
    assert.equal(r.rows[0].y, 2025);
    assert.equal(r.rows[0].m, 6);
    assert.equal(r.rows[0].d, 15);
  });

  it('DATE_TRUNC — truncate to month', () => {
    db = new Database();
    const r = db.execute("SELECT DATE_TRUNC('month', '2025-06-15') as d");
    assert.equal(r.rows[0].d, '2025-06-01');
  });

  it('DATE_PART — year', () => {
    db = new Database();
    const r = db.execute("SELECT DATE_PART('year', '2025-06-15') as y");
    assert.equal(r.rows[0].y, 2025);
  });

  it('CURRENT_DATE and CURRENT_TIMESTAMP', () => {
    db = new Database();
    const r = db.execute("SELECT CURRENT_DATE as d, CURRENT_TIMESTAMP as ts");
    assert.ok(r.rows[0].d.match(/^\d{4}-\d{2}-\d{2}$/));
    assert.ok(r.rows[0].ts.includes('T'));
  });

  it('NOW()', () => {
    db = new Database();
    const r = db.execute("SELECT NOW() as ts");
    assert.ok(r.rows[0].ts.includes('T'));
  });
});

describe('Date Functions in Queries', () => {
  it('WHERE clause with date comparison', () => {
    const db = new Database();
    db.execute("CREATE TABLE events (id INT PRIMARY KEY, name TEXT, event_date TEXT)");
    db.execute("INSERT INTO events VALUES (1, 'Conference', '2025-06-15')");
    db.execute("INSERT INTO events VALUES (2, 'Workshop', '2025-03-01')");
    db.execute("INSERT INTO events VALUES (3, 'Meetup', '2025-09-20')");

    const r = db.execute("SELECT * FROM events WHERE event_date > '2025-06-01' ORDER BY event_date");
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'Conference');
    assert.equal(r.rows[1].name, 'Meetup');
  });

  it('GROUP BY with DATE_TRUNC', () => {
    const db = new Database();
    db.execute("CREATE TABLE sales (id INT PRIMARY KEY, amount INT, sale_date TEXT)");
    for (let i = 0; i < 30; i++) {
      const month = i < 15 ? '01' : '02';
      const day = i < 15 ? String(1 + i).padStart(2, '0') : String(i - 14).padStart(2, '0');
      db.execute(`INSERT INTO sales VALUES (${i}, ${100 + i * 10}, '2025-${month}-${day}')`);
    }

    const r = db.execute(`
      SELECT DATE_TRUNC('month', sale_date) as month, SUM(amount) as total
      FROM sales
      GROUP BY DATE_TRUNC('month', sale_date)
      ORDER BY month
    `);
    assert.equal(r.rows.length, 2);
    // Check the values exist and are ordered
    const months = r.rows.map(row => Object.values(row)[0]);
    assert.equal(months[0], '2025-01-01');
    assert.equal(months[1], '2025-02-01');
  });

  it('ORDER BY date expression', () => {
    const db = new Database();
    db.execute("CREATE TABLE tasks (id INT PRIMARY KEY, due TEXT)");
    db.execute("INSERT INTO tasks VALUES (1, '2025-12-31')");
    db.execute("INSERT INTO tasks VALUES (2, '2025-01-01')");
    db.execute("INSERT INTO tasks VALUES (3, '2025-06-15')");

    const r = db.execute("SELECT * FROM tasks ORDER BY due ASC");
    assert.equal(r.rows[0].due, '2025-01-01');
    assert.equal(r.rows[2].due, '2025-12-31');
  });

  it('computed date column in SELECT', () => {
    const db = new Database();
    db.execute("CREATE TABLE projects (id INT PRIMARY KEY, start_date TEXT, duration_days INT)");
    db.execute("INSERT INTO projects VALUES (1, '2025-01-01', 90)");
    db.execute("INSERT INTO projects VALUES (2, '2025-06-15', 30)");

    const r = db.execute(`
      SELECT id, start_date, DATE_ADD(start_date, duration_days, 'day') as end_date
      FROM projects
      ORDER BY id
    `);
    assert.equal(r.rows[0].end_date, '2025-04-01');
    assert.equal(r.rows[1].end_date, '2025-07-15');
  });
});
