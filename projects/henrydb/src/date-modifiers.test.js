// date-modifiers.test.js — Tests for SQLite-compatible DATE/TIME modifiers
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('DATE modifiers (SQLite-compatible)', () => {
  let db;

  it('setup', () => {
    db = new Database();
  });

  // Basic modifiers
  it('+N days', () => {
    const r = db.execute("SELECT DATE('2024-01-15', '+10 days') as d");
    assert.equal(r.rows[0].d, '2024-01-25');
  });

  it('-N days', () => {
    const r = db.execute("SELECT DATE('2024-01-15', '-3 days') as d");
    assert.equal(r.rows[0].d, '2024-01-12');
  });

  it('+N months', () => {
    const r = db.execute("SELECT DATE('2024-01-15', '+1 month') as d");
    assert.equal(r.rows[0].d, '2024-02-15');
  });

  it('-N months', () => {
    const r = db.execute("SELECT DATE('2024-03-15', '-2 months') as d");
    assert.equal(r.rows[0].d, '2024-01-15');
  });

  it('+N years', () => {
    const r = db.execute("SELECT DATE('2024-01-15', '+1 year') as d");
    assert.equal(r.rows[0].d, '2025-01-15');
  });

  it('start of month', () => {
    const r = db.execute("SELECT DATE('2024-03-15', 'start of month') as d");
    assert.equal(r.rows[0].d, '2024-03-01');
  });

  it('start of year', () => {
    const r = db.execute("SELECT DATE('2024-06-15', 'start of year') as d");
    assert.equal(r.rows[0].d, '2024-01-01');
  });

  it('start of day', () => {
    const r = db.execute("SELECT DATE('2024-01-15', 'start of day') as d");
    assert.equal(r.rows[0].d, '2024-01-15');
  });

  // Chained modifiers
  it('chain: +1 month then start of month', () => {
    const r = db.execute("SELECT DATE('2024-01-15', '+1 month', 'start of month') as d");
    assert.equal(r.rows[0].d, '2024-02-01');
  });

  it('chain: start of year then +6 months', () => {
    const r = db.execute("SELECT DATE('2024-08-20', 'start of year', '+6 months') as d");
    assert.equal(r.rows[0].d, '2024-07-01');
  });

  it('chain: multiple additions', () => {
    const r = db.execute("SELECT DATE('2024-01-01', '+1 year', '+6 months', '+15 days') as d");
    assert.equal(r.rows[0].d, '2025-07-16');
  });

  // 'now' keyword
  it("DATE('now') returns today", () => {
    const r = db.execute("SELECT DATE('now') as d");
    const today = new Date().toISOString().split('T')[0];
    assert.equal(r.rows[0].d, today);
  });

  it("DATE('now', '+30 days')", () => {
    const r = db.execute("SELECT DATE('now', '+30 days') as d");
    const expected = new Date(Date.now() + 30 * 86400000).toISOString().split('T')[0];
    assert.equal(r.rows[0].d, expected);
  });

  // Edge cases
  it('month overflow: Jan 31 + 1 month', () => {
    const r = db.execute("SELECT DATE('2024-01-31', '+1 month') as d");
    // JS Date handles this: Jan 31 + 1 month = Mar 2 (Feb has 29 days in 2024)
    assert.equal(r.rows[0].d, '2024-03-02');
  });

  it('leap year: Feb 29', () => {
    const r = db.execute("SELECT DATE('2024-02-29', '+1 year') as d");
    // 2025 is not a leap year, Feb 29 + 1 year = Mar 1 2025
    assert.equal(r.rows[0].d, '2025-03-01');
  });

  it('null input returns null', () => {
    const r = db.execute("SELECT DATE(NULL) as d");
    assert.equal(r.rows[0].d, null);
  });

  // Backward compatibility
  it('DATE(value) without modifiers still works', () => {
    const r = db.execute("SELECT DATE('2024-01-15T10:30:00Z') as d");
    assert.equal(r.rows[0].d, '2024-01-15');
  });

  // In a query context
  it('DATE modifiers in WHERE clause', () => {
    db.execute('CREATE TABLE events (id INT, event_date TEXT)');
    db.execute("INSERT INTO events VALUES (1, '2024-01-15')");
    db.execute("INSERT INTO events VALUES (2, '2024-02-15')");
    db.execute("INSERT INTO events VALUES (3, '2024-03-15')");

    const r = db.execute("SELECT * FROM events WHERE event_date < DATE('2024-01-15', '+1 month')");
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].id, 1);
  });
});

describe('TIME modifiers (SQLite-compatible)', () => {
  let db;
  it('setup', () => { db = new Database(); });

  it('TIME() extracts time', () => {
    const r = db.execute("SELECT TIME('2024-01-15T10:30:45Z') as t");
    assert.equal(r.rows[0].t, '10:30:45');
  });

  it('TIME() with +N hours', () => {
    const r = db.execute("SELECT TIME('10:30:00', '+2 hours') as t");
    assert.equal(r.rows[0].t, '12:30:00');
  });

  it('TIME() with +N minutes', () => {
    const r = db.execute("SELECT TIME('10:30:00', '+45 minutes') as t");
    assert.equal(r.rows[0].t, '11:15:00');
  });

  it("TIME('now') returns current time", () => {
    const r = db.execute("SELECT TIME('now') as t");
    assert.ok(r.rows[0].t.match(/^\d{2}:\d{2}:\d{2}$/));
  });
});

describe('DATETIME modifiers (SQLite-compatible)', () => {
  let db;
  it('setup', () => { db = new Database(); });

  it('DATETIME() returns full timestamp', () => {
    const r = db.execute("SELECT DATETIME('2024-01-15T10:30:00Z') as dt");
    assert.equal(r.rows[0].dt, '2024-01-15 10:30:00');
  });

  it('DATETIME() with space-separated input', () => {
    const r = db.execute("SELECT DATETIME('2024-01-15 10:30:00', '+1 month') as dt");
    assert.equal(r.rows[0].dt, '2024-02-15 10:30:00');
  });

  it('DATETIME() with date-only input', () => {
    const r = db.execute("SELECT DATETIME('2024-01-15', '+1 month') as dt");
    assert.equal(r.rows[0].dt, '2024-02-15 00:00:00');
  });

  it('DATETIME() chained modifiers', () => {
    const r = db.execute("SELECT DATETIME('2024-01-15', '+1 month', '+2 hours', '+30 minutes') as dt");
    assert.equal(r.rows[0].dt, '2024-02-15 02:30:00');
  });

  it('DATETIME() start of month preserves time', () => {
    const r = db.execute("SELECT DATETIME('2024-03-15 14:30:00', 'start of month') as dt");
    assert.equal(r.rows[0].dt, '2024-03-01 00:00:00');
  });

  it("DATETIME('now')", () => {
    const r = db.execute("SELECT DATETIME('now') as dt");
    assert.ok(r.rows[0].dt.match(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/));
  });
});
