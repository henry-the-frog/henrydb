// date-functions.test.js — Tests for DATE/TIME/DATETIME functions
import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Date/Time Functions', () => {
  let db;
  
  test('DATE() extracts date from ISO string', () => {
    db = new Database();
    const r = db.execute("SELECT DATE('2024-06-15T10:30:00') as d");
    assert.equal(r.rows[0].d, '2024-06-15');
  });

  test('DATE() with date-only string', () => {
    db = new Database();
    const r = db.execute("SELECT DATE('2024-01-01') as d");
    assert.equal(r.rows[0].d, '2024-01-01');
  });

  test("DATE('now') returns today", () => {
    db = new Database();
    const r = db.execute("SELECT DATE('now') as d");
    const today = new Date().toISOString().split('T')[0];
    assert.equal(r.rows[0].d, today);
  });

  test('TIME() extracts time from ISO string', () => {
    db = new Database();
    const r = db.execute("SELECT TIME('2024-06-15T10:30:45') as t");
    assert.equal(r.rows[0].t, '10:30:45');
  });

  test('TIME() with time-only string', () => {
    db = new Database();
    const r = db.execute("SELECT TIME('14:30:00') as t");
    assert.equal(r.rows[0].t, '14:30:00');
  });

  test('DATETIME() formats datetime', () => {
    db = new Database();
    const r = db.execute("SELECT DATETIME('2024-06-15T10:30:00Z') as dt");
    assert.equal(r.rows[0].dt, '2024-06-15 10:30:00');
  });

  test('JULIANDAY() computes Julian day number', () => {
    db = new Database();
    const r = db.execute("SELECT JULIANDAY('2000-01-01') as j");
    // Jan 1, 2000 = JD 2451544.5 (approximately)
    assert.ok(Math.abs(r.rows[0].j - 2451544.5) < 1, `Expected ~2451544.5, got ${r.rows[0].j}`);
  });

  test('UNIXEPOCH() returns seconds since 1970', () => {
    db = new Database();
    const r = db.execute("SELECT UNIXEPOCH('2024-01-01') as u");
    assert.equal(r.rows[0].u, 1704067200);
  });

  test('DATE functions work in WHERE clauses', () => {
    db = new Database();
    db.execute('CREATE TABLE events (id INT, event_date TEXT)');
    db.execute("INSERT INTO events VALUES (1, '2024-01-15'), (2, '2024-06-15'), (3, '2024-12-01')");
    const r = db.execute("SELECT * FROM events WHERE DATE(event_date) > '2024-06-01'");
    assert.equal(r.rows.length, 2);
  });

  test('DATE functions work in ORDER BY', () => {
    db = new Database();
    db.execute('CREATE TABLE logdata (id INT, ts TEXT)');
    db.execute("INSERT INTO logdata VALUES (1, '2024-12-01'), (2, '2024-01-01'), (3, '2024-06-01')");
    const r = db.execute("SELECT id FROM logdata ORDER BY ts");
    assert.equal(r.rows[0].id, 2);
    assert.equal(r.rows[2].id, 1);
  });

  test('DATE difference via JULIANDAY', () => {
    db = new Database();
    const r = db.execute("SELECT JULIANDAY('2024-01-31') - JULIANDAY('2024-01-01') as diff");
    assert.equal(r.rows[0].diff, 30);
  });
});
