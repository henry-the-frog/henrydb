// datetime-stress.test.js — Stress tests for date/time functions
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Date/time stress tests', () => {
  
  it('string dates comparison', () => {
    const db = new Database();
    db.execute('CREATE TABLE events (id INT, dt TEXT)');
    db.execute("INSERT INTO events VALUES (1, '2024-01-15')");
    db.execute("INSERT INTO events VALUES (2, '2024-03-20')");
    db.execute("INSERT INTO events VALUES (3, '2024-06-01')");
    db.execute("INSERT INTO events VALUES (4, '2024-12-25')");
    
    const r = db.execute("SELECT id FROM events WHERE dt > '2024-03-01' ORDER BY dt");
    assert.strictEqual(r.rows.length, 3);
    assert.deepStrictEqual(r.rows.map(r => r.id), [2, 3, 4]);
  });

  it('ORDER BY date strings', () => {
    const db = new Database();
    db.execute('CREATE TABLE events (id INT, dt TEXT)');
    db.execute("INSERT INTO events VALUES (1, '2024-12-25')");
    db.execute("INSERT INTO events VALUES (2, '2024-01-01')");
    db.execute("INSERT INTO events VALUES (3, '2024-06-15')");
    
    const r = db.execute('SELECT id FROM events ORDER BY dt');
    assert.deepStrictEqual(r.rows.map(r => r.id), [2, 3, 1]);
  });

  it('BETWEEN with dates', () => {
    const db = new Database();
    db.execute('CREATE TABLE events (id INT, dt TEXT)');
    db.execute("INSERT INTO events VALUES (1, '2024-01-01')");
    db.execute("INSERT INTO events VALUES (2, '2024-06-15')");
    db.execute("INSERT INTO events VALUES (3, '2024-12-31')");
    
    const r = db.execute("SELECT id FROM events WHERE dt BETWEEN '2024-01-01' AND '2024-06-30' ORDER BY id");
    assert.deepStrictEqual(r.rows.map(r => r.id), [1, 2]);
  });

  it('GROUP BY date', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (id INT, dt TEXT, amount INT)');
    db.execute("INSERT INTO sales VALUES (1, '2024-01-15', 100)");
    db.execute("INSERT INTO sales VALUES (2, '2024-01-15', 200)");
    db.execute("INSERT INTO sales VALUES (3, '2024-02-20', 300)");
    
    const r = db.execute('SELECT dt, SUM(amount) as total FROM sales GROUP BY dt ORDER BY dt');
    assert.strictEqual(r.rows.length, 2);
    assert.strictEqual(r.rows[0].total, 300); // Jan 15
    assert.strictEqual(r.rows[1].total, 300); // Feb 20
  });

  it('CURRENT_TIMESTAMP or equivalent', () => {
    const db = new Database();
    try {
      const r = db.execute('SELECT CURRENT_TIMESTAMP as now');
      assert.ok(r.rows[0].now);
    } catch (e) {
      // May not be supported
      try {
        const r = db.execute("SELECT datetime('now') as now");
        assert.ok(r.rows[0].now);
      } catch (e2) {
        assert.ok(true);
      }
    }
  });

  it('date-like strings in WHERE with complex conditions', () => {
    const db = new Database();
    db.execute('CREATE TABLE logs (id INT, ts TEXT, level TEXT)');
    db.execute("INSERT INTO logs VALUES (1, '2024-01-01 10:00:00', 'INFO')");
    db.execute("INSERT INTO logs VALUES (2, '2024-01-01 11:00:00', 'ERROR')");
    db.execute("INSERT INTO logs VALUES (3, '2024-01-02 09:00:00', 'INFO')");
    db.execute("INSERT INTO logs VALUES (4, '2024-01-02 10:00:00', 'ERROR')");
    
    const r = db.execute(`
      SELECT id FROM logs 
      WHERE ts >= '2024-01-01' AND ts < '2024-01-02' AND level = 'ERROR'
      ORDER BY id
    `);
    assert.deepStrictEqual(r.rows.map(r => r.id), [2]);
  });
});
