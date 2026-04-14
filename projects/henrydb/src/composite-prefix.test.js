import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

test('Composite index: single-column prefix scan', () => {
  const db = new Database();
  db.execute('CREATE TABLE t (a INT, b INT, c INT, val TEXT)');
  for (let i = 0; i < 100; i++) {
    db.execute(`INSERT INTO t VALUES (${i % 10}, ${i % 5}, ${i}, 'val${i}')`);
  }
  db.execute('CREATE INDEX idx_abc ON t(a, b, c)');
  
  const r = db.execute('SELECT * FROM t WHERE a = 5');
  assert.equal(r.rows.length, 10);
  assert.ok(r.rows.every(row => row.a === 5));
});

test('Composite index: two-column prefix scan', () => {
  const db = new Database();
  db.execute('CREATE TABLE t (a INT, b INT, c INT, val TEXT)');
  for (let i = 0; i < 100; i++) {
    db.execute(`INSERT INTO t VALUES (${i % 10}, ${i % 5}, ${i}, 'val${i}')`);
  }
  db.execute('CREATE INDEX idx_abc ON t(a, b, c)');
  
  const r = db.execute('SELECT * FROM t WHERE a = 5 AND b = 0');
  assert.equal(r.rows.length, 10); // a=5 always has b=0
  assert.ok(r.rows.every(row => row.a === 5 && row.b === 0));
});

test('Composite index: full key match', () => {
  const db = new Database();
  db.execute('CREATE TABLE t (a INT, b INT, c INT, val TEXT)');
  for (let i = 0; i < 100; i++) {
    db.execute(`INSERT INTO t VALUES (${i % 10}, ${i % 5}, ${i}, 'val${i}')`);
  }
  db.execute('CREATE INDEX idx_abc ON t(a, b, c)');
  
  const r = db.execute('SELECT * FROM t WHERE a = 5 AND b = 0 AND c = 5');
  assert.equal(r.rows.length, 1);
  assert.equal(r.rows[0].val, 'val5');
});

test('Composite index: non-prefix column only does not use index', () => {
  const db = new Database();
  db.execute('CREATE TABLE t (a INT, b INT, c INT)');
  for (let i = 0; i < 50; i++) {
    db.execute(`INSERT INTO t VALUES (${i % 5}, ${i % 3}, ${i})`);
  }
  db.execute('CREATE INDEX idx_ab ON t(a, b)');
  
  // Query on b only (not a prefix) — should still get correct results via seq scan
  const r = db.execute('SELECT * FROM t WHERE b = 1');
  assert.ok(r.rows.length > 0);
  assert.ok(r.rows.every(row => row.b === 1));
});

test('Composite index: different data distribution', () => {
  const db = new Database();
  db.execute('CREATE TABLE orders (region TEXT, status TEXT, amount INT)');
  db.execute("INSERT INTO orders VALUES ('east', 'shipped', 100)");
  db.execute("INSERT INTO orders VALUES ('east', 'pending', 200)");
  db.execute("INSERT INTO orders VALUES ('east', 'shipped', 150)");
  db.execute("INSERT INTO orders VALUES ('west', 'shipped', 300)");
  db.execute("INSERT INTO orders VALUES ('west', 'pending', 250)");
  
  db.execute('CREATE INDEX idx_region_status ON orders(region, status)');
  
  // Prefix: region only
  const r1 = db.execute("SELECT * FROM orders WHERE region = 'east'");
  assert.equal(r1.rows.length, 3);
  
  // Full: region + status
  const r2 = db.execute("SELECT * FROM orders WHERE region = 'east' AND status = 'shipped'");
  assert.equal(r2.rows.length, 2);
});

test('Composite index: three columns with diverse data', () => {
  const db = new Database();
  db.execute('CREATE TABLE eventlog (year INT, month INT, day INT, event TEXT)');
  for (let y = 2020; y <= 2023; y++) {
    for (let m = 1; m <= 12; m++) {
      for (let d = 1; d <= 3; d++) {
        db.execute(`INSERT INTO eventlog VALUES (${y}, ${m}, ${d}, 'event_${y}_${m}_${d}')`);
      }
    }
  }
  db.execute('CREATE INDEX idx_ymd ON eventlog(year, month, day)');
  
  // Year prefix
  const r1 = db.execute('SELECT * FROM eventlog WHERE year = 2022');
  assert.equal(r1.rows.length, 36); // 12 months * 3 days
  
  // Year + month prefix
  const r2 = db.execute('SELECT * FROM eventlog WHERE year = 2022 AND month = 6');
  assert.equal(r2.rows.length, 3); // 3 days
  
  // Full key
  const r3 = db.execute('SELECT * FROM eventlog WHERE year = 2022 AND month = 6 AND day = 2');
  assert.equal(r3.rows.length, 1);
  assert.equal(r3.rows[0].event, 'event_2022_6_2');
});
