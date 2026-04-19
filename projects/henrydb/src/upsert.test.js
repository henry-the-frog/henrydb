import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('UPSERT (INSERT ON CONFLICT) Tests (2026-04-19)', () => {
  it('basic UPSERT - update on conflict', () => {
    const db = new Database();
    db.execute('CREATE TABLE kv (key TEXT PRIMARY KEY, value INT)');
    db.execute("INSERT INTO kv VALUES ('a', 1)");
    db.execute("INSERT INTO kv VALUES ('a', 2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value");
    const r = db.execute("SELECT value FROM kv WHERE key = 'a'");
    assert.equal(r.rows[0].value, 2);
  });

  it('UPSERT DO NOTHING', () => {
    const db = new Database();
    db.execute('CREATE TABLE kv (key TEXT PRIMARY KEY, value INT)');
    db.execute("INSERT INTO kv VALUES ('a', 1)");
    db.execute("INSERT INTO kv VALUES ('a', 999) ON CONFLICT (key) DO NOTHING");
    const r = db.execute("SELECT value FROM kv WHERE key = 'a'");
    assert.equal(r.rows[0].value, 1);
  });

  it('UPSERT with accumulation', () => {
    const db = new Database();
    db.execute('CREATE TABLE counters (name TEXT PRIMARY KEY, count INT)');
    db.execute("INSERT INTO counters VALUES ('hits', 1)");
    db.execute("INSERT INTO counters VALUES ('hits', 1) ON CONFLICT (name) DO UPDATE SET count = counters.count + EXCLUDED.count");
    db.execute("INSERT INTO counters VALUES ('hits', 1) ON CONFLICT (name) DO UPDATE SET count = counters.count + EXCLUDED.count");
    const r = db.execute("SELECT count FROM counters WHERE name = 'hits'");
    assert.equal(r.rows[0].count, 3);
  });

  it('bulk UPSERT - mix of inserts and updates', () => {
    const db = new Database();
    db.execute('CREATE TABLE kv (key TEXT PRIMARY KEY, value INT)');
    db.execute("INSERT INTO kv VALUES ('a', 1), ('b', 2)");
    db.execute("INSERT INTO kv VALUES ('b', 20), ('c', 30) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value");
    const r = db.execute('SELECT * FROM kv ORDER BY key');
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].value, 1);   // a unchanged
    assert.equal(r.rows[1].value, 20);  // b updated
    assert.equal(r.rows[2].value, 30);  // c inserted
  });

  it('UPSERT with RETURNING', () => {
    const db = new Database();
    db.execute('CREATE TABLE kv (key TEXT PRIMARY KEY, value INT)');
    db.execute("INSERT INTO kv VALUES ('a', 1)");
    const r = db.execute("INSERT INTO kv VALUES ('a', 2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value RETURNING key, value");
    assert.equal(r.rows[0].value, 2);
  });

  it('UPSERT on integer PRIMARY KEY', () => {
    const db = new Database();
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, name TEXT, qty INT)');
    db.execute("INSERT INTO items VALUES (1, 'Widget', 10)");
    db.execute("INSERT INTO items VALUES (1, 'Widget', 5) ON CONFLICT (id) DO UPDATE SET qty = items.qty + EXCLUDED.qty");
    const r = db.execute('SELECT qty FROM items WHERE id = 1');
    assert.equal(r.rows[0].qty, 15);
  });
});
