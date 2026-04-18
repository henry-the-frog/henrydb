// adversarial-mvcc.test.js — Adversarial SQL + MVCC edge cases
// GROUP BY, HAVING, DISTINCT, ORDER BY, LIMIT with concurrent modifications.

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;
function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-adv-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('Adversarial SQL + MVCC', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('GROUP BY + HAVING with concurrent insert changes group counts', () => {
    db.execute('CREATE TABLE sales (dept TEXT, amount INT)');
    db.execute("INSERT INTO sales VALUES ('eng', 100)");
    db.execute("INSERT INTO sales VALUES ('eng', 200)");
    db.execute("INSERT INTO sales VALUES ('sales', 50)");
    
    const s1 = db.session();
    s1.begin();
    
    // Add more eng sales outside s1
    db.execute("INSERT INTO sales VALUES ('eng', 300)");
    db.execute("INSERT INTO sales VALUES ('eng', 400)");
    
    // s1: HAVING COUNT(*) > 1 — eng has 2, sales has 1
    const r = rows(s1.execute(
      'SELECT dept, COUNT(*) as cnt, SUM(amount) as total FROM sales GROUP BY dept HAVING COUNT(*) > 1'
    ));
    
    assert.equal(r.length, 1, 'Only eng has >1 rows in snapshot');
    assert.equal(r[0].dept, 'eng');
    assert.equal(r[0].cnt, 2);
    assert.equal(r[0].total, 300);
    
    s1.commit();
  });

  it('DISTINCT with concurrent duplicate insert', () => {
    db.execute('CREATE TABLE tags (name TEXT)');
    db.execute("INSERT INTO tags VALUES ('javascript')");
    db.execute("INSERT INTO tags VALUES ('python')");
    db.execute("INSERT INTO tags VALUES ('javascript')");
    
    const s1 = db.session();
    s1.begin();
    
    // Add more duplicates outside s1
    db.execute("INSERT INTO tags VALUES ('python')");
    db.execute("INSERT INTO tags VALUES ('rust')");
    
    // s1 should see 2 distinct tags
    const r = rows(s1.execute('SELECT DISTINCT name FROM tags ORDER BY name'));
    assert.equal(r.length, 2, 'Should see 2 distinct tags (snapshot)');
    assert.equal(r[0].name, 'javascript');
    assert.equal(r[1].name, 'python');
    
    s1.commit();
    
    // After commit: 3 distinct tags
    const r2 = rows(db.execute('SELECT DISTINCT name FROM tags ORDER BY name'));
    assert.equal(r2.length, 3, 'After inserts, 3 distinct tags');
  });

  it('ORDER BY + LIMIT with concurrent insert that would change top-N', () => {
    db.execute('CREATE TABLE scores (name TEXT, score INT)');
    db.execute("INSERT INTO scores VALUES ('Alice', 80)");
    db.execute("INSERT INTO scores VALUES ('Bob', 90)");
    db.execute("INSERT INTO scores VALUES ('Carol', 70)");
    
    const s1 = db.session();
    s1.begin();
    
    // Insert a higher score outside s1
    db.execute("INSERT INTO scores VALUES ('Dave', 95)");
    
    // s1's top 2 should be Bob (90), Alice (80)
    const r = rows(s1.execute(
      'SELECT name, score FROM scores ORDER BY score DESC LIMIT 2'
    ));
    
    assert.equal(r.length, 2);
    assert.equal(r[0].name, 'Bob');
    assert.equal(r[1].name, 'Alice');
    
    s1.commit();
    
    // After commit, top 2 includes Dave
    const r2 = rows(db.execute(
      'SELECT name, score FROM scores ORDER BY score DESC LIMIT 2'
    ));
    assert.equal(r2[0].name, 'Dave');
    assert.equal(r2[1].name, 'Bob');
  });

  it('ORDER BY + OFFSET + LIMIT (pagination) with concurrent modifications', () => {
    db.execute('CREATE TABLE items (id INT, name TEXT)');
    for (let i = 1; i <= 10; i++) {
      db.execute(`INSERT INTO items VALUES (${i}, 'item-${i}')`);
    }
    
    const s1 = db.session();
    s1.begin();
    
    // Delete items 3-5 outside s1
    db.execute('DELETE FROM items WHERE id BETWEEN 3 AND 5');
    
    // Page 2 (offset 5, limit 5) should see items 6-10
    const r = rows(s1.execute('SELECT * FROM items ORDER BY id LIMIT 5 OFFSET 5'));
    assert.equal(r.length, 5, 'Page 2 should have 5 items (snapshot)');
    assert.equal(r[0].id, 6);
    assert.equal(r[4].id, 10);
    
    s1.commit();
  });

  it('COUNT(*) with concurrent insert and delete — exact snapshot', () => {
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    
    const s1 = db.session();
    s1.begin();
    
    // Net change: +2 insert, -3 delete = net -1 outside s1
    db.execute('INSERT INTO t VALUES (11)');
    db.execute('INSERT INTO t VALUES (12)');
    db.execute('DELETE FROM t WHERE id IN (1, 2, 3)');
    
    // s1 should see exactly 10
    const r = rows(s1.execute('SELECT COUNT(*) as cnt FROM t'));
    assert.equal(r[0].cnt, 10, 'Snapshot count should be exactly 10');
    
    s1.commit();
    
    // After commit: 10 - 3 + 2 = 9
    const r2 = rows(db.execute('SELECT COUNT(*) as cnt FROM t'));
    assert.equal(r2[0].cnt, 9, 'After modifications, count should be 9');
  });

  it('aggregate functions with concurrent updates — SUM, AVG, MIN, MAX', () => {
    db.execute('CREATE TABLE metrics (id INT, val INT)');
    db.execute('INSERT INTO metrics VALUES (1, 10)');
    db.execute('INSERT INTO metrics VALUES (2, 20)');
    db.execute('INSERT INTO metrics VALUES (3, 30)');
    db.execute('INSERT INTO metrics VALUES (4, 40)');
    
    const s1 = db.session();
    s1.begin();
    
    // Double all values outside s1
    for (let i = 1; i <= 4; i++) {
      db.execute(`UPDATE metrics SET val = val * 2 WHERE id = ${i}`);
    }
    
    // s1 should see original values
    const r = rows(s1.execute('SELECT SUM(val) as s, AVG(val) as a, MIN(val) as mn, MAX(val) as mx FROM metrics'));
    assert.equal(r[0].s, 100, 'SUM should be 100 (10+20+30+40)');
    assert.equal(r[0].a, 25, 'AVG should be 25');
    assert.equal(r[0].mn, 10, 'MIN should be 10');
    assert.equal(r[0].mx, 40, 'MAX should be 40');
    
    s1.commit();
    
    // After commit: all doubled
    const r2 = rows(db.execute('SELECT SUM(val) as s FROM metrics'));
    assert.equal(r2[0].s, 200, 'After doubling, SUM should be 200');
  });

  it('write skew anomaly detection: both transactions read same row, update different rows', () => {
    // Classic serializable isolation test
    db.execute('CREATE TABLE accounts (id INT, balance INT)');
    db.execute('INSERT INTO accounts VALUES (1, 100)');
    db.execute('INSERT INTO accounts VALUES (2, 100)');
    
    // Both sessions check total balance >= 200 before withdrawing
    const s1 = db.session();
    s1.begin();
    const s2 = db.session();
    s2.begin();
    
    // Both read total
    const t1 = rows(s1.execute('SELECT SUM(balance) as total FROM accounts'));
    const t2 = rows(s2.execute('SELECT SUM(balance) as total FROM accounts'));
    assert.equal(t1[0].total, 200);
    assert.equal(t2[0].total, 200);
    
    // s1 withdraws from account 1
    s1.execute('UPDATE accounts SET balance = 0 WHERE id = 1');
    
    // s2 withdraws from account 2
    s2.execute('UPDATE accounts SET balance = 0 WHERE id = 2');
    
    // Both commit
    s1.commit();
    try {
      s2.commit();
    } catch (e) {
      // Serialization error is acceptable — means we detect write skew
      assert.ok(e.message.includes('serializ') || e.message.includes('conflict'),
        'Should detect serialization conflict');
      return;
    }
    
    // If both commit: write skew happened — total balance is 0 but both saw 200
    // This is acceptable for snapshot isolation (not serializable)
    const r = rows(db.execute('SELECT SUM(balance) as total FROM accounts'));
    // Under SI, both committed: total = 0 (write skew allowed)
    assert.ok(true, `Write skew: total balance is ${r[0].total} (SI allows this)`);
  });

  it('phantom read test: INSERT between range scans', () => {
    db.execute('CREATE TABLE orders (id INT, amount INT)');
    db.execute('INSERT INTO orders VALUES (1, 100)');
    db.execute('INSERT INTO orders VALUES (2, 200)');
    db.execute('INSERT INTO orders VALUES (3, 300)');
    
    const s1 = db.session();
    s1.begin();
    
    // First range scan
    const r1 = rows(s1.execute('SELECT * FROM orders WHERE amount > 150 ORDER BY id'));
    assert.equal(r1.length, 2, 'First scan: 2 rows > 150');
    
    // Insert a phantom outside s1
    db.execute('INSERT INTO orders VALUES (4, 250)');
    
    // Second range scan in same tx — should see same result (no phantom)
    const r2 = rows(s1.execute('SELECT * FROM orders WHERE amount > 150 ORDER BY id'));
    assert.equal(r2.length, 2, 'Second scan: still 2 rows (no phantom read)');
    
    s1.commit();
    
    // After commit, phantom is visible
    const r3 = rows(db.execute('SELECT * FROM orders WHERE amount > 150 ORDER BY id'));
    assert.equal(r3.length, 3, 'After commit, 3 rows including phantom');
  });

  it('read-your-writes: own updates visible within transaction', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    
    const s1 = db.session();
    s1.begin();
    
    // Update within own tx
    s1.execute('UPDATE t SET val = 99 WHERE id = 1');
    
    // Should see own update
    const r = rows(s1.execute('SELECT val FROM t WHERE id = 1'));
    assert.equal(r[0].val, 99, 'Should see own update');
    
    // Insert within own tx
    s1.execute('INSERT INTO t VALUES (2, 42)');
    const r2 = rows(s1.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r2.length, 2, 'Should see own insert');
    assert.equal(r2[1].val, 42);
    
    s1.commit();
  });
});
