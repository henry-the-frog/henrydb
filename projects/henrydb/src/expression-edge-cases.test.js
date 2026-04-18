// expression-edge-cases.test.js — CASE WHEN, COALESCE, NULLIF, CAST + MVCC

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;
function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-expr-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('Expression Edge Cases + MVCC', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('CASE WHEN with concurrent update changes classification', () => {
    db.execute('CREATE TABLE items (id INT, score INT)');
    db.execute('INSERT INTO items VALUES (1, 90)');
    db.execute('INSERT INTO items VALUES (2, 50)');
    db.execute('INSERT INTO items VALUES (3, 30)');
    
    const s1 = db.session();
    s1.begin();
    
    // Update scores outside s1
    db.execute('UPDATE items SET score = 10 WHERE id = 1'); // 90 → 10
    
    // s1 should classify based on original values
    const r = rows(s1.execute(
      "SELECT id, CASE WHEN score >= 80 THEN 'A' WHEN score >= 60 THEN 'B' ELSE 'C' END as grade FROM items ORDER BY id"
    ));
    assert.equal(r[0].grade, 'A', 'id=1 was 90 in snapshot → A');
    assert.equal(r[1].grade, 'C', 'id=2 was 50 → C');
    assert.equal(r[2].grade, 'C', 'id=3 was 30 → C');
    
    s1.commit();
  });

  it('COALESCE with NULL propagation', () => {
    db.execute('CREATE TABLE t (id INT, a INT, b INT, c INT)');
    db.execute('INSERT INTO t VALUES (1, NULL, NULL, 99)');
    db.execute('INSERT INTO t VALUES (2, NULL, 50, 99)');
    db.execute('INSERT INTO t VALUES (3, 10, 50, 99)');
    
    const r = rows(db.execute('SELECT id, COALESCE(a, b, c) as first_non_null FROM t ORDER BY id'));
    assert.equal(r[0].first_non_null, 99, 'Both a,b NULL → c');
    assert.equal(r[1].first_non_null, 50, 'a NULL → b');
    assert.equal(r[2].first_non_null, 10, 'a not NULL → a');
  });

  it('COALESCE with all NULL returns NULL', () => {
    db.execute('CREATE TABLE t (id INT, a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, NULL, NULL)');
    
    const r = rows(db.execute('SELECT COALESCE(a, b) as result FROM t'));
    assert.equal(r[0].result, null, 'All NULL → NULL');
  });

  it('NULLIF returns NULL when args are equal', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 0)');
    db.execute('INSERT INTO t VALUES (2, 5)');
    
    const r = rows(db.execute('SELECT id, NULLIF(val, 0) as safe FROM t ORDER BY id'));
    assert.equal(r[0].safe, null, 'val=0 should become NULL');
    assert.equal(r[1].safe, 5, 'val=5 stays 5');
  });

  it('CASE WHEN with ELSE NULL', () => {
    db.execute('CREATE TABLE t (id INT, status TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'active')");
    db.execute("INSERT INTO t VALUES (2, 'inactive')");
    db.execute("INSERT INTO t VALUES (3, 'pending')");
    
    const r = rows(db.execute(
      "SELECT id, CASE status WHEN 'active' THEN 1 WHEN 'pending' THEN 0 END as is_active FROM t ORDER BY id"
    ));
    assert.equal(r[0].is_active, 1);
    assert.equal(r[1].is_active, null, 'No matching WHEN → NULL');
    assert.equal(r[2].is_active, 0);
  });

  it('nested CASE expressions', () => {
    db.execute('CREATE TABLE t (id INT, a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 20)');
    db.execute('INSERT INTO t VALUES (2, 30, 5)');
    
    const r = rows(db.execute(
      "SELECT id, CASE WHEN a > b THEN CASE WHEN a > 20 THEN 'big' ELSE 'medium' END ELSE 'small' END as size FROM t ORDER BY id"
    ));
    assert.equal(r[0].size, 'small', 'a=10 < b=20 → small');
    assert.equal(r[1].size, 'big', 'a=30 > b=5, a > 20 → big');
  });

  it('CAST between types', () => {
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, '42')");
    
    // CAST TEXT to INT
    const r = rows(db.execute("SELECT CAST(val AS INT) as num FROM t"));
    assert.equal(r[0].num, 42);
    
    // CAST INT to TEXT
    const r2 = rows(db.execute("SELECT CAST(id AS TEXT) as str FROM t"));
    assert.equal(r2[0].str, '1');
  });

  it('CASE WHEN in WHERE clause', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    const r = rows(db.execute(
      "SELECT id FROM t WHERE CASE WHEN val > 30 THEN 1 ELSE 0 END = 1 ORDER BY id"
    ));
    assert.equal(r.length, 2, 'Only val > 30: id 4,5');
    assert.equal(r[0].id, 4);
    assert.equal(r[1].id, 5);
  });

  it('COALESCE in UPDATE SET', () => {
    db.execute('CREATE TABLE t (id INT, val INT, backup INT)');
    db.execute('INSERT INTO t VALUES (1, NULL, 99)');
    db.execute('INSERT INTO t VALUES (2, 50, 99)');
    
    db.execute('UPDATE t SET val = COALESCE(val, backup)');
    
    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r[0].val, 99, 'NULL val → backup');
    assert.equal(r[1].val, 50, 'Non-NULL val unchanged');
  });

  it('expressions survive close/reopen', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    db.execute('INSERT INTO t VALUES (2, 42)');
    
    db.close();
    db = TransactionalDatabase.open(dbDir);
    
    const r = rows(db.execute('SELECT id, COALESCE(val, -1) as safe FROM t ORDER BY id'));
    assert.equal(r[0].safe, -1);
    assert.equal(r[1].safe, 42);
  });
});
