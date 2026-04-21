// volcano-setops.test.js — Set operations (UNION/INTERSECT/EXCEPT) in volcano planner
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { buildPlan } from './volcano-planner.js';
import { parse } from './sql.js';

describe('Volcano Set Operations', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE t1 (id INT, name TEXT)');
    db.execute("INSERT INTO t1 VALUES (1, 'a')");
    db.execute("INSERT INTO t1 VALUES (2, 'b')");
    db.execute("INSERT INTO t1 VALUES (2, 'b')"); // duplicate
    
    db.execute('CREATE TABLE t2 (id INT, name TEXT)');
    db.execute("INSERT INTO t2 VALUES (2, 'b')");
    db.execute("INSERT INTO t2 VALUES (3, 'c')");
  });

  function volcanoQuery(sql) {
    const ast = parse(sql);
    const plan = buildPlan(ast, db.tables);
    return plan.toArray();
  }

  // ===== UNION =====

  it('UNION ALL appends all rows', () => {
    const rows = volcanoQuery('SELECT * FROM t1 UNION ALL SELECT * FROM t2');
    assert.equal(rows.length, 5); // 3 + 2
  });

  it('UNION removes duplicates', () => {
    const rows = volcanoQuery('SELECT * FROM t1 UNION SELECT * FROM t2');
    assert.equal(rows.length, 3); // {1,a}, {2,b}, {3,c}
    const ids = rows.map(r => r.id).sort();
    assert.deepEqual(ids, [1, 2, 3]);
  });

  it('UNION with ORDER BY', () => {
    const rows = volcanoQuery('SELECT * FROM t1 UNION SELECT * FROM t2 ORDER BY id DESC');
    assert.equal(rows.length, 3);
    assert.equal(rows[0].id, 3);
    assert.equal(rows[2].id, 1);
  });

  it('UNION with LIMIT', () => {
    const rows = volcanoQuery('SELECT * FROM t1 UNION ALL SELECT * FROM t2 LIMIT 2');
    assert.equal(rows.length, 2);
  });

  // ===== INTERSECT =====

  it('INTERSECT finds common rows', () => {
    const rows = volcanoQuery('SELECT * FROM t1 INTERSECT SELECT * FROM t2');
    assert.equal(rows.length, 1);
    assert.equal(rows[0].id, 2);
    assert.equal(rows[0].name, 'b');
  });

  it('INTERSECT ALL respects multiplicities', () => {
    const rows = volcanoQuery('SELECT * FROM t1 INTERSECT ALL SELECT * FROM t2');
    // t1 has {2,b} twice, t2 has {2,b} once → 1 match
    assert.equal(rows.length, 1);
  });

  it('INTERSECT with no overlap returns empty', () => {
    db.execute('CREATE TABLE t3 (id INT, name TEXT)');
    db.execute("INSERT INTO t3 VALUES (99, 'z')");
    const rows = volcanoQuery('SELECT * FROM t1 INTERSECT SELECT * FROM t3');
    assert.equal(rows.length, 0);
  });

  // ===== EXCEPT =====

  it('EXCEPT removes matching rows', () => {
    const rows = volcanoQuery('SELECT * FROM t1 EXCEPT SELECT * FROM t2');
    assert.equal(rows.length, 1);
    assert.equal(rows[0].id, 1);
    assert.equal(rows[0].name, 'a');
  });

  it('EXCEPT ALL respects multiplicities', () => {
    const rows = volcanoQuery('SELECT * FROM t1 EXCEPT ALL SELECT * FROM t2');
    // t1: {1,a}, {2,b}, {2,b}. t2: {2,b}. Result: {1,a}, {2,b}
    assert.equal(rows.length, 2);
  });

  it('EXCEPT with no overlap returns all left rows (deduplicated)', () => {
    db.execute('CREATE TABLE t3 (id INT, name TEXT)');
    db.execute("INSERT INTO t3 VALUES (99, 'z')");
    const rows = volcanoQuery('SELECT * FROM t1 EXCEPT SELECT * FROM t3');
    assert.equal(rows.length, 2); // {1,a} and {2,b} (deduped)
  });

  // ===== Edge cases =====

  it('UNION with empty table', () => {
    db.execute('CREATE TABLE empty_t (id INT, name TEXT)');
    const rows = volcanoQuery('SELECT * FROM t1 UNION SELECT * FROM empty_t');
    assert.equal(rows.length, 2); // deduped t1
  });

  it('UNION with different column names works for matching columns', () => {
    const rows = volcanoQuery('SELECT id FROM t1 UNION SELECT id FROM t2');
    assert.equal(rows.length, 3); // 1, 2, 3
  });

  it('EXPLAIN for UNION', () => {
    const ast = parse('SELECT * FROM t1 UNION ALL SELECT * FROM t2');
    const plan = buildPlan(ast, db.tables);
    const desc = plan.describe();
    assert.ok(desc); // Just verify it doesn't crash
  });
});
