// orderby-depth.test.js — ORDER BY depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-order-'));
  db = TransactionalDatabase.open(dbDir);
  db.execute('CREATE TABLE t (id INT, name TEXT, score INT, dept TEXT)');
  db.execute("INSERT INTO t VALUES (1, 'Alice', 90, 'eng')");
  db.execute("INSERT INTO t VALUES (2, 'Bob', 85, 'sales')");
  db.execute("INSERT INTO t VALUES (3, 'Carol', 90, 'eng')");
  db.execute("INSERT INTO t VALUES (4, 'Dave', NULL, 'hr')");
  db.execute("INSERT INTO t VALUES (5, 'Eve', 85, 'sales')");
  db.execute("INSERT INTO t VALUES (6, NULL, 80, 'eng')");
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('Multi-Column Sort', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('ORDER BY two columns', () => {
    const r = rows(db.execute('SELECT name FROM t WHERE name IS NOT NULL ORDER BY score DESC, name ASC'));
    // score=90: Alice, Carol → Alice, Carol
    // score=85: Bob, Eve → Bob, Eve
    assert.equal(r[0].name, 'Alice');
    assert.equal(r[1].name, 'Carol');
    assert.equal(r[2].name, 'Bob');
    assert.equal(r[3].name, 'Eve');
  });

  it('mixed ASC/DESC', () => {
    const r = rows(db.execute('SELECT id FROM t WHERE score IS NOT NULL ORDER BY dept ASC, score DESC'));
    // eng: 90(1,3), 80(6) → 1,3,6 or 3,1,6
    // sales: 85(2,5) → 2,5 or 5,2
    // First should be eng with score 90
    assert.ok(r[0].id === 1 || r[0].id === 3, 'First should be eng with high score');
  });
});

describe('NULL Ordering', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('NULLs sort position (default)', () => {
    const r = rows(db.execute('SELECT id, score FROM t ORDER BY score'));
    // NULLs typically sort first or last
    // Check that NULL row is at one end
    const first = r[0].score;
    const last = r[r.length - 1].score;
    assert.ok(first === null || last === null, 'NULL should be at an end');
  });

  it('ORDER BY with NULL names', () => {
    const r = rows(db.execute('SELECT id, name FROM t ORDER BY name'));
    // NULL name should be at an end
    const nullIdx = r.findIndex(x => x.name === null);
    assert.ok(nullIdx === 0 || nullIdx === r.length - 1, 'NULL should sort to an end');
  });
});

describe('Expression ORDER BY', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('ORDER BY expression', () => {
    const r = rows(db.execute('SELECT id, score FROM t WHERE score IS NOT NULL ORDER BY score * -1'));
    // Lowest score * -1 = most negative → first
    // score 80 * -1 = -80 (smallest)
    assert.equal(r[0].score, 90); // 90 * -1 = -90 is smallest
  });

  it('ORDER BY column number', () => {
    const r = rows(db.execute('SELECT name, score FROM t WHERE name IS NOT NULL ORDER BY 2 DESC'));
    // ORDER BY 2 = ORDER BY score DESC
    assert.equal(r[0].score, 90);
  });
});

describe('ORDER BY with DISTINCT', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('DISTINCT + ORDER BY', () => {
    const r = rows(db.execute('SELECT DISTINCT score FROM t WHERE score IS NOT NULL ORDER BY score'));
    assert.equal(r.length, 3); // 80, 85, 90
    assert.equal(r[0].score, 80);
    assert.equal(r[1].score, 85);
    assert.equal(r[2].score, 90);
  });

  it('DISTINCT dept + ORDER BY', () => {
    const r = rows(db.execute('SELECT DISTINCT dept FROM t ORDER BY dept'));
    assert.equal(r.length, 3); // eng, hr, sales
    assert.equal(r[0].dept, 'eng');
    assert.equal(r[1].dept, 'hr');
    assert.equal(r[2].dept, 'sales');
  });
});

describe('Stability and Correctness', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('ORDER BY preserves all rows', () => {
    const r = rows(db.execute('SELECT id FROM t ORDER BY score'));
    assert.equal(r.length, 6, 'ORDER BY should not lose rows');
  });

  it('ORDER BY with LIMIT', () => {
    const r = rows(db.execute('SELECT name FROM t WHERE name IS NOT NULL ORDER BY score DESC LIMIT 2'));
    assert.equal(r.length, 2);
    assert.ok(r[0].name === 'Alice' || r[0].name === 'Carol');
  });

  it('ORDER BY same column ASC and DESC give reverse order', () => {
    const asc = rows(db.execute('SELECT id FROM t WHERE score IS NOT NULL ORDER BY score ASC'));
    const desc = rows(db.execute('SELECT id FROM t WHERE score IS NOT NULL ORDER BY score DESC'));
    assert.equal(asc.length, desc.length);
    assert.equal(asc[0].id, desc[desc.length - 1].id);
  });
});
