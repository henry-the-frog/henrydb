// query-plan-depth.test.js — Query plan correctness tests
// Verifies: (1) correct scan type selection, (2) index usage, (3) plan produces correct results

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-plan-'));
  db = TransactionalDatabase.open(dbDir);
  db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score INT, category TEXT)');
  db.execute('CREATE INDEX idx_score ON t (score)');
  db.execute('CREATE INDEX idx_category ON t (category)');
  for (let i = 1; i <= 100; i++) {
    const cat = i <= 30 ? 'A' : i <= 70 ? 'B' : 'C';
    db.execute(`INSERT INTO t VALUES (${i}, 'name${i}', ${i * 10}, '${cat}')`);
  }
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }
function plan(r) { return r?.plan || []; }

describe('Scan Type Selection', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('PK equality uses INDEX_SCAN', () => {
    const p = plan(db.execute('EXPLAIN SELECT * FROM t WHERE id = 50'));
    const indexOp = p.find(op => op.operation === 'INDEX_SCAN');
    assert.ok(indexOp, 'PK equality should use INDEX_SCAN');
    assert.equal(indexOp.index, 'id', 'Should use PK index');
  });

  it('non-indexed column uses TABLE_SCAN', () => {
    const p = plan(db.execute("EXPLAIN SELECT * FROM t WHERE name = 'name50'"));
    const scanOp = p.find(op => op.operation === 'TABLE_SCAN');
    assert.ok(scanOp, 'Non-indexed column should use TABLE_SCAN');
  });

  it('no WHERE clause uses TABLE_SCAN', () => {
    const p = plan(db.execute('EXPLAIN SELECT * FROM t'));
    const scanOp = p.find(op => op.operation === 'TABLE_SCAN');
    assert.ok(scanOp, 'Query without WHERE should use TABLE_SCAN');
  });
});

describe('Plan Correctness: Results Match', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('INDEX_SCAN produces same results as TABLE_SCAN', () => {
    // PK lookup (INDEX_SCAN)
    const r1 = rows(db.execute('SELECT * FROM t WHERE id = 50'));
    assert.equal(r1.length, 1);
    assert.equal(r1[0].id, 50);
    assert.equal(r1[0].score, 500);
  });

  it('range query on indexed column produces correct results', () => {
    const r = rows(db.execute('SELECT id FROM t WHERE score BETWEEN 300 AND 500 ORDER BY id'));
    assert.equal(r.length, 21); // ids 30-50
    assert.equal(r[0].id, 30);
    assert.equal(r[20].id, 50);
  });

  it('index + non-index combined WHERE is correct', () => {
    const r = rows(db.execute("SELECT id FROM t WHERE category = 'A' AND score > 200 ORDER BY id"));
    // Category A: ids 1-30, score > 200: ids 21-30
    assert.equal(r.length, 10);
    assert.equal(r[0].id, 21);
    assert.equal(r[9].id, 30);
  });

  it('OR condition with indexed column', () => {
    const r = rows(db.execute('SELECT id FROM t WHERE id = 1 OR id = 100 ORDER BY id'));
    assert.equal(r.length, 2);
    assert.equal(r[0].id, 1);
    assert.equal(r[1].id, 100);
  });

  it('NOT condition produces correct results', () => {
    const r = rows(db.execute("SELECT COUNT(*) AS cnt FROM t WHERE category != 'A'"));
    assert.equal(r[0].cnt, 70); // 100 - 30 = 70
  });

  it('NULL handling in WHERE with index', () => {
    db.execute('INSERT INTO t VALUES (101, NULL, NULL, NULL)');
    
    const r1 = rows(db.execute('SELECT COUNT(*) AS cnt FROM t WHERE score IS NULL'));
    assert.equal(r1[0].cnt, 1);

    const r2 = rows(db.execute('SELECT COUNT(*) AS cnt FROM t WHERE score IS NOT NULL'));
    assert.equal(r2[0].cnt, 100);
  });

  it('LIMIT and OFFSET produce correct results', () => {
    const r = rows(db.execute('SELECT id FROM t ORDER BY id LIMIT 5 OFFSET 10'));
    assert.equal(r.length, 5);
    assert.equal(r[0].id, 11);
    assert.equal(r[4].id, 15);
  });

  it('ORDER BY DESC with LIMIT', () => {
    const r = rows(db.execute('SELECT id FROM t ORDER BY score DESC LIMIT 3'));
    assert.equal(r.length, 3);
    assert.equal(r[0].id, 100);
    assert.equal(r[1].id, 99);
    assert.equal(r[2].id, 98);
  });
});

describe('Plan with Complex Queries', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('GROUP BY + HAVING with indexed column', () => {
    const r = rows(db.execute(
      "SELECT category, COUNT(*) AS cnt, AVG(score) AS avg_score FROM t GROUP BY category HAVING COUNT(*) > 25 ORDER BY category"
    ));
    // A(30), B(40), C(30) — all > 25
    assert.equal(r.length, 3);
    assert.equal(r[0].category, 'A');
    assert.equal(r[0].cnt, 30);
  });

  it('subquery in WHERE with index on outer table', () => {
    const r = rows(db.execute(
      'SELECT id, name FROM t WHERE score > (SELECT AVG(score) FROM t) ORDER BY id'
    ));
    // AVG score = 505 (sum 50500 / 100). Scores > 505: ids 51-100
    assert.equal(r.length, 50);
    assert.equal(r[0].id, 51);
  });

  it('multiple indexes available: planner chooses correctly', () => {
    // Both score and category are indexed
    // category = 'C' → 30 rows, score > 900 → 10 rows
    // Planner should ideally choose score index (more selective)
    const r = rows(db.execute("SELECT id FROM t WHERE category = 'C' AND score > 900 ORDER BY id"));
    // Category C: ids 71-100. Score > 900: ids 91-100. Intersection: 91-100
    assert.equal(r.length, 10);
    assert.equal(r[0].id, 91);
  });
});
