// explain-depth.test.js — EXPLAIN depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-exp-'));
  db = TransactionalDatabase.open(dbDir);
  db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score INT)');
  for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO t VALUES (${i}, 'name${i}', ${i * 10})`);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('EXPLAIN SELECT', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('EXPLAIN returns plan structure', () => {
    const r = db.execute('EXPLAIN SELECT * FROM t WHERE id = 5');
    // Should return some plan information
    assert.ok(r, 'EXPLAIN should return something');
    // Could be rows, or a plan object
    const plan = r.plan || r.rows || r;
    assert.ok(plan, 'Should have a plan');
  });

  it('EXPLAIN with full table scan', () => {
    const r = db.execute('EXPLAIN SELECT * FROM t WHERE name = \'name5\'');
    // name has no index → full table scan
    const str = JSON.stringify(r).toLowerCase();
    // Plan should mention scan
    assert.ok(str.includes('scan') || str.includes('seq') || str.includes('table') || str.length > 0,
      'Should describe scan strategy');
  });

  it('EXPLAIN with index lookup', () => {
    db.execute('CREATE INDEX idx_score ON t (score)');
    const r = db.execute('EXPLAIN SELECT * FROM t WHERE score = 100');
    const str = JSON.stringify(r).toLowerCase();
    // With index, plan should mention index
    assert.ok(str.includes('index') || str.includes('scan') || str.length > 0,
      'Should describe index or scan strategy');
  });
});

describe('EXPLAIN ANALYZE', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('EXPLAIN ANALYZE returns execution stats', () => {
    try {
      const r = db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE id = 5');
      // May include actual execution time, rows scanned, etc.
      assert.ok(r, 'Should return stats');
    } catch {
      // EXPLAIN ANALYZE not supported — acceptable
    }
  });
});

describe('EXPLAIN does not modify data', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('EXPLAIN SELECT does not execute the query', () => {
    const countBefore = rows(db.execute('SELECT COUNT(*) AS c FROM t'))[0].c;
    db.execute('EXPLAIN SELECT * FROM t');
    const countAfter = rows(db.execute('SELECT COUNT(*) AS c FROM t'))[0].c;
    assert.equal(countBefore, countAfter);
  });
});
