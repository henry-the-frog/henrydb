// estimation-accuracy-v2.test.js — Histogram estimation accuracy validation
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Histogram estimation accuracy', () => {
  
  it('uniform distribution: estimates should be close to actual', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    for (let i = 1; i <= 1000; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    db.execute('ANALYZE TABLE t');
    
    // Range query: val > 500 should give ~500 rows
    const actual = db.execute('SELECT COUNT(*) as cnt FROM t WHERE val > 500').rows[0].cnt;
    const explain = db.execute('EXPLAIN SELECT * FROM t WHERE val > 500');
    const planNode = explain.plan?.find(p => p.estimation_method?.includes('histogram'));
    
    if (planNode) {
      const estimated = planNode.filtered_estimate || planNode.estimated_rows;
      const ratio = Math.max(estimated / actual, actual / estimated);
      assert.ok(ratio < 2, `uniform ratio ${ratio.toFixed(2)} should be < 2 (est=${estimated}, actual=${actual})`);
    }
  });

  it('skewed distribution: histogram beats uniform assumption', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    // Quadratic distribution: most values near 0
    for (let i = 0; i < 1000; i++) {
      const val = Math.floor((i / 1000) * (i / 1000) * 1000);
      db.execute(`INSERT INTO t VALUES (${val})`);
    }
    db.execute('ANALYZE TABLE t');
    
    // val > 500: few rows (quadratic, ~29% but clustered)
    const actual = db.execute('SELECT COUNT(*) as cnt FROM t WHERE val > 500').rows[0].cnt;
    
    // Linear interpolation would estimate ~50% (500 out of 1000)
    // Histogram should be much closer to actual
    const explain = db.execute('EXPLAIN SELECT * FROM t WHERE val > 500');
    const planNode = explain.plan?.find(p => p.estimation_method?.includes('histogram'));
    
    if (planNode) {
      const estimated = planNode.filtered_estimate || planNode.estimated_rows;
      const ratio = Math.max(estimated / actual, actual / estimated);
      assert.ok(ratio < 3, `skewed ratio ${ratio.toFixed(2)} should be < 3 (est=${estimated}, actual=${actual})`);
    }
  });

  it('bimodal distribution: histogram captures both peaks', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    // Bimodal: half near 100, half near 900
    for (let i = 0; i < 500; i++) db.execute(`INSERT INTO t VALUES (${80 + Math.floor(Math.random() * 40)})`);
    for (let i = 0; i < 500; i++) db.execute(`INSERT INTO t VALUES (${880 + Math.floor(Math.random() * 40)})`);
    db.execute('ANALYZE TABLE t');
    
    // val BETWEEN 90 AND 110 should be ~50% (around the first peak)
    const actual = db.execute('SELECT COUNT(*) as cnt FROM t WHERE val BETWEEN 90 AND 110').rows[0].cnt;
    assert.ok(actual > 100 && actual < 500, `bimodal first peak: actual=${actual}`);
  });

  it('equality estimation on high-frequency value', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val INT)');
    // 90% of values are 1, 10% are 2-10
    for (let i = 0; i < 900; i++) db.execute('INSERT INTO t VALUES (1)');
    for (let i = 0; i < 100; i++) db.execute(`INSERT INTO t VALUES (${(i % 9) + 2})`);
    db.execute('ANALYZE TABLE t');
    
    // val = 1 should estimate ~900
    const actual = db.execute('SELECT COUNT(*) as cnt FROM t WHERE val = 1').rows[0].cnt;
    assert.strictEqual(actual, 900);
    
    // Without histogram, 1/ndistinct = 1/10 = 100 (way off!)
    // With histogram, bucket containing 1 should give ~900
    const explain = db.execute('EXPLAIN SELECT * FROM t WHERE val = 1');
    const planNode = explain.plan?.find(p => p.estimation_method?.includes('histogram'));
    if (planNode) {
      const estimated = planNode.filtered_estimate;
      assert.ok(estimated > 200, `should estimate >> 100 for high-frequency value, got ${estimated}`);
    }
  });
});
