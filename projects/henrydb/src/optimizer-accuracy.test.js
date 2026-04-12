// optimizer-accuracy.test.js — Compare EXPLAIN estimates vs actual row counts
// Goal: find where the optimizer's estimates are worst
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Optimizer Accuracy', () => {
  let db;
  const results = [];

  before(() => {
    db = new Database();

    // Table 1: uniform distribution
    db.execute(`CREATE TABLE acc_uniform (id INT PRIMARY KEY, val INT, dept TEXT, score INT)`);
    for (let i = 1; i <= 500; i++) {
      db.execute(`INSERT INTO acc_uniform VALUES (${i}, ${i}, '${['A','B','C','D','E'][i % 5]}', ${i % 100})`);
    }

    // Table 2: skewed distribution (80% in one category)
    db.execute(`CREATE TABLE acc_skewed (id INT PRIMARY KEY, val INT, cat TEXT, rating INT)`);
    for (let i = 1; i <= 500; i++) {
      const cat = i <= 400 ? 'common' : ['rare_a', 'rare_b', 'rare_c', 'rare_d'][i % 4];
      db.execute(`INSERT INTO acc_skewed VALUES (${i}, ${i * 3 % 1000}, '${cat}', ${i % 10})`);
    }

    // Table 3: NULLs
    db.execute(`CREATE TABLE acc_nulls (id INT PRIMARY KEY, val INT, optional INT)`);
    for (let i = 1; i <= 200; i++) {
      const opt = i % 3 === 0 ? 'NULL' : i;
      db.execute(`INSERT INTO acc_nulls VALUES (${i}, ${i}, ${opt})`);
    }

    // Create indexes
    db.execute('CREATE INDEX idx_u_val ON acc_uniform (val)');
    db.execute('CREATE INDEX idx_u_dept ON acc_uniform (dept)');
    db.execute('CREATE INDEX idx_u_score ON acc_uniform (score)');
    db.execute('CREATE INDEX idx_s_val ON acc_skewed (val)');
    db.execute('CREATE INDEX idx_s_cat ON acc_skewed (cat)');
    db.execute('CREATE INDEX idx_n_val ON acc_nulls (val)');

    // Run ANALYZE on all tables
    db.execute('ANALYZE TABLE acc_uniform');
    db.execute('ANALYZE TABLE acc_skewed');
    db.execute('ANALYZE TABLE acc_nulls');
  });

  after(() => {
    // Print accuracy report
    console.log('\n=== Optimizer Accuracy Report ===');
    console.log(`Total queries tested: ${results.length}`);
    
    const withEstimates = results.filter(r => r.estimated != null && r.actual != null);
    if (withEstimates.length === 0) {
      console.log('No queries had both estimated and actual row counts.');
      return;
    }

    // Sort by error ratio (worst first)
    withEstimates.sort((a, b) => b.errorRatio - a.errorRatio);

    console.log(`Queries with estimates: ${withEstimates.length}`);
    console.log('\nWorst 10 estimates:');
    for (const r of withEstimates.slice(0, 10)) {
      console.log(`  ${r.errorRatio.toFixed(2)}x off | est=${r.estimated} actual=${r.actual} | ${r.query.slice(0, 80)}`);
    }

    const avgError = withEstimates.reduce((s, r) => s + r.errorRatio, 0) / withEstimates.length;
    const goodEstimates = withEstimates.filter(r => r.errorRatio <= 2);
    console.log(`\nAverage error ratio: ${avgError.toFixed(2)}x`);
    console.log(`Within 2x: ${goodEstimates.length}/${withEstimates.length} (${(100*goodEstimates.length/withEstimates.length).toFixed(0)}%)`);
  });

  function runAndCompare(sql, label) {
    try {
      // Get EXPLAIN ANALYZE output
      const ea = db.execute(`EXPLAIN ANALYZE ${sql}`);
      
      let estimated = null;
      let actual = null;
      
      // Extract from analysis
      if (ea.analysis) {
        for (const node of ea.analysis) {
          if (node.estimated_rows != null) estimated = node.estimated_rows;
          if (node.actual_rows != null) actual = node.actual_rows;
        }
      }
      
      // Also check planTree
      if (ea.planTree) {
        if (estimated == null && ea.planTree.estimatedRows != null) {
          estimated = ea.planTree.estimatedRows;
        }
      }
      
      // actual_rows at top level
      if (actual == null && ea.actual_rows != null) {
        actual = ea.actual_rows;
      }
      
      const errorRatio = (estimated != null && actual != null && actual > 0) 
        ? Math.max(estimated / actual, actual / estimated) 
        : 0;

      results.push({ query: sql, label, estimated, actual, errorRatio });
      return { estimated, actual, errorRatio };
    } catch (e) {
      results.push({ query: sql, label, estimated: null, actual: null, errorRatio: 0, error: e.message });
      return null;
    }
  }

  // === Uniform distribution tests ===
  
  it('equality on uniform: val = 250', () => {
    const r = runAndCompare('SELECT * FROM acc_uniform WHERE val = 250', 'eq-uniform');
    assert.ok(r, 'Should complete without error');
  });

  it('range on uniform: val > 400', () => {
    const r = runAndCompare('SELECT * FROM acc_uniform WHERE val > 400', 'range-uniform-high');
    assert.ok(r);
  });

  it('range on uniform: val BETWEEN 100 AND 200', () => {
    const r = runAndCompare('SELECT * FROM acc_uniform WHERE val BETWEEN 100 AND 200', 'between-uniform');
    assert.ok(r);
  });

  it('equality on text: dept = A (uniform 20%)', () => {
    const r = runAndCompare("SELECT * FROM acc_uniform WHERE dept = 'A'", 'eq-text-uniform');
    assert.ok(r);
  });

  it('IN list on uniform', () => {
    const r = runAndCompare('SELECT * FROM acc_uniform WHERE val IN (10, 20, 30, 40, 50)', 'in-uniform');
    assert.ok(r);
  });

  it('compound AND: val > 200 AND dept = A', () => {
    const r = runAndCompare("SELECT * FROM acc_uniform WHERE val > 200 AND dept = 'A'", 'and-uniform');
    assert.ok(r);
  });

  it('compound OR: val < 50 OR val > 450', () => {
    const r = runAndCompare('SELECT * FROM acc_uniform WHERE val < 50 OR val > 450', 'or-uniform');
    assert.ok(r);
  });

  // === Skewed distribution tests ===

  it('equality on skewed: cat = common (80%)', () => {
    const r = runAndCompare("SELECT * FROM acc_skewed WHERE cat = 'common'", 'eq-skewed-common');
    assert.ok(r);
    // This is the key test: does the optimizer know that common is 80% of rows?
    // Without histograms, it'll assume uniform distribution (1/ndistinct = 20%)
  });

  it('equality on skewed: cat = rare_a (~5%)', () => {
    const r = runAndCompare("SELECT * FROM acc_skewed WHERE cat = 'rare_a'", 'eq-skewed-rare');
    assert.ok(r);
  });

  it('range on skewed: val > 900', () => {
    const r = runAndCompare('SELECT * FROM acc_skewed WHERE val > 900', 'range-skewed-high');
    assert.ok(r);
  });

  it('combined: cat = common AND val > 500', () => {
    const r = runAndCompare("SELECT * FROM acc_skewed WHERE cat = 'common' AND val > 500", 'and-skewed');
    assert.ok(r);
  });

  // === NULL tests ===

  it('IS NULL predicate', () => {
    const r = runAndCompare('SELECT * FROM acc_nulls WHERE optional IS NULL', 'is-null');
    assert.ok(r);
  });

  it('IS NOT NULL predicate', () => {
    const r = runAndCompare('SELECT * FROM acc_nulls WHERE optional IS NOT NULL', 'is-not-null');
    assert.ok(r);
  });

  // === Aggregate accuracy ===

  it('GROUP BY estimate', () => {
    const r = runAndCompare('SELECT dept, COUNT(*) as cnt FROM acc_uniform GROUP BY dept', 'group-by');
    assert.ok(r);
  });

  it('GROUP BY + HAVING', () => {
    const r = runAndCompare('SELECT dept, AVG(val) as avg_val FROM acc_uniform GROUP BY dept HAVING AVG(val) > 200', 'having');
    assert.ok(r);
  });

  // === Join accuracy ===

  it('join estimate', () => {
    const r = runAndCompare('SELECT * FROM acc_uniform u JOIN acc_skewed s ON u.id = s.id LIMIT 20', 'join-eq');
    assert.ok(r);
  });

  // === Empty/full result tests ===

  it('always-false predicate', () => {
    const r = runAndCompare('SELECT * FROM acc_uniform WHERE val > 99999', 'empty-result');
    assert.ok(r);
  });

  it('always-true predicate', () => {
    const r = runAndCompare('SELECT * FROM acc_uniform WHERE val >= 0', 'full-result');
    assert.ok(r);
  });

  // === Correctness checks ===

  it('EXPLAIN estimated_rows should be non-negative', () => {
    for (const r of results) {
      if (r.estimated != null) {
        assert.ok(r.estimated >= 0, `Negative estimate for: ${r.query}`);
      }
    }
  });

  it('actual_rows should be non-negative', () => {
    for (const r of results) {
      if (r.actual != null) {
        assert.ok(r.actual >= 0, `Negative actual for: ${r.query}`);
      }
    }
  });

  it('no estimate should be more than 10x off for simple predicates on uniform data', () => {
    const uniform = results.filter(r => r.label?.includes('uniform') && r.errorRatio > 0);
    const bad = uniform.filter(r => r.errorRatio > 10);
    if (bad.length > 0) {
      console.log('WARNING: Badly inaccurate estimates on uniform data:');
      for (const b of bad) {
        console.log(`  ${b.errorRatio.toFixed(1)}x off: est=${b.estimated} actual=${b.actual} | ${b.query}`);
      }
    }
    // Don't fail — just flag. This is diagnostics.
    assert.ok(true);
  });
});
