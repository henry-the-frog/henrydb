// histogram-estimation.test.js — Histogram-based query estimation tests
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Histogram-based estimation', () => {
  let db;
  
  before(() => {
    db = new Database();
    db.execute('CREATE TABLE skewed (id INT, val INT, category TEXT)');
    for (let i = 1; i <= 200; i++) {
      const val = Math.floor((i / 200) * (i / 200) * 1000);
      const cat = i <= 100 ? 'A' : (i <= 180 ? 'B' : 'C');
      db.execute(`INSERT INTO skewed VALUES (${i}, ${val}, '${cat}')`);
    }
    db.execute('ANALYZE TABLE skewed');
  });

  function getEstimation(db, sql) {
    const e = db.execute('EXPLAIN ' + sql);
    if (e.plan) {
      for (const node of e.plan) {
        if (node.estimation_method) return node;
        if (node.filtered_estimate != null) return node;
      }
    }
    return null;
  }

  it('ANALYZE produces histograms for numeric columns', () => {
    const stats = db._tableStats.get('skewed');
    assert.ok(stats);
    assert.ok(stats.columns.val.histogram);
    assert.ok(stats.columns.val.histogram.length >= 10);
    assert.ok(stats.columns.id.histogram);
    assert.strictEqual(stats.columns.category.histogram, null);
  });

  it('histogram buckets cover the full range', () => {
    const stats = db._tableStats.get('skewed');
    const hist = stats.columns.val.histogram;
    assert.strictEqual(hist[0].lo, stats.columns.val.min);
    assert.strictEqual(hist[hist.length - 1].hi, stats.columns.val.max);
    const totalCount = hist.reduce((s, b) => s + b.count, 0);
    assert.strictEqual(totalCount, 200);
  });

  it('histogram buckets have valid ndv', () => {
    const stats = db._tableStats.get('skewed');
    for (const bucket of stats.columns.val.histogram) {
      assert.ok(bucket.ndv > 0, `ndv should be > 0, got ${bucket.ndv}`);
      assert.ok(bucket.ndv <= bucket.count, `ndv ${bucket.ndv} > count ${bucket.count}`);
    }
  });

  it('equality estimation uses histogram for numeric values', () => {
    const node = getEstimation(db, 'SELECT * FROM skewed WHERE val = 10');
    assert.ok(node, 'should get estimation node');
    assert.ok(node.estimation_method.includes('histogram_eq'), `method was: ${node.estimation_method}`);
  });

  it('range estimation uses histogram', () => {
    const node = getEstimation(db, 'SELECT * FROM skewed WHERE val > 500');
    assert.ok(node, 'should get estimation node');
    assert.ok(node.estimation_method.includes('histogram_range'), `method was: ${node.estimation_method}`);
  });

  it('histogram range estimate is accurate for skewed data', () => {
    const actual = db.execute('SELECT COUNT(*) as cnt FROM skewed WHERE val > 500').rows[0].cnt;
    const node = getEstimation(db, 'SELECT * FROM skewed WHERE val > 500');
    assert.ok(node);
    const estimated = node.filtered_estimate || node.estimated_rows;
    if (actual > 0) {
      const ratio = Math.max(estimated / actual, actual / estimated);
      assert.ok(ratio < 5, `ratio ${ratio.toFixed(2)} should be < 5 (est=${estimated}, actual=${actual})`);
    }
  });

  it('low-value equality estimates more rows than high-value for skewed data', () => {
    const lowNode = getEstimation(db, 'SELECT * FROM skewed WHERE val = 0');
    const highNode = getEstimation(db, 'SELECT * FROM skewed WHERE val = 950');
    assert.ok(lowNode);
    assert.ok(highNode);
    const lowEst = lowNode.filtered_estimate || lowNode.estimated_rows;
    const highEst = highNode.filtered_estimate || highNode.estimated_rows;
    assert.ok(lowEst >= highEst, `low estimate ${lowEst} should be >= high estimate ${highEst}`);
  });

  it('out-of-range equality estimates 1 row', () => {
    const node = getEstimation(db, 'SELECT * FROM skewed WHERE val = 9999');
    assert.ok(node);
    assert.ok(node.estimation_method.includes('out_of_range'));
    assert.strictEqual(node.filtered_estimate, 1);
  });

  it('uniform data histogram gives accurate equality estimate', () => {
    const db2 = new Database();
    db2.execute('CREATE TABLE uniform (id INT, val INT)');
    for (let i = 1; i <= 100; i++) {
      db2.execute(`INSERT INTO uniform VALUES (${i}, ${i})`);
    }
    db2.execute('ANALYZE TABLE uniform');
    const node = getEstimation(db2, 'SELECT * FROM uniform WHERE val = 50');
    assert.ok(node);
    const est = node.filtered_estimate || node.estimated_rows;
    assert.ok(est <= 3, `estimate ${est} should be <= 3 for uniform data`);
  });

  it('too few values — no histogram, falls back to selectivity', () => {
    const db2 = new Database();
    db2.execute('CREATE TABLE tiny (id INT, val INT)');
    for (let i = 1; i <= 5; i++) {
      db2.execute(`INSERT INTO tiny VALUES (${i}, ${i * 10})`);
    }
    db2.execute('ANALYZE TABLE tiny');
    const stats = db2._tableStats.get('tiny');
    assert.strictEqual(stats.columns.val.histogram, null);
    const node = getEstimation(db2, 'SELECT * FROM tiny WHERE val = 30');
    assert.ok(node);
    assert.ok(node.estimation_method.includes('selectivity'), `method was: ${node.estimation_method}`);
  });

  it('LT range uses histogram', () => {
    const node = getEstimation(db, 'SELECT * FROM skewed WHERE val < 100');
    assert.ok(node);
    assert.ok(node.estimation_method.includes('histogram'), `method was: ${node.estimation_method}`);
  });

  it('end-to-end accuracy: multiple range queries on skewed data', () => {
    const ranges = ['val < 50', 'val > 800', 'val >= 200 AND val <= 400'];
    for (const query of ranges) {
      const actual = db.execute(`SELECT COUNT(*) as cnt FROM skewed WHERE ${query}`).rows[0].cnt;
      const node = getEstimation(db, `SELECT * FROM skewed WHERE ${query}`);
      if (node && actual > 0) {
        const estimated = node.filtered_estimate || node.estimated_rows;
        const ratio = Math.max(estimated / actual, actual / estimated);
        assert.ok(ratio < 5, `query "${query}": ratio ${ratio.toFixed(2)} >= 5 (est=${estimated}, actual=${actual})`);
      }
    }
  });
});
