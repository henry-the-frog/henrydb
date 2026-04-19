import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Performance Benchmarks (2026-04-19)', () => {
  let db;
  const N = 500;

  before(() => {
    db = new Database();
    db.execute('CREATE TABLE bench (id INT PRIMARY KEY, val INT, grp TEXT, data TEXT)');
    db.execute('CREATE INDEX idx_val ON bench (val)');
    db.execute('CREATE INDEX idx_grp ON bench (grp)');
    
    for (let i = 1; i <= N; i++) {
      db.execute(`INSERT INTO bench VALUES (${i}, ${Math.floor(Math.random() * 1000)}, '${['a','b','c','d','e'][i%5]}', 'data${i}')`);
    }
  });

  it('sequential scan: SELECT * (should be fast)', () => {
    const start = performance.now();
    const r = db.execute('SELECT * FROM bench');
    const ms = performance.now() - start;
    assert.equal(r.rows.length, N);
    assert.ok(ms < 100, `Sequential scan took ${ms}ms, expected < 100ms`);
  });

  it('index scan: WHERE with indexed column', () => {
    const start = performance.now();
    const r = db.execute('SELECT * FROM bench WHERE val > 500');
    const ms = performance.now() - start;
    assert.ok(r.rows.length > 0);
    assert.ok(ms < 100, `Index scan took ${ms}ms`);
  });

  it('aggregate: GROUP BY', () => {
    const start = performance.now();
    const r = db.execute('SELECT grp, COUNT(*) AS cnt, AVG(val) AS avg FROM bench GROUP BY grp');
    const ms = performance.now() - start;
    assert.equal(r.rows.length, 5);
    assert.ok(ms < 200, `GROUP BY took ${ms}ms`);
  });

  it('window function: ROW_NUMBER OVER', () => {
    const start = performance.now();
    const r = db.execute('SELECT id, val, ROW_NUMBER() OVER (ORDER BY val) AS rn FROM bench');
    const ms = performance.now() - start;
    assert.equal(r.rows.length, N);
    assert.ok(ms < 500, `Window function took ${ms}ms`);
  });

  it('CTE with aggregation', () => {
    const start = performance.now();
    const r = db.execute(`
      WITH stats AS (
        SELECT grp, SUM(val) AS total, COUNT(*) AS cnt
        FROM bench GROUP BY grp
      )
      SELECT grp, total, CAST(total AS FLOAT) / cnt AS avg
      FROM stats ORDER BY total DESC
    `);
    const ms = performance.now() - start;
    assert.equal(r.rows.length, 5);
    assert.ok(ms < 200, `CTE took ${ms}ms`);
  });

  it('multi-row INSERT (batch performance)', () => {
    db.execute('CREATE TABLE batch_test (id INT, val INT)');
    const values = [];
    for (let i = 0; i < 100; i++) values.push(`(${i}, ${i * 10})`);
    
    const start = performance.now();
    db.execute(`INSERT INTO batch_test VALUES ${values.join(',')}`);
    const ms = performance.now() - start;
    
    assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM batch_test').rows[0].cnt, 100);
    assert.ok(ms < 100, `Batch INSERT took ${ms}ms`);
  });
});
