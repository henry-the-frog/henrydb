// benchmark.test.js — Performance benchmarks for HenryDB
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Performance Benchmarks', () => {
  it('bulk INSERT: 1,000 rows < 5s', () => {
    const db = new Database();
    db.execute('CREATE TABLE bench (id INT PRIMARY KEY, name TEXT, val INT)');
    const start = performance.now();
    for (let i = 0; i < 1000; i++) {
      db.execute(`INSERT INTO bench VALUES (${i}, 'row${i}', ${i * 7})`);
    }
    const elapsed = performance.now() - start;
    assert.ok(elapsed < 5000, `1K inserts took ${elapsed.toFixed(0)}ms`);
    assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM bench').rows[0].cnt, 1000);
  });

  it('point lookup with PK index: 100 lookups < 2s', () => {
    const db = new Database();
    db.execute('CREATE TABLE bench (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 1000; i++) db.execute(`INSERT INTO bench VALUES (${i}, ${i * 3})`);

    const start = performance.now();
    for (let i = 0; i < 100; i++) {
      const id = Math.floor(Math.random() * 1000);
      db.execute(`SELECT val FROM bench WHERE id = ${id}`);
    }
    const elapsed = performance.now() - start;
    assert.ok(elapsed < 2000, `100 lookups took ${elapsed.toFixed(0)}ms`);
  });

  it('aggregate query on 1K rows < 500ms', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, amount INT)');
    for (let i = 0; i < 1000; i++) {
      db.execute(`INSERT INTO sales VALUES (${i}, '${['East', 'West', 'North', 'South'][i % 4]}', ${100 + i % 1000})`);
    }

    const start = performance.now();
    const r = db.execute('SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales GROUP BY region');
    const elapsed = performance.now() - start;
    assert.ok(elapsed < 500, `Aggregate on 1K took ${elapsed.toFixed(0)}ms`);
    assert.equal(r.rows.length, 4);
  });

  it('JOIN on 200x200 rows < 10s', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT PRIMARY KEY, val TEXT)');
    db.execute('CREATE TABLE b (id INT PRIMARY KEY, a_id INT, data TEXT)');
    for (let i = 0; i < 200; i++) db.execute(`INSERT INTO a VALUES (${i}, 'a${i}')`);
    for (let i = 0; i < 200; i++) db.execute(`INSERT INTO b VALUES (${i}, ${i % 200}, 'b${i}')`);

    const start = performance.now();
    const r = db.execute('SELECT COUNT(*) AS cnt FROM a JOIN b ON a.id = b.a_id');
    const elapsed = performance.now() - start;
    assert.ok(elapsed < 10000, `200x200 join took ${elapsed.toFixed(0)}ms`);
    assert.equal(r.rows[0].cnt, 200);
  });

  it('window function on 500 rows < 5s', () => {
    const db = new Database();
    db.execute('CREATE TABLE bench (id INT PRIMARY KEY, dept TEXT, val INT)');
    for (let i = 0; i < 500; i++) {
      db.execute(`INSERT INTO bench VALUES (${i}, 'dept${i % 10}', ${i})`);
    }

    const start = performance.now();
    const r = db.execute('SELECT id, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY val DESC) AS rn FROM bench');
    const elapsed = performance.now() - start;
    assert.ok(elapsed < 5000, `Window on 500 took ${elapsed.toFixed(0)}ms`);
    assert.equal(r.rows.length, 500);
  });

  it('prepared statement 500 executions < 10s', () => {
    const db = new Database();
    db.execute('CREATE TABLE bench (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 500; i++) db.execute(`INSERT INTO bench VALUES (${i}, ${i})`);

    const stmt = db.prepare('SELECT val FROM bench WHERE id = $1');
    const start = performance.now();
    for (let i = 0; i < 500; i++) {
      stmt.execute([i % 500]);
    }
    const elapsed = performance.now() - start;
    assert.ok(elapsed < 10000, `500 prepared took ${elapsed.toFixed(0)}ms`);
  });
});
