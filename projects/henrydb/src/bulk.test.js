// bulk.test.js — Bulk operations tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Bulk Insert', () => {
  it('inserts multiple rows efficiently', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, val INT)');
    
    const rows = [];
    for (let i = 0; i < 1000; i++) {
      rows.push([i, `name_${i}`, i * 10]);
    }
    
    const result = db.bulkInsert('t', rows);
    assert.equal(result.count, 1000);
    assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM t').rows[0].cnt, 1000);
  });

  it('bulk insert is faster than individual INSERTs', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, val INT)');
    db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, val INT)');
    
    const rows = Array.from({ length: 500 }, (_, i) => [i, i * 2]);
    
    // Bulk insert
    const start1 = performance.now();
    db.bulkInsert('t1', rows);
    const bulkTime = performance.now() - start1;
    
    // Individual inserts
    const start2 = performance.now();
    for (const [id, val] of rows) {
      db.execute(`INSERT INTO t2 VALUES (${id}, ${val})`);
    }
    const individualTime = performance.now() - start2;
    
    // Bulk should be faster (no SQL parsing per row)
    assert.ok(bulkTime < individualTime, 
      `Bulk: ${bulkTime.toFixed(1)}ms, Individual: ${individualTime.toFixed(1)}ms`);
  });

  it('data is queryable after bulk insert', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.bulkInsert('t', [[1, 10], [2, 20], [3, 30]]);
    
    assert.equal(db.execute('SELECT SUM(val) AS total FROM t').rows[0].total, 60);
    assert.equal(db.execute('SELECT val FROM t WHERE id = 2').rows[0].val, 20);
  });
});

describe('Paginated Queries', () => {
  function createPaginatedDB() {
    const db = new Database();
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, name TEXT)');
    db.bulkInsert('items', Array.from({ length: 50 }, (_, i) => [i + 1, `item_${i + 1}`]));
    return db;
  }

  it('returns correct page of results', () => {
    const db = createPaginatedDB();
    const r = db.executePaginated('SELECT * FROM items ORDER BY id', 1, 10);
    
    assert.equal(r.rows.length, 10);
    assert.equal(r.rows[0].id, 1);
    assert.equal(r.rows[9].id, 10);
  });

  it('pagination metadata is correct', () => {
    const db = createPaginatedDB();
    const r = db.executePaginated('SELECT * FROM items ORDER BY id', 2, 10);
    
    assert.equal(r.pagination.page, 2);
    assert.equal(r.pagination.totalRows, 50);
    assert.equal(r.pagination.totalPages, 5);
    assert.equal(r.pagination.hasNext, true);
    assert.equal(r.pagination.hasPrev, true);
  });

  it('last page has correct count', () => {
    const db = createPaginatedDB();
    const r = db.executePaginated('SELECT * FROM items ORDER BY id', 5, 10);
    
    assert.equal(r.rows.length, 10);
    assert.equal(r.pagination.hasNext, false);
    assert.equal(r.rows[0].id, 41);
  });

  it('page beyond range returns empty', () => {
    const db = createPaginatedDB();
    const r = db.executePaginated('SELECT * FROM items ORDER BY id', 10, 10);
    
    assert.equal(r.rows.length, 0);
  });

  it('custom page size', () => {
    const db = createPaginatedDB();
    const r = db.executePaginated('SELECT * FROM items ORDER BY id', 1, 25);
    
    assert.equal(r.rows.length, 25);
    assert.equal(r.pagination.totalPages, 2);
  });
});
