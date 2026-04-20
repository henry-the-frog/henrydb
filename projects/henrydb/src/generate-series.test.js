// generate-series.test.js — GENERATE_SERIES function tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('GENERATE_SERIES', () => {
  it('basic ascending', () => {
    const db = new Database();
    const r = db.execute('SELECT value FROM generate_series(1, 5)');
    assert.deepEqual(r.rows.map(r => r.value), [1, 2, 3, 4, 5]);
  });

  it('with step', () => {
    const db = new Database();
    const r = db.execute('SELECT value FROM generate_series(0, 10, 2)');
    assert.deepEqual(r.rows.map(r => r.value), [0, 2, 4, 6, 8, 10]);
  });

  it('descending with negative step', () => {
    const db = new Database();
    const r = db.execute('SELECT value FROM generate_series(5, 1, -1)');
    assert.deepEqual(r.rows.map(r => r.value), [5, 4, 3, 2, 1]);
  });

  it('single value (start = end)', () => {
    const db = new Database();
    const r = db.execute('SELECT value FROM generate_series(1, 1)');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].value, 1);
  });

  it('empty series (start > end, positive step)', () => {
    const db = new Database();
    const r = db.execute('SELECT value FROM generate_series(5, 1)');
    assert.equal(r.rows.length, 0);
  });

  it('with WHERE filter', () => {
    const db = new Database();
    const r = db.execute('SELECT value FROM generate_series(1, 10) WHERE value % 3 = 0');
    assert.deepEqual(r.rows.map(r => r.value), [3, 6, 9]);
  });

  it('with window function', () => {
    const db = new Database();
    const r = db.execute(`
      SELECT value, SUM(value) OVER (ORDER BY value) as running_sum
      FROM generate_series(1, 5)
    `);
    assert.equal(r.rows[4].running_sum, 15); // 1+2+3+4+5
    assert.equal(r.rows[0].running_sum, 1);
    assert.equal(r.rows[2].running_sum, 6); // 1+2+3
  });

  it('with aggregate', () => {
    const db = new Database();
    const r = db.execute('SELECT SUM(value) as total, COUNT(*) as cnt FROM generate_series(1, 100)');
    assert.equal(r.rows[0].total, 5050);
    assert.equal(r.rows[0].cnt, 100);
  });

  it('in subquery', () => {
    const db = new Database();
    const r = db.execute(`
      SELECT * FROM generate_series(1, 5) s
      WHERE s.value IN (SELECT value FROM generate_series(3, 7))
    `);
    assert.deepEqual(r.rows.map(r => r.value), [3, 4, 5]);
  });

  it('generate_series result can be queried', () => {
    const db = new Database();
    const r = db.execute(`
      SELECT value, value * value as squared
      FROM generate_series(1, 5)
    `);
    assert.equal(r.rows.length, 5);
    assert.equal(r.rows[2].squared, 9); // 3*3
  });
});
