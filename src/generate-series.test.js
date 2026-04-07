// generate-series.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('GENERATE_SERIES', () => {
  it('generates ascending integer sequence', () => {
    const db = new Database();
    const r = db.execute('SELECT * FROM GENERATE_SERIES(1, 5)');
    assert.equal(r.rows.length, 5);
    assert.deepEqual(r.rows.map(r => r.value), [1, 2, 3, 4, 5]);
  });

  it('supports custom step', () => {
    const db = new Database();
    const r = db.execute('SELECT value FROM GENERATE_SERIES(0, 10, 3)');
    assert.deepEqual(r.rows.map(r => r.value), [0, 3, 6, 9]);
  });

  it('supports descending with negative step', () => {
    const db = new Database();
    const r = db.execute('SELECT value FROM GENERATE_SERIES(5, 1, -1)');
    assert.deepEqual(r.rows.map(r => r.value), [5, 4, 3, 2, 1]);
  });

  it('supports expressions in SELECT', () => {
    const db = new Database();
    const r = db.execute('SELECT value * value AS sq FROM GENERATE_SERIES(1, 4)');
    assert.deepEqual(r.rows.map(r => r.sq), [1, 4, 9, 16]);
  });

  it('supports WHERE clause', () => {
    const db = new Database();
    const r = db.execute('SELECT value FROM GENERATE_SERIES(1, 10) WHERE value > 7');
    assert.deepEqual(r.rows.map(r => r.value), [8, 9, 10]);
  });

  it('supports LIMIT', () => {
    const db = new Database();
    const r = db.execute('SELECT value FROM GENERATE_SERIES(1, 100) LIMIT 3');
    assert.equal(r.rows.length, 3);
    assert.deepEqual(r.rows.map(r => r.value), [1, 2, 3]);
  });

  it('empty range returns empty', () => {
    const db = new Database();
    const r = db.execute('SELECT * FROM GENERATE_SERIES(5, 1)');
    assert.equal(r.rows.length, 0);
  });
});
