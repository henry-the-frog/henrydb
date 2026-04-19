import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Nested Aggregate Validation (2026-04-19)', () => {
  let db;

  function setup() {
    db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT, grp TEXT)');
    db.execute("INSERT INTO t VALUES (1,10,'a'),(2,20,'a'),(3,30,'b'),(4,40,'b')");
    return db;
  }

  it('SUM(COUNT(*)) throws error', () => {
    setup();
    assert.throws(() => db.execute('SELECT SUM(COUNT(*)) FROM t'),
      /nested/i);
  });

  it('AVG(SUM(val)) throws error', () => {
    setup();
    assert.throws(() => db.execute('SELECT AVG(SUM(val)) FROM t'),
      /nested/i);
  });

  it('MAX(MIN(val)) throws error', () => {
    setup();
    assert.throws(() => db.execute('SELECT MAX(MIN(val)) FROM t'),
      /nested/i);
  });

  it('COUNT(SUM(val)) throws error with GROUP BY', () => {
    setup();
    assert.throws(() => db.execute('SELECT grp, COUNT(SUM(val)) FROM t GROUP BY grp'),
      /nested/i);
  });

  it('non-nested SUM still works', () => {
    setup();
    const r = db.execute('SELECT SUM(val) AS total FROM t');
    assert.equal(r.rows[0].total, 100);
  });

  it('COALESCE(SUM(val), 0) still works', () => {
    setup();
    const r = db.execute('SELECT COALESCE(SUM(val), 0) AS total FROM t');
    assert.equal(r.rows[0].total, 100);
  });

  it('SUM(val * 2) still works (expression arg, not nested aggregate)', () => {
    setup();
    const r = db.execute('SELECT SUM(val * 2) AS total FROM t');
    assert.equal(r.rows[0].total, 200);
  });

  it('CAST(SUM(val) AS FLOAT) / COUNT(*) still works', () => {
    setup();
    const r = db.execute('SELECT CAST(SUM(val) AS FLOAT) / COUNT(*) AS avg FROM t');
    assert.equal(r.rows[0].avg, 25);
  });
});
