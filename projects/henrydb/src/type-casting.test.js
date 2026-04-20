// type-casting.test.js — Type casting and coercion tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Type Casting', () => {
  it('CAST string to INT', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT CAST('42' AS INT) as r").rows[0].r, 42);
  });

  it('CAST float to INT (truncates)', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT CAST(3.99 AS INT) as r').rows[0].r, 3);
  });

  it('CAST int to TEXT', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT CAST(42 AS TEXT) as r').rows[0].r, '42');
  });

  it(':: shorthand syntax', () => {
    const db = new Database();
    assert.equal(db.execute("SELECT '42'::INT as r").rows[0].r, 42);
    assert.equal(db.execute("SELECT 42::TEXT as r").rows[0].r, '42');
  });

  it('implicit coercion in comparisons', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (val TEXT)');
    db.execute("INSERT INTO t VALUES ('1'),('2'),('10'),('20')");
    // Comparing text '10' > '2' lexicographically: '10' < '2' 
    // But with CAST: CAST('10' AS INT) > CAST('2' AS INT)
    const r = db.execute("SELECT val FROM t WHERE CAST(val AS INT) > 5 ORDER BY CAST(val AS INT)");
    assert.deepEqual(r.rows.map(r => r.val), ['10', '20']);
  });

  it('CAST NULL returns NULL', () => {
    const db = new Database();
    assert.equal(db.execute('SELECT CAST(NULL AS INT) as r').rows[0].r, null);
  });

  it('CAST boolean-like values', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (active BOOLEAN)');
    db.execute('INSERT INTO t VALUES (true),(false)');
    const r = db.execute('SELECT * FROM t WHERE active = true');
    assert.equal(r.rows.length, 1);
  });
});
