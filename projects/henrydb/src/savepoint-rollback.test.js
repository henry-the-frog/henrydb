// savepoint-rollback.test.js — Savepoint ROLLBACK TO correctness tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Savepoint ROLLBACK TO', () => {
  it('basic: rollback restores INSERT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'alice')");
    db.execute('SAVEPOINT sp1');
    db.execute("INSERT INTO t VALUES (2, 'bob')");
    
    // Before rollback: 2 rows
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t').rows[0].c, 2);
    
    db.execute('ROLLBACK TO sp1');
    
    // After rollback: back to 1 row
    const r = db.execute('SELECT * FROM t');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].id, 1);
    assert.equal(r.rows[0].name, 'alice');
  });

  it('rollback restores DELETE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1),(2),(3)');
    db.execute('SAVEPOINT sp1');
    db.execute('DELETE FROM t WHERE id = 2');
    
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t').rows[0].c, 2);
    
    db.execute('ROLLBACK TO sp1');
    
    const r = db.execute('SELECT id FROM t ORDER BY id');
    assert.deepEqual(r.rows.map(r => r.id), [1, 2, 3]);
  });

  it('rollback restores UPDATE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'original')");
    db.execute('SAVEPOINT sp1');
    db.execute("UPDATE t SET val = 'modified' WHERE id = 1");
    
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 'modified');
    
    db.execute('ROLLBACK TO sp1');
    
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 'original');
  });

  it('nested savepoints: rollback to outer restores inner changes', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('SAVEPOINT outer');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('SAVEPOINT inner');
    db.execute('INSERT INTO t VALUES (3)');
    
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t').rows[0].c, 3);
    
    // Rollback to outer — should undo both inner and its changes
    db.execute('ROLLBACK TO outer');
    
    const r = db.execute('SELECT id FROM t ORDER BY id');
    assert.deepEqual(r.rows.map(r => r.id), [1]);
  });

  it('rollback to inner keeps outer changes', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('SAVEPOINT outer');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('SAVEPOINT inner');
    db.execute('INSERT INTO t VALUES (3)');
    
    // Rollback to inner — keeps row 2 from outer
    db.execute('ROLLBACK TO inner');
    
    const r = db.execute('SELECT id FROM t ORDER BY id');
    assert.deepEqual(r.rows.map(r => r.id), [1, 2]);
  });

  it('can continue working after rollback', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('SAVEPOINT sp1');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('ROLLBACK TO sp1');
    
    // Insert new data after rollback
    db.execute('INSERT INTO t VALUES (3)');
    
    const r = db.execute('SELECT id FROM t ORDER BY id');
    assert.deepEqual(r.rows.map(r => r.id), [1, 3]);
  });

  it('rollback and re-savepoint', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('SAVEPOINT sp1');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('ROLLBACK TO sp1');
    
    // Create new savepoint with same name
    db.execute('SAVEPOINT sp1');
    db.execute('INSERT INTO t VALUES (3)');
    db.execute('ROLLBACK TO sp1');
    
    const r = db.execute('SELECT id FROM t ORDER BY id');
    assert.deepEqual(r.rows.map(r => r.id), [1]);
  });

  it('multiple tables', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT)');
    db.execute('CREATE TABLE t2 (val TEXT)');
    db.execute('INSERT INTO t1 VALUES (1)');
    db.execute("INSERT INTO t2 VALUES ('a')");
    db.execute('SAVEPOINT sp1');
    db.execute('INSERT INTO t1 VALUES (2)');
    db.execute("INSERT INTO t2 VALUES ('b')");
    
    db.execute('ROLLBACK TO sp1');
    
    assert.deepEqual(db.execute('SELECT id FROM t1').rows.map(r => r.id), [1]);
    assert.deepEqual(db.execute('SELECT val FROM t2').rows.map(r => r.val), ['a']);
  });

  it('rollback to nonexistent savepoint throws', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    assert.throws(() => db.execute('ROLLBACK TO nonexistent'), /not found/i);
  });

  it('RELEASE removes savepoint', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('SAVEPOINT sp1');
    db.execute('INSERT INTO t VALUES (2)');
    db.execute('RELEASE sp1');
    
    // Can't rollback to released savepoint
    assert.throws(() => db.execute('ROLLBACK TO sp1'), /not found/i);
    
    // But the data is still there
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t').rows[0].c, 2);
  });

  it('rollback with NULL values preserved', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    db.execute('SAVEPOINT sp1');
    db.execute("UPDATE t SET val = 'set' WHERE id = 1");
    
    db.execute('ROLLBACK TO sp1');
    
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, null);
  });

  it('rollback preserves row count correctly', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    for (let i = 1; i <= 100; i++) {
      db.execute(`INSERT INTO t VALUES (${i})`);
    }
    db.execute('SAVEPOINT sp1');
    db.execute('DELETE FROM t WHERE id > 50');
    
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t').rows[0].c, 50);
    
    db.execute('ROLLBACK TO sp1');
    
    assert.equal(db.execute('SELECT COUNT(*) as c FROM t').rows[0].c, 100);
  });
});
