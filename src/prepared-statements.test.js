// prepared-statements.test.js — Tests for PREPARE/EXECUTE/DEALLOCATE
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Prepared Statements', () => {
  it('PREPARE and EXECUTE SELECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25)");
    
    db.execute('PREPARE get_user AS SELECT * FROM users WHERE id = $1');
    
    const r1 = db.execute('EXECUTE get_user (1)');
    assert.equal(r1.rows.length, 1);
    assert.equal(r1.rows[0].name, 'Alice');
    
    const r2 = db.execute('EXECUTE get_user (2)');
    assert.equal(r2.rows.length, 1);
    assert.equal(r2.rows[0].name, 'Bob');
    
    const r3 = db.execute('EXECUTE get_user (99)');
    assert.equal(r3.rows.length, 0);
  });

  it('PREPARE and EXECUTE INSERT', () => {
    const db = new Database();
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, name TEXT)');
    db.execute('PREPARE add_item AS INSERT INTO items VALUES ( $1 , $2 )');
    
    db.execute("EXECUTE add_item (1, 'Widget')");
    db.execute("EXECUTE add_item (2, 'Gadget')");
    
    const r = db.execute('SELECT * FROM items ORDER BY id');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'Widget');
    assert.equal(r.rows[1].name, 'Gadget');
  });

  it('PREPARE and EXECUTE UPDATE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')");
    
    db.execute('PREPARE upd AS UPDATE t SET val = $2 WHERE id = $1');
    db.execute("EXECUTE upd (1, 'updated')");
    
    const r = db.execute('SELECT * FROM t WHERE id = 1');
    assert.equal(r.rows[0].val, 'updated');
  });

  it('PREPARE and EXECUTE DELETE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    
    db.execute('PREPARE del AS DELETE FROM t WHERE id = $1');
    db.execute('EXECUTE del (2)');
    
    const r = db.execute('SELECT COUNT(*) as cnt FROM t');
    assert.equal(r.rows[0].cnt, 2);
  });

  it('multiple parameters', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT)');
    db.execute('INSERT INTO t VALUES (1, 10, 20, 30), (2, 40, 50, 60)');
    
    db.execute('PREPARE q AS SELECT * FROM t WHERE a >= $1 AND b <= $2');
    const r = db.execute('EXECUTE q (10, 25)');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].id, 1);
  });

  it('DEALLOCATE removes statement', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('PREPARE q AS SELECT * FROM t');
    db.execute('DEALLOCATE q');
    
    assert.throws(() => db.execute('EXECUTE q ()'), /does not exist/);
  });

  it('DEALLOCATE ALL removes all statements', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('PREPARE q1 AS SELECT * FROM t');
    db.execute('PREPARE q2 AS SELECT * FROM t');
    db.execute('DEALLOCATE ALL');
    
    assert.throws(() => db.execute('EXECUTE q1 ()'), /does not exist/);
    assert.throws(() => db.execute('EXECUTE q2 ()'), /does not exist/);
  });

  it('duplicate PREPARE name throws', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('PREPARE q AS SELECT * FROM t');
    assert.throws(() => db.execute('PREPARE q AS SELECT * FROM t'), /already exists/);
  });

  it('EXECUTE nonexistent throws', () => {
    const db = new Database();
    assert.throws(() => db.execute('EXECUTE nonexistent (1)'), /does not exist/);
  });

  it('DEALLOCATE nonexistent throws', () => {
    const db = new Database();
    assert.throws(() => db.execute('DEALLOCATE nonexistent'), /does not exist/);
  });

  it('reuse same prepared statement many times', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 20; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    db.execute('PREPARE q AS SELECT val FROM t WHERE id = $1');
    for (let i = 0; i < 20; i++) {
      const r = db.execute(`EXECUTE q (${i})`);
      assert.equal(r.rows[0].val, i * 10);
    }
  });

  it('string parameters with quotes', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'test')");
    
    db.execute('PREPARE q AS SELECT * FROM t WHERE name = $1');
    const r = db.execute("EXECUTE q ('test')");
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'test');
  });

  it('PREPARE with parameter types (ignored but parsed)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('PREPARE q (INT) AS SELECT * FROM t WHERE id = $1');
    const r = db.execute('EXECUTE q (42)');
    assert.equal(r.rows.length, 0); // No data, but no error
  });
});
