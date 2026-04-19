// advanced-features.test.js — LATERAL, generate_series, sequences, CTAS, etc.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('generate_series', () => {
  it('basic range', () => {
    const db = new Database();
    const r = db.execute('SELECT * FROM generate_series(1, 5)');
    assert.equal(r.rows.length, 5);
    assert.deepStrictEqual(r.rows.map(r => r.value), [1, 2, 3, 4, 5]);
  });

  it('with step', () => {
    const db = new Database();
    const r = db.execute('SELECT * FROM generate_series(0, 100, 25)');
    assert.deepStrictEqual(r.rows.map(r => r.value), [0, 25, 50, 75, 100]);
  });

  it('single value', () => {
    const db = new Database();
    const r = db.execute('SELECT * FROM generate_series(5, 5)');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].value, 5);
  });
});

describe('LATERAL Join', () => {
  it('correlates with outer table', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, n INT)');
    db.execute('INSERT INTO t VALUES (1, 3), (2, 2)');
    
    const r = db.execute('SELECT t.id, s.value FROM t, LATERAL (SELECT * FROM generate_series(1, t.n)) s ORDER BY t.id, s.value');
    assert.equal(r.rows.length, 5); // 3 + 2
    assert.deepStrictEqual(r.rows.filter(r => r.id === 1).map(r => r.value), [1, 2, 3]);
    assert.deepStrictEqual(r.rows.filter(r => r.id === 2).map(r => r.value), [1, 2]);
  });

  it('LATERAL with subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE dept (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE emp (id INT PRIMARY KEY, dept_id INT, salary INT)');
    db.execute("INSERT INTO dept VALUES (1, 'Eng'), (2, 'Sales')");
    db.execute('INSERT INTO emp VALUES (1, 1, 80000), (2, 1, 90000), (3, 2, 60000)');
    
    const r = db.execute(`
      SELECT d.name, top.salary
      FROM dept d, LATERAL (
        SELECT e.salary FROM emp e WHERE e.dept_id = d.id ORDER BY e.salary DESC LIMIT 1
      ) top
    `);
    assert.equal(r.rows.length, 2);
  });
});

describe('CREATE TABLE AS SELECT', () => {
  it('creates table from query', () => {
    const db = new Database();
    db.execute('CREATE TABLE src (id INT PRIMARY KEY, val TEXT, num INT)');
    db.execute("INSERT INTO src VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)");
    
    db.execute("CREATE TABLE dst AS SELECT id, val FROM src WHERE num > 15");
    const r = db.execute('SELECT * FROM dst ORDER BY id');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].val, 'b');
  });

  it('CTAS with aggregation', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, amount INT)');
    db.execute("INSERT INTO sales VALUES (1, 'E', 100), (2, 'W', 200), (3, 'E', 150)");
    
    db.execute('CREATE TABLE summary AS SELECT region, SUM(amount) as total FROM sales GROUP BY region');
    const r = db.execute('SELECT * FROM summary ORDER BY region');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].total, 250);
  });
});

describe('Sequences', () => {
  it('CREATE SEQUENCE and NEXTVAL', () => {
    const db = new Database();
    db.execute('CREATE SEQUENCE seq START 1');
    assert.equal(db.execute("SELECT NEXTVAL('seq') as n").rows[0].n, 1);
    assert.equal(db.execute("SELECT NEXTVAL('seq') as n").rows[0].n, 2);
    assert.equal(db.execute("SELECT NEXTVAL('seq') as n").rows[0].n, 3);
  });

  it('CURRVAL returns last value', () => {
    const db = new Database();
    db.execute('CREATE SEQUENCE seq START 10');
    db.execute("SELECT NEXTVAL('seq')");
    assert.equal(db.execute("SELECT CURRVAL('seq') as n").rows[0].n, 10);
  });

  it('sequence as DEFAULT', () => {
    const db = new Database();
    db.execute('CREATE SEQUENCE id_seq START 1');
    db.execute("CREATE TABLE t (id INT PRIMARY KEY DEFAULT NEXTVAL('id_seq'), name TEXT)");
    db.execute("INSERT INTO t (name) VALUES ('first')");
    db.execute("INSERT INTO t (name) VALUES ('second')");
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows[0].id, 1);
    assert.equal(r.rows[1].id, 2);
  });
});

describe('VALUES Clause', () => {
  it('standalone VALUES', () => {
    const db = new Database();
    const r = db.execute("VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    assert.equal(r.rows.length, 3);
  });
});

describe('TRUNCATE', () => {
  it('removes all rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)');
    db.execute('TRUNCATE TABLE t');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM t').rows[0].cnt, 0);
  });

  it('table still usable after TRUNCATE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('TRUNCATE TABLE t');
    db.execute('INSERT INTO t VALUES (2, 20)');
    assert.equal(db.execute('SELECT * FROM t').rows[0].id, 2);
  });
});
