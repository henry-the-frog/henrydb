// select-star-collision.test.js — SELECT * with column name collisions
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('SELECT * column name collision handling', () => {
  let db;
  
  function setup() {
    db = new Database();
    db.execute('CREATE TABLE a (id INT, name TEXT)');
    db.execute('CREATE TABLE b (id INT, name TEXT)');
    db.execute("INSERT INTO a VALUES (1, 'Alice'), (2, 'Bob')");
    db.execute("INSERT INTO b VALUES (1, 'Eng'), (3, 'HR')");
  }
  
  it('INNER JOIN * with same-name columns preserves both', () => {
    setup();
    const r = db.execute('SELECT * FROM a INNER JOIN b ON a.id = b.id');
    assert.equal(r.rows.length, 1);
    // id should merge (same value in equi-join)
    assert.equal(r.rows[0].id, 1);
    // name should be qualified (different values)
    assert.equal(r.rows[0]['a.name'], 'Alice');
    assert.equal(r.rows[0]['b.name'], 'Eng');
  });
  
  it('FULL JOIN * preserves all rows with correct values', () => {
    setup();
    const r = db.execute('SELECT * FROM a FULL JOIN b ON a.id = b.id');
    assert.equal(r.rows.length, 3);
    
    // Matched row: id=1
    const matched = r.rows.find(row => (row.id === 1 || row['a.id'] === 1));
    assert.ok(matched);
    assert.equal(matched['a.name'] || matched.name, 'Alice');
    
    // Unmatched left: id=2 (Bob)
    const bob = r.rows.find(row => (row.id === 2 || row['a.id'] === 2));
    assert.ok(bob, 'Bob should appear in FULL JOIN');
    assert.equal(bob.name || bob['a.name'], 'Bob');
    
    // Unmatched right: id=3 (HR)
    const hr = r.rows.find(row => (row.id === 3 || row['b.id'] === 3));
    assert.ok(hr, 'HR should appear in FULL JOIN');
    assert.equal(hr.name || hr['b.name'], 'HR');
  });
  
  it('LEFT JOIN * preserves unmatched left rows', () => {
    setup();
    const r = db.execute('SELECT * FROM a LEFT JOIN b ON a.id = b.id');
    assert.equal(r.rows.length, 2);
    const bob = r.rows.find(row => (row.id === 2 || row['a.id'] === 2));
    assert.ok(bob, 'Bob should appear in LEFT JOIN');
    assert.equal(bob.name || bob['a.name'], 'Bob');
  });
  
  it('JOIN USING merges USING columns', () => {
    const db2 = new Database();
    db2.execute('CREATE TABLE u1 (id INT, val TEXT)');
    db2.execute('CREATE TABLE u2 (id INT, data TEXT)');
    db2.execute("INSERT INTO u1 VALUES (1, 'a'), (2, 'b')");
    db2.execute("INSERT INTO u2 VALUES (2, 'x'), (3, 'y')");
    const r = db2.execute('SELECT * FROM u1 JOIN u2 USING (id)');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].id, 2);
    assert.equal(r.rows[0].val, 'b');
    assert.equal(r.rows[0].data, 'x');
  });
  
  it('no collision with different column names', () => {
    const db2 = new Database();
    db2.execute('CREATE TABLE emp (id INT, name TEXT, dept_id INT)');
    db2.execute('CREATE TABLE dept (did INT, dname TEXT)');
    db2.execute("INSERT INTO emp VALUES (1, 'Alice', 10)");
    db2.execute("INSERT INTO dept VALUES (10, 'Eng')");
    const r = db2.execute('SELECT * FROM emp JOIN dept ON emp.dept_id = dept.did');
    assert.equal(r.rows.length, 1);
    // No collision: all columns should be unqualified
    assert.equal(r.rows[0].id, 1);
    assert.equal(r.rows[0].name, 'Alice');
    assert.equal(r.rows[0].dname, 'Eng');
  });
  
  it('single table SELECT * unchanged', () => {
    setup();
    const r = db.execute('SELECT * FROM a');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].id, 1);
    assert.equal(r.rows[0].name, 'Alice');
  });
});
