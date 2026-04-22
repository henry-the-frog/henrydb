// right-full-join.test.js — E2E tests for RIGHT and FULL outer joins
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('RIGHT JOIN', () => {
  let db;
  
  function setup() {
    db = new Database();
    db.execute('CREATE TABLE emp (id INT, name TEXT, dept_id INT)');
    db.execute('CREATE TABLE dept (id INT, dname TEXT)');
    db.execute("INSERT INTO emp VALUES (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Charlie', 30)");
    db.execute("INSERT INTO dept VALUES (10, 'Eng'), (20, 'Sales'), (40, 'HR')");
  }
  
  it('includes all right table rows', () => {
    setup();
    const r = db.execute('SELECT e.name, d.dname FROM emp e RIGHT JOIN dept d ON e.dept_id = d.id');
    assert.equal(r.rows.length, 3); // Alice+Eng, Bob+Sales, null+HR
    assert.ok(r.rows.find(row => row.dname === 'HR'));
  });
  
  it('unmatched right rows have null left columns', () => {
    setup();
    const r = db.execute('SELECT e.name, d.dname FROM emp e RIGHT JOIN dept d ON e.dept_id = d.id ORDER BY d.dname');
    const hr = r.rows.find(row => row.dname === 'HR');
    assert.equal(hr.name, null);
  });
  
  it('excludes unmatched left rows', () => {
    setup();
    const r = db.execute('SELECT e.name, d.dname FROM emp e RIGHT JOIN dept d ON e.dept_id = d.id');
    assert.equal(r.rows.find(row => row.name === 'Charlie'), undefined);
  });
  
  it('with WHERE clause', () => {
    setup();
    const r = db.execute("SELECT e.name, d.dname FROM emp e RIGHT JOIN dept d ON e.dept_id = d.id WHERE d.dname != 'HR'");
    assert.equal(r.rows.length, 2);
  });
  
  it('with ORDER BY', () => {
    setup();
    const r = db.execute('SELECT d.dname FROM emp e RIGHT JOIN dept d ON e.dept_id = d.id ORDER BY d.dname');
    assert.deepEqual(r.rows.map(r => r.dname), ['Eng', 'HR', 'Sales']);
  });
});

describe('FULL JOIN', () => {
  let db;
  
  function setup() {
    db = new Database();
    db.execute('CREATE TABLE emp (id INT, name TEXT, dept_id INT)');
    db.execute('CREATE TABLE dept (id INT, dname TEXT)');
    db.execute("INSERT INTO emp VALUES (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Charlie', 30)");
    db.execute("INSERT INTO dept VALUES (10, 'Eng'), (20, 'Sales'), (40, 'HR')");
  }
  
  it('includes all rows from both tables', () => {
    setup();
    const r = db.execute('SELECT e.name, d.dname FROM emp e FULL JOIN dept d ON e.dept_id = d.id');
    assert.equal(r.rows.length, 4); // Alice+Eng, Bob+Sales, Charlie+null, null+HR
  });
  
  it('unmatched left rows have null right columns', () => {
    setup();
    const r = db.execute('SELECT e.name, d.dname FROM emp e FULL JOIN dept d ON e.dept_id = d.id');
    const charlie = r.rows.find(row => row.name === 'Charlie');
    assert.ok(charlie);
    assert.equal(charlie.dname, null);
  });
  
  it('unmatched right rows have null left columns', () => {
    setup();
    const r = db.execute('SELECT e.name, d.dname FROM emp e FULL JOIN dept d ON e.dept_id = d.id');
    const hr = r.rows.find(row => row.dname === 'HR');
    assert.ok(hr);
    assert.equal(hr.name, null);
  });
  
  it('with COALESCE for merged output', () => {
    setup();
    const r = db.execute(`
      SELECT COALESCE(e.name, 'N/A') as emp_name, COALESCE(d.dname, 'N/A') as dept_name
      FROM emp e FULL JOIN dept d ON e.dept_id = d.id
      ORDER BY emp_name
    `);
    assert.equal(r.rows.length, 4);
    assert.equal(r.rows[0].emp_name, 'Alice');
    assert.equal(r.rows[3].dept_name, 'HR');
  });
  
  it('with GROUP BY', () => {
    setup();
    const r = db.execute(`
      SELECT COUNT(*) as cnt FROM emp e FULL JOIN dept d ON e.dept_id = d.id
    `);
    assert.equal(r.rows[0].cnt, 4);
  });
  
  it('empty left table', () => {
    const db2 = new Database();
    db2.execute('CREATE TABLE a (id INT)');
    db2.execute('CREATE TABLE b (id INT)');
    db2.execute('INSERT INTO b VALUES (1), (2)');
    const r = db2.execute('SELECT * FROM a FULL JOIN b ON a.id = b.id');
    assert.equal(r.rows.length, 2);
  });
  
  it('empty right table', () => {
    const db2 = new Database();
    db2.execute('CREATE TABLE a (id INT)');
    db2.execute('CREATE TABLE b (id INT)');
    db2.execute('INSERT INTO a VALUES (1), (2)');
    const r = db2.execute('SELECT * FROM a FULL JOIN b ON a.id = b.id');
    assert.equal(r.rows.length, 2);
  });
});
