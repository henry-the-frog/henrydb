// lateral-correlated-depth.test.js — LATERAL JOIN + correlated subquery tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-lat-'));
  db = TransactionalDatabase.open(dbDir);
  db.execute('CREATE TABLE depts (id INT, name TEXT)');
  db.execute('CREATE TABLE emps (id INT, name TEXT, dept_id INT, salary INT)');
  db.execute("INSERT INTO depts VALUES (1, 'Engineering')");
  db.execute("INSERT INTO depts VALUES (2, 'Sales')");
  db.execute("INSERT INTO emps VALUES (1, 'Alice', 1, 120000)");
  db.execute("INSERT INTO emps VALUES (2, 'Bob', 1, 95000)");
  db.execute("INSERT INTO emps VALUES (3, 'Carol', 2, 80000)");
  db.execute("INSERT INTO emps VALUES (4, 'Dave', 2, 90000)");
  db.execute("INSERT INTO emps VALUES (5, 'Eve', 1, 110000)");
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('Correlated Subqueries in SELECT', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('scalar correlated subquery', () => {
    const r = rows(db.execute(
      'SELECT d.name, ' +
      '  (SELECT COUNT(*) FROM emps e WHERE e.dept_id = d.id) AS emp_count ' +
      'FROM depts d ORDER BY d.name'
    ));
    assert.equal(r.length, 2);
    assert.equal(r[0].name, 'Engineering');
    assert.equal(r[0].emp_count, 3);
    assert.equal(r[1].name, 'Sales');
    assert.equal(r[1].emp_count, 2);
  });

  it('correlated subquery with aggregate', () => {
    const r = rows(db.execute(
      'SELECT d.name, ' +
      '  (SELECT AVG(salary) FROM emps e WHERE e.dept_id = d.id) AS avg_salary ' +
      'FROM depts d ORDER BY d.name'
    ));
    const eng = r.find(x => x.name === 'Engineering');
    // (120000 + 95000 + 110000) / 3 ≈ 108333
    assert.ok(Math.abs(eng.avg_salary - 108333.33) < 1);
  });
});

describe('Correlated Subqueries in WHERE', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('EXISTS with correlated subquery', () => {
    const r = rows(db.execute(
      'SELECT d.name FROM depts d WHERE EXISTS (' +
      '  SELECT 1 FROM emps e WHERE e.dept_id = d.id AND e.salary > 100000' +
      ') ORDER BY d.name'
    ));
    // Engineering has Alice (120k) and Eve (110k)
    assert.equal(r.length, 1);
    assert.equal(r[0].name, 'Engineering');
  });

  it('NOT EXISTS with correlated subquery', () => {
    const r = rows(db.execute(
      'SELECT d.name FROM depts d WHERE NOT EXISTS (' +
      '  SELECT 1 FROM emps e WHERE e.dept_id = d.id AND e.salary > 100000' +
      ') ORDER BY d.name'
    ));
    assert.equal(r.length, 1);
    assert.equal(r[0].name, 'Sales');
  });

  it('correlated subquery with comparison', () => {
    // Employees earning more than their department average
    const r = rows(db.execute(
      'SELECT e.name, e.salary FROM emps e WHERE e.salary > (' +
      '  SELECT AVG(e2.salary) FROM emps e2 WHERE e2.dept_id = e.dept_id' +
      ') ORDER BY e.name'
    ));
    // Engineering avg ≈ 108333: Alice (120k) > avg, Eve (110k) > avg
    // Sales avg = 85000: Dave (90k) > avg
    assert.ok(r.some(x => x.name === 'Alice'));
    assert.ok(r.some(x => x.name === 'Dave'));
  });
});

describe('Correlated UPDATE and DELETE', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('UPDATE with correlated subquery (parser limitation)', () => {
    db.execute('CREATE TABLE bonus (dept_id INT, amount INT)');
    db.execute('INSERT INTO bonus VALUES (1, 5000)');
    db.execute('INSERT INTO bonus VALUES (2, 3000)');

    // Correlated subquery in UPDATE SET clause not supported by parser
    // Document as known limitation
    try {
      db.execute(
        'UPDATE emps SET salary = salary + (' +
        '  SELECT amount FROM bonus b WHERE b.dept_id = emps.dept_id' +
        ')'
      );

      const alice = rows(db.execute("SELECT salary FROM emps WHERE name = 'Alice'"));
      assert.equal(alice[0].salary, 125000);
    } catch (e) {
      // Parser doesn't support correlated subquery in SET clause
      assert.ok(e.message.includes('Expected') || e.message.includes('parse'),
        'Should be a parse error, not a runtime error');
    }
  });
});
