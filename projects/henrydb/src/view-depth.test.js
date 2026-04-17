// view-depth.test.js — VIEW correctness depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-view-'));
  db = TransactionalDatabase.open(dbDir);
  db.execute('CREATE TABLE employees (id INT, name TEXT, dept TEXT, salary INT)');
  db.execute("INSERT INTO employees VALUES (1, 'Alice', 'eng', 90000)");
  db.execute("INSERT INTO employees VALUES (2, 'Bob', 'eng', 85000)");
  db.execute("INSERT INTO employees VALUES (3, 'Carol', 'sales', 95000)");
  db.execute("INSERT INTO employees VALUES (4, 'Dave', 'sales', 80000)");
  db.execute("INSERT INTO employees VALUES (5, 'Eve', 'hr', 70000)");
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('CREATE VIEW and SELECT', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('basic view creation and query', () => {
    db.execute('CREATE VIEW eng_employees AS SELECT * FROM employees WHERE dept = \'eng\'');
    const r = rows(db.execute('SELECT * FROM eng_employees ORDER BY name'));
    assert.equal(r.length, 2);
    assert.equal(r[0].name, 'Alice');
    assert.equal(r[1].name, 'Bob');
  });

  it('view with column selection', () => {
    db.execute('CREATE VIEW names_only AS SELECT id, name FROM employees');
    const r = rows(db.execute('SELECT * FROM names_only ORDER BY id'));
    assert.equal(r.length, 5);
    assert.equal(r[0].name, 'Alice');
    // Should NOT have salary column
    assert.equal(r[0].salary, undefined);
  });

  it('view with aggregate', () => {
    db.execute('CREATE VIEW dept_stats AS SELECT dept, COUNT(*) AS cnt, AVG(salary) AS avg_sal FROM employees GROUP BY dept');
    const r = rows(db.execute('SELECT * FROM dept_stats ORDER BY dept'));
    assert.equal(r.length, 3);
    assert.equal(r[0].dept, 'eng');
    assert.equal(r[0].cnt, 2);
  });

  it('view reflects underlying table changes', () => {
    db.execute('CREATE VIEW high_earners AS SELECT * FROM employees WHERE salary > 85000');
    
    const r1 = rows(db.execute('SELECT COUNT(*) AS c FROM high_earners'));
    assert.equal(r1[0].c, 2); // Alice 90k, Carol 95k

    // Modify underlying table
    db.execute("INSERT INTO employees VALUES (6, 'Frank', 'eng', 100000)");

    const r2 = rows(db.execute('SELECT COUNT(*) AS c FROM high_earners'));
    assert.equal(r2[0].c, 3); // Now includes Frank
  });

  it('view with JOIN', () => {
    db.execute('CREATE TABLE departments (name TEXT, budget INT)');
    db.execute("INSERT INTO departments VALUES ('eng', 500000)");
    db.execute("INSERT INTO departments VALUES ('sales', 300000)");
    db.execute("INSERT INTO departments VALUES ('hr', 100000)");

    db.execute(
      'CREATE VIEW employee_dept AS ' +
      'SELECT e.name, e.dept, e.salary, d.budget ' +
      'FROM employees e INNER JOIN departments d ON e.dept = d.name'
    );

    const r = rows(db.execute('SELECT * FROM employee_dept ORDER BY name'));
    assert.equal(r.length, 5);
    assert.equal(r[0].name, 'Alice');
    assert.equal(r[0].budget, 500000);
  });
});

describe('DROP VIEW', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('DROP VIEW removes view', () => {
    db.execute('CREATE VIEW v AS SELECT * FROM employees');
    db.execute('DROP VIEW v');
    assert.throws(() => db.execute('SELECT * FROM v'), /not found/i);
  });

  it('DROP VIEW IF EXISTS on non-existent view', () => {
    // Should not throw
    db.execute('DROP VIEW IF EXISTS nonexistent');
  });

  it('DROP VIEW does not affect underlying table', () => {
    db.execute('CREATE VIEW v AS SELECT * FROM employees');
    db.execute('DROP VIEW v');
    
    const r = rows(db.execute('SELECT COUNT(*) AS c FROM employees'));
    assert.equal(r[0].c, 5, 'Underlying table should not be affected');
  });
});

describe('VIEW + MVCC', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('view query in transaction sees snapshot-consistent data', () => {
    db.execute('CREATE VIEW high_earners AS SELECT * FROM employees WHERE salary > 85000');

    const s1 = db.session();
    s1.begin();
    const r1 = rows(s1.execute('SELECT COUNT(*) AS c FROM high_earners'));
    assert.equal(r1[0].c, 2);

    // Concurrent insert
    db.execute("INSERT INTO employees VALUES (6, 'Frank', 'eng', 100000)");

    // s1 should still see 2
    const r2 = rows(s1.execute('SELECT COUNT(*) AS c FROM high_earners'));
    assert.equal(r2[0].c, 2, 'View in snapshot should not see new row');

    s1.commit();
    s1.close();

    // New read sees 3
    assert.equal(rows(db.execute('SELECT COUNT(*) AS c FROM high_earners'))[0].c, 3);
  });
});

describe('VIEW Crash Recovery', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('view definition survives crash', () => {
    db.execute('CREATE VIEW eng AS SELECT * FROM employees WHERE dept = \'eng\'');
    
    db.close();
    db = TransactionalDatabase.open(dbDir);

    const r = rows(db.execute('SELECT * FROM eng ORDER BY name'));
    assert.equal(r.length, 2);
    assert.equal(r[0].name, 'Alice');
  });
});
