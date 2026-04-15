import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('LATERAL JOIN WHERE filter', () => {
  it('applies WHERE IS NOT NULL on lateral column', () => {
    const db = new Database();
    db.execute('CREATE TABLE emp (id INT, name TEXT, mgr_id INT, salary INT)');
    db.execute("INSERT INTO emp VALUES (1,'CEO',NULL,500),(2,'VP',1,300),(3,'Dir',2,200),(4,'Mgr',3,150),(5,'Dev',4,100),(6,'Dev2',4,110)");

    const r = db.execute(`
      SELECT e.name, sub.max_sal
      FROM emp e,
      LATERAL (SELECT MAX(salary) as max_sal FROM emp WHERE mgr_id = e.id) sub
      WHERE sub.max_sal IS NOT NULL
    `);
    assert.equal(r.rows.length, 4, 'Should filter out rows where max_sal is null');
    const names = r.rows.map(r => r.name);
    assert.ok(!names.includes('Dev'), 'Dev should be filtered out');
    assert.ok(!names.includes('Dev2'), 'Dev2 should be filtered out');
  });

  it('applies WHERE comparison on lateral column', () => {
    const db = new Database();
    db.execute('CREATE TABLE emp (id INT, name TEXT, mgr_id INT, salary INT)');
    db.execute("INSERT INTO emp VALUES (1,'CEO',NULL,500),(2,'VP',1,300),(3,'Dir',2,200),(4,'Mgr',3,150),(5,'Dev',4,100),(6,'Dev2',4,110)");

    const r = db.execute(`
      SELECT e.name, sub.max_sal
      FROM emp e,
      LATERAL (SELECT MAX(salary) as max_sal FROM emp WHERE mgr_id = e.id) sub
      WHERE sub.max_sal > 150
    `);
    assert.equal(r.rows.length, 2, 'Only CEO and VP have subordinates with max salary > 150');
    assert.equal(r.rows[0].name, 'CEO');
    assert.equal(r.rows[1].name, 'VP');
  });

  it('applies WHERE with AND on lateral and base columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE emp (id INT, name TEXT, mgr_id INT, salary INT)');
    db.execute("INSERT INTO emp VALUES (1,'CEO',NULL,500),(2,'VP',1,300),(3,'Dir',2,200),(4,'Mgr',3,150),(5,'Dev',4,100),(6,'Dev2',4,110)");

    const r = db.execute(`
      SELECT e.name, e.salary, sub.max_sal
      FROM emp e,
      LATERAL (SELECT MAX(salary) as max_sal FROM emp WHERE mgr_id = e.id) sub
      WHERE sub.max_sal IS NOT NULL AND e.salary > 200
    `);
    // CEO (salary 500, max_sub 300) and VP (salary 300, max_sub 200)
    assert.equal(r.rows.length, 2);
  });

  it('LATERAL with LEFT JOIN preserves null rows without WHERE', () => {
    const db = new Database();
    db.execute('CREATE TABLE emp (id INT, name TEXT, mgr_id INT)');
    db.execute("INSERT INTO emp VALUES (1,'Boss',NULL),(2,'Worker',1)");

    const r = db.execute(`
      SELECT e.name, sub.cnt
      FROM emp e
      LEFT JOIN LATERAL (SELECT COUNT(*) as cnt FROM emp WHERE mgr_id = e.id) sub ON true
    `);
    assert.equal(r.rows.length, 2, 'LEFT JOIN LATERAL should keep all rows');
  });
});
